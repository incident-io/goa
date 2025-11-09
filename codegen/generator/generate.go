package generator

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"goa.design/goa/v3/codegen"
	"goa.design/goa/v3/eval"
	"golang.org/x/tools/go/packages"
)

// Generate runs the code generation algorithms.
func Generate(dir, cmd string, debug bool) (outputs []string, err1 error) {
	startGenerate := time.Now()
	if debug {
		fmt.Fprintf(os.Stderr, "[TIMING]     [generate] Starting generator.Generate()\n")
	}

	// 1. Compute design roots.
	var roots []eval.Root
	{
		start := time.Now()
		rs, err := eval.Context.Roots()
		if err != nil {
			return nil, err
		}
		roots = rs
		if debug {
			fmt.Fprintf(os.Stderr, "[TIMING]     [generate] Stage 1: Compute design roots took %v\n", time.Since(start))
		}
	}

	// 2. Compute "gen" package import path.
	var genpkg string
	{
		start := time.Now()
		base, err := filepath.Abs(dir)
		if err != nil {
			return nil, err
		}
		path := filepath.Join(base, codegen.Gendir)
		if err := os.MkdirAll(path, 0750); err != nil {
			return nil, err
		}

		// We create a temporary Go file to make sure the directory is a valid Go package
		dummy, err := os.CreateTemp(path, "temp.*.go")
		if err != nil {
			return nil, err
		}
		defer func() {
			if err := os.Remove(dummy.Name()); err != nil {
				outputs = nil
				err1 = err
			}
		}()
		if _, err = dummy.Write([]byte("package gen")); err != nil {
			return nil, err
		}
		if err = dummy.Close(); err != nil {
			return nil, err
		}

		startPkgLoad := time.Now()
		pkgs, err := packages.Load(&packages.Config{Mode: packages.NeedName}, path)
		if err != nil {
			return nil, err
		}
		genpkg = pkgs[0].PkgPath
		if debug {
			fmt.Fprintf(os.Stderr, "[TIMING]     [generate]   packages.Load took %v\n", time.Since(startPkgLoad))
			fmt.Fprintf(os.Stderr, "[TIMING]     [generate] Stage 2: Compute gen package import path took %v\n", time.Since(start))
		}
	}

	// 3. Retrieve goa generators for given command.
	var genfuncs []Genfunc
	{
		start := time.Now()
		gs, err := Generators(cmd)
		if err != nil {
			return nil, err
		}
		genfuncs = gs
		if debug {
			fmt.Fprintf(os.Stderr, "[TIMING]     [generate] Stage 3: Retrieve goa generators took %v (%d generators)\n", time.Since(start), len(genfuncs))
		}
	}

	// 4. Run the code pre generation plugins.
	{
		start := time.Now()
		err := codegen.RunPluginsPrepare(cmd, genpkg, roots)
		if err != nil {
			return nil, err
		}
		if debug {
			fmt.Fprintf(os.Stderr, "[TIMING]     [generate] Stage 4: Run pre-generation plugins took %v\n", time.Since(start))
		}
	}

	// 5. Generate initial set of files produced by goa code generators.
	// NOTE: Parallelization causes infinite recursion in AsObject() for circular type references
	var genfiles []*codegen.File
	{
		start := time.Now()
		for i, gen := range genfuncs {
			genStart := time.Now()
			fs, err := gen(genpkg, roots)
			if err != nil {
				return nil, err
			}
			genfiles = append(genfiles, fs...)
			if debug {
				fmt.Fprintf(os.Stderr, "[TIMING]     [generate]   Generator %d produced %d files in %v\n", i, len(fs), time.Since(genStart))
			}
		}
		if debug {
			fmt.Fprintf(os.Stderr, "[TIMING]     [generate] Stage 5: Generate initial files took %v (total %d files)\n", time.Since(start), len(genfiles))
		}
	}

	// 6. Run the code generation plugins.
	{
		start := time.Now()
		var err error
		genfiles, err = codegen.RunPlugins(cmd, genpkg, roots, genfiles)
		if err != nil {
			return nil, err
		}
		if debug {
			fmt.Fprintf(os.Stderr, "[TIMING]     [generate] Stage 6: Run post-generation plugins took %v (now %d files)\n", time.Since(start), len(genfiles))
		}
	}

	// 7. Write the files (in parallel).
	written := make(map[string]struct{})
	{
		start := time.Now()
		numWorkers := runtime.NumCPU()
		if debug {
			fmt.Fprintf(os.Stderr, "[TIMING]     [generate] Stage 7: Starting parallel file writing with %d workers\n", numWorkers)
		}

		// Channel for work items
		type workItem struct {
			index int
			file  *codegen.File
		}
		workChan := make(chan workItem, len(genfiles))

		// Channel for results
		type result struct {
			index    int
			filename string
			duration time.Duration
			err      error
		}
		resultChan := make(chan result, len(genfiles))

		// Start worker pool
		var wg sync.WaitGroup
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for work := range workChan {
					renderStart := time.Now()
					filename, err := work.file.Render(dir)
					resultChan <- result{
						index:    work.index,
						filename: filename,
						duration: time.Since(renderStart),
						err:      err,
					}
				}
			}(i)
		}

		// Send all files to work channel
		for i, f := range genfiles {
			workChan <- workItem{index: i, file: f}
		}
		close(workChan)

		// Wait for all workers to finish in a separate goroutine
		go func() {
			wg.Wait()
			close(resultChan)
		}()

		// Collect results
		var firstErr error
		slowRenders := 0
		for res := range resultChan {
			if res.err != nil && firstErr == nil {
				firstErr = res.err
			}
			if res.filename != "" {
				written[res.filename] = struct{}{}
			}
			// Only log slow renders (>100ms) to avoid spam
			if debug && res.duration > 100*time.Millisecond {
				fmt.Fprintf(os.Stderr, "[TIMING]     [generate]   File %d (%s) render took %v\n", res.index, res.filename, res.duration)
				slowRenders++
			}
		}

		if firstErr != nil {
			return nil, firstErr
		}

		if debug {
			fmt.Fprintf(os.Stderr, "[TIMING]     [generate] Stage 7: Write files took %v (%d files written, %d slow renders)\n", time.Since(start), len(written), slowRenders)
		}
	}

	// 8. Compute all output filenames.
	{
		start := time.Now()
		outputs = make([]string, len(written))
		cwd, err := os.Getwd()
		if err != nil {
			cwd = "."
		}
		i := 0
		for o := range written {
			rel, err := filepath.Rel(cwd, o)
			if err != nil {
				rel = o
			}
			outputs[i] = rel
			i++
		}
		if debug {
			fmt.Fprintf(os.Stderr, "[TIMING]     [generate] Stage 8: Compute output filenames took %v\n", time.Since(start))
		}
	}
	sort.Strings(outputs)

	if debug {
		fmt.Fprintf(os.Stderr, "[TIMING]     [generate] Total generator.Generate() took %v\n", time.Since(startGenerate))
	}
	return outputs, nil
}
