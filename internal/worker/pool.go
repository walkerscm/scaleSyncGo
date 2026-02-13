package worker

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/walkerscm/scaleSyncGo/internal/database"
)

// Job represents a batch of rows to insert.
type Job struct {
	BatchNum int
	Rows     [][]string
}

// Result reports the outcome of a single batch insert.
type Result struct {
	BatchNum int
	RowCount int
	Err      error
}

// Pool manages a set of worker goroutines that consume jobs from a channel.
type Pool struct {
	db          *sql.DB
	schemaTable string
	columns     []string
	pkColumns   []string
	mapping     []database.ColumnMapping
	workers     int
	jobs        chan Job
	results     chan Result
	wg          sync.WaitGroup
}

// NewPool creates a worker pool ready to process batches.
func NewPool(db *sql.DB, schemaTable string, columns []string, pkColumns []string, mapping []database.ColumnMapping, workers int) *Pool {
	return &Pool{
		db:          db,
		schemaTable: schemaTable,
		columns:     columns,
		pkColumns:   pkColumns,
		mapping:     mapping,
		workers:     workers,
		jobs:        make(chan Job, workers*2),
		results:     make(chan Result, workers*2),
	}
}

// Start launches the worker goroutines. They read from Jobs() and write to Results().
func (p *Pool) Start(ctx context.Context) {
	for i := range p.workers {
		p.wg.Add(1)
		go func(id int) {
			defer p.wg.Done()
			for job := range p.jobs {
				converted := ConvertBatch(job.Rows, p.mapping)
				err := database.InsertBatch(ctx, p.db, p.schemaTable, p.columns, p.pkColumns, converted)
				if err != nil {
					err = fmt.Errorf("worker %d, batch %d: %w", id, job.BatchNum, err)
				}
				p.results <- Result{
					BatchNum: job.BatchNum,
					RowCount: len(job.Rows),
					Err:      err,
				}
			}
		}(i)
	}

	// Close results channel when all workers are done
	go func() {
		p.wg.Wait()
		close(p.results)
	}()
}

// Submit sends a job to the worker pool.
func (p *Pool) Submit(job Job) {
	p.jobs <- job
}

// Done signals that no more jobs will be submitted.
func (p *Pool) Done() {
	close(p.jobs)
}

// Results returns the channel to receive batch results from.
func (p *Pool) Results() <-chan Result {
	return p.results
}
