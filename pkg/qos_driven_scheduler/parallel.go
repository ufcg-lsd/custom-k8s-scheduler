package qos_driven_scheduler

import (
	"context"
	"math"

	"k8s.io/client-go/util/workqueue"
)

const parallelism = 16

// chunkSizeFor returns a chunk size for the given number of items to use for
// parallel work. The size aims to produce good CPU utilization.
func chunkSizeFor(n int) workqueue.Options {
	s := int(math.Sqrt(float64(n)))
	if r := n/parallelism + 1; s > r {
		s = r
	} else if s < 1 {
		s = 1
	}
	return workqueue.WithChunkSize(s)
}

// Until is a wrapper around workqueue.ParallelizeUntil to use in scheduling algorithms.
func ParallelizeUntil(ctx context.Context, pieces int, doWorkPiece workqueue.DoWorkPieceFunc) {
	workqueue.ParallelizeUntil(ctx, parallelism, pieces, doWorkPiece, chunkSizeFor(pieces))
}
