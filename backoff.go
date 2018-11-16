package darner

import (
	"math"
	"time"
)

type Backoff struct {
	attempt, Factor float64
	Min, Max time.Duration
}

func (b *Backoff) Duration() time.Duration {
	d := b.ForAttempt(b.attempt)
	b.attempt++
	return d
}

const maxInt64 = float64(math.MaxInt64 - 512)

func (b *Backoff) ForAttempt(attempt float64) time.Duration {
	min := b.Min
	if min <= 0 {
		min = 100 * time.Millisecond
	}
	max := b.Max
	if max <= 0 {
		max = 10 * time.Second
	}
	if min >= max {
		return max
	}
	factor := b.Factor
	if factor <= 0 {
		factor = 2
	}

	minf := float64(min)
	durf := minf * math.Pow(factor, attempt)

	if durf > maxInt64 {
		return max
	}
	dur := time.Duration(durf)
	if dur < min {
		return min
	}
	if dur > max {
		return max
	}
	return dur
}

func (b *Backoff) Reset() {
	b.attempt = 0
}
