package stats

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/montanaflynn/stats"

	validator "gopkg.in/go-playground/validator.v9"
)

var allStats int64

type Config struct {
	RunFor   time.Duration `validate:"required"`
	ReqCount int           `validate:"required"`
}

func NewConfig() *Config {
	return new(Config)
}

func (c *Config) RegisterFlags() {
	flag.DurationVar(
		&c.RunFor,
		"run_for",
		5*time.Second,
		"how long to run the load test for; 0 to run forever until SIGTERM",
	)
	flag.IntVar(
		&c.ReqCount,
		"req_count",
		100,
		"number of concurrent requests",
	)
}

func (c Config) Validate() error {
	return validator.New().Struct(c)
}

type StatsFunc func(ctx context.Context, id int) error

type Stats struct {
	Config *Config
}

func NewStats(conf *Config) *Stats {
	return &Stats{Config: conf}
}

func (s Stats) Start(readFunc, writeFunc StatsFunc) (read, write Recorder, err error) {
	if !flag.Parsed() {
		flag.Parse()
	}
	if err = s.Config.Validate(); err != nil {
		return
	}

	var (
		ctx      = context.Background()
		sem      = make(chan struct{}, s.Config.ReqCount)
		wg       sync.WaitGroup
		stopTime = time.Now().Add(s.Config.RunFor)
	)

	for time.Now().Before(stopTime) || s.Config.RunFor == 0 {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			var (
				ok      = true
				opStart = time.Now()
				rec     *Recorder
			)
			defer func() {
				rec.record(ok, time.Since(opStart))
			}()

			id := rand.Intn(100)
			switch rand.Intn(10) {
			case 0, 1, 2, 3, 4: // write
				rec = &write
				if err := writeFunc(ctx, id); err != nil {
					log.Printf("Error doing write: %v", err)
					ok = false
				}
			default: // read
				rec = &read
				if err := readFunc(ctx, id); err != nil {
					log.Printf("Error doing read: %v", err)
					ok = false
				}
			}
		}()
	}
	return
}

type Recorder struct {
	mu        sync.Mutex
	Tries     int
	Ok        int
	durations []float64
}

func (r *Recorder) record(ok bool, d time.Duration) {
	r.mu.Lock()
	r.Tries++
	if ok {
		r.Ok++
	}
	r.durations = append(r.durations, float64(d))
	r.mu.Unlock()
	if n := atomic.AddInt64(&allStats, 1); n%1000 == 0 {
		log.Printf("Progress: done %d ops", n)
	}
}

func (r *Recorder) Aggregate() string {
	var (
		min, _    = stats.Min(r.durations)
		max, _    = stats.Max(r.durations)
		medi, _   = stats.Median(r.durations)
		tile25, _ = stats.Percentile(r.durations, 25)
		tile50, _ = stats.Percentile(r.durations, 50)
		tile75, _ = stats.Percentile(r.durations, 75)
		tile95, _ = stats.Percentile(r.durations, 95)
		tile99, _ = stats.Percentile(r.durations, 99)
	)
	return fmt.Sprintf(
		"min: %v\n"+
			"max: %v\n"+
			"median: %v\n"+
			"25th percentile: %v\n"+
			"50th percentile: %v\n"+
			"75th percentile: %v\n"+
			"95th percentile: %v\n"+
			"99th percentile: %v\n",
		time.Duration(min),
		time.Duration(max),
		time.Duration(medi),
		time.Duration(tile25),
		time.Duration(tile50),
		time.Duration(tile75),
		time.Duration(tile95),
		time.Duration(tile99),
	)
}
