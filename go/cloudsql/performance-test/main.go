package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	validator "gopkg.in/go-playground/validator.v9"

	_ "github.com/go-sql-driver/mysql"
	mstats "github.com/montanaflynn/stats"
)

type config struct {
	RunFor   time.Duration `validate:"required"`
	Table    string        `validate:"required"`
	DB       string        `validate:"required"`
	Conn     string        `validate:"required"`
	User     string        `validate:"required"`
	Pass     string        `validate:"required"`
	Socket   string        `validate:"required"`
	ReqCount int           `validate:"required"`
}

func (c *config) registerFlags() {
	flag.DurationVar(&c.RunFor, "run_for", 5*time.Second, "how long to run the load test for; 0 to run forever until SIGTERM")
	flag.StringVar(&c.Table, "scratch_table", "scratch", "name of table to use; should not already exist")
	flag.StringVar(&c.DB, "db", "", "name of schema to use")
	flag.StringVar(&c.Conn, "conn", "", "connection name to use")
	flag.StringVar(&c.Socket, "socket", "/cloudsql", "socket file path for cloud sql")
	flag.StringVar(&c.User, "user", "", "database user name to use")
	flag.StringVar(&c.Pass, "pass", "", "password for user")
	flag.IntVar(&c.ReqCount, "req_count", 100, "number of concurrent requests")
}

func (c config) check() error {
	return validator.New().Struct(c)
}

var allStats int64

type stats struct {
	mu        sync.Mutex
	tries, ok int
	ds        []float64
}

func (s *stats) record(ok bool, d time.Duration) {
	s.mu.Lock()
	s.tries++
	if ok {
		s.ok++
	}
	s.ds = append(s.ds, float64(d))
	s.mu.Unlock()
	if n := atomic.AddInt64(&allStats, 1); n%1000 == 0 {
		log.Printf("Progress: done %d ops", n)
	}
}

func (s *stats) aggregate() string {
	var (
		min, _    = mstats.Min(s.ds)
		medi, _   = mstats.Median(s.ds)
		tile75, _ = mstats.Percentile(s.ds, 75)
		tile95, _ = mstats.Percentile(s.ds, 95)
		tile99, _ = mstats.Percentile(s.ds, 99)
	)
	return fmt.Sprintf(
		"min: %v\n"+
			"median: %v\n"+
			"75th percentile: %v\n"+
			"95th percentile: %v\n"+
			"99th percentile: %v\n",
		time.Duration(min),
		time.Duration(medi),
		time.Duration(tile75),
		time.Duration(tile95),
		time.Duration(tile99),
	)
}

func main() {
	conf, err := initialize()
	if err != nil {
		log.Fatalf(err.Error())
	}

	db, err := sql.Open("mysql", fmt.Sprintf(
		"%s:%s@unix(%s/%s)/%s",
		conf.User, conf.Pass, conf.Socket, conf.Conn, conf.DB,
	))
	defer db.Close()
	db.SetMaxIdleConns(conf.ReqCount)

	ctx := context.Background()

	if err := createTable(db, conf.Table); err != nil {
		log.Fatalf(err.Error())
	}
	defer dropTable(db, conf.Table)

	var (
		mapLock      sync.Mutex
		inserted     = make(map[int]bool)
		sem          = make(chan struct{}, conf.ReqCount)
		wg           sync.WaitGroup
		stopTime     = time.Now().Add(conf.RunFor)
		read, writes stats
	)
	for time.Now().Before(stopTime) || conf.RunFor == 0 {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			var (
				ok      = true
				opStart = time.Now()
				stats   *stats
			)
			defer func() {
				stats.record(ok, time.Since(opStart))
			}()

			id := rand.Intn(100)
			switch rand.Intn(10) {
			case 0, 1, 2, 3, 4:
				// write
				stats = &writes
				var writeErr error
				mapLock.Lock()
				if inserted[id] {
					writeErr = update(ctx, db, conf.Table, id)
					mapLock.Unlock()
				} else {
					inserted[id] = true
					mapLock.Unlock()
					writeErr = insert(ctx, db, conf.Table, id)
				}
				if writeErr != nil {
					log.Printf("Error doing write: %v", writeErr)
					ok = false
				}
			default:
				// read
				stats = &read
				if err := find(ctx, db, conf.Table, id); err != nil {
					log.Printf("Error doing read: %v", err)
					ok = false
				}
			}
		}()
	}
	wg.Wait()

	log.Printf("Reads (%d ok / %d tries):\n%v", read.ok, read.tries, read.aggregate())
	log.Printf("Writes (%d ok / %d tries):\n%v", writes.ok, writes.tries, writes.aggregate())
}

func initialize() (*config, error) {
	conf := new(config)
	conf.registerFlags()
	flag.Parse()

	if err := conf.check(); err != nil {
		return nil, err
	}
	return conf, nil
}

func createTable(db *sql.DB, table string) error {
	_, err := db.Exec(fmt.Sprintf(
		"CREATE TABLE %s(id int primary key, value blob)", table,
	))
	return err
}

func dropTable(db *sql.DB, table string) error {
	_, err := db.Exec(fmt.Sprintf("DROP TABLE %s", table))
	return err
}

func insert(ctx context.Context, db *sql.DB, tableName string, id int) error {
	// insert iKB row.
	_, err := db.ExecContext(
		ctx,
		fmt.Sprintf("INSERT INTO %s VALUES(?, ?)", tableName),
		id, make([]byte, 1<<10),
	)
	return err
}

func update(ctx context.Context, db *sql.DB, tableName string, id int) error {
	// update iKB row.
	_, err := db.ExecContext(
		ctx,
		fmt.Sprintf("UPDATE %s SET value=? WHERE id=?", tableName),
		make([]byte, 1<<10), id,
	)
	return err
}

func find(ctx context.Context, db *sql.DB, tableName string, id int) error {
	// select row
	rows, err := db.QueryContext(
		ctx,
		fmt.Sprintf("SELECT * FROM %s WHERE id = ?", tableName),
		id,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	// schan data to benchmarking
	for rows.Next() {
		var (
			id    int
			value []byte
		)
		if err := rows.Scan(&id, &value); err != nil {
			return err
		}
	}
	return nil
}
