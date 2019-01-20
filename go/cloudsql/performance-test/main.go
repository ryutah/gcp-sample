package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"sync"

	validator "gopkg.in/go-playground/validator.v9"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ryutah/gcp-sample/go/internal/stats"
)

type config struct {
	Table  string `validate:"required"`
	DB     string `validate:"required"`
	Conn   string `validate:"required"`
	User   string `validate:"required"`
	Pass   string `validate:"required"`
	Socket string `validate:"required"`
}

func (c *config) registerFlags() {
	flag.StringVar(&c.Table, "scratch_table", "scratch", "name of table to use; should not already exist")
	flag.StringVar(&c.DB, "db", "", "name of schema to use")
	flag.StringVar(&c.Conn, "conn", "", "connection name to use")
	flag.StringVar(&c.Socket, "socket", "/cloudsql", "socket file path for cloud sql")
	flag.StringVar(&c.User, "user", "", "database user name to use")
	flag.StringVar(&c.Pass, "pass", "", "password for user")
}

func (c config) check() error {
	return validator.New().Struct(c)
}

func main() {
	conf, sts, err := initialize()
	if err != nil {
		log.Fatalf(err.Error())
	}

	db, err := sql.Open("mysql", fmt.Sprintf(
		"%s:%s@unix(%s/%s)/%s",
		conf.User, conf.Pass, conf.Socket, conf.Conn, conf.DB,
	))
	defer db.Close()
	db.SetMaxIdleConns(sts.Config.ReqCount)

	if err := createTable(db, conf.Table); err != nil {
		log.Fatalf(err.Error())
	}
	defer dropTable(db, conf.Table)

	var (
		mapLock  sync.Mutex
		inserted = make(map[int]bool)
		readFunc = func(ctx context.Context, id int) error {
			return find(ctx, db, conf.Table, id)
		}
		writeFunc = func(ctx context.Context, id int) error {
			var err error
			mapLock.Lock()
			if inserted[id] {
				mapLock.Unlock()
				err = update(ctx, db, conf.Table, id)
			} else {
				inserted[id] = true
				mapLock.Unlock()
				err = insert(ctx, db, conf.Table, id)
			}
			return err
		}
	)
	readRec, writeRec, err := sts.Start(readFunc, writeFunc)
	if err != nil {
		log.Fatalf(err.Error())
	}

	log.Printf("Reads (%d ok / %d tries):\n%v", readRec.Ok, readRec.Tries, readRec.Aggregate())
	log.Printf("Writes (%d ok / %d tries):\n%v", writeRec.Ok, writeRec.Tries, writeRec.Aggregate())
}

func initialize() (*config, *stats.Stats, error) {
	var (
		conf  = new(config)
		sConf = stats.NewConfig()
	)
	conf.registerFlags()
	sConf.RegisterFlags()
	flag.Parse()

	if err := conf.check(); err != nil {
		return nil, nil, err
	}
	if err := sConf.Validate(); err != nil {
		return nil, nil, err
	}
	return conf, stats.NewStats(sConf), nil
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
