package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"

	"cloud.google.com/go/bigtable"

	"github.com/ryutah/gcp-sample/go/internal/stats"
	validator "gopkg.in/go-playground/validator.v9"
)

type config struct {
	Table    string `validate:"required"`
	Project  string `validate:"required"`
	Instance string `validate:"required"`
}

func (c *config) registerFlags() {
	flag.StringVar(&c.Table, "table", "scratch", "name of table to use; should not already exist")
	flag.StringVar(&c.Project, "project", "", "name of project to use")
	flag.StringVar(&c.Instance, "instance", "", "name of instance to use")
}

func (c config) validate() error {
	return validator.New().Struct(c)
}

func initialize() (*config, *stats.Stats, error) {
	var (
		conf  = new(config)
		sConf = stats.NewConfig()
	)
	conf.registerFlags()
	sConf.RegisterFlags()
	flag.Parse()

	if err := conf.validate(); err != nil {
		return nil, nil, err
	}
	if err := sConf.Validate(); err != nil {
		return nil, nil, err
	}
	return conf, stats.NewStats(sConf), nil
}

func main() {
	ctx := context.Background()
	conf, sts, err := initialize()
	if err != nil {
		log.Fatalf(err.Error())
	}

	var (
		adminClient, adminClientErr = bigtable.NewAdminClient(ctx, conf.Project, conf.Instance)
		client, clientErr           = bigtable.NewClient(ctx, conf.Project, conf.Instance)
	)
	if adminClientErr != nil || clientErr != nil {
		log.Fatalf("admin client error: %v\nclient error: %v", adminClientErr, clientErr)
	}
	defer func() {
		if adminClient != nil {
			adminClient.Close()
		}
		if client != nil {
			client.Close()
		}
	}()

	if err := createTable(ctx, adminClient, conf.Table); err != nil {
		log.Fatalf(err.Error())
	}
	defer deleteTable(ctx, adminClient, conf.Table)

	table := client.Open(conf.Table)
	var (
		readFunc = func(ctx context.Context, id int) error {
			_, err := table.ReadRow(context.Background(), fmt.Sprintf("row%d", id), bigtable.RowFilter(bigtable.LatestNFilter(1)))
			return err
		}
		writeFunc = func(ctx context.Context, id int) error {
			mut := bigtable.NewMutation()
			mut.Set("value", "col", bigtable.Now(), bytes.Repeat([]byte("0"), 1<<10))
			return table.Apply(context.Background(), fmt.Sprintf("row%d", id), mut)
		}
	)

	read, write, err := sts.Start(readFunc, writeFunc)
	if err != nil {
		log.Fatalf(err.Error())
	}
	log.Printf("Reads (%d ok / %d tries):\n%v", read.Ok, read.Tries, read.Aggregate())
	log.Printf("Writes (%d ok / %d tries):\n%v", write.Ok, write.Tries, write.Aggregate())
}

func createTable(ctx context.Context, client *bigtable.AdminClient, table string) error {
	if err := client.CreateTable(ctx, table); err != nil {
		return err
	}
	return client.CreateColumnFamily(ctx, table, "value")
}

func deleteTable(ctx context.Context, client *bigtable.AdminClient, table string) error {
	return client.DeleteTable(ctx, table)
}
