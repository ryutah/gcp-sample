package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"cloud.google.com/go/bigtable"
)

func main() {
	var (
		project, instance, tableName string
		delTable                     bool
	)
	flag.StringVar(&project, "project", "", "GCP Project ID")
	flag.StringVar(&instance, "instance", "", "Bigtable Instance ID")
	flag.StringVar(&tableName, "table", "Foo", "Create Table Name")
	flag.BoolVar(&delTable, "deltable", true, "Should Delete Table Before Exit")
	flag.Parse()

	if project == "" || instance == "" || tableName == "" {
		flag.Usage()
		os.Exit(1)
	}

	ctx := context.Background()
	var (
		admClient, aErr = bigtable.NewAdminClient(ctx, project, instance)
		client, cErr    = bigtable.NewClient(ctx, project, instance)
	)
	if aErr != nil || cErr != nil {
		panic(fmt.Sprintf("admin client error: %v\nclient error: %v", aErr, cErr))
	}
	defer func() {
		if admClient != nil {
			admClient.Close()
		}
		if client != nil {
			client.Close()
		}
	}()

	if err := createTable(ctx, admClient, tableName); err != nil {
		panic(err)
	}
	if delTable {
		defer deleteTable(ctx, admClient, tableName)
	}

	table := client.Open(tableName)
	if err := set(ctx, table, "example", value{
		Value: "Hello, World!!",
		Value2: value2{
			Foo: "Foo Value",
			Bar: "Bar Value",
		},
	}); err != nil {
		panic(err)
	}
	got, err := get(ctx, table, "example")
	if err != nil {
		panic(err)
	}
	fmt.Println(got)
}

func createTable(ctx context.Context, client *bigtable.AdminClient, tableName string) error {
	if err := client.CreateTable(ctx, tableName); err != nil {
		return err
	}
	var (
		valErr  = client.CreateColumnFamily(ctx, tableName, "val")
		val2Err = client.CreateColumnFamily(ctx, tableName, "val2")
	)
	if valErr != nil || val2Err != nil {
		return fmt.Errorf(
			"create val: %v\ncreate val2: %v",
			valErr, val2Err,
		)
	}
	return nil
}

func deleteTable(ctx context.Context, client *bigtable.AdminClient, tableName string) error {
	return client.DeleteTable(ctx, tableName)
}

type (
	value struct {
		Value  string
		Value2 value2
	}

	value2 struct {
		Foo string
		Bar string
	}
)

func (v value) String() string {
	s, _ := json.MarshalIndent(v, "", "  ")
	return string(s)
}

func set(ctx context.Context, table *bigtable.Table, key string, val value) error {
	mut := bigtable.NewMutation()
	mut.Set("val", val.Value, bigtable.Now(), nil)
	mut.Set("val2", "foo", bigtable.Now(), []byte(val.Value2.Foo))
	mut.Set("val2", "bar", bigtable.Now(), []byte(val.Value2.Bar))
	return table.Apply(ctx, key, mut)
}

func get(ctx context.Context, table *bigtable.Table, key string) (*value, error) {
	// read only has quarify named foo
	row, err := table.ReadRow(ctx, key,
		bigtable.RowFilter(bigtable.ChainFilters(
			bigtable.ColumnFilter("foo"),
		)),
	)
	if err != nil {
		return nil, err
	}

	val := new(value)
	for _, items := range row {
		for _, item := range items {
			families := strings.Split(item.Column, ":")
			switch family, column := families[0], families[1]; family {
			case "val":
				val.Value = column
			case "val2":
				if column == "foo" {
					val.Value2.Foo = string(item.Value)
				} else if column == "bar" {
					val.Value2.Bar = string(item.Value)
				}
			}
		}
	}
	return val, nil
}
