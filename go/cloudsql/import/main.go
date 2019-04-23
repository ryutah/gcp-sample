package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"golang.org/x/oauth2/google"
	sqladmin "google.golang.org/api/sqladmin/v1beta4"
)

var (
	projectID      = "[PROJECT_ID]"
	instanceName   = "[INSTANCE_NAME]"
	bucket         = "[BUCKET]"
	connectionName = "[CONNECTION_NAME]"
)

func main() {
	ctx := context.Background()

	client, err := google.DefaultClient(ctx)
	if err != nil {
		panic(err)
	}

	service, err := sqladmin.New(client)
	if err != nil {
		panic(err)
	}
	ope, err := service.Instances.Import(projectID, instanceName, &sqladmin.InstancesImportRequest{
		ImportContext: &sqladmin.ImportContext{
			CsvImportOptions: &sqladmin.ImportContextCsvImportOptions{
				Table: "foo_temp",
			},
			Database: "example",
			FileType: "csv",
			Uri:      fmt.Sprintf("gs://%s/sample.csv", bucket),
		},
	}).Do()
	if err != nil {
		panic(err)
	}
	resule, err := json.MarshalIndent(ope, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(resule))

	time.Sleep(300 * time.Millisecond)

	for {
		resp, err := client.Get(ope.SelfLink)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()

		payload := new(sqladmin.Operation)
		if err := json.NewDecoder(resp.Body).Decode(payload); err != nil {
			panic(err)
		}
		if payload.Status == "DONE" {
			break
		}
		fmt.Println("stay...")
		time.Sleep(1 * time.Second)
	}

	fmt.Println("finish!!")

	db, err := sql.Open("mysql", connectionName)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if _, err := db.Exec("insert into foo(id, value) select * from foo_temp on duplicate key update value = values(value)"); err != nil {
		panic(err)
	}
	if _, err := db.Exec("truncate table foo_temp;"); err != nil {
		panic(err)
	}
	fmt.Println("exit...")
}
