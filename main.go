package main

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/olivere/elastic"
)

const (
	indexName    = "applications"
	docType      = "log"
	appName      = "myApp"
	indexMapping = `{
						"mappings" : {
							"log" : {
								"properties" : {
									"app" : { "type" : "text", "index" : "false" },
									"message" : { "type" : "text", "index" : "false" },
									"time" : { "type" : "date" }
								}
							}
						}
					}`
)

type Log struct {
	App     string    `json:"app"`
	Message string    `json:"message"`
	Time    time.Time `json:"time"`
}

func main() {
	ctx := context.Background()

	client, err := elastic.NewClient(
		elastic.SetURL("http://localhost:9200"),
		elastic.SetSniff(false),
	)
	if err != nil {
		panic(err)
	}

	err = createIndexWithLogsIfDoesNotExist(ctx, client)
	if err != nil {
		panic(err)
	}

	err = findAndPrintAppLogs(ctx, client)
	if err != nil {
		panic(err)
	}
}

func createIndexWithLogsIfDoesNotExist(ctx context.Context, client *elastic.Client) error {
	exists, err := client.IndexExists(indexName).Do(ctx)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	res, err := client.CreateIndex(indexName).
		Body(indexMapping).
		Do(ctx)

	if err != nil {
		return err
	}
	if !res.Acknowledged {
		return errors.New("CreateIndex was not acknowledged. Check that timeout value is correct.")
	}

	return addLogsToIndex(ctx, client)
}

func addLogsToIndex(ctx context.Context, client *elastic.Client) error {
	for i := 0; i < 10; i++ {
		l := Log{
			App:     "myApp",
			Message: fmt.Sprintf("message %d", i),
			Time:    time.Now(),
		}

		_, err := client.Index().
			Index(indexName).
			Type(docType).
			BodyJson(l).
			Do(ctx)

		if err != nil {
			return err
		}
	}

	return nil
}

func findAndPrintAppLogs(ctx context.Context, client *elastic.Client) error {
	termQuery := elastic.NewTermQuery("app", appName)

	res, err := client.Search(indexName).
		Index(indexName).
		Query(termQuery).
		Sort("time", true).
		Do(ctx)

	if err != nil {
		return err
	}

	fmt.Println("Logs found:")
	var l Log
	for _, item := range res.Each(reflect.TypeOf(l)) {
		l := item.(Log)
		fmt.Printf("time: %s message: %s\n", l.Time, l.Message)
	}

	return nil
}
