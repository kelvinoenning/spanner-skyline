package data

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	"cloud.google.com/go/spanner"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

type BatchMutation struct {
	Mutation []*spanner.Mutation
}

func MapToBatchMutation(objects *[]map[string]interface{}) *[]BatchMutation {
	table := os.Getenv("SPANNER_SKYLINE_TABLE")

	batchMutations := []BatchMutation{}
	batchMutation := BatchMutation{}
	for index, element := range *objects {
		batchMutation.Mutation = append(batchMutation.Mutation, spanner.InsertOrUpdateMap(table, element))
		if (len(batchMutation.Mutation) == 1000) || (index == len((*objects))-1) {
			batchMutations = append(batchMutations, batchMutation)
			batchMutation = BatchMutation{}
		}
	}
	if len(batchMutations) == 0 && len(batchMutation.Mutation) > 0 {
		batchMutations = append(batchMutations, batchMutation)
	}
	return &batchMutations
}

func JSONToMap(jsonData []byte) *map[string]interface{} {
	object := map[string]interface{}{}
	err := json.Unmarshal(jsonData, &object)
	if err != nil {
		return nil
	}
	return &object
}

func Write(mutations []*spanner.Mutation, credentials []byte) bool {
	project := os.Getenv("SPANNER_SKYLINE_PROJECT")
	instance := os.Getenv("SPANNER_SKYLINE_INSTANCE")
	database := os.Getenv("SPANNER_SKYLINE_DATABASE")

	ctx := context.Background()

	db := "projects/" + project + "/instances/" + instance + "/databases/" + database
	client, err := spanner.NewClient(ctx, db, option.WithCredentialsJSON(credentials))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	_, err = client.Apply(ctx, mutations)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return false
	}
	return true
}

func worker(credentials []byte, job chan []*spanner.Mutation, done chan bool, wg *sync.WaitGroup) {
	for {
		select {
		case k := <-job:
			result := Write(k, credentials)
			wg.Done()
			done <- result
		}
	}
}

func SendToSpanner(objects *[]map[string]interface{}) {
	credentialsPath := os.Getenv("SPANNER_SKYLINE_CREDENTIALS_PATH")

	credentials, err := ioutil.ReadFile(credentialsPath)
	if err != nil {
		panic(err)
	}

	batchs := MapToBatchMutation(objects)

	batchSize := len(*batchs)

	job := make(chan []*spanner.Mutation)
	done := make(chan bool)

	var wg sync.WaitGroup

	fmt.Printf("START:    Batchs: %v\n", batchSize)

	count := 0

	for i := 0; i < 150; i++ {
		go worker(credentials, job, done, &wg)
		if count <= len((*batchs))-1 {
			wg.Add(1)
			job <- (*batchs)[count].Mutation
			count++
		}
	}
	run := true
	for run == true {
		select {
		case <-done:
			count++
			if count <= batchSize-1 {
				wg.Add(1)
				job <- (*batchs)[count].Mutation
			} else {
				run = false
				break
			}
		}
	}

	wg.Wait()
	fmt.Printf("FINISH:   Batchs: %v\n", batchSize)
}

func ReadByFile(path string) (*[]map[string]interface{}, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	objects := []map[string]interface{}{}

	for scanner.Scan() {
		object := JSONToMap(scanner.Bytes())
		if object != nil {
			objects = append(objects, *object)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return &objects, nil
}
