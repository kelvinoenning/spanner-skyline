package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/kelvinoenning/spanner-skyline/pkg/data"
)

func makeEnvVariable() {
	folterPath := flag.String("folder", "folder", "a string")
	credentialPath := flag.String("credentials", "foo", "a string")
	project := flag.String("project", "foo", "a string")
	instance := flag.String("instance", "foo", "a string")
	database := flag.String("database", "foo", "a string")
	table := flag.String("table", "foo", "a string")

	flag.Parse()

	os.Setenv("SPANNER_SKYLINE_FOLDER_PATH", *folterPath)
	os.Setenv("SPANNER_SKYLINE_CREDENTIALS_PATH", *credentialPath)
	os.Setenv("SPANNER_SKYLINE_PROJECT", *project)
	os.Setenv("SPANNER_SKYLINE_INSTANCE", *instance)
	os.Setenv("SPANNER_SKYLINE_DATABASE", *database)
	os.Setenv("SPANNER_SKYLINE_TABLE", *table)
}

func main() {
	makeEnvVariable()

	folderPath := os.Getenv("SPANNER_SKYLINE_FOLDER_PATH")

	files, err := ioutil.ReadDir(folderPath)
	if err != nil {
		log.Fatal(err)
	}

	count := 0

	for _, f := range files {
		count++
		fmt.Printf("===== START PROCESS FILE: %v =====> %v\n", f.Name(), count)
		objects, err := data.ReadByFile(folderPath + "/" + f.Name())
		if err != nil {
			log.Fatal(err)
		}
		data.SendToSpanner(objects)
		fmt.Printf("===== FINISH =====\n")
	}
}
