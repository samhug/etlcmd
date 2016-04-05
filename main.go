package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/dailyburn/ratchet"
	"github.com/dailyburn/ratchet/logger"
	"github.com/dailyburn/ratchet/processors"

	procs "github.com/samuelhug/ratchet_processors"
)

func main() {

	logger.LogLevel = logger.LevelError

	config, err := LoadConfig("etlcmd.conf")
	if err != nil {
		log.Fatalf("Unable to load configuration: %s\n", err)
	}

	// Initialize ETL's
	for _, processInfo := range config.Processes {

		var processorChain []ratchet.DataProcessor

		// Initialize Input
		var input ratchet.DataProcessor
		inputType := strings.ToLower(processInfo.Input.Type)
		inputConfig := processInfo.Input.Config
		switch inputType {
		default:
			log.Fatalf("Unsupported input type (%s)\n", inputType)
		case "csv":
			path := inputConfig["path"].(string)
			f, err := os.Open(path)
			if err != nil {
				log.Fatalf("Unable to open input file (%s): %s", path, err)
			}
			defer f.Close()

			input, err = procs.NewCSVReader(f)
			if err != nil {
				log.Fatalf("Error initializing input: %s\n", err)
			}
		case "json":
			path := inputConfig["path"].(string)
			f, err := os.Open(path)
			if err != nil {
				log.Fatalf("Unable to open input file (%s): %s", path, err)
			}
			defer f.Close()

			input = procs.NewJSONReader(f)

		case "unidata":
			c := &procs.UdtConfig{}

			c.Address = config.Unidata.Host
			c.Username = config.Unidata.Username
			c.Password = config.Unidata.Password

			query := inputConfig["query"].(string)
			if query == "" {
				log.Fatalf("You must specifiy a 'query' for input type 'udt'")
			}

			var recordSchema []*procs.UdtFieldInfo
			for _, f := range processInfo.Input.Fields {

				fieldInfo := &procs.UdtFieldInfo{
					Name:    f.Name,
					Type:    f.Type,
					IsMulti: f.IsMulti,
				}
				recordSchema = append(recordSchema, fieldInfo)
			}

			input, err = procs.NewUdtReader(c, query, recordSchema)
			if err != nil {
				log.Fatalf("Error initializing input: %s\n", err)
			}
		}
		processorChain = append(processorChain, input)

		// Initialize Transformations
		for _, transformInfo := range processInfo.Transforms {
			var transform ratchet.DataProcessor
			transformType := strings.ToLower(transformInfo.Type)
			transformConfig := transformInfo.Config
			switch transformType {
			default:
				log.Fatalf("Unsupported transform type (%s)\n", transformType)
			case "js":
				script := transformConfig["script"].(string)
				transform = procs.NewJsTransform(script)
				//if err != nil {
				//	log.Fatalf("Error initializing transform: %s\n", err)
				//}
			}
			processorChain = append(processorChain, transform)
		}

		// Initialize Output
		var output ratchet.DataProcessor
		outputType := strings.ToLower(processInfo.Output.Type)
		outputConfig := processInfo.Output.Config
		switch outputType {
		default:
			log.Fatalf("Unsupported output type (%s)\n", outputType)
		case "csv":
			path := outputConfig["path"].(string)
			f, err := os.Create(path)
			if err != nil {
				log.Fatalf("Unable to create output file (%s): %s", path, err)
			}
			defer f.Close()

			output = processors.NewCSVWriter(f)
		case "json":
			path := outputConfig["path"].(string)
			f, err := os.Create(path)
			if err != nil {
				log.Fatalf("Unable to create output file (%s): %s", path, err)
			}
			defer f.Close()

			output = procs.NewJSONWriter(f)

		case "mongodb":
			mgoConfig := &procs.MgoConfig{
				Server:     config.MongoDB.Server,
				Db:         config.MongoDB.Database,
				Collection: outputConfig["collection"].(string),
			}
			output, err = procs.NewMgoWriter(mgoConfig)
			if err != nil {
				log.Fatalf("Error initializing output: %s\n", err)
			}
		}
		processorChain = append(processorChain, output)

		pipeline := ratchet.NewPipeline(processorChain...)

		err = <-pipeline.Run()

		if err != nil {
			fmt.Println("An error occurred in the ratchet pipeline:", err.Error())
		}
	}
}
