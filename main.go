package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/codegangsta/cli"
	"github.com/dailyburn/ratchet"
	"github.com/dailyburn/ratchet/logger"
	"github.com/dailyburn/ratchet/processors"

	procs "github.com/samuelhug/ratchet_processors"
)

const CMDNAME = "etlcmd"

func main() {

	var configPath string

	app := cli.NewApp()
	app.Name = CMDNAME
	app.Usage = "A utility to assist with the automation of ETL tasks."
	app.Author = "Sam Hug"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "config, c",
			Value:       fmt.Sprintf("./%s.conf", CMDNAME),
			Usage:       "Path to configuration file",
			Destination: &configPath,
		},
	}
	app.Action = func(c *cli.Context) {
		config, err := LoadConfig(configPath)
		if err != nil {
			log.Fatalf("Unable to load configuration: %s\n", err)
		}

		runApp(config)
	}

	app.Run(os.Args)
}

func runApp(config *Config) {

	logger.LogLevel = logger.LevelError

	// Initialize ETL's
	for _, processInfo := range config.Processes {

		log.Printf("Initializing %s ETL", processInfo.Name)

		var processorChain []ratchet.DataProcessor
		var err error

		// Initialize Input
		var input ratchet.DataProcessor
		inputType := strings.ToLower(processInfo.Input.Type)
		inputConfig := processInfo.Input.Config

		log.Printf("Initializing %s input", inputType)

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

			log.Printf("Initializing %s transform", transformType)

			switch transformType {
			default:
				log.Fatalf("Unsupported transform type (%s)\n", transformType)
			case "js":
				script := transformConfig["script"].(string)
				transform, err = procs.NewJsTransform(script)
				if err != nil {
					log.Fatalf("Error initializing JS transform: %s", err)
				}
			}
			processorChain = append(processorChain, transform)
		}

		// Initialize Output
		var output ratchet.DataProcessor
		outputType := strings.ToLower(processInfo.Output.Type)
		outputConfig := processInfo.Output.Config

		log.Printf("Initializing %s output", outputType)

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

		log.Printf("Initializing data pipeline")
		pipeline := ratchet.NewPipeline(processorChain...)

		log.Printf("Processesing...")

		err = <-pipeline.Run()
		if err != nil {
			fmt.Println("An error occurred in the ratchet pipeline: ", err.Error())
		}

		log.Printf("Done...")
	}
}
