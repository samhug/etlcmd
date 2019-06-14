package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/codegangsta/cli"
	"github.com/rhansen2/ratchet"
	"github.com/rhansen2/ratchet/logger"
	"github.com/rhansen2/ratchet/processors"
	"github.com/rhansen2/ratchet/util"
	"golang.org/x/crypto/ssh"

	procs "github.com/samhug/ratchet_processors"
	"github.com/samhug/udt"
)

const (
	infoName    = "etlcmd"
	infoVersion = "0.3.5"
	infoAuthor  = "Sam Hug"
)

func main() {

	var configPath string
	var logPath string
	var quietFlag bool

	app := cli.NewApp()
	app.Name = infoName
	app.Usage = "A utility to assist with the automation of ETL tasks."
	app.Author = infoAuthor
	app.Version = infoVersion
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "config, c",
			Usage:       "Path to configuration file",
			Destination: &configPath,
		},
		cli.StringFlag{
			Name:        "logfile, l",
			Usage:       "Path to log file",
			Destination: &logPath,
		},
		cli.BoolFlag{
			Name:        "quiet, q",
			Usage:       "Suppress terminal output",
			Destination: &quietFlag,
		},
	}
	app.Action = func(c *cli.Context) error {

		if !quietFlag {
			fmt.Fprintf(os.Stderr, "%s v%s by %s\n\n", infoName, infoVersion, infoAuthor)
		}

		if logPath != "" {
			f, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
			if err != nil {
				log.Fatalf("error opening logfile: %v", err)
			}
			defer f.Close()

			// If the quiet flag was specified, log to file only. Otherwise log to file and stderr
			if quietFlag {
				log.SetOutput(f)
			} else {
				log.SetOutput(io.MultiWriter(os.Stderr, f))
			}
		}

		if configPath == "" {
			log.Fatalf("You must specify a configuration file.\n")
		}

		config, err := LoadConfig(configPath)
		if err != nil {
			log.Fatalf("Failed to load configuration: %s\n", err)
		}

		return runApp(config)
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

// Get handle to input file defined by path attribute, if defined, or stdin
func inputFile(config configMap) (f *os.File) {
	if config["path"] != nil {
		var err error
		path := config["path"].(string)
		f, err = os.Open(path)
		if err != nil {
			log.Fatalf("Failed to open input file (%s): %s", path, err)
		}
	} else {
		f = os.Stdin
	}
	return
}

// Get handle to output file defined by path attribute, if defined, or stdout
func outputFile(config configMap) (f *os.File) {
	if config["path"] != nil {
		var err error
		path := config["path"].(string)
		f, err = os.Create(path)
		if err != nil {
			log.Fatalf("Failed to create output file (%s): %s", path, err)
		}
	} else {
		f = os.Stdout
	}
	return
}

func runApp(config *Config) error {

	logger.LogLevel = logger.LevelError

	// Initialize ETL's
	for _, processInfo := range config.Processes {

		log.Printf("%s ETL Process", processInfo.Name)

		var processorChain []ratchet.DataProcessor
		var err error

		// Initialize Input
		var input ratchet.DataProcessor
		inputType := strings.ToLower(processInfo.Input.Type)
		inputConfig := processInfo.Input.Config

		log.Printf("  Initializing %s input", inputType)

		switch inputType {
		default:
			log.Fatalf("Unsupported input type (%s)\n", inputType)
		case "csv":
			f := inputFile(inputConfig)
			defer f.Close()

			input, err = procs.NewCSVReader(f)
			if err != nil {
				log.Fatalf("Failed to initialize input: %s\n", err)
			}
		case "json":
			f := inputFile(inputConfig)
			defer f.Close()

			input = procs.NewJSONReader(f)
		case "jsonl":
			f := inputFile(inputConfig)
			defer f.Close()

			input = procs.NewJSONLReader(f)

		case "unidata":
			udtEnv := &procs.UdtEnvConfig{}
			udtEnv.UdtBin = config.Unidata.UdtBin
			udtEnv.UdtHome = config.Unidata.UdtHome
			udtEnv.UdtAcct = config.Unidata.UdtAcct

			if udtEnv.UdtBin == "" {
				log.Fatalf("The 'udtbin' attribute for input type 'unidata' must not be empty")
			}
			if udtEnv.UdtHome == "" {
				log.Fatalf("The 'udthome' attribute for input type 'unidata' must not be empty")
			}
			if udtEnv.UdtAcct == "" {
				log.Fatalf("The 'udtacct' attribute for input type 'unidata' must not be empty")
			}

			fileField, ok := inputConfig["file"]
			if !ok {
				log.Fatalf("You must specify a 'file' attribute for input type 'unidata'")
			}
			file, ok := fileField.(string)
			if !ok {
				log.Fatalf("The 'file' attribute for input type 'unidata' must be a string")
			}

			// If there is a select statement provided, use it. Otherwise, default to selecting the whole file.
			var selectScript []string
			selectStmtField, ok := inputConfig["select"]
			if ok {
				// Check if we were given a list
				selectInterface, ok := selectStmtField.([]interface{})
				if ok {
					selectScript = make([]string, len(selectInterface))
					for i, v := range selectInterface {
						selectScript[i], ok = v.(string)
						if !ok {
							log.Fatalf("The 'select' attribute for input type 'unidata' must be a string or an array of strings")
						}
					}

				} else {
					// Check if we were given a single string
					selectStmt, ok := selectStmtField.(string)
					if !ok {
						log.Fatalf("The 'select' attribute for input type 'unidata' must be a string or an array of strings")
					}
					selectScript = []string{selectStmt}
				}
			} else {
				selectScript = []string{fmt.Sprintf("SELECT %s", file)}
			}

			fieldsField, ok := inputConfig["fields"]
			if !ok {
				log.Fatalf("You must specify a 'fields' attribute for input type 'unidata'")
			}
			fieldsInterface, ok := fieldsField.([]interface{})
			if !ok {
				log.Fatalf("The 'fields' attribute for input type 'unidata' must be a list of strings")
			}
			fields := make([]string, len(fieldsInterface))
			for i, v := range fieldsInterface {
				fields[i], ok = v.(string)
				if !ok {
					log.Fatalf("The 'fields' attribute for input type 'unidata' must be a list of strings")
				}
			}

			batchSize := 10000
			batchSizeField, ok := inputConfig["batch_size"]
			if ok {
				batchSize, ok = batchSizeField.(int)
				if !ok {
					log.Fatalf("The 'batch_size' attribute for input type 'unidata' must be an int")
				}
			}

			queryConfig := &procs.UdtQueryConfig{
				Select:    selectScript,
				File:      file,
				Fields:    fields,
				BatchSize: batchSize,
			}

			sshConfig := &ssh.ClientConfig{
				User: config.Unidata.Username,
				Auth: []ssh.AuthMethod{
					ssh.Password(config.Unidata.Password),
				},
				HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			}

			sshClient, err := ssh.Dial("tcp", config.Unidata.Host, sshConfig)
			if err != nil {
				log.Fatalf("Failed to connect to (%s) as user (%s): %s", config.Unidata.Host, config.Unidata.Username, err)
			}
			defer sshClient.Close()

			udtClient := udt.NewClient(sshClient, udtEnv)

			input, err = procs.NewUdtReader(udtClient, queryConfig)
			if err != nil {
				log.Fatalf("Failed to initialize input: %s\n", err)
			}
		}
		processorChain = append(processorChain, input)

		// Initialize Transformations
		for _, transformInfo := range processInfo.Transforms {
			var transform ratchet.DataProcessor
			transformType := strings.ToLower(transformInfo.Type)
			transformConfig := transformInfo.Config

			log.Printf("  Initializing %s transform", transformType)

			switch transformType {
			default:
				log.Fatalf("Unsupported transform type (%s)\n", transformType)
			case "js":
				script := transformConfig["script"].(string)
				transform, err = procs.NewJsTransform(script)
				if err != nil {
					log.Fatalf("Failed to initialize JS transform: %s", err)
				}
			}
			processorChain = append(processorChain, transform)
		}

		// Initialize Output
		var output ratchet.DataProcessor
		outputType := strings.ToLower(processInfo.Output.Type)
		outputConfig := processInfo.Output.Config

		log.Printf("  Initializing %s output", outputType)

		switch outputType {
		default:
			log.Fatalf("Unsupported output type (%s)\n", outputType)
		case "csv":
			f := outputFile(outputConfig)
			defer f.Close()

			var columnOrder []string
			if outputConfig["column_order"] != nil {
				v, ok := outputConfig["column_order"].([]interface{})
				if !ok {
					log.Fatal("Field 'column_order' for csv output must be and array of strings")
				}
				for i, c := range v {
					h, ok := c.(string)
					if !ok {
						log.Fatalf("Field 'column_order' for csv output: item %d must be a string", i)
					}
					columnOrder = append(columnOrder, h)
				}
			}

			output = newCSVWriter(f, columnOrder)
		case "json":
			f := outputFile(outputConfig)
			defer f.Close()
			output = procs.NewJSONWriter(f)
		case "jsonl":
			f := outputFile(outputConfig)
			defer f.Close()
			output = procs.NewJSONLWriter(f)
		}
		processorChain = append(processorChain, output)

		log.Printf("  Initializing data pipeline")
		pipeline := ratchet.NewPipeline(context.TODO(), func() {}, processorChain...)

		log.Printf("  Processesing...")

		err = <-pipeline.Run()
		if err != nil {
			log.Fatalf("An error occurred in the data pipeline: %s", err.Error())
		}

		//log.Println(pipeline.Stats())
		log.Printf(" Done...")

	}

	return nil
}

func newCSVWriter(w io.Writer, columnOrder []string) *processors.CSVWriter {
	writer := &util.CSVWriter{
		Comma:             ',',
		UseCRLF:           false,
		AlwaysEncapsulate: true,
		QuoteEscape:       `"`,
	}
	writer.SetWriter(w)

	return &processors.CSVWriter{
		Parameters: util.CSVParameters{
			Writer:      writer,
			WriteHeader: true,
			Header:      columnOrder,
		},
	}
}
