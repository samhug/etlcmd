package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/hcl"
	"github.com/hashicorp/hcl/hcl/ast"
	"github.com/mitchellh/mapstructure"
)

type configMap map[string]interface{}

// inputInfo represents the input specification for an ETL process
type inputInfo struct {
	Type   string
	Config configMap
}

// outputInfo represents the output specification for an ETL process
type outputInfo struct {
	Type   string
	Config configMap
}

// transformInfo represents the transform specification for an ETL process
type transformInfo struct {
	Type   string
	Config configMap
}

// processInfo represents the specification for an ETL process
type processInfo struct {
	Name       string `hcl:",key"`
	Input      *inputInfo
	Transforms []*transformInfo
	Output     *outputInfo
}

type unidataInfo struct {
	Host     string
	Username string
	Password string
	UdtBin   string `hcl:"udtbin,"`
	UdtHome  string `hcl:"udthome,"`
	UdtAcct  string `hcl:"udtacct,"`
}

// Config is the root configuration object that contains all ETL process specifications
type Config struct {
	Processes []*processInfo `hcl:"process,"`
	Unidata   *unidataInfo
}

// Parse consumes a Reader and returns a Config object
func Parse(r io.Reader) (*Config, error) {

	// Copy the reader into an in-memory buffer first since HCL requires it.
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return nil, err
	}

	// Parse the buffer
	root, err := hcl.Parse(buf.String())
	if err != nil {
		return nil, fmt.Errorf("error parsing: %s", err)
	}
	buf.Reset()

	// Top-level item should be the object list
	list, ok := root.Node.(*ast.ObjectList)
	if !ok {
		return nil, fmt.Errorf("error parsing: file doesn't contain a root object")
	}

	// Check for invalid keys
	valid := []string{
		"process",
		"unidata",
	}
	if err := checkHCLKeys(list, valid); err != nil {
		return nil, err
	}

	var result Config

	// Parse the process configs
	if o := list.Filter("process"); len(o.Items) > 0 {

		if err := parseProcesses(&result, o); err != nil {
			return nil, fmt.Errorf("error parsing 'process': %s", err)
		}
	}

	// Parse the unidata config
	if o := list.Filter("unidata"); len(o.Items) > 0 {

		if err := parseUnidata(&result, o); err != nil {
			return nil, fmt.Errorf("error parsing 'unidata': %s", err)
		}
	}

	return &result, nil
}

func parseUnidata(result *Config, list *ast.ObjectList) error {

	if len(list.Items) > 1 {
		return fmt.Errorf("only one 'unidata' block allowed")
	}

	// Get our one item
	item := list.Items[0]

	// Check for invalid keys
	valid := []string{"host", "username", "password", "udtbin", "udthome", "udtacct"}
	if err := checkHCLKeys(item.Val, valid); err != nil {
		return multierror.Prefix(err, "unidata:")
	}

	var m map[string]interface{}
	if err := hcl.DecodeObject(&m, item.Val); err != nil {
		return err
	}

	u := unidataInfo{}
	result.Unidata = &u
	return mapstructure.WeakDecode(m, &u)
}

func parseProcesses(result *Config, list *ast.ObjectList) error {

	list = list.Children()
	if len(list.Items) == 0 {
		return nil
	}

	// Go through each object and turn it into an actual result.
	collection := make([]*processInfo, 0, len(list.Items))
	seen := make(map[string]struct{})
	for _, item := range list.Items {
		n, ok := item.Keys[0].Token.Value().(string)
		if !ok {
			return fmt.Errorf("process name must be a string, got %q", item.Keys[0].Token.Value())
		}

		// Make sure we haven't already found this
		if _, ok := seen[n]; ok {
			return fmt.Errorf("process '%s' defined more than once", n)
		}
		seen[n] = struct{}{}

		// Check for invalid keys
		valid := []string{"input", "output", "transform"}
		if err := checkHCLKeys(item.Val, valid); err != nil {
			return multierror.Prefix(err, fmt.Sprintf(
				"process '%s':", n))
		}

		var listVal *ast.ObjectList
		if ot, ok := item.Val.(*ast.ObjectType); ok {
			listVal = ot.List
		} else {
			return fmt.Errorf("process '%s': should be an object", n)
		}

		var m map[string]interface{}
		if err := hcl.DecodeObject(&m, item.Val); err != nil {
			return err
		}

		var process processInfo

		process.Name = n

		// Parse input
		if o := listVal.Filter("input"); len(o.Items) == 0 {
			return fmt.Errorf("you must specify an 'input' block for process '%s'", process.Name)
		} else if err := parseInputs(&process, o); err != nil {
			return fmt.Errorf("error parsing 'input': %s", err)
		}

		// Parse transforms
		if o := listVal.Filter("transform"); len(o.Items) > 0 {
			if err := parseTransforms(&process, o); err != nil {
				return fmt.Errorf("error parsing 'transform': %s", err)
			}
		}

		// Parse outputs
		if o := listVal.Filter("output"); len(o.Items) == 0 {
			return fmt.Errorf("you must specify an 'output' block for process '%s'", process.Name)
		} else if err := parseOutputs(&process, o); err != nil {
			return fmt.Errorf("error parsing 'output': %s", err)
		}

		collection = append(collection, &process)
	}

	// Set the results
	result.Processes = collection
	return nil
}

func parseInputs(result *processInfo, list *ast.ObjectList) error {

	list = list.Children()
	if len(list.Items) == 0 {
		return nil
	}

	item := list.Items[0]

	if len(item.Keys) == 0 {
		return fmt.Errorf("you may only specify a type for inputs")
	}
	key, ok := item.Keys[0].Token.Value().(string)
	if !ok {
		return fmt.Errorf("input name must be a string, got %q", item.Keys[0].Token.Value())
	}

	var m map[string]interface{}
	if err := hcl.DecodeObject(&m, item.Val); err != nil {
		return err
	}

	var input inputInfo
	input.Type = strings.ToLower(key)
	input.Config = m

	result.Input = &input

	return nil
}

func parseTransforms(result *processInfo, list *ast.ObjectList) error {

	// Go through each object and turn it into an actual result.
	collection := make([]*transformInfo, 0, len(list.Items))
	for _, item := range list.Items {

		if len(item.Keys) == 0 {
			return fmt.Errorf("you may only specify a type for transforms")
		}
		key, ok := item.Keys[0].Token.Value().(string)
		if !ok {
			return fmt.Errorf("transform name must be a string, got %q", item.Keys[0].Token.Value())
		}

		var m map[string]interface{}
		if err := hcl.DecodeObject(&m, item.Val); err != nil {
			return err
		}

		var c transformInfo
		c.Type = strings.ToLower(key)
		c.Config = m

		collection = append(collection, &c)
	}

	result.Transforms = collection

	return nil
}

func parseOutputs(result *processInfo, list *ast.ObjectList) error {

	list = list.Children()
	if len(list.Items) != 1 {
		return fmt.Errorf("you may only specify one 'output'")
	}

	item := list.Items[0]

	if len(item.Keys) == 0 {
		return fmt.Errorf("you may only specify a type for outputs")
	}
	key, ok := item.Keys[0].Token.Value().(string)
	if !ok {
		return fmt.Errorf("output name must be a string, got %q", item.Keys[0].Token.Value())
	}

	var m map[string]interface{}
	if err := hcl.DecodeObject(&m, item.Val); err != nil {
		return err
	}

	var c outputInfo
	c.Type = strings.ToLower(key)
	c.Config = m

	result.Output = &c

	return nil
}

// LoadConfig loads configuration info from a file
func LoadConfig(path string) (*Config, error) {

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening configuration file (%s): %s", path, err)
	}

	config, err := Parse(file)
	if err != nil {
		return nil, fmt.Errorf("parsing configuration file (%s): %s", path, err)
	}

	return config, nil
}

func checkHCLKeys(node ast.Node, valid []string) error {
	var list *ast.ObjectList
	switch n := node.(type) {
	case *ast.ObjectList:
		list = n
	case *ast.ObjectType:
		list = n.List
	default:
		return fmt.Errorf("cannot check HCL keys of type %T", n)
	}

	validMap := make(map[string]struct{}, len(valid))
	for _, v := range valid {
		validMap[v] = struct{}{}
	}

	var result error
	for _, item := range list.Items {
		key := item.Keys[0].Token.Value().(string)
		if _, ok := validMap[key]; !ok {
			result = multierror.Append(result, fmt.Errorf(
				"invalid key '%s' on line %d", key, item.Assign.Line))
		}
	}

	return result
}
