package main

import (
	"bytes"
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/hcl"
	"github.com/hashicorp/hcl/hcl/ast"
	"github.com/mitchellh/mapstructure"
	"io"
	"os"
	"strings"
)

type FieldInfo struct {
	Name    string
	Type    string
	IsMulti bool
}

type InputInfo struct {
	Type   string
	Config map[string]interface{}
	Fields []*FieldInfo
}

type OutputInfo struct {
	Type   string
	Config map[string]interface{}
}

type TransformInfo struct {
	Type   string
	Config map[string]interface{}
}

type ProcessInfo struct {
	Name       string `hcl:",key"`
	Input      *InputInfo
	Transforms []*TransformInfo
	Output     *OutputInfo
}

type UnidataInfo struct {
	Host     string
	Username string
	Password string
	UdtBin   string `hcl:"udt_bin,"`
}

type MongoDBInfo struct {
	Server   string
	Database string
}

type Config struct {
	Processes []*ProcessInfo `hcl:"process,"`
	Unidata   *UnidataInfo
	MongoDB   *MongoDBInfo
}

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
		"mongodb",
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

	// Parse the mongodb config
	if o := list.Filter("mongodb"); len(o.Items) > 0 {

		if err := parseMongoDB(&result, o); err != nil {
			return nil, fmt.Errorf("error parsing 'mongodb': %s", err)
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
	valid := []string{"host", "username", "password", "udt_bin"}
	if err := checkHCLKeys(item.Val, valid); err != nil {
		return multierror.Prefix(err, "unidata:")
	}

	var m map[string]interface{}
	if err := hcl.DecodeObject(&m, item.Val); err != nil {
		return err
	}

	u := UnidataInfo{}
	result.Unidata = &u
	return mapstructure.WeakDecode(m, &u)
}

func parseMongoDB(result *Config, list *ast.ObjectList) error {

	if len(list.Items) > 1 {
		return fmt.Errorf("only one 'mongodb' block allowed")
	}

	// Get our one item
	item := list.Items[0]

	// Check for invalid keys
	valid := []string{"server", "database"}
	if err := checkHCLKeys(item.Val, valid); err != nil {
		return multierror.Prefix(err, "mongodb:")
	}

	var m map[string]interface{}
	if err := hcl.DecodeObject(&m, item.Val); err != nil {
		return err
	}

	i := MongoDBInfo{}
	result.MongoDB = &i
	return mapstructure.WeakDecode(m, &i)
}

func parseProcesses(result *Config, list *ast.ObjectList) error {

	list = list.Children()
	if len(list.Items) == 0 {
		return nil
	}

	// Go through each object and turn it into an actual result.
	collection := make([]*ProcessInfo, 0, len(list.Items))
	seen := make(map[string]struct{})
	for _, item := range list.Items {
		n := item.Keys[0].Token.Value().(string)

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

		var process ProcessInfo
		/*
			if err := mapstructure.WeakDecode(m, &process); err != nil {
				return fmt.Errorf(
					"error parsing process '%s': %s", n, err)
			}
		*/

		process.Name = n

		// Parse input
		if o := listVal.Filter("input"); len(o.Items) > 0 {
			if err := parseInputs(&process, o); err != nil {
				return fmt.Errorf("error parsing 'input': %s", err)
			}
		}

		// Parse transforms
		if o := listVal.Filter("transform"); len(o.Items) > 0 {
			if err := parseTransforms(&process, o); err != nil {
				return fmt.Errorf("error parsing 'transform': %s", err)
			}
		}

		// Parse outputs
		if o := listVal.Filter("output"); len(o.Items) > 0 {
			if err := parseOutputs(&process, o); err != nil {
				return fmt.Errorf("error parsing 'output': %s", err)
			}
		}

		collection = append(collection, &process)
	}

	// Set the results
	result.Processes = collection
	return nil
}

func parseInputs(result *ProcessInfo, list *ast.ObjectList) error {

	list = list.Children()
	if len(list.Items) == 0 {
		return nil
	}

	item := list.Items[0]

	if len(item.Keys) == 0 {
		return fmt.Errorf("you may only specify a type for inputs")
	}
	key := item.Keys[0].Token.Value().(string)

	var listVal *ast.ObjectList
	if ot, ok := item.Val.(*ast.ObjectType); ok {
		listVal = ot.List
	} else {
		return fmt.Errorf("process '%s': should be an object", key)
	}

	var m map[string]interface{}
	if err := hcl.DecodeObject(&m, item.Val); err != nil {
		return err
	}

	var input InputInfo
	input.Type = strings.ToLower(key)
	input.Config = m

	// Parse fields
	if o := listVal.Filter("field"); len(o.Items) > 0 {
		if err := parseFields(&input, o); err != nil {
			return fmt.Errorf("error parsing 'field': %s", err)
		}
	}

	result.Input = &input

	return nil
}

func parseFields(result *InputInfo, list *ast.ObjectList) error {

	// Go through each object and turn it into an actual result.
	collection := make([]*FieldInfo, 0, len(list.Items))
	seen := make(map[string]struct{})
	for _, item := range list.Items {
		n := item.Keys[0].Token.Value().(string)

		// Make sure we haven't already found this
		if _, ok := seen[n]; ok {
			return fmt.Errorf("field '%s' defined more than once", n)
		}
		seen[n] = struct{}{}

		// Check for invalid keys
		valid := []string{"type", "is_multi"}
		if err := checkHCLKeys(item.Val, valid); err != nil {
			return multierror.Prefix(err, fmt.Sprintf(
				"field '%s':", n))
		}

		var m map[string]interface{}
		if err := hcl.DecodeObject(&m, item.Val); err != nil {
			return err
		}

		var field FieldInfo
		if err := mapstructure.WeakDecode(m, &field); err != nil {
			return fmt.Errorf(
				"error parsing field '%s': %s", n, err)
		}

		field.Name = n

		if val, ok := m["type"]; ok {
			field.Type = val.(string)
		} else {
			return fmt.Errorf(
				"you must specify a type for field '%s'", field.Name)
		}

		if val, ok := m["is_multi"]; ok {
			field.IsMulti = val.(bool)
		} else {
			field.IsMulti = false
		}

		collection = append(collection, &field)
	}

	// Set the results
	result.Fields = collection
	return nil
}

func parseTransforms(result *ProcessInfo, list *ast.ObjectList) error {

	// Go through each object and turn it into an actual result.
	collection := make([]*TransformInfo, 0, len(list.Items))
	for _, item := range list.Items {

		if len(item.Keys) == 0 {
			return fmt.Errorf("you may only specify a type for transforms")
		}
		key := item.Keys[0].Token.Value().(string)

		var m map[string]interface{}
		if err := hcl.DecodeObject(&m, item.Val); err != nil {
			return err
		}

		var c TransformInfo
		c.Type = strings.ToLower(key)
		c.Config = m

		collection = append(collection, &c)
	}

	result.Transforms = collection

	return nil
}

func parseOutputs(result *ProcessInfo, list *ast.ObjectList) error {

	list = list.Children()
	if len(list.Items) != 1 {
		return fmt.Errorf("you may only specify one 'output'")
	}

	item := list.Items[0]

	if len(item.Keys) == 0 {
		return fmt.Errorf("you may only specify a type for outputs")
	}
	key := item.Keys[0].Token.Value().(string)

	var m map[string]interface{}
	if err := hcl.DecodeObject(&m, item.Val); err != nil {
		return err
	}

	var c OutputInfo
	c.Type = strings.ToLower(key)
	c.Config = m

	result.Output = &c

	return nil
}

func LoadConfig(path string) (*Config, error) {

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening configuation file (%s): %s", path, err)
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
