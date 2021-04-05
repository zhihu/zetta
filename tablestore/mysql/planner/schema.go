package planner

import (
	"github.com/pingcap/tidb/types"
	"github.com/zhihu/zetta/tablestore/mysql/expression"
)

// baseSchemaProducer stores the schema for the base plans who can produce schema directly.
type baseSchemaProducer struct {
	schema *expression.Schema
	names  types.NameSlice
	basePlan
}

// OutputNames returns the outputting names of each column.
func (s *baseSchemaProducer) OutputNames() types.NameSlice {
	return s.names
}

func (s *baseSchemaProducer) SetOutputNames(names types.NameSlice) {
	s.names = names
}

// Schema implements the Plan.Schema interface.
func (s *baseSchemaProducer) Schema() *expression.Schema {
	if s.schema == nil {
		s.schema = expression.NewSchema()
	}
	return s.schema
}

// SetSchema implements the Plan.SetSchema interface.
func (s *baseSchemaProducer) SetSchema(schema *expression.Schema) {
	s.schema = schema
}

func (s *baseSchemaProducer) setSchemaAndNames(schema *expression.Schema, names types.NameSlice) {
	s.schema = schema
	s.names = names
}

// logicalSchemaProducer stores the schema for the logical plans who can produce schema directly.
type logicalSchemaProducer struct {
	schema *expression.Schema
	names  types.NameSlice
	baseLogicalPlan
}

// Schema implements the Plan.Schema interface.
func (s *logicalSchemaProducer) Schema() *expression.Schema {
	if s.schema == nil {
		s.schema = expression.NewSchema()
	}
	return s.schema
}

func (s *logicalSchemaProducer) OutputNames() types.NameSlice {
	return s.names
}

func (s *logicalSchemaProducer) SetOutputNames(names types.NameSlice) {
	s.names = names
}

// SetSchema implements the Plan.SetSchema interface.
func (s *logicalSchemaProducer) SetSchema(schema *expression.Schema) {
	s.schema = schema
}

func (s *logicalSchemaProducer) setSchemaAndNames(schema *expression.Schema, names types.NameSlice) {
	s.schema = schema
	s.names = names
}
