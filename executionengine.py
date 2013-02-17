#!/usr/bin/python

import statements
import flatfile

def execute(statement):
	if isinstance(statement, statements.CreateStatement):
		execute_create(statement)
	elif isinstance(statement, statements.InsertStatement):
		execute_insert(statement)
	elif isinstance(statement, statements.SelectStatement):
		execute_select(statement)


def execute_create(statement):
	flatfile.create_flatfile(statement.tablename, [x[0] for x in statement.fields])

def execute_insert(statement):
	f = flatfile.FlatFile()
	f.open(statement.tablename)
	f.insert(flatfile.Record(statement.values[0], statement.values[1:]))
	f.close()

def execute_select(statement):
	f = flatfile.FlatFile()
	f.open(statement.tablename)
	records = f.get_all_records()
	for rec in records:
		print rec
	f.close()