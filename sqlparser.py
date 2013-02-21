#!/usr/bin/python
import re
import statements
import tablemetadata as tmd

class UnknownStatement(Exception):
	def __init__(self, msg):
		self.msg = msg

	def __str__(self):
		return repr(self.msg)

class ParseException(Exception):
	def __init__(self, msg):
		self.msg = msg

	def __str__(self):
		return repr(self.msg)

def parse_statement(str):
	statement = str.split(" ")[0].lower();
	if statement == "create":
		return parse_create(str)
	elif statement == "insert":
		return parse_insert(str)
	elif statement == "select":
		return parse_select(str)
	elif statement == "drop":
		return parse_drop(str)
	else:
		raise UnknownStatement(statement)

def parse_create(str):
	m = re.match(r"create table (\w+)\s*\((.*)\)", str, re.IGNORECASE)
	fields = [x.strip().split(" ") for x in m.group(2).split(",")];
	return statements.CreateStatement(m.group(1), [_get_attribute_by_column(x) for x in fields])

def _get_attribute_by_column(column):
	typename = column[1].lower()
	if typename == tmd.Types.INT:
		return tmd.Attribute(column[0], typename, 4, 0)
	elif typename == tmd.Types.DOUBLE:
		return tmd.Attribute(column[0], typename, 8, 0.0)
	elif typename.startswith(tmd.Types.VARCHAR):
		m = re.match(tmd.Types.VARCHAR+r"\s*\((\d+)\)", typename, re.IGNORECASE)
		return tmd.Attribute(column[0], tmd.Types.VARCHAR, m.group(1), "")
	else:
		raise Exception("Unkown typename: "+typename)

def parse_insert(str):
	m = re.match(r"insert into (\w+) values\s*\((.*)\)", str, re.IGNORECASE)
	values = [x.strip() for x in m.group(2).split(",")];
	return statements.InsertStatement(m.group(1), values)

def parse_select(str):
	m = re.match(r"select (.+) from (\w+)( where (.+))?", str, re.IGNORECASE)
	if m == None:
		raise ParseException("Can't parse: "+str)
	if m.group(4) == None:
		return statements.SelectStatement(m.group(2))
	else:
		return statements.SelectStatement(m.group(2), parse_where(m.group(4)))

def parse_drop(str):
	m = re.match(r"drop table (\w+)", str, re.IGNORECASE)
	return statements.DropStatement(m.group(1))

def parse_where(str):
	m = re.match(r"(\w+)(\s*)(\W{1,2})(\s*)(.+)", str, re.IGNORECASE)
	if m == None:
		raise ParseException("Can't parse: "+str)
	
	colname = m.group(1)
	operation = m.group(3).strip()
	value = m.group(5)
	
	if colname == None or operation == None or value == None:
		raise ParseException("Can't parse: "+str)
	
	if operation == "=":
		operation = statements.WhereOps.EQ
	elif operation == "!=":
		operation = statements.WhereOps.NEQ
	elif operation == "<":
		operation = statements.WhereOps.LT
	elif operation == ">":
		operation = statements.WhereOps.GT
	elif operation == "<=":
		operation = statements.WhereOps.LEQ
	elif operation == ">=":
		operation = statements.WhereOps.GEQ
	else:
		raise ParseException("Can't parse: " + str)
	
	value = value.strip('"').strip("'")
	
	return statements.WhereStatement(colname, operation, value)