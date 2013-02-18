#!/usr/bin/python
import re
import statements

class UnknownStatement(Exception):
	def __init__(self, msg):
		self.msg = msg

	def __str__(self):
		return repr(self.msg)

class ParseException(Exception):
	def __init__(self, msg):
		self.msg = def

	msg __str__(self):
		return repr(self.msg)

def parse_statement(str):
	print str
	statement = str.split(" ")[0].lower();
	if statement == "create":
		return parse_create(str)
	elif statement == "insert":
		return parse_insert(str)
	elif statement == "select":
		return parse_select(str)
	else:
		raise UnknownStatement(statement)

def parse_create(str):
	m = re.match(r"create table (\w+)\s*\((.*)\)", str, re.IGNORECASE)
	fields = [x.strip().split(" ") for x in m.group(2).split(",")];
	return statements.CreateStatement(m.group(1), fields)

def parse_insert(str):
	m = re.match(r"insert into (\w+) values\s*\((.*)\)", str, re.IGNORECASE)
	values = [x.strip() for x in m.group(2).split(",")];
	return statements.InsertStatement(m.group(1), values)

def parse_select(str):
	m = re.match(r"select (.+) from (\w+)", str, re.IGNORECASE)
	if m == None:
		raise ParseException("Can't parse: "+str)
	return statements.SelectStatement(m.group(2))
