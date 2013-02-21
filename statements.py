#!/usr/bin/python

class CreateStatement(object):
	def __init__(self, tablename, attributes):
		self.tablename = tablename
		self.attributes = attributes

	def __repr__(self):
		return "CreateStatement: [tablename: {0}, attributes: {1}]".format(self.tablename, self.attributes)

class InsertStatement(object):
	def __init__(self, tablename, values):
		self.tablename = tablename
		self.values = values

	def __repr__(self):
		return "InsertStatement: [tablename: {0}, values: {1}]".format(self.tablename, self.values)

class SelectStatement(object):
	def __init__(self, tablename, whereStmt=None):
		self.tablename = tablename
		self.whereStmt = whereStmt

	def __repr__(self):
		if self.whereStmt != None:
			return "SelectStatement: [tablename: {0}; {1}]".format(self.tablename, self.whereStmt)
		else:
			return "SelectStatement: [tablename: {0}]".format(self.tablename)

class DropStatement(object):
	def __init__(self, tablename):
		self.tablename = tablename

	def __repr__(self):
		return "DropStatement: [tablename: {0}]".format(self.tablename)

class WhereStatement(object):
	def __init__(self, colname, operation, value):
		self.colname = colname
		self.operation = operation
		self.value = value

	def __repr__(self):
		return "WhereStatement: [colname: {0}, operation: {1}, value: {2}]".format(self.colname, self.operation, self.value)
	
	def checkCondition(val):
		if self.operation == WhereOps.EQ:
			return self.value == val
		elif self.operation == WhereOps.NEQ:
			return self.value != val
		elif self.operation == WhereOps.LT:
			return self.value < val
		elif self.operation == WhereOps.GT:
			return self.value > val
		elif self.operation == WhereOps.LEQ:
			return self.value <= val
		elif self.operation == WhereOps.GEQ:
			return self.value >= val
		else:
			# TODO: error checking
			return False

def enum(**enums):
	return type('Enum', (), enums)

WhereOps = enum(EQ="=", NEQ="!=", LT="<", GT=">", LEQ="<=", GEQ=">=")
