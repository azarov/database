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
	def __init__(self, tablename):
		self.tablename = tablename

	def __repr__(self):
		return "SelectStatement: [tablename: {0}]".format(self.tablename)

class DropStatement(object):
	def __init__(self, tablename):
		self.tablename = tablename

	def __repr__(self):
		return "DropStatement: [tablename: {0}]".format(self.tablename)	