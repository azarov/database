#!/usr/bin/python

from struct import *
import page

def make_format_string(attributes):
	format = []
	for attr in attributes:
		if attr.typename == Types.INT:
			format.append("i")
		elif attr.typename == Types.DOUBLE:
			format.append("q")
		elif attr.typename == Types.VARCHAR:
			format.append(str(VARCHAR_MAX_SIZE)+"s")
		else:
			raise Exception("Unknown type: "+attr.typename)
	return "".join(format)

class Attribute(object):
	def __init__(self, name, typename, size, default_value):
		self.name = name
		self.typename = typename
		self.size = size
		self.default_value = default_value

class TableMetaData(object):
	def __init__(self, name, attributes, indices = []):
		self.name = name
		self.attributes = attributes
		self.format = _make_format_string(self.attributes)
		self.records_per_page = int(page.PAGESIZE/(calcsize(self.format)+1))
		self.record_size = calcsize(self.format)
		self.indices = indices
	
class IndexMetaData(object):
	def __init__(self, tablename, indexname, keys, is_unique = False, is_btree = False):
		""" columns - list of IndexColumn """
		self.tablename = tablename
		self.indexname = indexname		
		self.keys = keys
		self.is_unique = is_unique
		self.is_btree = is_btree

	def __repr__(self):
		return "CreateIndexStatement: [tablename: {0}]".format(self.tablename)		

class KeyInfo(object):
	def __init__(self, name, ascending):
		self.name = name
		self.ascending = ascending

def enum(**enums):
	return type('Enum', (), enums)

Types = enum(INT="int", DOUBLE="double", VARCHAR="varchar")
IndexTypes = enum(HASH="hash", BTREE="btree")
VARCHAR_MAX_SIZE = 128