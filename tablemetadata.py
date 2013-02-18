#!/usr/bin/python

class Attribute(object):
	def __init__(self, name, type, size):
		pass
		

class TableMetaData(object):
	def __init__(self, name, record_size, records_per_page):
		self.name = name
		self.record_size = record_size
		self.records_per_page = records_per_page
