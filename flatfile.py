#!/usr/bin/python

import os

class Record(object):
	def __init__(self, rid, fields):
		self.rid = rid
		self.fields = fields

	def __repr__(self):
		return self.rid+";"+reduce(lambda x,y: x+";"+y, self.fields)

class FlatFile(object):
	def __init__(self):
		pass

	def open(self, filename):
		self.f = open(filename, "a+")

	def close(self):
		if self.f.closed == False:
			self.f.close()

	def insert(self, record):
		self.f.write(str(record.rid)+";"+ reduce(lambda x,y: x+";"+y, record.fields)+os.linesep)

	def delete(self, rid):
		pass

	def get_record(self, rid):
		pass

	def get_all_records(self):
		return [Record(x.split(";")[0].strip(), x.split(";")[1:]) for x in self.f.readlines()[1:]]

def create_flatfile(filename, fieldnames):
	f = open(filename, "w")
	f.write(reduce(lambda x,y: x+";"+y, fieldnames)+os.linesep)
	f.close()