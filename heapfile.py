#!/usr/bin/python

import re

class Record(object):
	def __init__(self):
		pass

class HeaderPage(object):
	def __init__(self):
		self.fullPages = 0
		self.freePages = 0

class HeapFile(object):
	def __init__(self, filename):
		self.f = open(filename, "w")
		self.hp = self.read_header_page()

	def read_header_page(self):
		page = self.f.read(page_size)
		self.hp = HeaderPage()
		m = re.match(r"(\d+);(\d+)", page)
		if m != None:
			self.hp.fullPages = m.group(1)
			self.hp.freePages = m.group(2)


	def insert(self, tablename, record):
		pass

	def delete(self, rid):
		pass

	def get_record(self, rid):
		pass