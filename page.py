#!/usr/bin/python

from datetime import datetime

PAGESIZE = 4096

class PageId(object):
	def __init__(self, filename, pageno):
		self.filename = filename
		self.pageno = pageno

	def __hash__(self):
		return hash((self.filename, self.pageno))

	def __eq__(self, other):
		return (self.filename, self.pageno) == (other.filename, other.pageno)

class Page(object):
	def __init__(self, id, data, pin_count = 0, dirty = False):
		self.id = id
		self.data = data
		self.pin_count = pin_count
		self.dirty = dirty
		self.time = str(datetime.now())

	def set_dirty(self):
		self.dirty = True

	def reset_dirty(self):
		self.dirty = False

	def is_dirty(self):
		return self.dirty

	def pin(self):
		self.pin_count += 1

	def unpin(self):
		self.pin_count = max(self.pin_count-1, 0)

	def is_unpinned(self):
		return self.pin_count == 0

	def __repr__(self):
		print str(self.data).encode("hex")
