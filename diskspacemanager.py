#!/usr/bin/python

import page
import os

class _DiskSpaceManager(object):
	def __init__(self):
		self.currentfilename = ""
		self.file = None

	def get_page(self, pageid):

		self.file = open(pageid.filename, "rb+")

		if os.stat(pageid.filename).st_size < page.PAGESIZE*(pageid.pageno+1):
			self.file.seek(page.PAGESIZE*(pageid.pageno+1)-1)
			self.file.write("\0")

		self.file.seek(page.PAGESIZE*pageid.pageno)
		p = self.file.read(page.PAGESIZE)

		self.file.close()
		return page.Page(pageid, p)

	def write_page(self, p):
		if not p.is_dirty():
			return

		pageid = p.id
		self.file = open(pageid.filename, "rb+")
		self.file.seek(page.PAGESIZE*p.id.pageno)
		self.file.write(self._fit_data_to_page(p.data))
		self.file.close()
		p.reset_dirty()

	def _fit_data_to_page(self, data):
		if len(data) > page.PAGESIZE:
			return data[:page.PAGESIZE]
		else:
			return data+"\x00"*(page.PAGESIZE-len(data))

	def get_pages_number(self, filename):
		p = self.get_page(page.PageId(filename, 0))
		p.unpin()
		return os.stat(filename).st_size/page.PAGESIZE

	def __update(self, filename, mode):
		if self.currentfilename != filename:
			if self.file != None and not self.file.closed:
				self.file.close()
			self.file = open(filename, mode)
			self.currentfilename = filename

DiskSpaceManager = _DiskSpaceManager()