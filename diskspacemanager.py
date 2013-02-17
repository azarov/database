#!/usr/bin/python

import page
import os

class _DiskSpaceManager(object):
	def __init__(self):
		self.currentfilename = ""
		self.file = None

	def get_page(self, pageid):
		
		if self.currentfilename != pageid.filename:
			if self.file != None and not self.file.closed:
				self.file.close()
			self.file = open(pageid.filename, "rb+")
			self.currentfilename = pageid.filename

		if os.fstat(self.file.fileno()).st_size < page.PAGESIZE*(pageid.pageno+1):
			return None


		self.file.seek(page.PAGESIZE*pageid.pageno)
		p = self.file.read(page.PAGESIZE)

		return page.Page(pageid, p)

	def write_page(self, p):

		if not p.is_dirty():
			return

		pageid = p.id
		if self.currentfilename != pageid.filename:
			if self.file != None and not self.file.closed:
				self.file.close()
			self.file = open(pageid.filename, "wb+")
			self.currentfilename = pageid.filename

		self.file.seek(page.PAGESIZE*p.id.pageno)
		self.file.write(p.data)
		p.reset_dirty()

DiskSpaceManager = _DiskSpaceManager()