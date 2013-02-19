#!/usr/bin/python

import buffermanager as bm
import diskspacemanager as dsm
import page
import math

class PagesDirectory(object):
	def __init__(self, filename):
		self.filename = filename
		self.directoryfilename = filename+"_directory"

	def get_page_for_insert(self):
		pages_number = dsm.DiskSpaceManager.get_pages_number(self.filename)
		dir_pages_number = dsm.DiskSpaceManager.get_pages_number(self.directoryfilename)
		pageno = 0

		for dir_pageno in xrange(0, dir_pages_number):
			p = bm.BufferManager.find_page(page.PageId(self.directoryfilename, dir_pageno))
			data = bytearray(p.data)
			for x in data:
				if x == 0 and pageno < pages_number:
					p.unpin()
					return bm.BufferManager.find_page(page.PageId(self.filename, pageno))
				pageno += 1
			p.unpin()

		self.page_freed(pageno)
		return bm.BufferManager.find_page(page.PageId(self.filename, pageno))


	def page_get_filled(self, pageno):
		self._set_bit(pageno, 1)

	def page_freed(self, pageno):
		self._set_bit(pageno, 0)

	def _set_bit(self, pageno, bit):
		dir_pageno = self._get_dir_pagenumber(pageno)

		p = bm.BufferManager.find_page(page.PageId(self.directoryfilename, dir_pageno))
		p.data = bytearray(p.data)
		p.data[pageno % page.PAGESIZE] = bit
		p.set_dirty()
		p.unpin()

	def _get_dir_pagenumber(self, pageno):
		return int(math.ceil(pageno/page.PAGESIZE))

def create_pages_directory(filename):
	directoryfilename = filename+"_directory"
	first_page = bm.BufferManager.find_page(page.PageId(directoryfilename, 0))
	first_page.data = bytearray([0]*page.PAGESIZE)
	first_page.set_dirty()
	first_page.unpin()
	
def get_page_directory_name(tablename):
	return tablename+"_directory"