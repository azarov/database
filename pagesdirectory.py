#!/usr/bin/python

import buffermanager as bm
import page

class PagesDirectory(object):
	def __init__(self, filename):
		self.filename = filename
		self.diretoryfilename = filename+"directory"
		firstpage = bm.BufferManager.find_page(page.PageId(filename, 0))
		self.data = firstpage.data
		firstpage.unpin()

