#!/usr/bin/python

import re
import PagesDirectory as pd

class Record(object):
	def __init__(self):
		pass

class HeaderPage(object):
	def __init__(self):
		self.fullPages = 0
		self.freePages = 0

class HeapFile(object):
	def __init__(self):
		pass

	def insert(self, tablemetadata, values):
		'''values - is a dictionary with keys - name of columns, values - values of corresponding columns '''
		records_per_page = tablemetadata.records_per_page
		directory = pd.PagesDirectory(tablemetadata.name)
		p = directory.get_page_for_insert()



		#directory.page_get_filled(p.id.pageno) if it get filled
		p.set_dirty()
		p.unpin()


	def delete(self, rid):
		pass

	def get_record(self, rid):
		pass