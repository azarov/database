#!/usr/bin/python

class IndexOperationParams(object):
	def __init__(self, key, pageno, recordno):
		self.key = key
		self.pageno = pageno
		self.recordno = recordno
		