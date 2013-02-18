#!/usr/bin/python

import os

class __Config(object):
	def __init__(self):
		#self.path_to_db = os.curdir
		self.max_pages_number = 1024

	def update(self, path_to_db, max_pages_number):
		#self.path_to_db = path_to_db
		os.chdir(path_to_db)
		self.max_pages_number = max_pages_number

Config = __Config()
