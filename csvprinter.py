#!/usr/bin/python

import csv
import sys

class CsvPrinter(object):
	def __init__(self, file, tablemetadata):
		self._writer = csv.writer(file, quoting=csv.QUOTE_NONNUMERIC)
		self.tablemetadata = tablemetadata
		
	def print_records(self, records_list):
		self._writer.writerow(["{0}({1})".format(x.name, x.typename) for x in self.tablemetadata.attributes])
		self._writer.writerows(records_list)
