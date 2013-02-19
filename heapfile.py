#!/usr/bin/python

import PagesDirectory as pd
import struct
import tablemetadata as tmd
import buffermanager as bm
import page

class HeapFile(object):
	def __init__(self):
		pass

	def insert(self, tablemetadata, values):
		'''values - is a dictionary with keys - name of columns, values - values of corresponding columns '''
		records_per_page = tablemetadata.records_per_page
		directory = pd.PagesDirectory(tablemetadata.name)
		p = directory.get_page_for_insert()
		data = bytearray(p.data)

		records_table = data[-records_per_page:]
		try:
			recordno = records_per_page.index(0)
		except ValueError, e:
			#this shouldn't happen
			#should be logged
			raise e

		record = self._make_record(tablemetadata, values)
		record_begin = tablemetadata.record_size*recordno
		record_end = record_begin+tablemetadata.record_size

		records_table[recordno] = 1
		p.data = data[:record_begin]+record+data[record_end:-records_per_page]+str(records_table)

		if not 0 in records_table:
			directory.page_get_filled(p.id.pageno)

		p.set_dirty()
		p.unpin()

	def _make_record(self, tablemetadata, values):
		attributes = []
		for attr in tablemetadata.attributes:
			if attr.name in values:
				val = self._parse_value(attr, values[attr.name])
				attributes.append(val)
				del values[attr.name]
			else:
				attributes.append(attr.default_value)

		if len(values) != 0:
			raise Exception("Can't make record. Unknown columns: "+values.keys)

		return struct.pack(tablemetadata.format, *attributes)


	def _parse_value(self, attr, val):
		if attr.typename == tmd.Types.INT:
			return int(val)
		elif attr.typename == tmd.Types.DOUBLE:
			return float(val)
		elif attr.typename == tmd.Types.VARCHAR:
			if len(val) > tmd.VARCHAR_MAX_SIZE:
				raise Exception("String is too long. Size: "+len(val))
		else:
			raise UnkownTypeException(attr.typename)

	def delete(self, tablemetadata, pageno, recordno):
		records_per_page = tablemetadata.records_per_page
		directory = pd.PagesDirectory(tablemetadata.name)
		p = bm.BufferManager.find_page(page.PageId(tablemetadata.name, pageno))
		data = bytearray(p.data)

		records_table = data[-records_per_page:]
		records_table[recordno] = 0

		p.data = data[:-records_per_page]+str(records_table)
		directory.page_freed(pageno)

		p.set_dirty()
		p.unpin()

	def get_record(self, tablemetadata, pageno, recordno):
		p = bm.BufferManager.find_page(page.PageId(tablemetadata.name, pageno))
		record_begin = tablemetadata.record_size*recordno
		record_end = record_begin+tablemetadata.record_size

		record = p.data[record_begin:record_end]
		p.unpin()
		return record

	def update(self, tablemetadata, values):
		pass

class UnkownTypeException(Exception):
	def __init__(self, msg):
		super(UnkownTypeException, self).__init__()
		self.msg = msg
