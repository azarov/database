#!/usr/bin/python

import PagesDirectory as pd
import struct
import tablemetadata as tmd
import diskspacemanager as dsm
import buffermanager as bm
import page

class Record(object):
	def __init__(self, pageno, rid, values):
		self.pageno = pageno
		self.rid = rid
		self.values = values

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
			recordno = records_table.index(b'\x00')
		except ValueError, e:
			#this shouldn't happen
			#should be logged
			p.unpin()
			raise e

		#TODO: should be in try-catch block. Page should be unpinned if exception was occured
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
			if val.startswith("\"") and val.endswith("\""):
				val = val[1:-1]
				if len(val) > tmd.VARCHAR_MAX_SIZE or len(val) > attr.size:
					raise Exception("String is too long. Size: "+len(val))
				return val
			else:
				raise Exception("Attribute {0} should have string type.".format(attr.name))
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

	def update(self, tablemetadata, pageno, recordno, values):
		records_per_page = tablemetadata.records_per_page
		p = bm.BufferManager.find_page(page.Page(tablemetadata.name, pageno))

		records_table = p.data[-records_per_page:]
		if records_table[recordno] == 0:
			raise Exception("Can't update record with id {0}. Record doesn't exist. You should insert it before".format(recordno))

		record_begin = tablemetadata.record_size*recordno
		record_end = record_begin+tablemetadata.record_size

		record = str(p.data[record_begin:record_end])

		old_values = struct.unpack(tablemetadata.format, record)
		record = self._get_updated_record(tablemetadata, values, old_values)

		p.data = p.data[:record_begin]+record+p.data[record_end:-records_per_page]+str(records_table)

		p.set_dirty()
		p.unpin()

	def _get_updated_record(self, tablemetadata, values, old_values):
		attributes = []
		if len(old_values) != len(tablemetadata.attributes):
			raise Exception("Can't update record. Record format corrupted.")

		for attr in zip(tablemetadata.attributes, old_values):
			if attr[0].name in values:
				val = self._parse_value(attr[0], values[attr[0].name])
				attributes.append(val)
				del values[attr[0].name]
			else:
				attributes.append(attr[1])

		if len(values) != 0:
			raise Exception("Can't make record. Unknown columns: "+values.keys)

		return struct.pack(tablemetadata.format, *attributes)

	def get_all_records(self, tablemetadata, wherestatement = None):
		pages_number = dsm.DiskSpaceManager.get_pages_number(tablemetadata.name)
		records_per_page = tablemetadata.records_per_page

		for pageno in xrange(0,pages_number):
			p = bm.BufferManager.find_page(page.PageId(tablemetadata.name, pageno))
			records_table = bytearray(p.data[-records_per_page:])
			recordno = 0
			for i in records_table:
				if i == 1:
					record_begin = tablemetadata.record_size*recordno
					record_end = record_begin+tablemetadata.record_size
					record = struct.unpack(tablemetadata.format, p.data[record_begin:record_end])
					yield Record(pageno, recordno, record)
					
				recordno += 1
			p.unpin()

		return


class UnkownTypeException(Exception):
	def __init__(self, msg):
		super(UnkownTypeException, self).__init__()
		self.msg = msg
