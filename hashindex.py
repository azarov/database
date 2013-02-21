#!/usr/bin/python

import utils
import metadataprovider as mdp
import tablemetadata as tmd
import buffermanager as bm
import page
from array import array
import heapfile as hf
import struct

_INIT_DEPTH = 3
_INTS_PER_PAGE = page.PAGESIZE/4


def get_format_for_ints_array():
	return str(_INTS_PER_PAGE)+"i"

def get_array_with_first_element(first_element):
	arr = [0]*_INTS_PER_PAGE
	arr[0] = first_element
	return arr

class HashIndexManager(object):
	def __init__(self, indexmetadata):
		self.indexmetadata = indexmetadata
		self.index_filename = indexmetadata.tablename+"_hash_"+indexmetadata.indexname
		self.index_directory_filename = self.index_filename+"_directory"

		tablemetadata = mdp.MetaDataProvider.get_metadata(self.indexmetadata.tablename)
		columns = [x.name for x in tablemetadata.attributes]
		keysno = [columns.index(x.name) for x in self.indexmetadata.keys]
		self.key_format = tmd.make_format_string([tablemetadata.attributes[x] for x in keysno])
		self.record_format = "ii"+self.key_format
		

	def create_index(self):
		tablemetadata = mdp.MetaDataProvider.get_metadata(self.indexmetadata.tablename)
		utils.create_empty_file(self.index_filename)
		utils.create_empty_file(self.index_directory_filename)

		p = bm.BufferManager.find_page(page.PageId(self.index_directory_filename, 0))
		data = array("I", [0]*_INTS_PER_PAGE)
		data[0] = _INIT_DEPTH
		p.data = data.tostring()

		p.set_dirty()
		p.unpin()

		self._init_buckets_numbers()
		self._init_buckets(_INIT_DEPTH)

		heap = hf.HeapFile()

		for record in heap.get_all_records(tablemetadata):
			key = self._make_key(record, tablemetadata)
			iop = IndexOperationParams(key, record.pageno, record.rid)
			self._insert_value(iop)


	def _make_key(self, record, tablemetadata):
		columns = [x.name for x in tablemetadata.attributes]
		keysno = [columns.index(x.name) for x in self.indexmetadata.keys]
		#format = tmd.make_format_string([tablemetadata.attributes[x] for x in keysno])
		#key = struct.pack(format, [record.values[x] for x in keysno])
		return [record.values[x] for x in keysno]

	def _init_buckets_numbers(self):
		p = bm.BufferManager.find_page(page.PageId(self.index_directory_filename, 0))

		data = array("I")
		data.fromstring(p.data)
		records_number = 1 << int(data[0])

		records_to_go = 0
		offset = 0	

		for curr_hash in xrange(0, records_number):
			if records_to_go > 0:
				records_to_go -= 1
				offset += 1
			else:
				if p.id.pageno != 0:
					p.data[0] = _INTS_PER_PAGE-1
					p.data = array("I", p.data).tostring()
					p.set_dirty()
					p.unpin()
			
				p = bm.BufferManager.find_page(page.PageId(self.index_directory_filename, p.id.pageno+1))
				p.data = [0]*_INTS_PER_PAGE
				records_to_go = _INTS_PER_PAGE-2
				offset = 1
			p.data[offset] = curr_hash

		if p.id.pageno != 0:
			p.data[0] = _INTS_PER_PAGE-1-records_to_go
			p.data = array("I", p.data).tostring()
			p.set_dirty()
			p.unpin()

	def _init_buckets(self, depth):
		for pageno in xrange(0,1 << depth):
			p = bm.BufferManager.find_page(page.PageId(self.index_filename, pageno))
			p.data = self._set_bucket_info(p.data, (depth, 0))
			p.set_dirty()
			p.unpin()

	def _get_bucket_info(self, data):
		if isinstance(data, array):
			return (data[-1], data[-2])
		else:
			d = array("I")
			d.fromstring(data)
			return (d[-1], d[-2])

	def _set_bucket_info(self, data, info):
		if isinstance(data, array):
			data[-1],data[-2] = info[0],info[1]
			return data.tostring()
		else:
			d = array("I")
			d.fromstring(data)
			d[-1],d[-2]=info[0],info[1]
			return d.tostring()

	def _insert_value(self, iop):
		bucket_id = self.get_bucket_id(self._comute_hash(iop))

		p = bm.BufferManager.find_page(page.PageId(self.index_filename, bucket_id))

		record_size = struct.calcsize(self.record_format)
		if not self.bucket_has_free_slot(p, record_size):
			raise Exception("Not imlemented yet")

		rec = struct.pack(self.record_format, *([iop.pageno, iop.recordno]+iop.key))
		info = self._get_bucket_info(p.data)
		occupied = info[1]
		rec_begin = occupied*record_size
		rec_end = rec_begin+record_size
		p.data = p.data[:rec_begin]+rec+p.data[rec_end:]

		p.data = self._set_bucket_info(p.data, (info[0], occupied+1))
		p.set_dirty()
		p.unpin()


	def compute_key_size(self, indexmetadata):
			tablemetadata = mdp.MetaDataProvider.get_metadata(indexmetadata.tablename)
			keys = [x.name for x in indexmetadata.keys]
			return sum([y.size for y in [x for x in tablemetadata.attributes if x.name in keys]])

	def _comute_hash(self, iop):
		return hash(tuple(iop.key))

	def get_bucket_id(self, hash):
		p = bm.BufferManager.find_page(page.PageId(self.index_directory_filename, 0))
		data = array("I")
		data.fromstring(p.data)
		global_depth = data[0]

		ptr_number = hash & ((1 << global_depth)-1)
		p.unpin()

		ptr_page = ptr_number / _INTS_PER_PAGE + 1
		p = bm.BufferManager.find_page(page.PageId(self.index_directory_filename, ptr_page))
		data = array("I")
		data.fromstring(p.data)
		bucket_id = data[ptr_number % _INTS_PER_PAGE]
		p.unpin()

		return bucket_id


	def bucket_has_free_slot(self, p, recordsize):
		info = self._get_bucket_info(p.data)
		occupied = info[1]
		return occupied < (page.PAGESIZE-len(info)*2)/recordsize


class IndexOperationParams(object):
	def __init__(self, key, pageno, recordno):
		self.key = key
		self.pageno = pageno
		self.recordno = recordno
		