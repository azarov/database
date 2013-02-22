#!/usr/bin/python

import utils
import metadataprovider as mdp
import tablemetadata as tmd
import buffermanager as bm
import page
from array import array
import heapfile as hf
import struct
from indexoperationparams import IndexOperationParams

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
		data = array("i", [0]*_INTS_PER_PAGE)
		data[0] = _INIT_DEPTH
		p.data = data.tostring()

		p.set_dirty()
		p.unpin()

		self._init_buckets_numbers()
		# self._init_buckets(_INIT_DEPTH)
		self._init_buckets(1<<_INIT_DEPTH, _INIT_DEPTH)

		heap = hf.HeapFile()

		for record in heap.get_all_records(tablemetadata):
			key = self._make_key(record, tablemetadata)
			iop = IndexOperationParams(key, record.pageno, record.rid)
			self.insert_value(iop)


	def _get_global_depth(self):
		p = bm.BufferManager.find_page(page.PageId(self.index_directory_filename, 0))

		data = array("i")
		data.fromstring(p.data)
		depth = data[0]
		p.unpin()
		return depth

	def _make_key(self, record, tablemetadata):
		columns = [x.name for x in tablemetadata.attributes]
		keysno = [columns.index(x.name) for x in self.indexmetadata.keys]
		#format = tmd.make_format_string([tablemetadata.attributes[x] for x in keysno])
		#key = struct.pack(format, [record.values[x] for x in keysno])
		return [record.values[x] for x in keysno]

	def _init_buckets_numbers(self):
		p = bm.BufferManager.find_page(page.PageId(self.index_directory_filename, 0))

		data = array("i")
		data.fromstring(p.data)
		p.unpin()
		
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
					p.data = array("i", p.data).tostring()
					p.set_dirty()
					p.unpin()
			
				p = bm.BufferManager.find_page(page.PageId(self.index_directory_filename, p.id.pageno+1))
				p.data = [0]*_INTS_PER_PAGE
				records_to_go = _INTS_PER_PAGE-2
				offset = 1
			p.data[offset] = curr_hash

		if p.id.pageno != 0:
			p.data[0] = _INTS_PER_PAGE-1-records_to_go
			p.data = array("i", p.data).tostring()
			p.set_dirty()
			p.unpin()

	def _init_buckets(self, buckets_number, local_depth):
		for pageno in xrange(0, buckets_number):
			p = bm.BufferManager.find_page(page.PageId(self.index_filename, pageno))
			p.data = self._set_bucket_info(p.data, (local_depth, 0))
			p.set_dirty()
			p.unpin()

	def _get_bucket_info(self, data):
		if isinstance(data, array):
			return (data[-1], data[-2])
		else:
			d = array("i")
			d.fromstring(data)
			return (d[-1], d[-2])

	def _set_bucket_info(self, data, info):
		if isinstance(data, array):
			data[-1],data[-2] = info[0],info[1]
			return data.tostring()
		else:
			d = array("i")
			d.fromstring(data)
			d[-1],d[-2]=info[0],info[1]
			return d.tostring()

	def insert_value(self, iop):
		bucket_id = self.get_bucket_id(self._comute_hash(iop))

		p = bm.BufferManager.find_page(page.PageId(self.index_filename, bucket_id))

		record_size = struct.calcsize(self.record_format)
		if not self.bucket_has_free_slot(p, record_size):
			self._split_bucket(bucket_id, p)

		rec = struct.pack(self.record_format, *([iop.pageno, iop.recordno]+iop.key))
		info = self._get_bucket_info(p.data)
		occupied = info[1]
		rec_begin = occupied*record_size
		rec_end = rec_begin+record_size
		p.data = p.data[:rec_begin]+rec+p.data[rec_end:]

		p.data = self._set_bucket_info(p.data, (info[0], occupied+1))
		p.set_dirty()
		p.unpin()


	def _split_bucket(self, bucket_id, bucket_page):
		bucket_info = self._get_bucket_info(bucket_page.data)
		local_depth = bucket_info[0]
		global_depth = self._get_global_depth()

		if global_depth == local_depth:
			self._double_buckets_count()
			# raise Exception("")

		second_bucket_id = bucket_id | (1 << local_depth)

		local_depth += 1

		record_size = struct.calcsize(self.record_format)
		records_number = bucket_info[1]
		data = bucket_page.data[:records_number*record_size]
		records = [''.join(x) for x in zip(*[list(data[z::record_size]) for z in range(record_size)])]
		records = [struct.unpack(self.record_format, x) for x in records]

		bucket_page.data = "\x00"*page.PAGESIZE
		bucket_page.data = self._set_bucket_info(bucket_page.data, (local_depth, 0))
		bucket_page.set_dirty()
		bucket_page.unpin()

		second_bucket = bm.BufferManager.find_page(page.PageId(self.index_filename, second_bucket_id))
		second_bucket.data = "\x00"*page.PAGESIZE
		second_bucket.data = self._set_bucket_info(bucket_page.data, (local_depth, 0))
		second_bucket.set_dirty()
		second_bucket.unpin()

		ptr_page = second_bucket_id / (_INTS_PER_PAGE-1) + 1
		dir_page = bm.BufferManager.find_page(page.PageId(self.index_directory_filename, ptr_page))
		data = array("i")
		data.fromstring(dir_page.data)
		data[second_bucket_id % (_INTS_PER_PAGE-1) + 1] = second_bucket_id
		dir_page.data = data.tostring()
		dir_page.set_dirty()
		dir_page.unpin()

		for record in records:
			self.insert_value(IndexOperationParams(list(record[2:]), record[0], record[1]))


	def _double_buckets_count(self):
		p = bm.BufferManager.find_page(page.PageId(self.index_directory_filename, 0))
		data = array("i")
		data.fromstring(p.data)
		global_depth = data[0]
		data[0] += 1
		p.data = data.tostring()
		p.set_dirty()
		p.unpin()

		ptrs = 1 << global_depth
		records_to_go = 0
		offset = 0
		pageno = 0

		for curr_hash in xrange(0, ptrs):
			if records_to_go > 0:
				records_to_go -= 1
				offset += 1
			else:
				if pageno != 0:
					p.data = array("i", data).tostring()
					p.set_dirty()
					p.unpin()
			
				pageno += 1
				p = bm.BufferManager.find_page(page.PageId(self.index_directory_filename, pageno))

				data = array("i")
				data.fromstring(p.data)

				records_to_go = data[0]-1
				offset = 1


			dir_page_number = (ptrs+curr_hash)/(_INTS_PER_PAGE-1)+1
			dir_page_offset = (ptrs+curr_hash)%(_INTS_PER_PAGE-1)+1
			if dir_page_number == pageno:
				data[0] += 1
				data[dir_page_offset] = data[offset]
			else:
				self.add_ptr_to_index_dir(data[offset], ptrs+curr_hash)

		if pageno != 0:
			p.data = array("i", data).tostring()
			p.set_dirty()
			p.unpin()

	def add_ptr_to_index_dir(self, bucket_id, total):
		dir_page_number = total/(_INTS_PER_PAGE-1)+1
		dir_page_offset = total%(_INTS_PER_PAGE-1)+1
		p = bm.BufferManager.find_page(page.PageId(self.index_directory_filename, dir_page_number))
		data = array("i")
		data.fromstring(p.data)
		data[0] += 1
		data[dir_page_offset] = bucket_id
		p.data = data.tostring()
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
		data = array("i")
		data.fromstring(p.data)
		global_depth = data[0]

		ptr_number = hash & ((1 << global_depth)-1)
		p.unpin()

		ptr_page = ptr_number / (_INTS_PER_PAGE - 1) + 1
		p = bm.BufferManager.find_page(page.PageId(self.index_directory_filename, ptr_page))
		data = array("i")
		data.fromstring(p.data)
		bucket_id = data[ptr_number % (_INTS_PER_PAGE-1) + 1]
		p.unpin()

		return bucket_id


	def bucket_has_free_slot(self, p, recordsize):
		info = self._get_bucket_info(p.data)
		occupied = info[1]
		return occupied < (page.PAGESIZE-len(info)*2)/recordsize

	def find_value(self, iop):
		bucket_id = self.get_bucket_id(self._comute_hash(iop))

		p = bm.BufferManager.find_page(page.PageId(self.index_filename, bucket_id))
		info = self._get_bucket_info(p.data)
		occupied = info[1]
		if occupied == 0:
			p.unpin()
			return None

		record_size = struct.calcsize(self.record_format)
		data = p.data[:occupied*record_size]
		records = [''.join(x) for x in zip(*[list(data[z::record_size]) for z in range(record_size)])]
		records = [struct.unpack(self.record_format, x) for x in records]

		rid = 0
		for rec in records:
			if list(rec[2:]) == iop.key:
				iop.pageno = rec[0]
				iop.recordno = rec[1]
				break
			rid += 1

		p.unpin()
		return None if rid == occupied else rid

	def delete_value(self, key):
		iop = IndexOperationParams(key, 0, 0)
		rid = self.find_value(iop)
		if rid == None:
			raise Exception("Can't delete record with key {0}. Couldn't find it.".format(key))

		bucket_id = self.get_bucket_id(self._comute_hash(iop))
		p = bm.BufferManager.find_page(page.PageId(self.index_filename, bucket_id))

		record_size = struct.calcsize(self.record_format)
		info = self._get_bucket_info(p.data)
		occupied = info[1]
		record_begin = rid*record_size
		record_end = record_begin + record_size
		data = bytearray(p.data)
		data[record_begin:record_end] = data[(occupied-1)*record_size:occupied*record_size]
		if len(data) != page.PAGESIZE:
			raise Exception("Deletion internal error. Key {0}. Size of page {1}".format(key, len(data)))

		p.data = self._set_bucket_info(str(data), (info[0], occupied+1))
		p.set_dirty()
		p.unpin()


