#!/usr/bin/python

import metadataprovider as mdp
import tablemetadata as tmd
import utils
import buffermanager as bm
import page
from array import array
import struct
import heapfile as hf
from indexoperationparams import IndexOperationParams

class BTreeIndexManager(object):
	def __init__(self, indexmetadata, comparer):
		self.indexmetadata = indexmetadata
		self.index_filename = indexmetadata.tablename+"_btree_"+indexmetadata.indexname

		tablemetadata = mdp.MetaDataProvider.get_metadata(self.indexmetadata.tablename)
		columns = [x.name for x in tablemetadata.attributes]
		keysno = [columns.index(x.name) for x in self.indexmetadata.keys]
		self.key_format = tmd.make_format_string([tablemetadata.attributes[x] for x in keysno])
		self.record_format = "ii"+self.key_format
		self.node_key_format = "i"+self.key_format
		self.LEAF_TYPE = 0xEFBE
		self.NODE_TYPE = 0xDEDE
		self.comparer = comparer

		
	def create_index(self):
		tablemetadata = mdp.MetaDataProvider.get_metadata(self.indexmetadata.tablename)
		utils.create_empty_file(self.index_filename)

		self._init_meta_data_page()
		self._init_root_page()

		heap = hf.HeapFile()

		for record in heap.get_all_records(tablemetadata):
			key = self._make_key(record, tablemetadata)
			iop = IndexOperationParams(key, record.pageno, record.rid)
			self.insert_value(iop)

	def _init_meta_data_page(self):
		p = bm.BufferManager.find_page(page.PageId(self.index_filename, 0))

		data = array("i")
		data.fromstring(p.data)
		data[0] = 1
		data[1] = struct.calcsize(self.record_format)
		data[2] = 1
		p.data = data.tostring()
		p.set_dirty()
		p.unpin()

	def _init_root_page(self):
		p = bm.BufferManager.find_page(page.PageId(self.index_filename, 1))

		data = array("i")
		data.fromstring(p.data)
		p.data = self._set_node_info(p.data, (self.LEAF_TYPE, 0, 0))
		p.set_dirty()
		p.unpin()

	def _make_key(self, record, tablemetadata):
		columns = [x.name for x in tablemetadata.attributes]
		keysno = [columns.index(x.name) for x in self.indexmetadata.keys]
		return [record.values[x] for x in keysno]

	def _get_node_info(self, data):
		if isinstance(data, array):
			return (data[-1], data[-2], data[-3])
		else:
			d = array("i")
			d.fromstring(data)
			return (d[-1], d[-2], d[-3])


	def _set_node_info(self, data, info):
		if isinstance(data, array):
			data[-1],data[-2],data[-3] = info[0],info[1],info[2]
			return data.tostring()
		else:
			d = array("i")
			d.fromstring(data)
			d[-1],d[-2],d[-3]=info[0],info[1],info[2]
			return d.tostring()

	def insert_value(self, iop):
		new_child = self._tree_insert(self._get_root_node(), iop)
		if new_child != None:
			if self._get_root_node() != 1:
				raise Exception("Can't insert into btree. Internal problem.")


	def _tree_search(self, node_id, iop):
		p = bm.BufferManager.find_page(page.PageId(self.index_filename, node_id))
		data = array("i")
		data.fromstring(p.data)
		if data[0] == self.LEAF_TYPE:
			p.unpin()
			return node_id

		node_to_examine = self._find_key_in_node(p.data, iop)
		self._tree_search(node_to_examine, iop)

	def _tree_insert(self, node_id, iop):
		p = bm.BufferManager.find_page(page.PageId(self.index_filename, node_id))

		child_entry = None
		info = self._get_node_info(p.data)
		items_number = info[1]
		if info[0] == self.LEAF_TYPE:
			entries_per_page = (page.PAGESIZE - 12)/struct.calcsize(self.record_format)

			ins_index = self._find_offset_in_leaf(p.data, iop)
			
			if items_number < entries_per_page:
				self._insert_into_leaf(p, info, ins_index, iop)
			else:
				key_to_push = self._split_leaf(SplitNodeContext(p, None, ins_index, struct.calcsize(self.record_format), entries_per_page), iop)
				key_to_push[0] = node_id
				child_entry = struct.pack(self.node_key_format+"i", *key_to_push)

			p.set_dirty()
			p.unpin()
			 
		else:
			node_id = self._find_key_in_node(p.data, iop)
			p.set_dirty()
			p.unpin()

			child_entry = self._tree_insert(node_id, iop)
			if child_entry == None:
				return None


			p = bm.BufferManager.find_page(page.PageId(self.index_filename, node_id))

			entry_size = struct.calcsize(self.node_key_format)
			entries_per_page = (page.PAGESIZE - 16)/entry_size

			searchparams = IndexOperationParams(child_entry)
			raise NotImplementedError()

			p.set_dirty()
			p.unpin()

			raise NotImplementedError()
		
		return child_entry
		

	def _find_key_in_node(self, node, iop):
		info = self._get_node_info(node)
		records_number = info[1]
		record_size = struct.calcsize(self.node_key_format)

		for begin in xrange(0, (records_number-1)*record_size, record_size):
			record = node[begin:begin+record_size]
			record = struct.unpack(self.node_key_format, record)
			if self.comparer.compare(list(record[2:]), iop.key) >= 0:
				return record[0]

		last_ptr = record_size*records_number
		return struct.unpack("i", node[last_ptr:last_ptr+4])

	def _find_offset_in_leaf(self, node, iop):
		info = self._get_node_info(node)
		records_number = info[1]
		record_size = struct.calcsize(self.record_format)

		for begin in xrange(0, (records_number-1)*record_size, record_size):
			record = node[begin:begin+record_size]
			record = struct.unpack(self.record_format, record)
			if self.comparer.compare(list(record[2:]), iop.key) >= 0:
				return begin

		return records_number*record_size

	def _insert_into_leaf(self, leaf, info, ins_index, iop):
		leaf.data = self._set_node_info(leaf.data, (info[0], info[1]+1, info[2]))
		record = struct.pack(self.record_format, *([iop.pageno, iop.recordno]+list(iop.key)))
		record_size = struct.calcsize(self.record_format)
		end = info[1]*record_size
		data = bytearray(leaf.data)
		data[ins_index+record_size:end+record_size] = data[ins_index:end]
		data[ins_index:ins_index+record_size] = record
		leaf.data = str(data)
		if len(leaf.data) != page.PAGESIZE:
			raise Exception("Insertion in btree index internal error. Page size {0}".format(len(leaf.data)))

	def _insert_into_node(self, node, ins_index, iop, child_entry):
		info = self._get_node_info(node.data)
		self._set_node_info(node.data, (info[0], info[1]+1, info[2]))

		record_size = struct.calcsize(self.node_key_format)
		record = struct.pack(self.record_format, *([child_entry]+list(iop.key)))
		data = bytearray(node.data)
		end = info[1]*record_size
		data[ins_index+record_size:end+record_size] = data[ins_index:end]
		data[ins_index:ins_index+record_size] = record


	def _split_leaf(self, ctx, iop):
		node_is_leaf = (ctx.child_entry == None)
		info = self._get_node_info(ctx.p.data)
		items_to_move = info[1] - ctx.entries_per_page/2
		new_node_id = self._get_new_page_id()
		new_node = bm.BufferManager.find_page(page.PageId(self.index_filename, new_node_id))

		if node_is_leaf:
			new_node.data = self._set_node_info(new_node.data, (self.LEAF_TYPE, items_to_move, info[2]))
			ctx.p.data = self._set_node_info(ctx.p.data, (self.LEAF_TYPE, info[1]-items_to_move, new_node_id))
		else:
			new_node.data = self._set_node_info(new_node.data, (self.NODE_TYPE, items_to_move, 0))
			ctx.p.data = self._set_node_info(ctx.p.data, (self.NODE_TYPE, info[1]-items_to_move, new_node_id))

		info = self._get_node_info(ctx.p.data)
		begin = info[1]*ctx.entry_size
		size = items_to_move*ctx.entry_size+(0 if node_is_leaf else 4)
		data = bytearray(new_node.data)
		data[:size] = ctx.p.data[begin:begin+size]
		new_node.data = str(data)

		insertion_node = ctx.p
		if info[1] <= ctx.ins_index:
			insertion_node = new_node
			ctx.ins_index -= info[1]

		info = self._get_node_info(insertion_node.data)
		if node_is_leaf:
			self._insert_into_leaf(insertion_node, info, ctx.ins_index, iop)
		else:
			self._insert_into_node(insertion_node, info, ctx.ins_index, iop, ctx.child_entry)

		#first key
		data = new_node.data
		if node_is_leaf:
			key = list(struct.unpack(self.node_key_format, data[:struct.calcsize(self.node_key_format)])[1:])
		else:
			key = list(struct.unpack(self.record_format, data[:struct.calcsize(self.record_format)])[2:])

		key_to_push = [0]+key+[new_node_id]
		
		new_node.set_dirty()
		new_node.unpin()
		return key_to_push

	def _get_new_page_id(self):
		p = bm.BufferManager.find_page(page.PageId(self.index_filename, 0))
		data = array("i")
		data.fromstring(p.data)
		data[2] += 1
		pages_used = data[2]
		p.data = data.tostring()
		p.set_dirty()
		p.unpin()
		return pages_used

	def _get_root_node(self):
		p = bm.BufferManager.find_page(page.PageId(self.index_filename, 0))
		data = array("i")
		data.fromstring(p.data)
		root_node = data[0]
		p.unpin()
		return root_node

	def _change_root(self, entry):
		new_page_id = self._get_new_page_id()
		p = bm.BufferManager.find_page(page.PageId(self.index_filename, new_page_id))
		p.data = self._set_node_info(p.data, (self.NODE_TYPE, 1, 0))
		p.data = entry+p.data[len(entry):]
		p.set_dirty()
		p.unpin()

		meta = bm.BufferManager.find_page(page.PageId(self.index_filename, 0))
		data = array("i")
		data.fromstring(meta.data)
		data[0] = new_page_id
		meta.data = data.tostring()
		meta.set_dirty()
		meta.unpin()


class SplitNodeContext(object):
	def __init__(self, p, child_entry, ins_index, entry_size, entries_per_page):
		self.p = p
		self.child_entry = child_entry
		self.ins_index = ins_index
		self.entry_size = entry_size
		self.entries_per_page = entries_per_page
		