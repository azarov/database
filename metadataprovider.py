#!/usr/bin/python

import config
import pickle

class _MetaDataProvider(object):
	def __init__(self):
		self.cache = {}
		self.max_cache_size = 10

	def add_index_info(self, tablename, indexmetadata):
		tablemetadata = self.get_metadata(tablename)
		if indexmetadata.name in [x.indexname for x in tablemetadata.indices]:
			raise Exception("Can'r create index. Index with name {0} already exists for table {1}.".format(indexmetadata.name,tablemetadata.name))

		table_column_names = [x.name for x in tablemetadata.attributes]
		for index_key in indexmetadata.keys:
			if not index_key.name in table_column_names:
				raise Exception("Can't create index. Column with name {0} doesn't exists in table {1}".format(index_key.name,tablemetadata.name))

		tablemetadata.indices.append(indexmetadata)
		self.save_metadata(tablemetadata)

	def get_metadata(self, tablename):
		metadatafilepath = tablename+"_metadata"

		if metadatafilepath in self.cache:
			return self.cache[metadatafilepath]

		metadata = None
		with open(metadatafilepath, "rb") as f:
			metadata = pickle.load(f)

		if len(self.cache) >= self.max_cache_size:
			del self.cache[self.cache.keys()[0]]
			
		self.cache[metadatafilepath] = metadata

		return metadata

	def save_metadata(self, metadata):
		metadatafilepath = metadata.name+"_metadata"
		with open(metadatafilepath, "wb") as f:
			pickle.dump(metadata, f)

MetaDataProvider = _MetaDataProvider()

def get_metadata_name(tablename):
	return tablename+"_metadata"