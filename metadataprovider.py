#!/usr/bin/python

import config
import tablemetadata
import pickle

class _MetaDataProvider(object):
	def __init__(self):
		self.cache = {}
		self.max_cache_size = 10

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