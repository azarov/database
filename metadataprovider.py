#!/usr/bin/python

import config
import tablemetadata
import pickle

class _MetaDataProvider(object):
	def __init__(self):
		pass

	def get_metadata(self, tablename):
		metadatafilepath = tablename+"_metadata"
		metadata = None
		with open(metadatafilepath, "rb") as f:
			metadata = pickle.load(f)

		return metadata

	def save_metadata(self, metadata):
		metadatafilepath = metadata.name+"_metadata"
		with open(metadatafilepath, "wb") as f:
			pickle.dump(metadata, f)

MetaDataProvider = _MetaDataProvider()

def get_metadata_name(tablename):
	return tablename+"_metadata"