#!/usr/bin/python

import config
import tablemetadata
import pickle

class MetaDataProvider(object):
	def __init__(self):
		pass

	def get_metadata(self, tablename):
		metadatafilepath = config.Config.path_to_db+tablename
		metadata = None
		with open(metadatafilepath, "rb") as f:
			metadata = pickle.load(f)

		return metadata

	def save_metadata(self, metadata):
		metadatafilepath = config.Config.path_to_db+metadata.name
		with open(metadatafilepath, "wb") as f:
			pickle.dump(metadata, f)

