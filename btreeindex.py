#!/usr/bin/python

def create_index(indexmetadata):
	index_filename = indexmetadata.tablename+"_btree_"+indexmetadata.indexname
	index_directory_filename = index_filename+"_directory"
