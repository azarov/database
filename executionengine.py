#!/usr/bin/python

import statements
import flatfile
import tablemetadata as tmd
import metadataprovider as mdp
import PagesDirectory as pd
import buffermanager as bm
import heapfile as hf
import hashindex
import btreeindex
import struct
import csvprinter
import sys
import os

def execute(statement):
	if isinstance(statement, statements.CreateTableStatement):
		execute_create(statement)
	elif isinstance(statement, statements.InsertStatement):
		execute_insert(statement)
	elif isinstance(statement, statements.SelectStatement):
		execute_select(statement)
	elif isinstance(statement, statements.DropStatement):
		execute_drop(statement)
	elif isinstance(statement, statements.CreateIndexStatement):
		execute_create_index(statement)
	else:
		raise Exception("Unknown statement type: {0}", type(statement))


def execute_create(statement):
	if os.path.isfile(statement.tablename):
		raise Exception("Create table error. Table already exists.")

	tablemetadata = tmd.TableMetaData(statement.tablename, statement.attributes)
	f = open(statement.tablename, "wb")
	f.close()
	f = open(pd.get_page_directory_name(statement.tablename), "wb")
	f.close()
	mdp.MetaDataProvider.save_metadata(tablemetadata)
	pd.create_pages_directory(statement.tablename)

def execute_insert(statement):
	heap = hf.HeapFile()
	tablemetadata = mdp.MetaDataProvider.get_metadata(statement.tablename)
	values = dict(map(None, [x.name for x in tablemetadata.attributes], statement.values))
	heap.insert(tablemetadata, values)

def execute_select(statement):
	heap = hf.HeapFile()
	tablemetadata = mdp.MetaDataProvider.get_metadata(statement.tablename)

	records = [x.values for x in heap.get_all_records(tablemetadata, statement.whereStmt)]
	printer = csvprinter.CsvPrinter(sys.stdout, tablemetadata)
	printer.print_records(records)

def execute_drop(statement):
	bm.BufferManager.force()
	if os.path.isfile(statement.tablename):
		os.remove(statement.tablename)

	if os.path.isfile(pd.get_page_directory_name(statement.tablename)):
		os.remove(pd.get_page_directory_name(statement.tablename))

	if os.path.isfile(mdp.get_metadata_name(statement.tablename)):
		os.remove(mdp.get_metadata_name(statement.tablename))

def execute_create_index(st):
	indexmetadata = tmd.IndexMetaData(st.tablename, st.indexname, [tmd.KeyInfo(x.column_name, x.ascending) for x in st.columns], st.is_unique, st.is_btree)
	mdp.MetaDataProvider.add_index_info(st.tablename, indexmetadata)

	if indexmetadata.type == tmd.IndexTypes.HASH:
		hashindex.create_index(st.tablename, indexmetadata)
	elif indexmetadata.type == tmd.IndexTypes.BTREE:
		btreeindex.create_index(st.tablename, indexmetadata)
	else:
		raise Exception("Can't create index. Unknown type of index: {0}".format(indexmetadata.type))
