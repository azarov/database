#!/usr/bin/python

import statements
import flatfile
import tablemetadata as tmd
import metadataprovider as mdp
import PagesDirectory as pd
import buffermanager as bm
import heapfile as hf
import struct
import csvprinter
import sys
import os

def execute(statement):
	if isinstance(statement, statements.CreateStatement):
		execute_create(statement)
	elif isinstance(statement, statements.InsertStatement):
		execute_insert(statement)
	elif isinstance(statement, statements.SelectStatement):
		execute_select(statement)
	elif isinstance(statement, statements.DropStatement):
		execute_drop(statement)


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

	records = [struct.unpack(tablemetadata.format, x) for x in heap.get_all_records(tablemetadata, statement.whereStmt)]
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
