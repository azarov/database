#!/usr/bin/python

import heapfile as hf
import metadataprovider as mdp
import struct
import csvprinter
import sys

tablemetadata = mdp.MetaDataProvider.get_metadata("users")
heap = hf.HeapFile()

records = [struct.unpack(tablemetadata.format, x) for x in heap.get_all_records(tablemetadata)]
printer = csvprinter.CsvPrinter(sys.stdout, tablemetadata)
printer.print_records(records)