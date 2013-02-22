#!/usr/bin/python

import btreeindex
import metadataprovider as mdp
import tablemetadata as tmd
import buffermanager as bm
import executionengine as ee
import sqlparser
import comparers

indexmetadata = tmd.IndexMetaData("users", "users_index", [tmd.KeyInfo("id", True)])

index = btreeindex.BTreeIndexManager(indexmetadata, comparers.DefaultComparer())
index.create_index()

bm.BufferManager.force()
