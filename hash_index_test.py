#!/usr/bin/python

import hashindex
import metadataprovider as mdp
import tablemetadata as tmd
import buffermanager as bm
import executionengine as ee
import sqlparser

indexmetadata = tmd.IndexMetaData("users", "users_index", [tmd.KeyInfo("id", True)])

ee.execute(sqlparser.parse_statement("select * from users"))

index = hashindex.HashIndexManager(indexmetadata)
index.create_index()

bm.BufferManager.force()

index.delete_value([1])
ee.execute(sqlparser.parse_statement("select * from users"))

bm.BufferManager.force()