#!/usr/bin/python

import hashindex
import metadataprovider as mdp
import tablemetadata as tmd
import buffermanager as bm

indexmetadata = tmd.IndexMetaData("users", "users_index", [tmd.KeyInfo("id", True)])

index = hashindex.HashIndexManager(indexmetadata)
index.create_index()
bm.BufferManager.force()