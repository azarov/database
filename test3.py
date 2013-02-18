#!/usr/bin/python

import PagesDirectory as pd
import buffermanager as bm
import page

pd.create_pages_directory("pagefile")
directory = pd.PagesDirectory("pagefile")
p = directory.get_page_for_insert()
p.data = bytearray([1]*page.PAGESIZE)
p.set_dirty()
p.unpin()
bm.BufferManager.force()