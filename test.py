#!/usr/bin/python

import diskspacemanager
import page

pid = page.PageId("pagefile", 5)
data = bytearray([1]*page.PAGESIZE)
p = page.Page(pid, data, 0, True)
diskspacemanager.DiskSpaceManager.write_page(p)

pid2 = page.PageId("pagefile", 10)
page2 = diskspacemanager.DiskSpaceManager.get_page(pid2)
page2 = page.Page(pid2, bytearray([50]*page.PAGESIZE),0, True)
diskspacemanager.DiskSpaceManager.write_page(page2)
page2 = diskspacemanager.DiskSpaceManager.get_page(pid2)
print diskspacemanager.DiskSpaceManager.get_pages_number("pagefile")
