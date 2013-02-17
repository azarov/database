#!/usr/bin/python

import diskspacemanager as dsm
import launcher

class _BufferManager(object):
	def __init__(self, max_size):
		self.pages = {}
		self.max_size = max_size

	def find_page(self, pageid):

		if pageid in self.pages:
			p = self.pages[pageid]
		else:
			p = dsm.DiskSpaceManager.get_page(pageid)
			if len(self.pages) < self.max_size:
				self.pages[pageid] = p
			else:
				self.replace(p)
				
		p.pin()
		return p

	def replace(self, page):
		pages_for_replacement = sorted(filter(lambda v: v.is_unpinned(), self.pages.values()), key=lambda x: x.time, reverse = True)
		if len(pages_for_replacement) == 0:
			return

		page_for_replacement = pages_for_replacement[0]
		dsm.DiskSpaceManager.write_page(page_for_replacement)
		del self.pages[pages_for_replacement[0].pageid]
		self.pages[page.pageid] = page

	def force(self):
		for page in self.pages.values():
			dsm.DiskSpaceManager.write_page(page)


BufferManager = _BufferManager(launcher.max_pages_number)
