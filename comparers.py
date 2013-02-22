#!/usr/bin/python

class DefaultComparer(object):
	def __init__(self):
		pass

	def compare(self, key1, key2):
		if key1 > key2:
			return 1
		elif key1 == key2:
			return 0
		else:
			return -1
