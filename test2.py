#!/usr/bin/python

import PagesDirectory as pd

directory = pd.PagesDirectory("pagefile")
print bytearray(directory.data)