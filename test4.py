#!/usr/bin/python

import cProfile
import pstats

import config
config.Config.max_pages_number = 5

import sqlparser
import executionengine as ee
import buffermanager as bm

def test():
	str = 'drop table users'
	stmt = sqlparser.parse_statement(str)
	ee.execute(stmt)

	str = 'create table users (   id    int ,   name  vARchar(100) )'
	str = " ".join(str.split())
	stmt = sqlparser.parse_statement(str)
	ee.execute(stmt)

	for x in xrange(0,20000):
		str = 'Insert into users values(1, "Vasya")'
		stmt = sqlparser.parse_statement(str)
		ee.execute(stmt)

		str = 'Insert into users values(2, "Vladimir petrovich")'
		stmt = sqlparser.parse_statement(str)
		ee.execute(stmt)

		str = 'Insert into users values(3, "Peter")'
		stmt = sqlparser.parse_statement(str)
		ee.execute(stmt)

		str = 'Insert into users values(4, "Captain America")'
		stmt = sqlparser.parse_statement(str)
		ee.execute(stmt)

		str = 'Insert into users values(5, "John Doe")'
		stmt = sqlparser.parse_statement(str)
		ee.execute(stmt)

	bm.BufferManager.force()

test()
# cProfile.run("test()", "test4_stats")
# p = pstats.Stats("test4_stats")
# p.strip_dirs().sort_stats("cumulative").print_stats(10)