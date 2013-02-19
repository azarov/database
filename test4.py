#!/usr/bin/python

import sqlparser
import executionengine as ee
import buffermanager as bm

str = 'drop table users'
stmt = sqlparser.parse_statement(str)
ee.execute(stmt)
str = 'create table users(id int, name VARchar(100))'
stmt = sqlparser.parse_statement(str)
ee.execute(stmt)
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