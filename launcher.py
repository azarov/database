#!/usr/bin/python

import sys
import os
import re
import sqlparser
import statements
import executionengine
import config

path_to_db = os.curdir+os.sep
max_pages_number = 1024

def read_input_arguments():
	if len(sys.argv) > 1:
		global path_to_db
		path_to_db = sys.argv[1]

	if len(sys.argv) > 2:
		global max_pages_number
		max_pages_number = int(sys.argv[2])


def read_command():
	command = ""
	line = sys.stdin.readline()
	while line == os.linesep:
		line = sys.stdin.readline()

	while line != os.linesep and line != "":
		command += line
		line = sys.stdin.readline()
	command = " ".join(command.split())
	return command

def launch():
	read_input_arguments()
	config.Config.update(path_to_db, max_pages_number)
	commands = []
	while True:
		command = read_command()
		if command == "":
			break

		for sub_command in command.split(";"):
			try:
				if sub_command != "":
					stmt = sqlparser.parse_statement(sub_command.strip())
					executionengine.execute(stmt)
			except sqlparser.UnknownStatement, e:
				print "UnknownStatement: ", e.msg
			except sqlparser.ParseException, e:
				print "UnknownStatement: ", e.msg

		commands.append(command)
