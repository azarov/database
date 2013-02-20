#!/usr/bin/python

text = " CREATE  tabLe  MyTable   (   country  varchar(45),ID integer  ,XYI double  ) ;   "

def parseCreateTable(sql):
    sql = sql.strip()
    sql = sql[sql.find(" "):].strip()
    keyWord = str(sql[:sql.find(" ")]).upper()
    if keyWord != "TABLE":
        print "error: expected keyword \"Table\""
        return
    sql = sql[sql.find(" "):].strip() 
    pos = min(sql.find(" "),sql.find("("))
    if pos < 1:
        print "error: expected Table Name"
        return
    tableName = str(sql[:pos].strip())
    print "table: ", tableName
    sql = sql[pos:].strip()
    if sql[0] != "(":
        print "error: expected \"(\""
        return
    if sql[-1] != ";":
        print "error: expected \";\""
        return
    sql = sql[:len(sql)-1].strip()
    if sql[-1] != ")":
        print "error: expected \")\""
        return
    sql = sql[1:len(sql)-1].strip()
    fields = sql.split(",")
    for fi in fields:
        part = fi.split()
        if len(part) == 0:
            print "error: expected field name"
            return
        elif len(part) == 1:
            print "error: expected field type"
            return
        elif len(part) > 2:
            print "error: expected \",\""
            return
        print "----------"
        fieldName = part[0].strip()
        fieldType = part[1].strip()
        print "field name:", fieldName
        print "field type:", fieldType

parseCreateTable(text)
