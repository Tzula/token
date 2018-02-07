#!/usr/bin/env python
# -*- coding: utf-8 -*-
from dao import db_module
def get_allwebinfo():
    table_name = "trackingtokens"
    sql = "select webname,webtype,routing from {}".format(table_name)
    ret = db_module.execute_getinfo(sql)
    return ret

def get_allwebname():
    table_name = "trackingtokens"
    sql = "select webname from {}".format(table_name)
    ret = db_module.execute_getinfo(sql)
    return ret
def get_detailinfo(key):
    key = "../{}/".format(key)
    table_name = "trackingtokens"
    sql ="select minititle,title,macros_num,traffic_platform,token_name,token_description,conversion_tracking," \
         "panel_body,webname from {} where routing='{}'".format(table_name, key)
    ret = db_module.execute_getinfo(sql)
    return ret

