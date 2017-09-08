#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys

def get_server(server_id, super_core_list):
    core_id = server_id[0]
    db_id = server_id[1]
    host_id = server_id[2]
    if core_id >= len(super_core_list) or core_id < 0:
        print "super_core " + str(core_id) + " not exists!"
        return None
    if db_id >= len(super_core_list[core_id].idc_list) or db_id < 0:
        print "idc " + str(db_id) + " not exists!"
        return None
    if host_id >= len(super_core_list[core_id].idc_list[db_id].server_list) or host_id < 0:
        print ""
    return super_core_list[core_id].idc_list[db_id].server_list[host_id]