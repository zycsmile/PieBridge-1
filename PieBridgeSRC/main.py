#!/usr/bin/python
# -*- coding: utf-8 -*-

import os

import server
import link
import block
import idc
import supercore
import util

LINK_UPLOAD_CAPACITY = 20 #MB/s
LINK_DOWNLOAD_CAPACITY = 20 #MB/s
TASK_SIZE = 10*1024
BLOCK_SIZE = 2 #MB/S
SCHEDULE_CYCLE = 5 # Second
PEER_NUM = 3

SUPER_CORE_NUM = 3
IDC_NUM = 2 # super core 自身不再存储一份数据，也不再设有 server。因此，不管 idc_id 等于几都是 super core 下属的一个普通 idc
SERVER_NUM = 10

super_core_list = []

def init():
    # init all object
    for i in range(SUPER_CORE_NUM):
        # init super core
        super_core = supercore.SuperCore(i)
        for j in range(IDC_NUM):
            # init idc of the super core
            dc = idc.IDC(i, j)
            for k in range(SERVER_NUM):
                # init the server of the idf
                host = server.Server(i, j, k, LINK_UPLOAD_CAPACITY, LINK_DOWNLOAD_CAPACITY)
                dc.server_list.append(host)
            super_core.idc_list.append(dc) 
        super_core_list.append(super_core)
    # init task
    for super_core in super_core_list:
        for dc in super_core.idc_list:
            server_num = len(dc.server_list)
            average_block_num = TASK_SIZE/server_num
            for i in range(TASK_SIZE):
                server_id = i/average_block_num
                dc.server_list[server_id].task_status[i] = 0

def set_src_idc(core_id, idc_id):
    """
    设置src idc, 其他机器在src idc 下载数据
    """
    super_core = super_core_list[core_id]
    dc = super_core.idc_list[idc_id]
    server_num = len(dc.server_list)
    average_block_num = TASK_SIZE/server_num # server_num < Task_size

    for i in range(TASK_SIZE):
        block_task = block.Block(i, BLOCK_SIZE)
        server_id = i/average_block_num
        dc.server_list[server_id].add_block(block_task)
    for s in dc.server_list:
        s.update_status()

def check_finished():
    """
    判断每个机器是否已经下完了所有的数据
    """
    flag = True
    for super_core in super_core_list:
        for dc in super_core.idc_list:
            for host in dc.server_list:
                host.update_status()
                if (host.status == 1 and host.completion_time == 0):
                    host.completion_time = CYCLE_NUM * SCHEDULE_CYCLE
                elif host.status == 0:
                    flag = False
    return flag

def init_server_quota():
    for super_core in super_core_list:
        for dc in super_core.idc_list:
            for host in dc.server_list:
                max_upload_num = int(host.link.upload_capacity*SCHEDULE_CYCLE/BLOCK_SIZE) # 每个 server 每轮最多能上传多少个 blk
                max_download_num = int(host.link.download_capacity*SCHEDULE_CYCLE/BLOCK_SIZE) # 每个 server 每轮最多能下载多少个 blk
                host.set_upload_quota(max_upload_num)
                host.set_download_quota(max_download_num)




if __name__ == "__main__":
    print "init..."
    init()
    print "set src IDC..."
    set_src_idc(0, 1) # 选择0号super_core的1号IDC为src_IDC.

    print "start data distribution... "
    CYCLE_NUM = 0
    while check_finished() == 0:
        CYCLE_NUM += 1
        init_server_quota()
        for super_core in super_core_list:
            for dc in super_core.idc_list:
                for host in dc.server_list:
                    if host.status == 0:
                        host.piebridge()