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
IDC_NUM = 2 # the idc num in one super core, including super core's idc
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
   
def del_inefficient_sender_receiver():
    for super_core in super_core_list:
        for dc in super_core.idc_list:
            for host in dc.server_list:
                host.del_sender_receiver(super_core_list, PEER_NUM)

def update_servers_send_recv_list():
    """
    1. super core 先选sender, 选sender不仅仅判断分数，同时判断receiver是否已满
    2. 其他机器候选server, 首先看super core有没有数据，如果有，一定从super core上下载，其他的在别的机器上下
    """
    ## super core 先选
    print "super core selection round"
    for super_core in super_core_list:
        dc = super_core.idc_list[0]
        for host in dc.server_list:
            host.update_senders(super_core_list, PEER_NUM, 1)
    print "others selection round"
    for super_core in super_core_list:
        for i in range(1, len(super_core.idc_list)):
            dc = super_core.idc_list[i]
            for host in dc.server_list:
                host.update_senders(super_core_list, PEER_NUM, 0)


def begin_trans():
    """
    每台server更新自己的下载的数据
    1. 如果自己的super core 上有需要的block, 那就去自己的super core上下载, 其他的block都去别的sender下
    2. 去别的sender下，按照取模的方式下载数据
    """
    for super_core in super_core_list:
        for dc in super_core.idc_list:
            for host in dc.server_list:
                if len(host.sender_list) == 0: # 没有sender情况
                    continue
                else:
                    mean_sender_bw = host.link.download_capacity/len(host.sender_list) # 平分host的接收带宽
                    # 从super core下载
                    super_core_object = []  
                    normal_sender_num = len(host.sender_list) # 除了super core, 其他的sender个数
                    for sender_id in host.sender_list:
                        if host.super_core_id == sender_id[0] and sender_id[1] == 0:
                            normal_sender_num -= 1 # 去掉super core

                            sender_server = util.get_server(sender_id, super_core_list)
                            mean_receiver_bw = sender_server.link.download_capacity/len(sender_server.receiver_list)
                            object_num = int(min(mean_sender_bw, mean_receiver_bw)*SCHEDULE_CYCLE/BLOCK_SIZE) # 本周期最多下载多少block

                            for i in host.task_status: # 从super core上下载的block
                                if host.task_status[i] == 0 and sender_server.task_status[i] == 1:
                                    super_core_object.append(i)

                            objects = sorted(super_core_object)
                            download_obj_num = min(object_num,len(objects))
                            real_download_obj_num = 0
                            for i in range(download_obj_num):
                                if sender_server.task_status[objects[i]] == 1:
                                    host.task_status[objects[i]] = 1
                                    real_download_obj_num += 1
                            host.sender_list[sender_id] = real_download_obj_num
                            sender_server.receiver_list[(host.super_core_id, host.idc_id, host.id)] = real_download_obj_num
                    # 从普通sender下载
                    objects = [] #需要下载的block
                    for i in host.task_status:
                        if (i not in super_core_object) and host.task_status[i] == 0:
                            objects.append(i)
                            
                    index = 0
                    for sender_id in host.sender_list:
                        if host.super_core_id == sender_id[0] and sender_id[1] == 0:
                            continue
                        temp_objects = [] # 从本sender下载的block, 取模
                        for i in objects:
                            if i%normal_sender_num == index:
                                temp_objects.append(i)
                        index += 1
                        sender_server = util.get_server(sender_id, super_core_list)
                        mean_receiver_bw = sender_server.link.download_capacity/len(sender_server.receiver_list)
                        object_num = int(min(mean_sender_bw, mean_receiver_bw)*SCHEDULE_CYCLE/BLOCK_SIZE)
                        temp_objects = sorted(temp_objects)
                        download_obj_num = min(object_num,len(temp_objects))
                        real_download_obj_num = 0
                        for i in range(download_obj_num):
                            if sender_server.task_status[objects[i]] == 1:
                                host.task_status[objects[i]] = 1
                                real_download_obj_num += 1
                        host.sender_list[sender_id] = real_download_obj_num
                        sender_server.receiver_list[(host.super_core_id, host.idc_id, host.id)] = real_download_obj_num



def check_finished():
    """
    判断每个机器是否已经下完了所有的数据
    """
    for super_core in super_core_list:
        for dc in super_core.idc_list:
            for host in dc.server_list:
                host.update_status()
                if host.status == 0:
                    return False
    return True




if __name__ == "__main__":
    print "init..."
    init()
    print "set src IDC..."
    set_src_idc(0, 1) # 选择0号super_core的1号IDC为src_IDC.

    print "start data distribution... "
    while check_finished() == 0:
        del_inefficient_sender_receiver()
        update_servers_send_recv_list()
        begin_trans()
