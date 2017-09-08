#!/usr/bin/python
# -*- coding: utf-8 -*-
import block
import link
import sys
import util

class Server:
    """
    server info
    """

    def __init__(self, core_id, idc_id, id, upload_capacity, download_capacity):
        self.id = id
        self.super_core_id = core_id
        self.idc_id = idc_id

        self.block_list = {} # blockid: blockinfo
        self.link = link.Link(upload_capacity, download_capacity)

        self.task_status = {} # blockid: finished or not(0:1)
        self.status = 0 # finished all
        self.sender_list = {} # {(super_core_id, idc_id, server_id): download block info}
        self.receiver_list = {} # {(super_core_id, idc_id, server_id): upload block info}

    def add_block(self, b):
        self.block_list[b.id] = b
        self.task_status[b.id] = 1

    def set_link_capacity(upload_capacity, download_capacity):
        self.link.update_link_capacity(upload_capacity, download_capacity)

    def add_sender(self, sender_id, block_num):
        self.sender_list[sender_id] = block_num

    def add_receiver(self, recv_id, block_num):
        self.receiver_list[recv_id] = block_num

    def del_sender(self, sender_id):
        if sender_id not in self.sender_list:
            print sender_id, "not exists!"
        else:
            del self.sender_list[sender_id]

    def del_receiver(self, recv_id):
        if recv_id not in self.receiver_list:
            print recv_id, "not exists!"
        else:
            del self.receiver_list[recv_id]

    def init_task(self, start_id, end_id):
        """
        init task list
        """
        for i in range(star_id, end_id + 1):
            self.task_status[i] = 0

    def update_status(self):
        """
        check whether all blocks have been downloaded. 
        """
        if len(self.task_status) != 0:
            for i in self.task_status:
                if self.task_status[i] == 0:
                    self.status = 0
                    return
            self.status = 1

    def status_diff(self, task_status):
        score = 0
        for i in self.task_status:
            if self.task_status[i] != task_status[i]:
                score += 1
        return score

    def update_senders(self, super_core_list, PEER_NUM, flag):
        """
        super_core_list: 包含系统所有的信息
        PEER_NUM: sender和receiver的上线个数
        flag: 0 表示普通的server, 1表示super_core的server
        """
        """
        逻辑：
        - 除了 super core，其他普通 server 在选 sender 的时候，
          优选自己的 super core，只要是他自己 super core 里有的 block，
          都默认从 super core 去下 (不管需要几个 cycle 才能下完)

        - 选sender时需要确认是否对方的receive已经达到上限， 如果receive不够就换下一个server
        """
        if self.status == 1: # 完成任务
            self.sender_list = {}
            return

        if flag == 1:
            serverid_score = {}
            for super_core in super_core_list:
                for dc in super_core.idc_list:
                    if dc.id == self.idc_id:
                        continue
                    host = dc.server_list[self.id]
                    server_id = (host.super_core_id, host.idc_id, host.id)
                    score = self.status_diff(host.task_status)
                    if score == 0:
                        continue
                    serverid_score[server_id] = score
            result = sorted(serverid_score.items(), key=lambda d:d[1], reverse = True)
            sender_num = PEER_NUM - len(self.sender_list)
            i = 0
            for i in range(len(result)):
                if sender_num <= 0: # 选完了
                    break
                sender_id = result[i][0]
                if sender_id in self.sender_list:
                    continue
                sender = util.get_server(sender_id, super_core_list)

                if len(sender.receiver_list) < PEER_NUM:
                    self.add_sender(sender_id, 0)
                    sender.add_receiver((self.super_core_id, self.idc_id, self.id), 0)
                    sender_num -= 1

        elif flag == 0:
            core_host = super_core_list[self.super_core_id].idc_list[0].server_list[self.id]
            score = self.status_diff(core_host.task_status)
            if score != 0:
                if len(core_host.receiver_list) < PEER_NUM:
                    sender_id = (core_host.super_core_id, core_host.idc_id, core_host.id)
                    self.add_sender(sender_id, 0)
                    core_host.add_receiver((self.super_core_id, self.idc_id, self.id), 0)

            serverid_score = {}
            for super_core in super_core_list:
                for index in range(1, len(super_core.idc_list)):
                    dc = super_core.idc_list[index]
                    if dc.id == self.idc_id:
                        continue
                    host = dc.server_list[self.id]
                    server_id = (host.super_core_id, host.idc_id, host.id)
                    score = self.status_diff(host.task_status)
                    if score == 0:
                        continue
                    serverid_score[server_id] = score
            result = sorted(serverid_score.items(), key=lambda d:d[1], reverse = True)
            sender_num = PEER_NUM - len(self.sender_list)
            i = 0
            for i in range(len(result)):
                if sender_num <= 0: # 选完了
                    break
                sender_id = result[i][0]
                if sender_id in self.sender_list:
                    continue
                sender = util.get_server(sender_id, super_core_list)

                if len(sender.receiver_list) < PEER_NUM:
                    self.add_sender(sender_id, 0)
                    sender.add_receiver((self.super_core_id, self.idc_id, self.id), 0)
                    sender_num -= 1

    def del_sender_receiver(self, super_core_list, PEER_NUM):
        """
        删除最慢的sender和receiver
        """
        server_id = (self.super_core_id, self.idc_id, self.id)
        if len(self.sender_list) != 0:
            min_value = sys.maxint
            min_index = (-1, -1, -1)
            for i in self.sender_list:
                if self.sender_list[i] < min_value:
                    min_value = self.sender_list[i]
                    min_index = i
            #if len(self.sender_list) == PEER_NUM:
            self.del_sender(min_index)
            receiver_server = util.get_server(min_index, super_core_list)
            receiver_server.del_receiver(server_id)
            print "server", server_id, "del sender", min_index, min_value
        if len(self.receiver_list) != 0:
            min_value = sys.maxint
            min_index = (-1, -1, -1)
            for i in self.receiver_list:
                if self.receiver_list[i] < min_value:
                    min_value = self.receiver_list[i]
                    min_index = i
                    
            #if len(self.receiver_list) == PEER_NUM:
            self.del_receiver(min_index)
            sender_server = util.get_server(min_index, super_core_list)
            sender_server.del_sender(server_id)
            print "server", server_id, "del receiver", min_index, min_value




        


