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
        self.completion_time = 0
        self.upload_quota = 0
        self.download_quota = 0

    def add_block(self, b):
        self.block_list[b.id] = b
        self.task_status[b.id] = 1

    def set_link_capacity(upload_capacity, download_capacity):
        self.link.update_link_capacity(upload_capacity, download_capacity)



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



    def set_upload_quota(self, quota):
        self.upload_quota = quota

    def set_download_quota(self, quota):
        self.download_quota = quota

    def count_blk_num(self, b): # 根据 status 计数该 block 在全局一共有的数量

    def select_best_src(self, blk):
        """
        选源时：
        1. 找谁的这个 blk 的 status == 1，就都是候选源
        2. 若有多个候选源，则选取拥有 blks 总数最少的那个候选源。因为 blks 总数多的那些源还能为其他 blk 提供下载，而 blks 总数少的这个能被其他人选取的几率低
        3. 如果没有选到合适的源(可能由于源的 upload_quota 被别人用完了)，返回 0
        """

    def piebridge(self):
        """
        所有 block 按全局副本数量由少到多排序
        """
        for i in range(TASK_SIZE): # 计数全局 blk 的副本个数
            block_task = block.Block(i, BLOCK_SIZE)
            duplicate_num[i] = self.count_blk_num(block_task)
        objects = sorted(duplicate_num) # 先下载副本数量最少的


        """
        优先选取副本数量最少的 blocks，共选取 max_object_num 块去下载
        """
        bw_limit_num = int(self.link.download_capacity*SCHEDULE_CYCLE/BLOCK_SIZE) #该 host 每轮最多能下多少个 blk
        quota_limit_num = self.download_quota
        max_object_num = min(bw_limit_num, quota_limit_num)


        """
        要为每个 blk 选取一个源，同时自身的 download_quota -= 1，源的 upload_quota -= 1
        """
        download_num = 0
        for i in objects:
            if download_num >= max_object_num:
                break
            if self.task_status[i] == 1:
                continue
            block_task = block.Block(i, BLOCK_SIZE)
            src = self.select_best_src(block_task)
            if src != 0:
                self.download_quota -= 1
                src.upload_quota -= 1
                self.task_status[i] = 1
                download_num += 1
        self.update_status()
        server_id = (self.super_core_id, self.idc_id, self.id)
        print "server", server_id, "downloads", download_num, "blocks"


