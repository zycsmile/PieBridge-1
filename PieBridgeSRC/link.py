#!/usr/bin/python
# -*- coding: utf-8 -*-

class Link:
    def __init__(self, upload_capacity, download_capacity):
        self.upload_capacity  = upload_capacity
        self.download_capacity = download_capacity

    def update_link_capacity(self, upload_capacity, download_capacity):
        self.upload_capacity  = upload_capacity
        self.download_capacity = download_capacity