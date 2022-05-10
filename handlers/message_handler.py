#!/usr/bin/env python
# encoding: utf-8
"""
task.py
This is super class that all tasks should implement

Created by Tien M. Le on 2017-02-23.
Copyright (c) 2017 __tielehut@gmail.com__. All rights reserved.
"""


class MessageHandler:

    def __init__(self):
        pass

    def message_handler(self, message):
        return self.process(message)

    def process(self, message):
        raise Exception("Not implemented")
