#!/usr/bin/env python
# encoding: utf-8
"""
log_util.py

Created by Tien M. Le on 2017-02-28.
Copyright (c) 2017 __tielehut@gmail.com__. All rights reserved.
"""

import sys


def log(message):  # simple wrapper for logging to stdout on heroku
    print(str(message))
    sys.stdout.flush()
