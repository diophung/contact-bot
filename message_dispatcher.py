#!/usr/bin/env python
# encoding: utf-8
"""
message_dispatcher.py

Created by Tien M. Le on 2017-02-23.
Copyright (c) 2017 __tielehut@gmail.com__. All rights reserved.
"""

import json
from importlib import util as import_util


class MessageDispatcher:

    def __init__(self, tasks_config_file=u"available_tasks.json"):
        self.handlers = dict()
        config = json.loads(open(tasks_config_file, 'r').read())
        for handler_name, handler_config in config.items():
            spec = import_util.spec_from_file_location("module.name", handler_config['path'])
            module = import_util.module_from_spec(spec)
            spec.loader.exec_module(module)
            handler = getattr(module, handler_config['class'])()
            self.handlers[handler_name] = handler

    # TODO(tien): dispatch message to a real its handler
    # Now all text messages will be dispatched to ContactRegistration handler, otherwise pass
    def dispatch_message(self, message):
        if message is not None and message['type'] == "text":
            return self.handlers['contact_registration']
        return None

    def dispatch_and_process(self, sender_id, message):
        handler = self.dispatch_message(message)
        if handler is not None:
            return handler.process(sender_id, message)
        return "Sorry, I can't understand this at the moment"


def main():
    dispatcher = MessageDispatcher()
    reply = dispatcher.process_message_get_reply("this is a test message")
    print(reply)

if __name__ == "__main__":
    main()
