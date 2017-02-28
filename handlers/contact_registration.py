#!/usr/bin/env python
# encoding: utf-8
"""
contact_registration.py

Created by Tien M. Le on 2017-02-23.
Copyright (c) 2017 __tielehut@gmail.com__. All rights reserved.
"""

import re
from common.log_util import log
from handlers.message_handler import MessageHandler
from db.mongo import mongo_contacts


class ContactRegistration(MessageHandler):

    def __init__(self):
        self.extractor = Extractor()

    def process(self, message):
        message_text = message["message"]["data"]
        reply = ""
        email, phone = self.extractor.extract_details(message_text)
        if email != "" and phone != "":
            self.store_contact(message['sender_id'], email, phone)
            reply = "Got it. Your email is " + email + " and phone is " + phone + ". Thanks."
        else:
            reply = "Hi, can I have your email & phone number please?"
        return reply

    def store_contact(facebook_id, email, phone):
        check_exist = mongo_contacts.check_exist(
            query={"facebook_id": facebook_id})
        log(check_exist)
        if check_exist is None:
            # Store facebook user, name, contact email, phone
            mongo_contacts.insert_one(query={
                "facebook_id": facebook_id,
                "email": email,
                "phone": phone
            })
        else:
            mongo_contacts.update_one(query={
                "facebook_id": facebook_id,
            }, update={
                "$set": {
                    "email": email,
                    "phone": phone
                }

            })


class Extractor:
    def extract_details(self, from_msg):
        """
        rtype: email, phone
        """
        # email format
        email = re.search(r'[\w\.-]+@[\w\.-]+', from_msg)

        # US & VN mobile phone formats: 555-555-5555, 0988 123 456, 0165 123 4567
        phone = re.search(r"\(?\b[0-9][0-9]{2}\)?[-. ]?[0-9][0-9]{2}[-. ]?[0-9]{4}\b|(\d{4}[-. ]?\d{3}[-. ]?(\d{3,4}|\d{4}))|(\d{10})", from_msg)
        if email and phone and email.group(0) and phone.group(0):
            return email.group(0), phone.group(0)
        return "", ""
