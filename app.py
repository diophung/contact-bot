import os
import sys
import json
import datetime
import requests
import re
from extractor import Extractor
from flask import Flask, request
from flask_pymongo import PyMongo

# Create the Flask app
app = Flask(__name__)
app.config.from_object('configuration.Config')
if isinstance(app.config['MGDB_PREFIX'], str):
    app.mongo = PyMongo(app, config_prefix=app.config['MGDB_PREFIX'])

# Import auto-created mongodb collection helpers
from db.mongo import mongo_contacts


@app.route('/', methods=['GET'])
def verify():
    # when the endpoint is registered as a webhook, it must echo back
    # the 'hub.challenge' value it receives in the query arguments
    if request.args.get("hub.mode") == "subscribe" and request.args.get("hub.challenge"):
        if not request.args.get("hub.verify_token") == os.environ["VERIFY_TOKEN"]:
            return "Verification token mismatch", 403
        return request.args["hub.challenge"], 200

    msg = "It's now " + str(datetime.datetime.now())
    return msg, 200


@app.route('/', methods=['POST'])
def webhook():
    # endpoint for processing incoming messaging events
    data = request.get_json()
    log(data)  # log all msg, ok for this chatbot
    if data["object"] == "page":
        for entry in data["entry"]:
            for messaging_event in entry["messaging"]:
                # someone sent us a message
                if messaging_event.get("message"):
                    sender_id = messaging_event["sender"]["id"]
                    message_text = messaging_event["message"]["text"]
                    ext = Extractor()
                    reply = ""
                    email, phone = ext.extract_details(message_text)
                    if email != "" and phone != "":
                        store_contact(sender_id, email, phone)
                        reply = "Got it. Your email is " + email + " and phone is " + phone + ". Thanks."
                    else:
                        reply = "Hi, can I have your email & phone number please?"
                    send_message(sender_id, reply)

                if messaging_event.get("delivery") or \
                    messaging_event.get("optin") or \
                        messaging_event.get("postback"):
                    pass

    return "ok", 200


def send_message(recipient_id, message_text):
    params = {"access_token": os.environ["PAGE_ACCESS_TOKEN"]}
    headers = {"Content-Type": "application/json"}
    data = json.dumps(
        {"recipient": {"id": recipient_id},
         "message": {"text": message_text}}
    )
    r = requests.post("https://graph.facebook.com/v2.6/me/messages",
                      params=params, headers=headers, data=data)
    if r.status_code != 200:
        log(r.status_code)
        log(r.text)


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


def log(message):  # simple wrapper for logging to stdout on heroku
    print(str(message))
    sys.stdout.flush()


if __name__ == '__main__':
    app.run(debug=False)
