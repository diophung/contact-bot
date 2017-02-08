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
    log(data)  # you may not want to log every incoming message in production, but it's good for testing
    if data["object"] == "page":
        for entry in data["entry"]:
            for messaging_event in entry["messaging"]:
                if messaging_event.get("message"):  # someone sent us a message
                    # the facebook ID of the person sending you the message
                    sender_id = messaging_event["sender"]["id"]
                    # the recipient's ID, which should be your page's facebook
                    # ID
                    recipient_id = messaging_event["recipient"]["id"]
                    message_text = messaging_event["message"][
                        "text"]  # the message's text
                    return_msg = "Sorry, I can't get your email and phone number. Please try again"
                    ext = Extractor()
                    email, phone = ext.extract_details(message_text)
                    if email != "" and phone != "":
                        # Check for existing contact

                        facebook_id = messaging_event["sender"]["id"]
                        check_exist = mongo_contacts.check_exist(
                            query={"facebook_id": facebook_id})
                        log(check_exist)
                        if check_exist is None:
                            # Store facebook user, name, contact email, phone
                            mongo_contacts.insert_one(query={
                                "facebook_id": facebook_id,
                            }, update={
                                "email": email,
                                "phone": phone
                            })
                        else:
                            mongo_contacts.update_one(query={
                                "facebook_id": facebook_id,
                                "email": email,
                                "phone": phone
                            })

                        return_msg = "Your email: " + email + ", and phone:" + phone + ". Is it correct?"

                    send_message(sender_id, return_msg)

                if messaging_event.get("delivery") or \
                    messaging_event.get("optin") or \
                        messaging_event.get("postback"):
                    pass

    return "ok", 200


def send_message(recipient_id, message_text):
    #log("sending message to {recipient}: {text}".format(recipient=recipient_id, text=message_text))
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


def log(message):  # simple wrapper for logging to stdout on heroku
    print(str(message))
    sys.stdout.flush()


if __name__ == '__main__':
    app.run(debug=False)
