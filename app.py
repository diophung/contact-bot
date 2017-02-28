import os
import sys
import json
import datetime
import requests
import re
from flask import Flask, request
from flask_pymongo import PyMongo
from common.log_util import log
from message_dispatcher import MessageDispatcher
# Create the Flask app
app = Flask(__name__)
app.config.from_object('configuration.Config')
if isinstance(app.config['MGDB_PREFIX'], str):
    app.mongo = PyMongo(app, config_prefix=app.config['MGDB_PREFIX'])

dispatcher = MessageDispatcher()

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
                    message_data = messaging_data(messaging_event['message'])
                    reply = dispatcher.dispatch_and_process(sender_id, message_data)
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


def messaging_data(message_data):
    # Text only
    if "text" in message_data and "quick_reply" not in message_data:
        text = message_data['text'].encode('unicode_escape')
        data = {
            "type": "text",
            "data": text,
            "messaging_id": message_data['mid']
        }
        return data
    # Includes attachment(photo, audio, file, location...)
    elif "attachments" in message_data:
        if "location" == message_data['attachments'][0]['type']:
            coordinates = message_data['attachments'][0]['payload']['coordinates']
            latitude = coordinates['lat']
            longtitude = coordinates['long']
            data = {
                "type": "location",
                "data": [latitude, longtitude],
                "message_id": message_data['mid']
            }
            return data
        elif "audio" == message_data['attachments'][0]['type']:
            audio_url = message_data['attachments'][0]['payload']['url']
            data = {
                "type": "audio",
                "data": audio_url,
                "message_id": message_data['mid']
            }
            return data
        elif "photo" == message_data['attactments'][0]['type']:
            photo_url = message_data['attachments'][0]['payload']['url']
            data = {
                "type": "audio",
                "data": photo_url,
                "message_id": message_data['mid']
            }
            return data
        else:
            data = {
                "type": "text",
                "data": "Sorry, I don't understand",
                "message_id": message_data['mid']
            }
            return data
    return None

if __name__ == '__main__':
    app.run(debug=False)
