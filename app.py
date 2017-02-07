import os
import sys
import json
import datetime
import requests
import re
from flask import Flask, request

app = Flask(__name__)

"""
FB msg JSON
{
  "entry": [
    {
      "id": "250739768699976",
      "messaging": [
        {
          "message": {
            "mid": "mid.1486432321387:d0e4611277",
            "seq": 51407,
            "text": "hi"
          },
          "recipient": {
            "id": "250739768699976"
          },
          "sender": {
            "id": "1348096228598500"
          },
          "timestamp": 1486432321387
        }
      ],
      "time": 1486432321525
    }
  ],
  "object": "page"
}
"""


@app.route('/', methods=['GET'])
def verify():
    # when the endpoint is registered as a webhook, it must echo back
    # the 'hub.challenge' value it receives in the query arguments
    if request.args.get("hub.mode") == "subscribe" and request.args.get("hub.challenge"):
        if not request.args.get("hub.verify_token") == os.environ["VERIFY_TOKEN"]:
            return "Verification token mismatch", 403
        return request.args["hub.challenge"], 200

    msg = "It's now :" + str(datetime.datetime.now())
    return msg, 200


@app.route('/', methods=['POST'])
def webhook():
    # endpoint for processing incoming messaging events
    data = request.get_json()
    # log(data)  # you may not want to log every incoming message in production, but it's good for testing
    if data["object"] == "page":
        for entry in data["entry"]:
            for messaging_event in entry["messaging"]:
                if messaging_event.get("message"):  # someone sent us a message
                    sender_id = messaging_event["sender"]["id"]        # the facebook ID of the person sending you the message
                    recipient_id = messaging_event["recipient"]["id"]  # the recipient's ID, which should be your page's facebook ID
                    message_text = messaging_event["message"]["text"]  # the message's text

                    email, phone = extract_details(message_text)
                    return_msg = "Your email: " + email + ", and phone:" + phone + ". Is it correct?"

                    # todo: store name, contact, phone
                    send_message(sender_id, return_msg)

                if messaging_event.get("delivery") or \
                    messaging_event.get("optin") or \
                        messaging_event.get("postback"):
                    pass

    return "ok", 200

def extract_details(from_msg):
    """
    rtype: email, phone
    """
    email = re.search(r'[\w\.-]+@[\w\.-]+', from_msg)
    phone = re.search(r"\(?\b[2-9][0-9]{2}\)?[-. ]?[2-9][0-9]{2}[-. ]?[0-9]{4}\b", from_msg)
    return email.group(0), phone.group(0)


def send_message(recipient_id, message_text):
    #log("sending message to {recipient}: {text}".format(recipient=recipient_id, text=message_text))
    params = {"access_token": os.environ["PAGE_ACCESS_TOKEN"]}
    headers = {"Content-Type": "application/json"}
    data = json.dumps(
        {"recipient": {"id": recipient_id },
        "message": {"text": message_text }}
    )
    r = requests.post("https://graph.facebook.com/v2.6/me/messages", params=params, headers=headers, data=data)
    if r.status_code != 200:
        log(r.status_code)
        log(r.text)


def log(message):  # simple wrapper for logging to stdout on heroku
    print(str(message))
    sys.stdout.flush()


if __name__ == '__main__':
    app.run(debug=False)
