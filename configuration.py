import os

class Config(object):
    MGDB_PREFIX = "MONGO"
    MONGO_URI = os.environ["MONGO_URI"]
    MONGO_DBNAME = "contact_bot"
    MONGO_COLLECTIONS = ['contacts']
