# -*- coding: utf-8 -*-
"""
    MongoDb Util functions
    Providing helper functions and auto create convenient collection access (example users.find())
    Support multi mongodb instance

    Author: Hai Nguyen (Jin) haibeo at gmail dot com / skype jiimmy.hai
    Date: 03/2016
"""
from backend.sangia import app

################################
# Base functions


def insert_one(collection=None, query={}, mongodb="mongo"):
    """ Insert one document to a collection
        Args:
            :param collection: (string) Mongodb collection name
            :param query: (dict) The document to insert. Must be a mutable mapping type. If the document does not have an _id field one will be added automatically.
        Returns:
            An instance of InsertOneResult if success
            None if failed
    """
    if check_one_query(collection=collection, query=query):
        return get_db_instance(mongodb=mongodb).db[collection].insert_one(query)
    else:
        return None

def update_many(collection=None, query={}, update=None, mongodb="mongo"):
    """ Update many document in a collection
        Args:
            :param collection: (string) Mongodb collection name
            :param query: (dict) Document query data
    """
    if check_one_query(collection=collection, query=query):
        return get_db_instance(mongodb=mongodb).db[collection].update_many(query, update)
    else:
        return None


def update_one(collection=None, query={}, update=None, mongodb="mongo"):
    """ Update one document in a collection
        Args:
            :param collection: (string) Mongodb collection name
            :param query: (dict) Document query data
    """
    if check_one_query(collection=collection, query=query):
        return get_db_instance(mongodb=mongodb).db[collection].update_one(query, update)
    else:
        return None


def find_one(collection=None, query={}, mongodb="mongo"):
    """ Find one document from a colllection
        Args:
            :param collection: (string) Mongodb collection name
            :param query: (dict) Document query data
    """
    if check_one_query(collection=collection, query=query):
        return get_db_instance(mongodb=mongodb).db[collection].find_one(query)
    else:
        return None


def find_by_id(collection=None, id_array=None, mongodb="mongo"):
    return find(mongodb=mongodb, collection=collection, query={"_id": {"$in": id_array}}, limit=0)


def find(collection=None, query={}, limit=20, skip=0, sort=None, sort_field =None, sort_order = -1, mongodb="mongo"):
    """ Find documents from a collection
        Args:
            :param collection: (string) Mongodb collection name
            :param query: (dict) Document query data
            :param limit: (int) Number of documents to get
            :param skip: (int) Number of documents to skip
            :param sort: (str) Currently support "id_desc" or "id_asc" for sorting by document id
    """
    if check_find_query(collection=collection, query=query, limit=limit, skip=skip):
        # Get the cursor from mongodb
        cursor = get_db_instance(mongodb=mongodb).db[collection].find(
            query).limit(limit).skip(skip)
        if sort is not None:
            if sort == "id_desc":
                # Sort cursor by id descending
                cursor.sort('_id', -1)
            elif sort == "id_asc":
                # Sort cursor by id ascending
                cursor.sort('_id', 1)
        if sort_field is not None:
            cursor.sort(sort_field,sort_order)
        return cursor
    else:
        return None


def delete_one(collection=None, query={}, mongodb="mongo"):
    """ Delete one document from a collection
        Args:
            :param collection: (string) Mongodb collection name
            :param query: (dict) Document query data
    """
    if check_one_query(collection=collection, query=query):
        return get_db_instance(mongodb=mongodb).db[collection].delete_one(query)
    else:
        return None

################################
# Helper functions


def get_db_instance(mongodb="mongo"):
    """ Get mongo instance by prefix

        :param mongodb: default = "mongo"
    """

    if isinstance(app.config['MGDB_PREFIX'],str) and mongodb == "mongo":
        return app.mongo
    else:
        db_key = "mgdb_" + mongodb
        db_instance = getattr(app, db_key)
        return db_instance


def check_one_query(collection=None, query=None):
    """ Check query params of 'do-one' function
    """
    return isinstance(collection, str) and isinstance(query, dict)


def check_find_query(collection=None, query=None, limit=None, skip=None):
    """ Check query params of find function
    """
    return (isinstance(collection, str)
            and isinstance(query, dict)
            and isinstance(limit, int)
            and isinstance(skip, int))


class MongoCollection():

    def __init__(self, mongodb="mongo", collection=None):
        """ Init new db collection
        """
        self.collection = collection
        self.mongodb = mongodb

    def find_one(self, query={}):
        return find_one(collection=self.collection, query=query, mongodb=self.mongodb)

    def find(self, query={}, limit=20, skip=0, sort=None, sort_field=None, sort_order=-1):
        return find(collection=self.collection, query=query,
                    limit=limit, skip=skip, sort=sort, sort_field=sort_field, sort_order=sort_order,
                    mongodb=self.mongodb)

    def find_by_id(self, id_array=None):
        return find(collection=self.collection, query={"_id": {"$in": id_array}},
                    mongodb=self.mongodb,limit=0)

    def update_one(self, query={}, update={}):
        return update_one(collection=self.collection, query=query,
                          update=update,  mongodb=self.mongodb)

    def update_many(self, query={}, update={}):
        return update_many(collection=self.collection, query=query,
                          update=update,  mongodb=self.mongodb)

    def insert_one(self, query={}):
        return insert_one(collection=self.collection, query=query,
                          mongodb=self.mongodb)

    def delete_one(self, query={}):
        return delete_one(collection=self.collection, query=query,
                          mongodb=self.mongodb)

"""
    Auto create convenient collection accessor:
    users.find(), posts.update(), sessions.insert_one()


    Single mongodb instance:
    If mongodb prefix is "MONGO", and has collection 'users','posts'
    => can access via mongo_users, mongo_posts


    Multi mongodb instance:
    If mongodb 1 is "MONGO", and has collection 'users','posts'
    => can access via mongo_users, mongo_posts

    If mongodb 2 is "SANGIA", and has collection 'users','posts'
    => can access via sangia_users, sangia_posts

"""

mgdb_prefix = app.config['MGDB_PREFIX']
if isinstance(mgdb_prefix, str):
    for collection in app.config[mgdb_prefix + '_COLLECTIONS']:
        globals()[mgdb_prefix.lower() + "_" +
                  collection] = MongoCollection(collection=collection)
elif isinstance(mgdb_prefix, list):
    for prefix in mgdb_prefix:
        for collection in app.config[prefix + '_COLLECTIONS']:
            globals()[prefix.lower() + "_" + collection] = MongoCollection(
                mongodb=prefix.lower(), collection=collection)
