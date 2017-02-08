# -*- coding: utf-8 -*-
"""
    Document utils

    Providing convenient functions for map/join a mongodb document with many other collections (even from different mongodb! ) with minimum query
    Example:

    Collect user data from document[user_id] => document[user_data], [post_id] => document[post_data], ...
    Remove some unused key in [user_data] before mapping to document[user_data]
    All of them in a single call :)

    Support for both modeled class / base collection mapping
    Return type will always be dict list in case you want collection mapping

    Author: Hai Nguyen (Jin) haibeo at gmail dot com / skype jiimmy.hai
"""


from bson.objectid import ObjectId
import inspect


def map_models(cursor=None, documents=None,
               doc_keys_to_remove=None, doc_keys_to_rename=None,
               classes_to_map=None, collections_to_map=None,
               doc_class=None, doc_collection=None, return_type="model"):
    """
        :param doc_class:                   Model Class - Document model to create
        :param doc_collection:              DbCollection - mongo created collection
        :param doc_keys_to_remove:          List - Keys to remove from the documents
        :param doc_keys_to_rename:          Dict List - Keys to rename in the documents
        :param classes_to_map:              Dict List - Document models to map
        :param collections_to_map:          Dict List - Document collections to map
        :param cursor:                      Cursor - Document cursor
        :param documents:                   List - Document modeled objects
        :param return_type:                 Return data type

        Example use:

        classes_to_map = [{"class": ClientUser, "keys": [
                                                {"id": "owner_user_id", # will read document[owner_user_id]
                                                    "data": "owner_user_data"}, # will set document[owner_user_data]
                                                {"id": "target_user_id",
                                                    "data": "target_user_data"}
                                                        ]},...
                         ]

        collections_to_map = [
        {
                    "collection": sangia_users, "key_id": "user_id",
                    "key_data": "user_data", "keys_to_remove": [], "keys_to_rename": []
        },  ...]

    """

    # First we need the mongodb cursor
    if cursor is not None:

        # Then main document model and models to map data to it :)
        if doc_class is not None:
            if documents is None:

                # Create list of document model instance if not setted
                documents = map_with_class(doc_class=doc_class,
                                           cursor=cursor, return_type=return_type,
                                           doc_keys_to_remove=doc_keys_to_remove)

            if documents is not None:

                # Remove un-needed keys from document if required
                if doc_keys_to_remove is not None:
                    for document in documents:
                        if isinstance(document, doc_model):
                            for key in keys_to_remove:
                                delattr(document, key)
                        elif isinstance(document, dict):
                            for key in keys_to_remove:
                                document.pop(key, None)

                # Rename some keys if required
                if doc_keys_to_rename is not None:
                    for document in documents:
                        if isinstance(document, dict):
                            for old_key, new_key in doc_keys_to_rename.iteritems():
                                document[new_key] = document.pop(old_key)

                if classes_to_map is not None:
                    # Map each model to our documents
                    for map_class in classes_to_map:

                        # Map many keys in target model
                        documents = map_with_class(doc_class=doc_class,
                                                   map_class=map_class[
                                                       'class'],
                                                   cursor=cursor,
                                                   documents=documents,
                                                   map_keys=map_class['keys'],
                                                   return_type=return_type)

        # If we need to map plain dict collection
        elif doc_collection is not None:
            if documents in None:

                # Create a list of dicts and removed some keys if required
                documents = map_with_collection(doc_collection=doc_collection,
                                                cursor=cursor,
                                                keys_to_remove=keys_to_remove)

            if documents is not None:
                # Remove un-needed keys from document if required
                if doc_keys_to_remove is not None:
                    for document in documents:
                        for key in keys_to_remove:
                            document.pop(key, None)

                # Rename some keys if required
                if doc_keys_to_rename is not None:
                    for document in documents:
                        for old_key, new_key in doc_keys_to_rename.iteritems():
                            document[new_key] = document.pop(old_key)
                if collections_to_map is not None:
                    # Map each collection data to our documents
                    for collection in collections_to_map:

                        documents = map_with_collection(doc_collection=doc_collection,
                                                        map_collection=collection[
                                                            'collection'],
                                                        cursor=cursor,
                                                        documents=documents,
                                                        keys=collection['keys'])

    return documents


def map_with_class(doc_class=None, map_class=None, map_keys=None,
                   cursor=None, documents=None, doc_keys_to_remove=None,
                   return_type="model"):
    """
        Map a document with another model, based by key

        :param doc_class:               Document class
        :param map_class:               Map class
        :param map_keys:                Map keys
        :param cursor:                  Mongodb query cursor
        :param documents:               Existing documents
        :param doc_keys_to_remove:      Document keys to remove
        :param return_type:             Return data type (model/dict)
    """

    # Check the mongodb cursor
    if cursor is not None:

        # Create some key list to match our data later
        if map_keys is not None:
            map_keys_data = {}
            for key in map_keys:
                map_keys_data[dyn_key_list(key['id'])] = []   # Hold key ids
                map_keys_data[dyn_key_data(key['id'])] = {}   # Hold key data

        # We need the main document model
        if doc_class is not None:

            # If we want to map our document with other model
            if map_class is not None and map_keys is not None:
                map_class_ids = []
                map_class_data = {}

                # Create a list of modeled documents if we have nothing
                if documents is None:
                    documents = []
                    for document in cursor:

                        if return_type == "model":
                            documents.append(doc_class(document=document))
                        elif return_type == "dict":
                            documents.append(document)

                        for key in map_keys:
                            if key["id"] in document and document[key["id"]] is not None:
                                map_class_ids.append(
                                    ObjectId(document[key["id"]]))

                                # Append data for later matching
                                map_keys_data[dyn_key_list(key['id'])].append(
                                    dyn_key_map(document[key['id']]))

                else:

                    # Create id lists for later matching
                    for document in documents:
                        for key in map_keys:
                            if isinstance(document, doc_class):
                                if hasattr(document, key["id"]) and getattr(document, key["id"]) is not None:
                                    map_class_ids.append(
                                        ObjectId(getattr(document, key["id"])))
                                    map_keys_data[dyn_key_list(key['id'])].append(
                                        getattr(document, key["id"]))
                            elif isinstance(document, dict):
                                if key["id"] in document and document[key["id"]] is not None:
                                    map_class_ids.append(
                                        ObjectId(document[key["id"]]))
                                    map_keys_data[dyn_key_list(key['id'])].append(
                                        document[key['id']])

                # We should only need 1 query to get all of the field data
                # Since we provided the key model here, we ran find_by_id on it
                # (ref model class + mongo_util)
                map_class_ids = list(set(map_class_ids))
                map_cursor = map_class.find_by_id(map_class_ids)

                # Build field data dict
                for map_document in map_cursor:
                    map_id = str(map_document['_id'])
                    for key in map_keys:
                        if map_id in map_keys_data[dyn_key_list(key['id'])]:
                            map_keys_data[dyn_key_data(key['id'])][
                                dyn_key_map(map_id)] = map_document

                # Remove and rename map key if required
                for key in map_keys:
                    for map_id, document in map_keys_data[dyn_key_data(key['id'])].iteritems():
                        if "keys_to_remove" in key:
                            for k in key["keys_to_remove"]:
                                document.pop(k, None)

                        if "keys_to_rename" in key:
                            for old_key, new_key in key["keys_to_rename"].iteritems():
                                document[new_key] = document.pop(old_key)

                # Map document keys
                for document in documents:
                    for key in map_keys:
                        if isinstance(document, doc_class):
                            if hasattr(document, key["id"]) and getattr(document, key["id"]) is not None:
                                map_id = dyn_key_map(
                                    getattr(document, key['id']))
                                setattr(document,
                                        key['data'],
                                        map_keys_data[dyn_key_data(key['id'])][map_id])
                        elif isinstance(document, dict):
                            if key["id"] in document and document[key["id"]] is not None:
                                map_id = dyn_key_map(document[key['id']])
                                document[key['data']] = map_keys_data[dyn_key_data(key['id'])][
                                    dyn_key_map(map_id)]

                return documents

            else:
                documents = []
                for document in cursor:

                    if doc_keys_to_remove is not None:
                        for key in doc_keys_to_remove:
                            document.pop(key, None)
                    if return_type == "model":
                        documents.append(doc_class(document=document))
                    elif return_type == "dict":
                        documents.append(document)

                return documents

    return None


def map_with_collection(doc_collection=None, map_collection=None, map_keys=None,
                        cursor=None, documents=None, doc_keys_to_remove=None):
    """
        Map a document with another collection, based by key

        :param doc_collection:          Document collection
        :param map_collection:          Map collection
        :param map_keys:                Map keys
        :param cursor:                  Mongodb query cursor
        :param documents:               Existing documents
        :param doc_keys_to_remove:      Document keys to remove

        documents = map_collection(doc_collection=doc_collection,
                             cursor=cursor,
                             keys_to_remove=keys_to_remove)


        documents = map_collection(doc_collection=doc_collection,
                                  map_collection=collection['collection'],
                                  cursor=cursor,
                                  documents=documents,
                                  keys=collection['keys'])

    """

    # Check the mongodb cursor
    if cursor is not None:

        # Create some key list to match our data later
        if map_keys is not None:
            map_keys_data = {}
            for key in map_keys:
                map_keys_data[dyn_key_list(key['id'])] = []   # Hold key ids
                map_keys_data[dyn_key_data(key['id'])] = {}   # Hold key data

        # We need the main document model
        if doc_collection is not None:

            # If we want to map our document with other model
            if map_collection is not None and map_keys is not None:
                map_collection_ids = []
                map_collection_data = {}

                # Create a list of modeled documents if we have nothing
                if documents is None:
                    documents = []
                    for document in cursor:

                        documents.append(document)
                        for key in map_keys:
                            if key["id"] in document and document[key["id"]] is not None:
                                map_collection_ids.append(
                                    ObjectId(document[key["id"]]))
                                # Append data for later matching
                                map_keys_data[dyn_key_list(key['id'])].append(
                                    dyn_key_map(document[key['id']]))

                else:

                    # Create id lists for later matching
                    for document in documents:
                        for key in map_keys:
                            if key["id"] in document and document[key["id"]] is not None:
                                map_collection_ids.append(
                                    ObjectId(document[key["id"]]))
                                map_keys_data[dyn_key_list(key['id'])].append(
                                    document[key['id']])

                # We should only need 1 query to get all of the field data
                # Since we provided the key model here, we ran find_by_id on it
                # (ref model class + mongo_util)
                #

                key_model_ids = list(set(key_model_ids))
                map_cursor = map_collection.find_by_id(key_model_ids)

                # Build field data dict
                for map_document in map_cursor:
                    map_id = str(map_document['_id'])
                    for key in map_keys:
                        if map_id in map_keys_data[dyn_key_list(key['id'])]:
                            map_keys_data[dyn_key_data(key['id'])][
                                dyn_key_map(map_id)] = map_document

                # Remove and rename map key if required
                for key in map_keys:
                    for map_id, document in map_keys_data[dyn_key_data(key['id'])].iteritems():
                        if "keys_to_remove" in key:
                            for k in key["keys_to_remove"]:
                                document.pop(k, None)

                        if "keys_to_rename" in key:
                            for old_key, new_key in key["keys_to_rename"].iteritems():
                                document[new_key] = document.pop(old_key)

                # Map document keys
                for document in documents:
                    for key in map_keys:
                        if key["id"] in document and document[key["id"]] is not None:
                            map_id = dyn_key_map(document[key['id']])
                            document[key['data']] = map_keys_data[dyn_key_data(key['id'])][                              dyn_key_map(map_id)]

                return documents

            else:
                documents = []
                for document in cursor:
                    document['_id'] = str(document['_id'])
                    if doc_keys_to_remove is not None:
                        for key in doc_keys_to_remove:
                            document.pop(key, None)
                    documents.append(document)

                return documents

    return None


def map_one_with_models(document=None,
                        doc_keys_to_remove=None, doc_keys_to_rename=None,
                        classes_to_map=None, collections_to_map=None,
                        doc_class=None, doc_collection=None, return_type="model"):

    # Then main document model and models to map data to it :)
    if doc_class is not None:
        if document is not None:

            # Remove un-needed keys from document if required
            if doc_keys_to_remove is not None:

                if isinstance(document, doc_model):
                    for key in keys_to_remove:
                        delattr(document, key)
                elif isinstance(document, dict):
                    for key in keys_to_remove:
                        document.pop(key, None)

            # Rename some keys if required
            if doc_keys_to_rename is not None:

                if isinstance(document, dict):
                    for old_key, new_key in doc_keys_to_rename.iteritems():
                        document[new_key] = document.pop(old_key)

            if classes_to_map is not None:
                # Map each model to our documents
                for map_class in classes_to_map:

                    # Map many keys in target model
                    document = map_one_with_class(doc_class=doc_class,
                                                  map_class=map_class['class'],
                                                  document=document,
                                                  map_keys=map_class['keys'],
                                                  return_type=return_type)

    # If we need to map plain dict collection
    elif doc_collection is not None:
        if document is not None:
            # Remove un-needed keys from document if required
            if doc_keys_to_remove is not None:
                for key in keys_to_remove:
                    document.pop(key, None)

            # Rename some keys if required
            if doc_keys_to_rename is not None:
                for old_key, new_key in doc_keys_to_rename.iteritems():
                    document[new_key] = document.pop(old_key)
            if collections_to_map is not None:
                # Map each collection data to our documents
                for collection in collections_to_map:

                    document = map_with_collection(doc_collection=doc_collection,
                                                   map_collection=collection[
                                                       'collection'],
                                                   document=document,
                                                   keys=collection['keys'])

    return document


def map_one_with_class(doc_class=None, map_class=None, map_keys=None,
                       document=None, doc_keys_to_remove=None,
                       return_type="model"):

    # Create some key list to match our data later
    if map_keys is not None:
        map_keys_data = {}
        for key in map_keys:
            map_keys_data[dyn_key_list(key['id'])] = []   # Hold key ids
            map_keys_data[dyn_key_data(key['id'])] = {}   # Hold key data

    # We need the main document model
    if doc_class is not None:

        # If we want to map our document with other model
        if map_class is not None and map_keys is not None:
            map_class_ids = []
            map_class_data = {}

            for key in map_keys:
                if isinstance(document, doc_class):
                    if hasattr(document, key["id"]) and getattr(document, key["id"]) is not None:
                        map_class_ids.append(
                            ObjectId(getattr(document, key["id"])))
                        map_keys_data[dyn_key_list(key['id'])].append(
                            getattr(document, key["id"]))
                elif isinstance(document, dict):
                    if key["id"] in document and document[key["id"]] is not None:
                        map_class_ids.append(
                            ObjectId(document[key["id"]]))
                        map_keys_data[dyn_key_list(key['id'])].append(
                            document[key['id']])

            # We should only need 1 query to get all of the field data
            # Since we provided the key model here, we ran find_by_id on it
            # (ref model class + mongo_util)
            map_class_ids = list(set(map_class_ids))
            map_cursor = map_class.find_by_id(map_class_ids)

            # Build field data dict
            for map_document in map_cursor:
                map_id = str(map_document['_id'])
                for key in map_keys:
                    if map_id in map_keys_data[dyn_key_list(key['id'])]:
                        map_keys_data[dyn_key_data(key['id'])][
                            dyn_key_map(map_id)] = map_document

            # Remove and rename map key if required
            for key in map_keys:
                for map_id, document in map_keys_data[dyn_key_data(key['id'])].iteritems():
                    if "keys_to_remove" in key:
                        for k in key["keys_to_remove"]:
                            document.pop(k, None)

                    if "keys_to_rename" in key:
                        for old_key, new_key in key["keys_to_rename"].iteritems():
                            document[new_key] = document.pop(old_key)

            # Map document keys

            for key in map_keys:
                if isinstance(document, doc_class):
                    if hasattr(document, key["id"]) and getattr(document, key["id"]) is not None:
                        map_id = dyn_key_map(
                            getattr(document, key['id']))
                        setattr(document,
                                key['data'],
                                map_keys_data[dyn_key_data(key['id'])][map_id])
                elif isinstance(document, dict):
                    if key["id"] in document and document[key["id"]] is not None:
                        map_id = dyn_key_map(document[key['id']])
                        document[key['data']] = map_keys_data[dyn_key_data(key['id'])][
                            dyn_key_map(map_id)]

        else:

            if doc_keys_to_remove is not None:
                for key in doc_keys_to_remove:
                    document.pop(key, None)
            if return_type == "model":
                document = doc_class(document=document)

    return document


def map_one_with_collection(doc_collection=None, map_collection=None, map_keys=None,
                            document=None, doc_keys_to_remove=None):

    # Create some key list to match our data later
    if map_keys is not None:
        map_keys_data = {}
        for key in map_keys:
            map_keys_data[dyn_key_list(key['id'])] = []   # Hold key ids
            map_keys_data[dyn_key_data(key['id'])] = {}   # Hold key data

    # We need the main document model
    if doc_collection is not None:

        # If we want to map our document with other model
        if map_collection is not None and map_keys is not None:
            map_collection_ids = []
            map_collection_data = {}


            # Create id lists for later matching

            for key in map_keys:
                if key["id"] in document and document[key["id"]] is not None:
                    map_collection_ids.append(
                        ObjectId(document[key["id"]]))
                    map_keys_data[dyn_key_list(key['id'])].append(
                        document[key['id']])

            # We should only need 1 query to get all of the field data
            # Since we provided the key model here, we ran find_by_id on it
            # (ref model class + mongo_util)
            #

            key_model_ids = list(set(key_model_ids))
            map_cursor = map_collection.find_by_id(key_model_ids)

            # Build field data dict
            for map_document in map_cursor:
                map_id = str(map_document['_id'])
                for key in map_keys:
                    if map_id in map_keys_data[dyn_key_list(key['id'])]:
                        map_keys_data[dyn_key_data(key['id'])][
                            dyn_key_map(map_id)] = map_document

            # Remove and rename map key if required
            for key in map_keys:
                for map_id, document in map_keys_data[dyn_key_data(key['id'])].iteritems():
                    if "keys_to_remove" in key:
                        for k in key["keys_to_remove"]:
                            document.pop(k, None)

                    if "keys_to_rename" in key:
                        for old_key, new_key in key["keys_to_rename"].iteritems():
                            document[new_key] = document.pop(old_key)

            # Map document keys

            for key in map_keys:
                if key["id"] in document and document[key["id"]] is not None:
                    map_id = dyn_key_map(document[key['id']])
                    document[key['data']] = map_keys_data[dyn_key_data(key['id'])][                           dyn_key_map(map_id)]





    return document



def dyn_key_data(key_id):
    return "{key_id}_data".format(**locals())


def dyn_key_list(key_id):
    return "{key_id}_list".format(**locals())


def dyn_key_map(key_id):
    return "id_{key_id}".format(**locals())
