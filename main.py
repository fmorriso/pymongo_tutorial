import datetime
import os
import sys

import pymongo
from datetime import *

from dotenv import load_dotenv
from pymongo import MongoClient
from dateutil import parser
from dateutil.relativedelta import *


def get_python_version() -> str:
    return f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}'


def get_connection() -> MongoClient:
    pwd = get_mongo_db_pwd()
    CONNECTION_STRING = f'mongodb+srv://frederickmorrison1953:{pwd}@pymongocluster.6sstkik.mongodb.net/'
    # print(CONNECTION_STRING)
    connection = MongoClient(CONNECTION_STRING)
    # print(type(connection))
    return connection

def display_databases():
    connection: MongoClient = get_connection()
    dbs = connection.list_database_names()
    #print(type(dbs[0]))
    print(f'databases in current collection: {dbs}')

def display_collections(database_name: str):
    connection: MongoClient = get_connection()
    db = connection.get_database(database_name)
    collections = db.list_collection_names()
    print(f'collections in database {database_name}: {collections}')


def get_database(database_name:str) -> pymongo.database.Database:
    connection: MongoClient = get_connection()
    db = connection[database_name]
    #print(type(db))
    return db

def get_collection(database_name:str, collection_name: str) -> pymongo.collection.Collection:
    db = get_database(database_name)
    collection = db[collection_name]
    # print(type(collection))
    return collection

def get_mongo_db_pwd() -> str:
    load_dotenv()
    return os.getenv('MONGODB_PWD')

# THIS METHOD IS NOT ACTUALLYU DROPPING THE SPECIFIED COLLECTION - WHY ???
def drop_existing_collection():
    db = get_database('user_shopping_list')
    # Does the collection still exist?
    print('user_1_items' in db.list_collection_names())
    collection = db['user_1_items']
    collection.drop()
    display_collections(db.name)

def create_user_1_collection() -> pymongo.collection.Collection:
    db = get_database('user_shopping_list')
    collection = db['user_1_items']
    print(type(collection))
    # now add some documents to the collection
    item_1 = {
        "_id": "U1IT00001",
        "item_name": "Blender",
        "max_discount": "10%",
        "batch_number": "RR450020FRG",
        "price": 340,
        "category": "kitchen appliance"
    }

    item_2 = {
        "_id": "U1IT00002",
        "item_name": "Egg",
        "category": "food",
        "quantity": 12,
        "price": 36,
        "item_description": "brown country eggs"
    }

    collection.insert_many([item_1, item_2])

    return collection

def add_user_1_document():
    # start with the day after today ...
    expiry = date.today() + relativedelta(days=1)
    print(f'expiry = {expiry}')
    # add 12 months to that date
    expiry += relativedelta(months=12)
    print(f'expiry = {expiry}')
    # convert a date to a datetime that goes to the end of the day
    expiry = datetime.combine(expiry, datetime.max.time())
    print(f'expiry = {expiry}')
    item_3 = {
        "item_name": "Bread",
        "quantity": 2,
        "ingredients": "all-purpose flour",
        "expiry_date": expiry
    }
    print(item_3)

    db = get_database('user_shopping_list')
    collection: pymongo.collection.Collection = db['user_1_items']
    collection.insert_one(item_3)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print(f"Python version: {get_python_version()}")
    display_databases()
    display_collections('user_shopping_list')
    drop_existing_collection()
    collection = create_user_1_collection()
    display_collections('user_shopping_list')
    #add_user_1_document()

