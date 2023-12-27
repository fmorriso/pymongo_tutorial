import datetime
import os
import sys
from datetime import *
import pandas as pd
# WARNING: to use the DataFrame to_markdown() method YOU MUST SEPARATELY INSTALL tabulate
from pandas import DataFrame

import pymongo
from dateutil import parser
from dateutil.relativedelta import *
from dotenv import load_dotenv
from pymongo import MongoClient
# alias some types to shorter ones to save on typing and avoid namespace collisions with other packages
from pymongo.collection import Collection as mongoCollection
from pymongo.database import Database as mongoDatabase
from pymongo.cursor import Cursor as mongoCursor

# GLOBAL constants (see get_environment_information() )
DATABASE_NAME: str = ''
COLLECTION_NAME: str = ''


def get_python_version() -> str:
    return f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}'

def get_environment_information():
    """initialize the global database name and collection names by reading them from the .env file"""
    global DATABASE_NAME,COLLECTION_NAME
    load_dotenv()
    DATABASE_NAME = os.getenv('DATABASE_NAME')
    COLLECTION_NAME = os.getenv('COLLECTION_NAME')

def get_connection() -> MongoClient:
    """get a client connection to my personal MongoDB Atlas cluster using my personal usrid and password"""
    uid = get_mongo_db_uid()
    pwd = get_mongo_db_pwd()
    clusterName = get_mongo_db_clusterName()
    connection_string = f'mongodb+srv://{uid}:{pwd}@{clusterName}.6sstkik.mongodb.net/'
    connection: mongoCollection = MongoClient(connection_string)
    return connection


def display_databases():
    connection: MongoClient = get_connection()
    dbs = connection.list_database_names()
    # print(type(dbs[0]))
    print(f'databases in current connection: {dbs}')


def display_collections(database_name: str):
    connection: MongoClient = get_connection()
    db: mongoDatabase = connection.get_database(database_name)
    collections = db.list_collection_names()
    print(f'collections in database {database_name}: {collections}')


def get_database(database_name: str) -> mongoDatabase:
    connection: MongoClient = get_connection()
    db: mongoDatabase = connection[database_name]
    # print(type(db))
    return db


def get_collection(database_name: str, collection_name: str) -> mongoCollection:
    db: mongoDatabase = get_database(database_name)
    collection: mongoCollection = db[collection_name]
    # print(type(collection))
    return collection


def get_mongo_db_uid() -> str:
    load_dotenv()
    return os.getenv('MONGODB_UID')


def get_mongo_db_pwd() -> str:
    load_dotenv()
    return os.getenv('MONGODB_PWD')


def get_mongo_db_clusterName() -> str:
    load_dotenv()
    return os.getenv('MONGODB_CLUSTER_NAME')

def keep_existing_data() -> bool:
    """Use entry in .env to determine if we keep all existing data or start from scratch"""
    load_dotenv()
    val = os.getenv('KEEP_EXISTING_DATA')
    retval = eval(val)
    return retval


def collection_exists(database_name: str, collection_name: str) -> bool:
    db: mongoDatabase = get_database(database_name)
    return collection_name in db.list_collection_names()


def drop_existing_collection(database_name: str, collection_name: str):
    db: mongoDatabase = get_database(database_name)
    # Does the collection still exist?
    exists: bool = collection_exists(db.name, collection_name)
    if exists:
        print(f'Before drop, collection {collection_name} exists? {exists}')
        collection: mongoCollection = db[collection_name]
        collection.drop()
    else:
        print(f'Collection {collection_name} does not exist in database {db.name}')
    exists: bool = collection_exists(db.name, collection_name)
    print(f'After  drop, collection {collection_name} exists? {exists}')


def create_user_1_collection() -> mongoCollection:
    db: mongoDatabase = get_database(DATABASE_NAME)
    collection: mongoCollection = db[COLLECTION_NAME]
    # print(type(collection))
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
    #print(f'expiry = {expiry}')
    # add 12 months to that date
    expiry += relativedelta(months=12)
    #print(f'expiry = {expiry}')
    # convert a date to a datetime that goes to the end of the day
    expiry = datetime.combine(expiry, datetime.max.time())
    #print(f'expiry = {expiry}')
    item_3 = {
        "item_name": "Bread",
        "quantity": 2,
        "ingredients": "all-purpose flour",
        "expiry_date": expiry
    }
    # print(f'item #3: {item_3}')

    db: mongoDatabase = get_database(DATABASE_NAME)
    collection: mongoCollection = db[COLLECTION_NAME]
    collection.insert_one(item_3)

def select_all(database_name: str, collection_name: str):
    db: mongoDatabase = get_database(database_name)
    collection: mongoCollection = db[COLLECTION_NAME]
    items_collection: mongoCursor = collection.find()
    display_collection(items_collection)

def filter_collection(database_name: str, collection_name: str, kvp: dict):
    print(kvp)
    db: mongoDatabase = get_database(database_name)
    collection: mongoCollection = db[COLLECTION_NAME]
    items_collection: mongoCursor = collection.find(kvp)
    display_collection(items_collection)

def index_exists(database_name: str, collection_name: str, column_name: str) -> (bool, str):
    """Determine if an index on the specified column already exists."""
    db: mongoDatabase = get_database(database_name)
    collection: mongoCollection = db[COLLECTION_NAME]
    indexes: dict = collection.index_information()
    
    found_existing_index: bool = False
    index_name: str = ''

    for index_key in indexes.keys():
        index_value = indexes[index_key]
        #print(index)
        index_list: list = index_value['key']
        #print(index_list)
        col_tuple: tuple = index_list[0]
        #print(type(col_tup;e))
        col_name: str = col_tuple[0]
        #print(col_name)
        if col_name == column_name:
            found_existing_index = True
            index_name = index_key
            break
    
    return found_existing_index, index_name

def create_index(database_name: str, collection_name: str, column_name: str) -> str:
    """ create an index on the specified column and return the name of the index created """
    db: mongoDatabase = get_database(database_name)
    collection: mongoCollection = db[COLLECTION_NAME]
    index_name = collection.create_index(column_name)
    return index_name


def display_collection(collection: mongoCollection):
    """display a MongoDB collection in a table with headers, allowing for missing fields"""
    # convert MongoDB collection to dataframe to help with missing key fields
    items = DataFrame(collection)
    print(items.to_markdown(index=False, tablefmt='grid'))


if __name__ == '__main__':
    print(f"Python version: {get_python_version()}")
    get_environment_information()
    display_databases()
    display_collections(DATABASE_NAME)

    keep_existing: bool = keep_existing_data()
    if not keep_existing:
        drop_existing_collection(DATABASE_NAME, COLLECTION_NAME)
        collection: mongoCollection = create_user_1_collection()
        display_collections(DATABASE_NAME)
        add_user_1_document()

    select_all(DATABASE_NAME, COLLECTION_NAME)
    column_to_index = 'category'
    found_existing_index, index_name = index_exists(DATABASE_NAME, COLLECTION_NAME, column_to_index)
    if found_existing_index:
        print(f'Index named {index_name} on column {column_to_index} already exists')
    else:
        index_name = create_index(DATABASE_NAME, COLLECTION_NAME, column_to_index)
        print(f'Index named {index_name} on column {column_to_index} has been created')

    filter_collection(DATABASE_NAME, DATABASE_NAME, {'category': 'food'})
