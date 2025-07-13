import datetime
import os
import sys
from importlib.metadata import version
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

from program_settings import ProgramSettings


def get_python_version() -> str:
    return f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}'


def get_connection_string() -> str:
    """
    Get a connection string for MongoDB using the key/values stored in the .env file.
    :return: a string containing the connection string.
    """
    template: str = ProgramSettings.get_setting('MONGODB_CONNECTION_TEMPLATE')
    uid: str = ProgramSettings.get_setting('MONGODB_UID')
    pwd: str = ProgramSettings.get_setting('MONGODB_PWD')

    conn_string = f'mongodb+srv://{uid}:{pwd}@{template}'
    # print(f'{conn_string=}')
    return conn_string


def get_connection() -> MongoClient:
    """get a client connection to my personal MongoDB Atlas cluster using my personal usrid and password"""
    connection_string: str = get_connection_string()
    connection: MongoClient = MongoClient(connection_string)
    return connection


def display_databases():
    connection: MongoClient = get_connection()
    dbs = connection.list_database_names()
    print(f'databases in current connection:')
    for db in dbs:
        print(f'\t{db}')


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
    val = ProgramSettings.get_setting('KEEP_EXISTING_DATA')
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
    db_name: str = ProgramSettings.get_setting('MONGODB_DATABASE_NAME')
    db: mongoDatabase = get_database(db_name)
    col_name: str = ProgramSettings.get_setting('MONGODB_COLLECTION_NAME')
    collection: mongoCollection = db[col_name]
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
    expiry = date.today() + relativedelta(days = 1)
    # print(f'expiry = {expiry}')
    # add 12 months to that date
    expiry += relativedelta(months = 12)
    # print(f'expiry = {expiry}')
    # convert a date to a datetime that goes to the end of the day
    expiry = datetime.combine(expiry, datetime.max.time())
    # print(f'expiry = {expiry}')
    item_3 = {
        "item_name": "Bread",
        "quantity": 2,
        "ingredients": "all-purpose flour",
        "expiry_date": expiry
    }
    # print(f'item #3: {item_3}')
    db_name: str = ProgramSettings.get_setting('MONGODB_DATABASE_NAME')
    db: mongoDatabase = get_database(db_name)
    col_name: str = ProgramSettings.get_setting('MONGODB_COLLECTION_NAME')
    collection: mongoCollection = db[col_name]
    collection.insert_one(item_3)


def select_all(database_name: str, collection_name: str):
    db: mongoDatabase = get_database(database_name)
    collection: mongoCollection = db[collection_name]
    items_collection: mongoCursor = collection.find()
    display_collection(items_collection)


def filter_collection(database_name: str, collection_name: str, kvp: dict):
    """Filter the specified collection in the specified database using the specified key/value pair and display the results"""
    # print(kvp)
    db: mongoDatabase = get_database(database_name)
    collection: mongoCollection = db[collection_name]
    items_collection: mongoCursor = collection.find(kvp)
    display_collection(items_collection)


def index_exists(database_name: str, collection_name: str, column_name: str) -> (bool, str):
    """Determine if an index on the specified column already exists."""
    db: mongoDatabase = get_database(database_name)
    collection: mongoCollection = db[column_name]
    indexes = collection.index_information()

    found_existing_index: bool = False
    index_name: str = ''

    for index_key in indexes.keys():
        index_value = indexes[index_key]
        # print(index)
        index_list: list = index_value['key']
        # print(index_list)
        col_tuple: tuple = index_list[0]
        # print(type(col_tup;e))
        col_name: str = col_tuple[0]
        # print(col_name)
        if col_name == column_name:
            found_existing_index = True
            index_name = index_key
            break

    return found_existing_index, index_name


def create_index(database_name: str, collection_name: str, column_name: str) -> str:
    """ create an index on the specified column and return the name of the index created """
    db: mongoDatabase = get_database(database_name)
    collection: mongoCollection = db[column_name]
    index_name = collection.create_index(column_name)
    return index_name


def display_collection(collection: mongoCollection):
    """display a MongoDB collection in a table with headers, allowing for missing fields"""
    # convert MongoDB collection to dataframe to help with missing key fields
    items = DataFrame(collection)
    print(items.to_markdown(index = False, tablefmt = 'grid'))


def get_mongodb_version() -> str:
    """return the MongoDB version being used"""
    client = get_connection()
    server_information = client.server_info()
    key: str = 'version'
    if key in server_information:
        version = server_information.get('version')
    else:
        version = 'unknown'

    return version

def get_required_package_names() -> list[str]:
    """
    read the requirements.txt file and return a sorted list of package names.
    :return: sorted list of package names
    :rtype: list[str
    """
    packages: list[str] = []
    with open('requirements.txt') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue  # skip blank lines and comments
            package = line.split('~')[0].strip()  # works for ~=, >=, ==, etc.
            packages.append(package)

    packages.sort()
    return packages

def get_package_version(package_name: str) -> str:
    return version(package_name)

if __name__ == '__main__':
    print(f"Python version: {get_python_version()}")

    package_names = get_required_package_names()

    for pkg in package_names:
        package_name = f'{pkg}'.ljust(18)
        try:
            print(f'{package_name}{get_package_version(pkg)}')
        except:
            print(e)

    package_name = 'MongoDB'.ljust(18)

    print(f'{package_name}{get_mongodb_version()}')

    # display_databases()
    """
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
        print(f'Existing index named {index_name} was found on column {column_to_index}')
    else:
        index_name = create_index(DATABASE_NAME, COLLECTION_NAME, column_to_index)
        print(f'New index named {index_name} has been created on column {column_to_index}')

    filter_collection(DATABASE_NAME, DATABASE_NAME, {'category': 'food'})
    """
