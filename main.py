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


def get_database() -> pymongo.database.Database:
    pwd = get_mongo_db_pwd()
    CONNECTION_STRING = f'mongodb+srv://frederickmorrison1953:{pwd}@pymongocluster.6sstkik.mongodb.net/'

    client = MongoClient(CONNECTION_STRING)
    db = client['user_shopping_list']
    print(type(db))
    return db


def get_mongo_db_pwd() -> str:
    load_dotenv()
    return os.getenv('MONGODB_PWD')


def create_user_1_collection():
    db = get_database()
    collection_name = db['user_1_items']
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

    collection_name.insert_many([item_1, item_2])

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

    db = get_database()
    collection_name = db['user_1_items']
    collection_name.insert_one(item_3)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print(f"Python version: {get_python_version()}")
    create_user_1_collection()
    # add_user_1_document()
