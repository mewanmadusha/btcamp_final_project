from pymongo import MongoClient

client = MongoClient("mongodb+srv://mewan:admin@clusterdemo.ueewbek.mongodb.net/?retryWrites=true&w=majority")

db = client.fastdb

collection_name = db["test"]