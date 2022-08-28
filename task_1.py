from bson.json_util import loads
from pymongo import MongoClient
from pprint import pprint


client = MongoClient('localhost', 27017)
db = client['SeriesDB']
collection = db['series']

with open("account.bson") as file:
    accounts = loads(file.read())

if not list(collection.find()):
    accounts_collection = collection.insert_many(accounts)

first_results = collection.aggregate(
    [
        {"$unwind": {"path": "$sessions"}},
        {"$unwind": {"path": "$sessions.actions"}},
        {"$group": {"_id": {"_id": "$_id", "number": "$number", "type": "$sessions.actions.type"},
                    "count": {"$sum": 1},
                    "last": {"$max": "$sessions.actions.created_at"}}},
        {"$group": {"_id": "$_id._id", "number": {"$first": "$_id.number"},
                    "actions": {"$push": {"type": "$_id.type", "last": "$last", "count": "$count", }}}},
        {"$project": {"_id": 0}}
    ]
)

pprint(list(first_results))
