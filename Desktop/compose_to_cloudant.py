import json
import argparse
import pymongo
import datetime
import requests
from pymongo import MongoClient 
from bson import json_util
from pprint import pprint

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
	    date = list(obj.timetuple())[0:6]
	    encoded_object={}
	    encoded_object['year'] = date[0]
            encoded_object['month'] = date[1]
	    encoded_object['day'] = date[2]
	    encoded_object['hour'] = date[3]
   	    encoded_object['minutes'] = date[4]
	    encoded_object['seconds'] = date[5]
        else:
            encoded_object =json.JSONEncoder.default(self, obj)
        return encoded_object



parser = argparse.ArgumentParser(description='Help transfer data from Compose to Cloudant.')

parser.add_argument('--composeid', required=True, help='Replica Set URI')
parser.add_argument('--cloudantid', required=True, help='username:password')
parser.add_argument('--startdb', required=True, help='Which database to look in')
parser.add_argument('--collection', required=True, help='Which collection to look in')
parser.add_argument('--enddb', required=True, help='Which database to put it in')

args = vars(parser.parse_args())

MONGOHQ_URL=args.pop('composeid')

client = MongoClient(MONGOHQ_URL)

startdb = args.pop('startdb')
startdb = client[startdb]

collection = args.pop('collection')

cursor = startdb[collection].find()

docs = []

for doc in cursor:
	doc["_id"] = str(doc["_id"])
	docs.append(doc)

payload = { 'docs' : docs }

cloudantid = args.pop('cloudantid').split(':',1)
USERNAME = cloudantid[0]
PASSWORD = cloudantid[1]

enddb = args.pop('enddb')

CLOUDANT_URI = "https://{0}.cloudant.com/{1}/_bulk_docs".format(USERNAME, enddb)

creds = (str(USERNAME), str(PASSWORD))

response = requests.post(CLOUDANT_URI, data=json.dumps(payload, cls=DateTimeEncoder), auth = creds, headers={"Content-Type": "application/json"})

response.raise_for_status()
print response.reason
