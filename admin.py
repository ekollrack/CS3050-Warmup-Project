import firebase_admin
from firebase_admin import credentials, firestore
import json
import sys

json_file = sys.argv[1]

cred = credentials.Certificate('/Users/elisabethkollrack/Desktop/CS 3050/mountains_private_key.json') # Need to fix
firebase_admin.initialize_app(cred)
db = firestore.client()


collection = db.collection("mountains")

with open(json_file, "r") as f:
    data = json.load(f)

for mountain in data:
    doc_id = mountain["Mountain Name"]
    collection.document(doc_id).set(mountain)

print("Data uploaded!")
