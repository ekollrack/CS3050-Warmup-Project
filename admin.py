import firebase_admin
from firebase_admin import credentials, firestore
import json
import sys

json_file = sys.argv[1]
firebase_admin.initialize_app()
db = firestore.client()

collection = db.collection("mountains")

with open(json_file, "r") as f:
    data = json.load(f)

for mountain in data:
    doc_id = mountain["Mountain Name"]
    collection.document(doc_id).set(mountain)

print("Data uploaded!")
