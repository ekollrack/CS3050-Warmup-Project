# The admin program will read data from a JSON file saved locally and will initialize and upload the data
# to a Google Firebase Cloud Datastore (not a Firebase Realtime Database). You’ll run this program one
# time. If you run it a second time, it should delete and recreate the datastore.

import firebase_admin
from firebase_admin import credentials, firestore
import json

cred = credentials.Certificate("mountains_private_key.json")
firebase_admin.initialize_app(cred)

db = firestore.client()

json_file = "mountains.json"

collection_name = "mountains"


coll_ref = db.collection(collection_name)
# Load JSON data
with open(json_file, "r") as f:
    data = json.load(f)

# Write JSON to Firestore
for key, value in data.items():
    coll_ref.document(key).set(value)

print("Data uploaded successfully!")


