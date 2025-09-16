# admin.py
import json
import sys
from firebase_connection import database_connection


def main():
    json_file = sys.argv[1]
    # Connect to Firestore
    db = database_connection()
    collection = db.collection("mountains")

    # Load data from json file
    with open(json_file, "r") as f:
        data = json.load(f)

    # Upload documents to Firestore
    for mountain in data:
        doc_id = mountain["Mountain Name"]
        collection.document(doc_id).set(mountain)

    print("Data uploaded!")

if __name__ == "__main__":
    main()
