import json
import sys
from firebase_connection import database_connection
import subprocess  # to call query.py

# Mountain class
class Mountain:
    def __init__(self, name, elevation, location, range_name, volcanic=None, last_eruption=None):
        self.name = name
        self.elevation = elevation
        self.location = location
        self.range_name = range_name
        self.volcanic = volcanic
        self.last_eruption = last_eruption

    @classmethod
    def from_dict(cls, data):
        return cls(
            name=data.get("MountainName") or data.get("Mountain Name"),
            elevation=data.get("Elevation"),
            location=data.get("Location"),
            range_name=data.get("Range"),
            volcanic=data.get("Volcanic"),
            last_eruption=data.get("LastEruption") or data.get("Last Eruption")
        )

    def to_dict(self):
        data = {
            "MountainName": self.name,
            "Elevation": self.elevation,
            "Location": self.location,
            "Range": self.range_name,
            "Volcanic": self.volcanic
        }
        if self.last_eruption is not None:
            data["LastEruption"] = self.last_eruption
        return data

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 admin.py mountains.json")
        return

    json_file = sys.argv[1]

    # Connect to Firestore
    db = database_connection()
    collection = db.collection("mountains")

    # Delete old documents
    old_docs = collection.stream()
    for doc in old_docs:
        collection.document(doc.id).delete()
    print("Old documents deleted.")

    # Load data
    with open(json_file, "r") as f:
        data = json.load(f)

    # Upload each mountain using its name as the document ID
    for mountain_data in data:
        mountain = Mountain.from_dict(mountain_data)
        if not mountain.name:
            print("Skipping entry with missing mountain name.")
            continue
        collection.document(mountain.name).set(mountain.to_dict())

    print("Data uploaded!")

    # Call query.py after upload
    subprocess.run(["python3", "query.py"])

if __name__ == "__main__":
    main()
