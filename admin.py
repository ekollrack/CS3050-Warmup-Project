# admin.py
import json
import sys
from firebase_connection import database_connection
import subprocess  # to call query.py

# Mountain class
class Mountain:
    def __init__(self, name, elevation, location, mountain_range, volcanic=None):
        self.name = name
        self.elevation = elevation
        self.location = location
        self.mountain_range = mountain_range
        self.volcanic = volcanic

    @classmethod
    def from_dict(cls, data):
        return cls(
            name=data.get("Mountain Name"),
            elevation=data.get("Elevation"),
            location=data.get("Location"),
            mountain_range=data.get("Mountain Range"),
            volcanic=data.get("Volcanic")
        )

    def to_dict(self):
        data = {
            "Mountain Name": self.name,
            "Elevation (m)": self.elevation,
            "Location (country)": self.location,
            "Mountain Range": self.mountain_range
        }
        if self.volcanic is not None:
            data["Volcanic"] = self.volcanic
        return data

def main():

    json_file = sys.argv[1]
    # Connect to Firestore
    db = database_connection()
    collection = db.collection("mountains")

    # Load and upload data
    with open(json_file, "r") as f:
        data = json.load(f)

    for mountain_data in data:
        mountain = Mountain.from_dict(mountain_data)
        doc_id = mountain.name
        collection.document(doc_id).set(mountain.to_dict())

    print("Data uploaded!")

    # Call query.py after upload
    subprocess.run(["python3", "query.py"])

if __name__ == "__main__":
    main()
