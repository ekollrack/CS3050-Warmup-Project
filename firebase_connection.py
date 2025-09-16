"""
This file connects our code to firebase
"""

import firebase_admin
from firebase_admin import firestore

def database_connection():
    firebase_admin.initialize_app()
    return firestore.client()

