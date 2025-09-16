# query.py
from firebase_connection import database_connection

def print_help():
    print("""
Welcome to the Mountains Query Program!

You can query the mountains collection using the following commands:

1. Basic queries:
   - Find mountains by field values:
       field == value        (string or boolean equality)
       field > number        (numeric comparison)
       field < number        (numeric comparison)

   Examples:
       Mountain Name == Mount Everest
       Elevation > 8000
       Volcanic == false

2. Compound queries:
   - Combine two conditions using AND or OR:
       field1 == value AND field2 > number
       field1 == value OR field2 == value

   Example:
       Location == Nepal AND Elevation > 8000

3. Get a specific field for a mountain:
       field of [Mountain Name]

   Example:
       Elevation K2
       Location Mount Everest

4. Available fields:

   | Field Name           | Type    | Details                          |
   |----------------------|---------|-----------------------------------|
   | Mountain Name        | String  | Name of the mountain             |
   | Elevation            | Number  | Elevation in meters              |
   | Location             | String  | Country or countries             |
   | Mountain Range       | String  | Mountain range name              |
   | Volcanic             | Boolean | true if volcanic, false if not   |

5. Special commands:
       help    -- Show this help menu

""")

def run_query():

    while True:
        user_input = input("> ").strip()
        if not user_input:
            continue
        if user_input.lower() == "quit":
            break
        elif user_input.lower() == "help":
            print_help()
        else:
            print("Command not recognized. Type 'help' to see available commands.")

if __name__ == "__main__":
    run_query()

