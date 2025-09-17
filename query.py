# query.py
from firebase_connection import database_connection
import pyparsing as pp

# ------------------ Firestore Connection ------------------
def get_collection():
    db = database_connection()
    return db.collection("mountains")

# ------------------ Help Menu ------------------
def print_help():
    print("""
Welcome to the Mountains Query Program!

Commands:
1. Show all details for a mountain:
   [Mountain Name]

2. Show a specific field for a mountain:
   [Field] [Mountain Name]  OR  [Mountain Name] [Field]

   Examples:
       Mount Everest Elevation
       Mount Hood Volcanic

3. Special commands:
   help    -- Show this help menu
   quit    -- Exit the program

Available fields:
   Mountain Name, Elevation, Location, Mountain Range, Volcanic, Last Eruption
""")

# ------------------ Show Mountain Details ------------------
def show_mountain_details(name, collection, field=None):
    doc_ref = collection.document(name)
    doc = doc_ref.get()
    if not doc.exists:
        print(f"No information found for '{name}'.\n")
        return

    data = doc.to_dict()
    field_mapping = {
        "Mountain Name": "Mountain Name",
        "Elevation": "Elevation (m)",
        "Location": "Location (country)",
        "Mountain Range": "Mountain Range",
        "Volcanic": "Volcanic",
        "Last Eruption": "Last Eruption"
    }

    if field is None:
        print(f"\nDetails for {name}:")
        for key, json_key in field_mapping.items():
            if json_key in data:
                print(f"{key}: {data[json_key]}")
        print()
    else:
        json_key = field_mapping.get(field.title())
        if json_key is None:
            print(f"Unknown field '{field}'. Available fields: {', '.join(field_mapping.keys())}\n")
        else:
            if field.lower() == "volcanic":
                print(f"Is {name} Volcanic? {data.get(json_key, 'N/A')}\n")
            else:
                print(f"{field.title()} of {name}: {data.get(json_key, 'N/A')}\n")

# ------------------ PyParsing Setup ------------------
def parse_input(user_input, mountain_names, valid_fields):
    # Case-insensitive field keywords
    field_parser = pp.MatchFirst([pp.CaselessKeyword(f) for f in valid_fields]).set_results_name("field")

    # Mountain names: all words until a field (non-greedy)
    mountain_parser = pp.OneOrMore(pp.Word(pp.alphanums + "-'")).set_results_name("mountain")

    # Explicit grammars
    grammar1 = field_parser + mountain_parser                        # Field then Mountain
    grammar2 = pp.SkipTo(field_parser).set_results_name("mountain") + field_parser  # Mountain then Field
    grammar3 = mountain_parser                                       # Just Mountain

    # Try Field + Mountain
    try:
        result = grammar1.parse_string(user_input, parse_all=True)
        field_candidate = result.get("field")
        mountain_candidate = " ".join(result.get("mountain", []))
    except pp.ParseException:
        # Try Mountain + Field
        try:
            result = grammar2.parse_string(user_input, parse_all=True)
            field_candidate = result.get("field")
            mountain_candidate = result.get("mountain", "").strip()
        except pp.ParseException:
            # Try Mountain only
            try:
                result = grammar3.parse_string(user_input, parse_all=True)
                field_candidate = None
                mountain_candidate = " ".join(result.get("mountain", []))
            except pp.ParseException:
                return None, None

    # Normalize mountain name
    mountain_name = None
    for name in mountain_names:
        if name.lower() == mountain_candidate.lower():
            mountain_name = name
            break

    # Normalize field
    field_name = None
    if field_candidate:
        for f in valid_fields:
            if f.lower() == field_candidate.lower():
                field_name = f
                break

    return mountain_name, field_name


# ------------------ Execute Command ------------------
def execute_command(user_input, collection):
    user_input_lower = user_input.lower().strip()
    if not user_input_lower:
        return
    if user_input_lower == "help":
        print_help()
        return
    if user_input_lower == "quit":
        exit()

    all_docs = collection.stream()
    mountain_names = [doc.id for doc in all_docs]
    valid_fields = ["Mountain Name", "Elevation", "Location", "Mountain Range", "Volcanic", "Last Eruption"]

    mountain_name, field = parse_input(user_input, mountain_names, valid_fields)
    if mountain_name:
        show_mountain_details(mountain_name, collection, field)
    else:
        print(f"No information found for '{user_input}'. Type 'help' for guidance.\n")

# ------------------ Interactive Loop ------------------
def run_query():
    collection = get_collection()
    print("Welcome to Mountains Query Program!")
    print("Type 'help' for commands. Type 'quit' to exit.")

    while True:
        user_input = input("> ").strip()
        execute_command(user_input, collection)

# ------------------ Entry Point ------------------
if __name__ == "__main__":
    run_query()
