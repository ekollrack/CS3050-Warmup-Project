from firebase_connection import database_connection
import pyparsing as pp
import warnings
warnings.filterwarnings("ignore")

# Connect to firebase_connection.py (which connects to firestore)
# References "mountains" collection
def get_collection():
    db = database_connection()
    return db.collection("mountains")

# Returns instructions for running the program
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
       Mount Hood Range

3. Comparison queries:
   [Field] [Operator] [Value]

   Examples:
       Elevation < 5000
       Volcanic == True
       Location == USA
       Range != Himalayas

4. Special commands:
   help    -- Show this help menu
   quit    -- Exit the program

Available fields:
   MountainName, Elevation, Location, Range, Volcanic, LastEruption
""")

# This function is for queries that mention the mountain name or mountain name + field (normal queries)
# Returns the details for the mountain or the specified field
# Parameters - name (str, mountain name), collection (collection in Firestore), field (str, optional)
def show_mountain_details(name, collection, field=None):
    # uses the mountain name to find it in Firestore
    doc_ref = collection.document(name)
    doc = doc_ref.get()
    if not doc.exists:
        print(f"No information found. Type 'help' for guidance. \n")
        return

    # Maps MountainName to Mountain Name and Last Eruption to LastEruption
    data = doc.to_dict()
    field_mapping = {
        "Mountain Name": "MountainName",
        "Elevation": "Elevation",
        "Location": "Location",
        "Range": "Range",
        "Volcanic": "Volcanic",
        "Last Eruption": "LastEruption"
    }

    # Show mountain details if no field was specified
    if field is None:
        print(f"\nDetails for {name}:")
        for key, json_key in field_mapping.items():
            if json_key in data:
                print(f"{key}: {data[json_key]}")
        print()

    # Show one field info
    else:
        # Normalize field name
        json_key = None
        field_input = field.lower().replace(" ", "")
        display_name = None
        for k, mapped in field_mapping.items():
            if field_input == k.lower().replace(" ", "") or field_input == mapped.lower().replace(" ", ""):
                json_key = mapped
                display_name = k
                break

        if not json_key:
            print("No information found. Type 'help' for guidance.\n")
            return

        if field.lower() == "volcanic":
            print(f"Is {name} Volcanic? {data.get(json_key, 'N/A')}\n")
        else:
            print(f"{display_name} of {name}: {data.get(json_key, 'N/A')}\n")


def sort(collection, sort_type, ascend):
    if sort_type == "name":
        sort_name(collection, ascend)


def sort_name(collection, ascend):
    docs = collection.stream()
    print("docs")
    names = []
    for doc in docs:
        names.append(doc.get("MountainName"))
    names.sort(reverse = True)
    print(names)

# Parse user input
# Returns dictionary separating the type of query (comparison, compound, mountain, mountain + field)
def parse(user_input, mountain_names, valid_fields):
    # Fields (multi-word allowed)
    fields = ["Mountain Name", "Elevation", "Location", "Range", "Volcanic", "Last Eruption"]
    field_parser = pp.MatchFirst([pp.CaselessKeyword(f) for f in fields]).set_results_name("field")

    # Operators
    operator_parser = pp.MatchFirst([pp.Literal(op) for op in ["==", "!=", ">=", "<=", "=", ">", "<"]]).set_results_name("operator")

    # Logical AND/OR
    logical_op = pp.MatchFirst([pp.CaselessKeyword("and"), pp.CaselessKeyword("or")]).set_results_name("logic")

    # Values: allow multi-word unquoted strings (mountain names, ranges, etc.)
    value_parser = pp.Combine(pp.OneOrMore(pp.Word(pp.alphanums + "-/.'"))).set_results_name("value")

    # Comparison and compound queries
    comparison = pp.Group(field_parser("field") + operator_parser("operator") + value_parser("value"))
    compound = pp.Group(comparison("first") + logical_op("logic") + comparison("second"))

    # Try compound query first
    try:
        result = compound.parse_string(user_input, parse_all=True)[0]
        return {
            "type": "compound",
            "conditions": [
                {"field": result.first.field, "operator": result.first.operator, "value": result.first.value},
                {"field": result.second.field, "operator": result.second.operator, "value": result.second.value}
            ],
            "logic": result.logic.lower()
        }
    except pp.ParseException:
        pass

    # Single comparison query
    try:
        result = comparison.parse_string(user_input, parse_all=True)[0]
        return {
            "type": "comparison",
            "field": result.field,
            "operator": result.operator,
            "value": result.value
        }
    except pp.ParseException:
        pass

    # Mountain name only (normal queries)
    mountain_parser = pp.Combine(pp.OneOrMore(pp.Word(pp.alphanums + "-.'"))).set_results_name("mountain")
    try:
        result = mountain_parser.parse_string(user_input, parse_all=True)
        mountain_candidate = result.mountain
    except pp.ParseException:
        return None

    # Match mountain name
    mountain_name = None
    for name in mountain_names:
        if name.lower() == mountain_candidate.lower():
            mountain_name = name
            break

    return {"type": "normal", "field": None, "value": None, "operator": None, "mountain_name": mountain_name}



# This function takes the dictionary from the parsed query and prints the results
def query(params, collection):
    if params is None:
        print("Invalid input. Type 'help' for guidance.\n")
        return

    all_docs = [doc.to_dict() for doc in collection.stream()]
    results = []

    # Normalize parser field names -> Firestore JSON keys
    field_map = {
        "last eruption": "LastEruption",
        "mountain name": "MountainName",
        "range": "Range",
        "location": "Location",
        "elevation": "Elevation",
        "volcanic": "Volcanic"
    }

    # Convert LastEruption from string to number
    def convert_val(compare_field, val):
        if compare_field == "LastEruption" and isinstance(val, str):
            try:
                return float(val)
            except ValueError:
                return None
        return val

    # Handle comparison query
    if params["type"] == "comparison":
        field = params["field"]
        operator = params["operator"]
        value = params["value"]

        # Normalize field name to Firestore key
        json_field = field_map.get(field.lower(), field)

        # Convert Volcanic to boolean
        if json_field == "Volcanic" and isinstance(value, str):
            value = value.lower() in ("true", "yes", "1")

        # Convert LastEruption value to number if possible
        if json_field == "LastEruption" and isinstance(value, str):
            try:
                value = float(value)
            except ValueError:
                value = None

        for doc in all_docs:
            doc_val = convert_val(json_field, doc.get(json_field, None))
            match = False

            # Location special handling
            if json_field == "Location" and isinstance(doc_val, str):
                countries = [c.strip().lower() for c in doc_val.replace(",", "/").split("/")]
                if operator == "==":
                    match = str(value).lower() in countries
                elif operator == "!=":
                    match = str(value).lower() not in countries
            else:
                # String comparison
                if isinstance(doc_val, str) and isinstance(value, str):
                    if operator == "==":
                        match = doc_val.strip().lower() == str(value).strip().lower()
                    elif operator == "!=":
                        match = doc_val.strip().lower() != str(value).strip().lower()
                else:
                    # Numeric comparison
                    if doc_val is not None and value is not None:
                        if operator == "==":
                            match = doc_val == value
                        elif operator == "!=":
                            match = doc_val != value
                        elif operator == ">":
                            match = doc_val > value
                        elif operator == ">=":
                            match = doc_val >= value
                        elif operator == "<":
                            match = doc_val < value
                        elif operator == "<=":
                            match = doc_val <= value

            if match:
                results.append(doc)

        if not results:
            print(f"No mountains found with {field} {operator} {value}.\n")
        else:
            print(f"Mountains with {field} {operator} {value}:")
            for doc in results:
                val_display = doc.get(json_field, "N/A")
                print(f"- {doc.get('MountainName', 'N/A')}: {field} = {val_display}")
            print()

    # Handle normal queries
    elif params["type"] == "normal":
        show_mountain_details(params["mountain_name"], collection, params["field"])

    # Handle compound queries
    elif params["type"] == "compound":
        conditions = params["conditions"]
        logic = params["logic"]

        for doc in all_docs:
            matches = []

            for cond in conditions:
                cond_field, operator, value = cond["field"], cond["operator"], cond["value"]
                json_field = field_map.get(cond_field.lower(), cond_field)
                doc_val = convert_val(json_field, doc.get(json_field, None))

                # Normalize values based on field type
                if json_field == "Volcanic" and isinstance(value, str):
                    value = value.lower() in ("true", "yes", "1")

                if json_field == "LastEruption" and isinstance(value, str):
                    try:
                        value = float(value)
                    except ValueError:
                        value = None

                # Location special handling (split multiple countries)
                if json_field == "Location" and isinstance(doc_val, str):
                    countries = [c.strip().lower() for c in doc_val.replace(",", "/").split("/")]
                    if operator == "==":
                        matches.append(str(value).lower() in countries)
                    elif operator == "!=":
                        matches.append(str(value).lower() not in countries)
                    else:
                        matches.append(False)
                    continue

                # String comparison for other string fields
                if isinstance(doc_val, str) and isinstance(value, str):
                    if operator == "==":
                        matches.append(doc_val.strip().lower() == str(value).strip().lower())
                    elif operator == "!=":
                        matches.append(doc_val.strip().lower() != str(value).strip().lower())
                    else:
                        matches.append(False)
                    continue

                # Numeric/boolean comparison
                if doc_val is not None and value is not None:
                    if operator == "==":
                        matches.append(doc_val == value)
                    elif operator == "!=":
                        matches.append(doc_val != value)
                    elif operator == ">":
                        matches.append(doc_val > value)
                    elif operator == ">=":
                        matches.append(doc_val >= value)
                    elif operator == "<":
                        matches.append(doc_val < value)
                    elif operator == "<=":
                        matches.append(doc_val <= value)
                    else:
                        matches.append(False)
                else:
                    matches.append(False)

            # Apply AND/OR logic
            if (logic == "and" and all(matches)) or (logic == "or" and any(matches)):
                results.append(doc)

        if not results:
            cond_str = f"{conditions[0]['field']} {conditions[0]['operator']} {conditions[0]['value']} {logic.upper()} {conditions[1]['field']} {conditions[1]['operator']} {conditions[1]['value']}"
            print(f"No mountains found with {cond_str}.\n")
        else:
            print(f"Mountains matching {logic.upper()} query:")
            for doc in results:
                print(f"- {doc.get('MountainName', 'N/A')}")
            print()


# First function called, calls other functions
def run_query():

    # Connect to firestore and get mountain names and fields
    collection = get_collection()
    all_docs = collection.stream()
    mountain_names = [doc.id for doc in all_docs]
    valid_fields = ["MountainName", "Elevation", "Location", "Range", "Volcanic", "LastEruption", "Mountain Name", "Last Eruption"]

    print("Welcome!")
    print("Type 'help' for commands. Type 'quit' to exit.")

    # Gets user input
    while True:
        user_input = input("> ").strip()
        if not user_input:
            continue
        if user_input.lower() == "help":
            print_help()
            continue
        if user_input.lower() == "sort_name":
            sort(collection, "name", True)
            continue
        if user_input.lower() == "quit":
            exit()

        # Parse and execute query
        parsed = parse(user_input, mountain_names, valid_fields)
        query(parsed, collection)

# Main functionality
if __name__ == "__main__":
    run_query()
