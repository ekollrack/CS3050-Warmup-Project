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
        for k, v in field_mapping.items():
            if field_input == k.lower().replace(" ", "") or field_input == v.lower().replace(" ", ""):
                json_key = v
                break

        if not json_key:
            print("No information found. Type 'help' for guidance.\n")
            return
        if field.lower() == "volcanic":
            print(f"Is {name} Volcanic? {data.get(json_key, 'N/A')}\n")
        else:
            print(f"{v} of {name}: {data.get(json_key, 'N/A')}\n")


# This function parses queries with comparison operators and returns a dictionary
# Parameters - user input (str from user), mountain names (list of str), valid fields (list of str)
def parse(user_input, mountain_names, valid_fields):
    # Field parser (case-insensitive) to match the specified field
    field_parser = pp.MatchFirst([
        pp.CaselessKeyword(f) for f in ["Mountain Name", "Elevation", "Location", "Range", "Volcanic", "Last Eruption"]
    ]).set_results_name("field")

    # Operator parser
    operator_parser = pp.MatchFirst(
        [pp.Literal(op) for op in ["==", "!=", ">=", "<=", "=", ">", "<"]]).set_results_name("operator")
    
    # And or parser
    logical_op = pp.CaselessKeyword("and") | pp.CaselessKeyword("or")

    # Value parser
    number_parser = pp.pyparsing_common.number
    quoted_string = pp.QuotedString('"') | pp.QuotedString("'")
    unquoted_string = pp.Word(pp.alphanums + "-/'")
    value_parser = (number_parser | quoted_string | unquoted_string).set_results_name("value")

    # Mountain parser: allow letters, numbers, hyphens, apostrophes, and dots
    mountain_parser = pp.OneOrMore(pp.Word(pp.alphanums + "-.'")).set_results_name("mountain")

    # field operator value structure
    comparison_grammar = field_parser + operator_parser + value_parser
<<<<<<< HEAD
    grammar1 = field_parser + mountain_parser  # Field first, then mountain
    grammar2 = pp.SkipTo(field_parser).set_results_name("mountain") + field_parser  # Mountain first, then field
    grammar3 = mountain_parser  # Only mountain name
    compound_grammar = comparison_grammar("cond1") + logical_op("logic") + comparison_grammar("cond2") # and or queries

    # ------------------ And Or Query ------------------
    try:
        result = compound_grammar.parse_string(user_input, parse_all=True)
        logic = str(result.logic).lower()

        def normalize(val):
            if isinstance(val, str):
                if val.lower() in ("true", "false"):
                    return val.lower() == "true"
                else:
                    try:
                        return float(val)
                    except ValueError:
                        return val.strip()
            return val
        cond1 = {
        "field": str(result.cond1.field),
        "operator": str(result.cond1.operator),
        "value": normalize(result.cond1.value),
        }
        cond2 = {
        "field": str(result.cond2.field),
        "operator": str(result.cond2.operator),
        "value": normalize(result.cond2.value),
        }
        cond1["field"] = normalize(cond1["field"])
        cond2["field"] = normalize(cond2["field"])

        return {"type": "compound", "logic": logic, "conditions": [cond1, cond2]}
    except pp.ParseException:
        pass
=======
    # Field first, then mountain
    grammar1 = field_parser + mountain_parser
    # Mountain first, then field
    grammar2 = pp.SkipTo(field_parser).set_results_name("mountain") + field_parser
    # Only mountain name
    grammar3 = mountain_parser
>>>>>>> 8498eee5a64a01a19464954c042d478bc6ef0cd7

    # For comparison queries (comparison_grammer)
    try:
        result = comparison_grammar.parse_string(user_input, parse_all=True)
        op = str(result.operator)
        if op == "=":
            op = "=="

        # Convert value to bool, float, or string
        val = result.value

        if isinstance(val, str):
            if val.lower() in ("true", "false"):
                val = val.lower() == "true"
            else:
                try:
                    val = float(val)
                except ValueError:
                    val = val.strip()
        # return comparison query
        return {"type": "comparison", "field": str(result.field), "operator": op, "value": val, "mountain_name": None}
    except pp.ParseException:
        pass

    # ------------------ Normal Query ------------------
    mountain_name = None
    field_name = None

    try:
        result = grammar1.parse_string(user_input, parse_all=True)
        field_name = str(result.field)
        mountain_candidate = " ".join(result.get("mountain", []))
    except pp.ParseException:
        try:
            result = grammar2.parse_string(user_input, parse_all=True)
            field_name = str(result.field)
            mountain_candidate = result.mountain.strip()
        except pp.ParseException:
            try:
                result = grammar3.parse_string(user_input, parse_all=True)
                mountain_candidate = " ".join(result.get("mountain", []))
            except pp.ParseException:
                return None

    # Match mountain name case-insensitively
    if mountain_candidate:
        for name in mountain_names:
            if name.lower() == mountain_candidate.lower():
                mountain_name = name
                break

    # Match field name case-insensitively
    if field_name:
        field_input = field_name.lower().replace(" ", "")
        for f in valid_fields:
            if field_input == f.lower().replace(" ", ""):
                field_name = f
                break

    return {"type": "normal", "field": field_name, "value": None, "operator": None, "mountain_name": mountain_name}


# This function takes the dictionary from the parsed query and prints the results
def query(params, collection):
    if params is None:
        print("Invalid input. Type 'help' for guidance.\n")
        return

    if params["type"] == "comparison":
        field = params["field"]
        operator = params["operator"]
        value = params["value"]

        # Convert string "true"/"false" to boolean for Volcanic field
        if field.lower() == "volcanic" and isinstance(value, str):
            value = value.lower() in ("true", "yes", "1")

        all_docs = [doc.to_dict() for doc in collection.stream()]
        results = []

        for doc in all_docs:
            doc_val = doc.get(field, None)
            match = False

            # Special handling for Location field (country1/country2)
            if field.lower() == "location" and isinstance(doc_val, str):
                countries = [c.strip().lower() for c in doc_val.replace(",", "/").split("/")]
                if operator == "==":
                    match = str(value).lower() in countries
                elif operator == "!=":
                    match = str(value).lower() not in countries
            else:
                # Case-insensitive string comparison
                if isinstance(doc_val, str) and isinstance(value, str):
                    doc_val_lower = doc_val.strip().lower()
                    value_lower = str(value).strip().lower()
                    if operator == "==":
                        match = doc_val_lower == value_lower
                    elif operator == "!=":
                        match = doc_val_lower != value_lower
                else:
                    # Numeric comparison
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
                val_display = doc.get(field, "N/A")
                print(f"- {doc.get('MountainName', 'N/A')}: {field} = {val_display}")
            print()

    elif params["type"] == "normal":
        show_mountain_details(params["mountain_name"], collection, params["field"])

    elif params["type"] == "compound":
        conditions = params["conditions"]
        logic = params["logic"]

        all_docs = [doc.to_dict() for doc in collection.stream()]
        results = []
        def match_condition(doc, cond):
            field, operator, value = cond["field"], cond["operator"], cond["value"]
            doc_val = doc.get(field, None)
            match = False

            # Special handling for Location field (list of countries)
            if field.lower() == "location" and isinstance(doc_val, str):
                countries = [c.strip().lower() for c in doc_val.replace(",", "/").split("/")]
                if operator == "==":
                    match = str(value).lower() in countries
                elif operator == "!=":
                    match = str(value).lower() not in countries
            else:
                # Case-insensitive string comparison
                if isinstance(doc_val, str) and isinstance(value, str):
                    doc_val_lower = doc_val.strip().lower()
                    value_lower = str(value).strip().lower()
                    if operator == "==":
                        match = doc_val_lower == value_lower
                    elif operator == "!=":
                        match = doc_val_lower != value_lower
                else:
                    # Numeric comparison
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
            return match
        
        for doc in all_docs:
            matches = [match_condition(doc, c) for c in conditions]
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
        if user_input.lower() == "quit":
            exit()

        # Parse and execute query
        parsed = parse(user_input, mountain_names, valid_fields)
        query(parsed, collection)

# Main functionality
if __name__ == "__main__":
    run_query()
