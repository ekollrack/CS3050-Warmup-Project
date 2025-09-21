from firebase_connection import database_connection
import pyparsing as pp
import warnings
warnings.filterwarnings("ignore")

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

# ------------------ Show Mountain Details ------------------
def show_mountain_details(name, collection, field=None):
    doc_ref = collection.document(name)
    doc = doc_ref.get()
    if not doc.exists:
        print(f"No information found. Type 'help' for guidance. \n")
        return

    data = doc.to_dict()
    field_mapping = {
        "Mountain Name": "MountainName",
        "Elevation": "Elevation",
        "Location": "Location",
        "Range": "Range",
        "Volcanic": "Volcanic",
        "Last Eruption": "LastEruption"
    }

    if field is None:
        print(f"\nDetails for {name}:")
        for key, json_key in field_mapping.items():
            if json_key in data:
                print(f"{key}: {data[json_key]}")
        print()
    else:
        # Normalize field name
        json_key = None
        for k, v in field_mapping.items():
            if field.lower() == k.lower().replace(" ", "") or field.lower() == v.lower():
                json_key = v
                break

        if not json_key:
            print("No information found. Type 'help' for guidance.\n")
            return

        if field.lower() == "volcanic":
            print(f"Is {name} Volcanic? {data.get(json_key, 'N/A')}\n")
        else:
            print(f"{field} of {name}: {data.get(json_key, 'N/A')}\n")

# ------------------ Parsing ------------------
def parse(user_input, mountain_names, valid_fields):
    # Field parser (case-insensitive)
    field_parser = pp.MatchFirst([pp.CaselessKeyword(f) for f in valid_fields]).set_results_name("field")

    # Operator parser
    operator_parser = pp.MatchFirst(
        [pp.Literal(op) for op in ["==", "!=", ">=", "<=", "=", ">", "<"]]).set_results_name("operator")

    # Value parser
    number_parser = pp.pyparsing_common.number
    quoted_string = pp.QuotedString('"') | pp.QuotedString("'")
    unquoted_string = pp.Word(pp.alphanums + "-/'")
    value_parser = (number_parser | quoted_string | unquoted_string).set_results_name("value")

    # Mountain parser: allow letters, numbers, hyphens, apostrophes, and dots
    mountain_parser = pp.OneOrMore(pp.Word(pp.alphanums + "-.'")).set_results_name("mountain")

    # Grammars
    comparison_grammar = field_parser + operator_parser + value_parser
    grammar1 = field_parser + mountain_parser  # Field first, then mountain
    grammar2 = pp.SkipTo(field_parser).set_results_name("mountain") + field_parser  # Mountain first, then field
    grammar3 = mountain_parser  # Only mountain name

    # ------------------ Comparison Query ------------------
    try:
        result = comparison_grammar.parse_string(user_input, parse_all=True)
        op = str(result.operator)
        if op == "=":
            op = "=="
        val = result.value

        if isinstance(val, str):
            if val.lower() in ("true", "false"):
                val = val.lower() == "true"
            else:
                try:
                    val = float(val)
                except ValueError:
                    val = val.strip()

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
        for f in valid_fields:
            if f.lower() == field_name.lower():
                field_name = f
                break

    return {"type": "normal", "field": field_name, "value": None, "operator": None, "mountain_name": mountain_name}


# ------------------ Query Execution ------------------
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


# ------------------ Interactive Loop ------------------
def run_query():
    collection = get_collection()
    all_docs = collection.stream()
    mountain_names = [doc.id for doc in all_docs]
    valid_fields = ["MountainName", "Elevation", "Location", "Range", "Volcanic", "LastEruption"]

    print("Welcome to Mountains Query Program!")
    print("Type 'help' for commands. Type 'quit' to exit.")

    while True:
        user_input = input("> ").strip()
        if not user_input:
            continue
        if user_input.lower() == "help":
            print_help()
            continue
        if user_input.lower() == "quit":
            exit()

        parsed = parse(user_input, mountain_names, valid_fields)
        query(parsed, collection)

# ------------------ Entry Point ------------------
if __name__ == "__main__":
    run_query()
