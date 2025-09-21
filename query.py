# query.py
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
        "Elevation": "Elevation",
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

# ------------------ Parsing ------------------
def parse(user_input, mountain_names, valid_fields):
    """
    Returns a dictionary:
    {
        'type': 'comparison' or 'normal',
        'field': field name or None,
        'operator': operator or None,
        'value': numeric value or None,
        'mountain_name': mountain name or None
    }
    """
    # Field parser
    field_parser = pp.MatchFirst([pp.CaselessKeyword(f) for f in valid_fields]).set_results_name("field")
    # Operator parser (longest first)
    operator_parser = pp.MatchFirst([pp.Literal(op) for op in ["==", "!=", ">=", "<=", "=", ">", "<"]]).set_results_name("operator")
    # Mountain parser
    mountain_parser = pp.OneOrMore(pp.Word(pp.alphanums + "-'")).set_results_name("mountain")
    # Number parser
    number_parser = pp.pyparsing_common.number.set_results_name("value")

    # Comparison grammar: Field OP Number
    comparison_grammar = field_parser + operator_parser + number_parser
    # Normal grammars
    grammar1 = field_parser + mountain_parser       # Field then Mountain
    grammar2 = pp.SkipTo(field_parser).set_results_name("mountain") + field_parser
    grammar3 = mountain_parser                     # Mountain only

    # Try comparison first
    try:
        result = comparison_grammar.parse_string(user_input, parse_all=True)
        return {
            "type": "comparison",
            "field": str(result.field),
            "operator": str(result.operator),
            "value": result.value,
            "mountain_name": None
        }
    except pp.ParseException:
        pass

    # Normal parsing
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

    # Normalize mountain name
    if mountain_candidate:
        for name in mountain_names:
            if name.lower() == mountain_candidate.lower():
                mountain_name = name
                break

    # Normalize field name
    if field_name:
        for f in valid_fields:
            if f.lower() == field_name.lower():
                field_name = f
                break

    return {
        "type": "normal",
        "field": field_name,
        "value": None,
        "operator": None,
        "mountain_name": mountain_name
    }

# ------------------ Query Execution ------------------
def query(parsed_result, collection):
    if parsed_result is None:
        print("Invalid input. Type 'help' for guidance.\n")
        return

    field_mapping = {
        "Mountain Name": "Mountain Name",
        "Elevation": "Elevation",
        "Location": "Location (country)",
        "Mountain Range": "Mountain Range",
        "Volcanic": "Volcanic",
        "Last Eruption": "Last Eruption"
    }

    if parsed_result["type"] == "comparison":
        field = parsed_result["field"]
        operator = parsed_result["operator"]
        value = parsed_result["value"]

        if field.title() != "Elevation":
            print("Comparison queries currently only supported for 'Elevation'.\n")
            return

        # normalize operator
        if operator == "=":
            operator = "=="

        json_key = field_mapping.get(field.title())
        if not json_key:
            print(f"Unknown field '{field}'.\n")
            return

        try:
            numeric_value = float(value)
        except Exception:
            print(f"Invalid numeric value: {value}\n")
            return

        try:
            results = list(collection.where(json_key, operator, numeric_value).stream())
        except Exception as e:
            print(f"Error querying database: {e}\n")
            return

        if not results:
            print(f"No mountains found with {field} {operator} {value}.\n")
        else:
            print(f"Mountains with {field} {operator} {value}:")
            for doc in results:
                data = doc.to_dict()
                print(f"- {doc.id}: Elevation (m) = {data.get(json_key, 'N/A')}")
            print()

    elif parsed_result["type"] == "normal":
        show_mountain_details(parsed_result["mountain_name"], collection, parsed_result["field"])

# ------------------ Interactive Loop ------------------
def run_query():
    collection = get_collection()
    all_docs = collection.stream()
    mountain_names = [doc.id for doc in all_docs]
    valid_fields = ["Mountain Name", "Elevation", "Location", "Mountain Range", "Volcanic", "Last Eruption"]

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
