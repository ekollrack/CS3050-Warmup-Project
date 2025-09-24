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
  Examples:
      Mount Hood
      Mount Everest
      K2

2. Show a specific field for a mountain:
  [Field] [Mountain Name]  OR  [Mountain Name] [Field]

  Examples:
      Mount Everest Elevation
      Mount Hood Range
      Last Erupted Mount St. Helens

3. Comparison queries:
  [Field] [Operator] [Value]

  Examples:
      Elevation < 5000
      Volcanic == True
      Location == USA
      Range != Himalayas

4. Compound queries:
    [Field] [Operator] [Value] and/or [Field] [Operator] [Value]
         
    Examples: 
        Elevation > 4000 or Range == Himalayas
        Location == China and Volcanic == True
         
5. Sort functions:
    sort name: shows all mountains in alphabetical order
    sort elevation: shows all mountains from tallest to shortest
         
6. Special commands:
  help    -- Show this help menu
  quit    -- Exit the program


Available fields:
   Elevation, Location, Range, Volcanic, Last Eruption
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

# Calls helper function based on what the user wants to sort
def sort(collection, sort_type, ascend):
   if sort_type == "name":
       sort_name(collection, ascend)
   elif sort_type == "elevation":
       sort_elevation(collection, ascend)

# Sorts name in specified order
def sort_name(collection, ascend):
   docs = collection.stream()
   names = []
   for doc in docs:
       names.append(doc.get("MountainName"))
   names.sort(reverse = not ascend)
   print("Mountain Name")
   print("------------------------------")
   for mountain in names:
       print(mountain)

# Sorts elevation in specified order
def sort_elevation(collection, ascend):
   data = []
   docs = collection.stream()
   for doc in docs:
       data.append((doc.get("MountainName"), doc.get("Elevation")))
   data.sort(reverse = not ascend, key=lambda item:item[1])
   print("Mountain Name        Elevation")
   print("------------------------------")
   for mountain in data:
       print(f"{mountain[0]:<21}{mountain[1]} m")

# Converts last eruption year to a float and volcanic  as true/false
def convert_val(compare_field, val):
    if compare_field == "LastEruption":
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
    if compare_field == "Volcanic" and isinstance(val, str):
        return val.lower() in ("true", "yes", "1")
    return val


# Safe comparison to prevent TypeErrors
def safe_compare(a, b, operator):
    try:
        if operator == "==": return a == b
        if operator == "!=": return a != b
        if operator == ">": return a > b
        if operator == ">=": return a >= b
        if operator == "<": return a < b
        if operator == "<=": return a <= b
    except TypeError:
        return False


# Parse user input
# Returns dictionary separating the type of query (comparison, compound, mountain, mountain + field)
def parse(user_input, mountain_names, valid_fields):
    mountain_name = None
    field_name = None

    # Parsers
    fields = ["Mountain Name", "Elevation", "Location", "Range", "Volcanic", "Last Eruption"]
    field_parser = pp.MatchFirst([pp.CaselessKeyword(f) for f in fields]).set_results_name("field")

    operator_parser = pp.MatchFirst([pp.Literal(op) for op in ["==", "!=", ">=", "<=", "=", ">", "<"]]).set_results_name("operator")
    logical_op = pp.MatchFirst([pp.CaselessKeyword("and"), pp.CaselessKeyword("or")]).set_results_name("logic")
    number_parser = pp.pyparsing_common.number

    # A value can be either a number (number_parser)
    # or a string captured by regex until "and", "or", or the end of the line.
    value_parser = (number_parser | pp.Regex(r".+?(?=\s+(and|or)\s+|$)")).set_parse_action(
        lambda t: (
            True if str(t[0]).strip().lower() == "true" else
            False if str(t[0]).strip().lower() == "false" else
            float(t[0]) if isinstance(t[0], (int, float)) or str(t[0]).replace(".", "", 1).isdigit() # convert string to number
            else t[0].strip()
        )
    ).set_results_name("value")

    # comparison grammars
    comparison = pp.Group(field_parser + operator_parser + value_parser)
    compound = pp.Group(comparison("first") + logical_op("logic") + comparison("second"))

    # Look for mountain name
    mountain_parser = pp.OneOrMore(pp.Word(pp.alphanums + "-.'")).set_results_name("mountain")

    # different ways to find mountain details
    field_then_mountain = field_parser + mountain_parser
    mountain_then_field = pp.SkipTo(field_parser).set_results_name("mountain") + field_parser
    mountain_only = mountain_parser

    # Compound queries
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

    # Single comparison queries
    try:
        result = comparison.parse_string(user_input, parse_all=True)[0]
        return {
            "type": "comparison",
            "field": result.field,
            "operator": result.operator,
            "value": result.value,
            "mountain_name": None
        }
    except pp.ParseException:
        pass

    # Mountain and fields
    try:
        result = field_then_mountain.parse_string(user_input, parse_all=True)
        field_name = result.field
        mountain_candidate = " ".join(result.mountain)
    except pp.ParseException:
        try:
            result = mountain_then_field.parse_string(user_input, parse_all=True)
            field_name = result.field
            mountain_candidate = result.mountain.strip()
        except pp.ParseException:
            try:
                result = mountain_only.parse_string(user_input, parse_all=True)
                mountain_candidate = " ".join(result.mountain)
            except pp.ParseException:
                return None

    # find mountain name and field name
    if mountain_candidate:
        for name in mountain_names:
            if name.lower() == mountain_candidate.lower():
                mountain_name = name
                break

    if field_name:
        field_input = field_name.lower().replace(" ", "")
        for f in valid_fields:
            if field_input == f.lower().replace(" ", ""):
                field_name = f
                break

    return {"type": "normal", "field": field_name, "value": None, "operator": None, "mountain_name": mountain_name}


# Execute query using the dictionary from parse()
# Returns matching queries from database
def query(params, collection):
    if params is None:
        print("Invalid input. Type 'help' for guidance.\n")
        return

    all_docs = [doc.to_dict() for doc in collection.stream()]
    results = []
    field_map = {
        "last eruption": "LastEruption",
        "mountain name": "MountainName",
        "range": "Range",
        "location": "Location",
        "elevation": "Elevation",
        "volcanic": "Volcanic"
    }

    # Comparison query
    if params["type"] == "comparison":
        field = params["field"]
        operator = params["operator"]
        value = params["value"]
        json_field = field_map.get(field.lower(), field)
        value = convert_val(json_field, value)

        # Look through docs
        for doc in all_docs:
            doc_val = convert_val(json_field, doc.get(json_field, None))
            match = False

            # deal with mountains that are in 2 locations
            if json_field == "Location" and isinstance(doc_val, str):
                countries = [c.strip().lower() for c in doc_val.replace(",", "/").split("/")]
                if operator == "==":
                    match = str(value).lower() in countries
                elif operator == "!=":
                    match = str(value).lower() not in countries

            # String to string comparisons
            elif isinstance(doc_val, str) and isinstance(value, str):
                match = safe_compare(doc_val.strip().lower(), str(value).strip().lower(), operator)
            else:
                match = safe_compare(doc_val, value, operator)
            if match:
                results.append(doc)

        # Show results
        if not results:
            print(f"No mountains found with {field} {operator} {value}.\n")
        else:
            print(f"Mountains with {field} {operator} {value}:")
            for doc in results:
                val_display = doc.get(json_field, "N/A")
                print(f"- {doc.get('MountainName', 'N/A')}: {field} = {val_display}")
            print()

    # Normal query
    elif params["type"] == "normal":
        show_mountain_details(params["mountain_name"], collection, params["field"])

    # Compound query
    elif params["type"] == "compound":
        conditions = params["conditions"]
        logic = params["logic"]

        # Look for matching documents
        for doc in all_docs:
            matches = []
            for cond in conditions:
                cond_field, operator, value = cond["field"], cond["operator"], cond["value"]
                json_field = field_map.get(cond_field.lower(), cond_field)
                doc_val = convert_val(json_field, doc.get(json_field, None))
                value = convert_val(json_field, value)

                # Special case for location
                if json_field == "Location" and isinstance(doc_val, str):
                    countries = [c.strip().lower() for c in doc_val.replace(",", "/").split("/")]
                    if operator == "==":
                        matches.append(str(value).lower() in countries)
                    elif operator == "!=":
                        matches.append(str(value).lower() not in countries)
                    else:
                        matches.append(False)
                    continue

                # Handling string comparisons
                if isinstance(doc_val, str) and isinstance(value, str):
                    matches.append(safe_compare(doc_val.strip().lower(), str(value).strip().lower(), operator))
                    continue

                matches.append(safe_compare(doc_val, value, operator))
            # Evaluate logic
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
       if user_input.lower() == "sort_name_ascending" or user_input.lower() == "sort name":
           sort(collection, "name", True)
           continue
       if user_input.lower() == "sort_name_decending":
           sort(collection, "name", False)
           continue
       if user_input.lower() == "sort_elevation_ascending":
           sort(collection, "elevation", True)
           continue
       if user_input.lower() == "sort_elevation_decending"  or user_input.lower() == "sort elevation":
           sort(collection, "elevation", False)
           continue
       if user_input.lower() == "quit":
           exit()

       # Parse and execute query
       parsed = parse(user_input, mountain_names, valid_fields)
       query(parsed, collection)


# Main functionality
if __name__ == "__main__":
   run_query()
