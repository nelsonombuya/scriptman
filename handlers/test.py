import json


def beautify_and_print_dictionary(dictionary):
    """
    Beautify a dictionary and print it on the console.

    Args:
        dictionary (dict): The dictionary to be beautified and printed.

    Example:
        dictionary = {
            "name": "John",
            "age": 30,
            "city": "New York"
        }
        beautify_and_print_dictionary(dictionary)

    Output:
        {
            "name": "John",
            "age": 30,
            "city": "New York"
        }
    """
    # Use json.dumps with indentation for beautification
    beautified_json = json.dumps(dictionary, indent=4)

    # Print the beautified JSON
    print(beautified_json)


# Example usage
dictionary = {"name": "John", "age": 30, "city": "New York"}
beautify_and_print_dictionary(dictionary)
