import re


def extract_bracket_content(string):
    match = re.search(r"\[.*\]", string, re.DOTALL)
    if match:
        return match.group(0)
    return None


def is_empty_array(string):
    return string.strip() == "[]"


def convert_to_valid_json_string(input_string):
    corrected_string = re.sub(
        r"(?<!\\)'(?=[^:]+?')|(?<=: )'(?=[^']+?')", '"', input_string
    )
    return corrected_string
