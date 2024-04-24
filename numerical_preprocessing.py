import re

def extract_numeric_values(description):
    # Extract numeric values using regular expressions
    numeric_values = re.findall(r'\b\d+\.\d+\b', description)
    return numeric_values

def convert_to_float(numeric_values):
    # Convert numeric values to floats
    numeric_values_float = [float(value) for value in numeric_values]
    return numeric_values_float

def preprocess_numeric_data(description):
    # Extract numeric values
    numeric_values = extract_numeric_values(description)
    
    # Convert numeric values to floats
    numeric_values_float = convert_to_float(numeric_values)
    
    return numeric_values_float


