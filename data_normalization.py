import re

def handle_missing_values(df):
    df.fillna('', inplace=True)
    return df

def extract_numerical_values(desc):
    pattern = r'(\d+\.\d+)'
    matches = re.findall(pattern, desc)
    return [float(match) for match in matches]



