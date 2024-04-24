import pandas as pd

def preprocess_data_dict(data_dict):
    # Determine the maximum length of lists in data_dict
    max_length = max(len(lst) for lst in data_dict.values() if isinstance(lst, list))

    # Pad shorter lists with an empty string to make them the same length
    padded_data_dict = {key: (value + [''] * (max_length - len(value)) if isinstance(value, list) else value) for key, value in data_dict.items()}

    # Create a DataFrame from the padded data dictionary
    df = pd.DataFrame(padded_data_dict)

    # Convert dictionaries to strings
    for col in df.columns:
        df[col] = df[col].apply(lambda x: ', '.join([f"{k}: {v}" for k, v in x.items()]) if isinstance(x, dict) else x)

    # Convert lists to strings
    for col in df.columns:
        df[col] = df[col].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

    # Remove duplicate rows
    df.drop_duplicates(inplace=True)

    # Remove columns with all NaNs
    df.dropna(axis=1, how='all', inplace=True)

    return df


def drop_columns(df, columns_to_drop):
    # Drop specified columns
    df.drop(columns=columns_to_drop, inplace=True)

    return df




