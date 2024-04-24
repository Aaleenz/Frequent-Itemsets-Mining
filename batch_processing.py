import json
from text_preprocessing import text_lower, remove_html_tags, tokenization, remove_stopwords, remove_numeric, remove_special_characters, perform_stemming
from numerical_preprocessing import preprocess_numeric_data
from data_cleaning import preprocess_data_dict, drop_columns


def preprocess_text(text):
    # Check if the input is a list
    if isinstance(text, list):
        # Concatenate all elements of the list into a single string
        text = ' '.join(str(item) for item in text)
    elif not isinstance(text, str):
        # If input is neither list nor string, convert it to a string
        text = str(text)
    
    text = text_lower(text)
    text = remove_html_tags(text)
    tokens = tokenization(text)
    tokens = remove_stopwords(tokens)
    tokens = remove_numeric(tokens)
    tokens = remove_special_characters(tokens)
    tokens = perform_stemming(tokens)
    preprocessed_text = ' '.join(tokens)
    return preprocessed_text

    
def preprocess_batch(batch):
    for item in batch:
        for key, value in item.items():
            if isinstance(value, str):
                item[key] = remove_html_tags(value)
            else:
                item[key] = preprocess_text(value)
        yield item


def batch_preprocess_data(input_file, output_file, batch_size):
    with open(input_file, 'r', encoding='utf-8') as f_in, open(output_file, 'w', encoding='utf-8') as f_out:
        while True:
            batch = [json.loads(next(f_in)) for _ in range(batch_size)]
            if not batch:
                break  

            for item in batch:
                # Drop specified columns if they exist
                keys_to_drop = ['description', 'rank', 'tech1', 'tech2', 'feature', 'rank', 'price', 'details', 'image', 'fit', 'main_cat', 'similar_item', 'date']
                for key in keys_to_drop:
                    if key in item:
                        del item[key]

                # Preprocess text fields
                for key, value in item.items():
                    if isinstance(value, str):
                        item[key] = remove_html_tags(value)
                    else:
                        item[key] = preprocess_text(value)

                json.dump(item, f_out, ensure_ascii=False)
                f_out.write('\n')


input_file = 'Sampled_Amazon_Meta.json'
output_file = 'preprocessed_data.json'
batch_size = 100
batch_preprocess_data(input_file, output_file, batch_size)

