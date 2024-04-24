import nltk
import string
import re
from bs4 import BeautifulSoup
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

nltk.download('punkt')
nltk.download('stopwords')

def text_lower(text):
    return text.lower()

def remove_html_tags(text):
    return re.sub(r'<[^>]+>', '', text)


def tokenization(sentence):
    tokens = nltk.word_tokenize(sentence)
    return tokens

def remove_stopwords(tokens):
    stop_words = set(stopwords.words('english'))
    filtered_tokens = [word for word in tokens if word.lower() not in stop_words]
    return filtered_tokens

def remove_numeric(tokens):
    cleaned_tokens = [token for token in tokens if not token.isdigit()]
    return cleaned_tokens

def remove_special_characters(tokens):
    cleaned_tokens = [re.sub(r'[^a-zA-Z0-9\s]', '', token) for token in tokens]
    cleaned_tokens = [token for token in cleaned_tokens if token]  
    return cleaned_tokens

from nltk.stem import PorterStemmer

def perform_stemming(tokens):
    stemmer = PorterStemmer()
    stemmed_tokens = [stemmer.stem(token) for token in tokens]
    return stemmed_tokens


