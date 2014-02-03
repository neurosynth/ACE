# from nltk import *
import re
from collections import Counter
import database
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
import pandas as pd


def extract_word_features(db, pattern=r'[^\sa-z\-]+', tfidf=True, min_df=0.005, max_df=0.8, save=None, **kwargs):
    ''' Takes text from an article as input and returns a matrix of document --> term weights.
    At the moment, only extracts terms from abstracts. '''

    # Extract article texts--for now, uses abstracts
    corpus = [a.abstract for a in db.articles]
    pmids = [a.id for a in db.articles]

    # Instantiate vectorizer--either simple counts, or tf-idf
    vectorizer = TfidfVectorizer if tfidf else CountVectorizer
    vectorizer = vectorizer(min_df=min_df, max_df=max_df, stop_words='english')

    # Transform texts
    weights = vectorizer.fit_transform(corpus).toarray()
    names = vectorizer.get_feature_names()

    data = pd.DataFrame(weights, columns=names, index=pmids)

    print data.shape

    if save is not None:
        data.to_csv(save, sep='\t', index_label='pmid', encoding='utf-8')
    else:
        return data