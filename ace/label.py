# from nltk import *
import re
from collections import Counter
from database import Article
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
import pandas as pd


def extract_ngram_features(db, tfidf=True, save=None, vocabulary=None, require_activations=True, **kwargs):
    ''' Takes text from an article as input and returns a matrix of document --> 
    ngram weights. At the moment, only extracts terms from abstracts. 
    Args:
        db: A database instance
        tfidf: If True, uses a tf-idf tokenizer; otherwise uses raw counts
        save: an optional path to save a CSV to; if None, returns the resulting data
        vocabulary: an optional list of ngrams to restrict extraction to
        require_activations: When True, only articles containing at least one fMRI activation
            table will be included. When False, use all articles in DB.
        kwargs: Optional keywords passed onto the scikit-learn vectorizer. Common args are
            ngram_range, min_df, max_df, stop_words, and vocabulary.
    '''

    # Extract article texts--for now, uses abstracts
    articles = db.session.query(Article.id, Article.abstract)
    if require_activations:
        articles = articles.filter(Article.tables.any())
    pmids, corpus = zip(*articles.all())

    # Instantiate vectorizer--either simple counts, or tf-idf
    vectorizer = TfidfVectorizer if tfidf else CountVectorizer
    vectorizer = vectorizer(vocabulary=vocabulary, **kwargs)

    # Transform texts
    weights = vectorizer.fit_transform(corpus).toarray()
    names = vectorizer.get_feature_names()

    data = pd.DataFrame(weights, columns=names, index=pmids)

    if save is not None:
        data.to_csv(save, sep='\t', index_label='pmid', encoding='utf-8')
    else:
        return data

