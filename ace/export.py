
from ace.database import Database, Article, Table, Activation
import logging
import re
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
import pandas as pd

logger = logging.getLogger(__name__)

def _screen_article(a):
    ''' Screen article for fMRI content. Returns True if identified as fMRI,
    False if non-fMRI. Note that this is imperfect and there will be both
    false positives and false negatives. '''
    mesh = a.pubmed_metadata['mesh']
    # Drop VBM studies, meta-analyses, non-human animal studies,
    # EEG/MEG studies, or any other non-fMRI studies.
    bool_eval = (re.search('VBM|voxel-?based.*?morphom', a.title)
    or re.search('meta-analy', a.title)
    or 'Diffusion Tensor Imaging' in mesh
    or ('Animals' in mesh and 'Humans' not in mesh)
    or (('Electroencephalography' in mesh or 'Magnetoencephalography'
        in mesh) and 'fMRI' not in a.text)
    or not re.search('fMRI|functional magnetic', a.text))

    return not bool_eval


def export_activations(db, filename, metadata=True, groups=False, screen=True):
    ''' Export all activations in the DB to a text file.
    Args:
        filename: the tab-delimited text file to write to
        metadata: if True, writes all available metadata for every article
        groups: if True, includes the extracted coordinate group information
        screen: if True, attempts to screen out non-fMRI studies (with mixed
            success--there are both false positives and false negatives).
    '''

    res = ['id\tdoi\tx\ty\tz\tspace\tpeak_id\ttable_id\ttable_num']

    if metadata:
        res[0] += '\ttitle\tauthors\tyear\tjournal'

    if groups:
        res[0] += '\tgroups'

    articles = db.session.query(Article).filter(Article.tables.any()).all()

    screened_out = 0

    for i, a in enumerate(articles):
        article_valid = _screen_article(a)
        if not article_valid:
            screened_out += 1
            continue

        logger.info('Processing article %s...' % a.id)
        for t in a.tables:
            for p in t.activations:
                if t.number is None: t.number = ''
                fields = [a.id, a.doi, p.x, p.y, p.z, a.space, p.id, t.id,
                          t.number.strip('\t\r\n')]
                if metadata:
                    fields += [a.title, a.authors, a.year, a.journal]
                if groups:
                    if isinstance(p.groups, basestring):
                        p.groups = [p.groups]
                    elif p.groups is None:
                        p.groups = []
                    fields += ['///'.join(p.groups).encode('utf-8')]
                res.append('\t'.join(str(x) for x in fields))

    if screen:
        print "%d suspected non-fMRI studies filtered out." % screened_out

    open(filename, 'w').write('\n'.join(res))


def export_features(db, filename=None, tfidf=True, vocabulary=None,
                    require_activations=True, screen=True,
                    save_stopwords=None, **kwargs):
    ''' Takes text from an article as input and returns a matrix of document -->
    ngram weights. At the moment, only extracts terms from abstracts.
    Args:
        db: A database instance
        filename: an optional path to write results to; if None, returns the
        resulting data as a pandas DF
        tfidf: If True, uses a tf-idf tokenizer; otherwise uses raw counts
        vocabulary: an optional list of ngrams to restrict extraction to
        require_activations: When True, only articles containing at least one
            fMRI activation table will be included. When False, use all articles
            in DB.
        screen: if True, filters out suspected non-fMRI studies.
        save_stopwords: if not None, save list of stopwords to this file.
        kwargs: Optional keywords passed onto the scikit-learn vectorizer.
            Common args are ngram_range, min_df, max_df, stop_words, and
            vocabulary.
    '''

    # Extract article texts--for now, uses abstracts
    # articles = db.session.query(Article.id, Article.abstract)
    articles = db.session.query(Article)
    if require_activations:
        articles = articles.filter(Article.tables.any())
    # pmids, corpus = zip(*articles.all())
    articles = articles.all()

    if screen:
        articles = filter(_screen_article, articles)

    pmids, corpus = zip(*[(a.id, a.abstract) for a in articles])

    # Instantiate vectorizer--either simple counts, or tf-idf
    vectorizer = TfidfVectorizer if tfidf else CountVectorizer
    vectorizer = vectorizer(vocabulary=vocabulary, **kwargs)

    # Transform texts
    weights = vectorizer.fit_transform(corpus).toarray()
    if save_stopwords is not None:
        ss = open(save_stopwords, 'w')
        ss.write('\n'.join(list(vectorizer.stop_words_)))
    names = vectorizer.get_feature_names()
    print len(names), names
    print "writing data..."

    data = pd.DataFrame(weights, columns=names, index=pmids)

    if filename is not None:
        data.to_csv(filename, sep='\t', index_label='pmid', encoding='utf-8')
    else:
        return data
