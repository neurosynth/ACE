# Database stuff and models

from sqlalchemy import (TypeDecorator, Table, Column, Integer, Float, String,
                        ForeignKey, Boolean, DateTime, Text)
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.sql import exists
from datetime import datetime
from ace import config
import simplejson as json
import logging
import sys
from os import path
import datetime

from . import sources
from . import config
from . import extract

logger = logging.getLogger(__name__)

Base = declarative_base()

# Backend-dependent column for full text
LongText = Text().with_variant(MEDIUMTEXT, 'mysql')

# Handles all Database loading/saving stuff
class Database:

    def __init__(self, adapter=None, db_name=None, user=None, password=None):
        ''' Connect to DB and initialize instance. '''

        # Default to settings in config file if none passed
        if adapter is None: adapter = config.SQL_ADAPTER

        # Generate DB URI
        if adapter == 'sqlite':
            db_uri = config.SQLITE_URI if db_name is None else db_name
        elif adapter == 'mysql':
            db_name = config.MYSQL_DB if db_name is None else db_name
            if user is None: user = config.MYSQL_USER
            if password is None: password = config.MYSQL_PASSWORD
            db_uri = 'mysql://%s:%s@localhost/%s' % (user, password, db_name)
        else:
            raise ValueError("Value of SQL_ADAPTER in settings must be either 'sqlite' or 'mysql'")

        engine = create_engine(db_uri, echo=False)

        if adapter == 'mysql': engine.execute("SET sql_mode=''")

        Session = sessionmaker(bind=engine)
        Base.metadata.create_all(engine)
        self.session = Session()

    def add(self, record):
        ''' Add a record to the DB. '''
        self.session.add(record)

    def save(self):
        ''' Commit all stored records to file. '''
        self.session.commit()

    def add_articles(self, files, commit=True, table_dir=None, limit=None,
                     pmid_filenames=False, metadata_dir=None):
        ''' Process articles and add their data to the DB.
        Args:
            files: The path to the article(s) to process. Can be a single
                filename (string), a list of filenames, or a path to pass
                to glob (e.g., "article_dir/NIMG*html")
            commit: Whether or not to save records to DB file after adding them.
            table_dir: Directory to store downloaded tables in (if None, tables 
                will not be saved.)
            limit: Optional integer indicating max number of articles to add 
                (selected randomly from all available). When None, will add all
                available articles.
            pmid_filenames: When True, assume that the file basename is a PMID.
                This saves us from having to retrieve metadata from PubMed When
                checking if a file is already in the DB, and greatly speeds up 
                batch processing when overwrite is off.
            metadata_dir: Location to read/write PubMed metadata for articles.
                When None (default), retrieves new metadata each time. If a 
                path is provided, will check there first before querying PubMed,
                and will save the result of the query if it doesn't already
                exist.
        '''

        manager = sources.SourceManager(self, table_dir)

        if isinstance(files, str):
            from glob import glob
            files = glob(files)
            if limit is not None:
                from random import shuffle
                shuffle(files)
                files = files[:limit]

        for i, f in enumerate(files):
            logger.info("Processing article %s..." % f)
            html = open(f).read()
            source = manager.identify_source(html)
            if source is None:
                logger.warning("Could not identify source for %s" % f)
                continue
            try:
                pmid = path.splitext(path.basename(f))[0] if pmid_filenames else None
                article = source.parse_article(html, pmid, metadata_dir=metadata_dir)
                if article and (config.SAVE_ARTICLES_WITHOUT_ACTIVATIONS or article.tables):
                    self.add(article)
                    if commit and (i % 100 == 0 or i == len(files) - 1):
                        self.save()
            except Exception as err:
                print(err)

    def delete_article(self, pmid):
        article = self.session.query(Article).filter_by(id=pmid).first()
        self.session.delete(article)
        self.session.commit()

    def print_stats(self):
        ''' Summarize the current state of the DB. '''
        n_articles = self.session.query(Article).count()
        n_tables = self.session.query(Table).count()
        n_activations = self.session.query(Activation).count()
        print("The database currently contains:\n\t%d articles\n\t%d tables\n\t%d activations" % n_articles, n_tables, n_activations)

    def article_exists(self, pmid):
        ''' Check if an article already exists in the database. '''
        return self.session.query(exists().where(Article.id==pmid)).scalar()

    @property
    def articles(self):
        return self.session.query(Article).all()

# Create a JSONString column type for convenience
class JsonString(TypeDecorator):
    impl = Text

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        else:
            return json.loads(value)

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        else:
            return json.dumps(value)


class Article(Base):

    __tablename__ = 'articles'

    id = Column(Integer, primary_key=True)
    title = Column(String(200))
    text = Column(LongText)
    journal = Column(String(200))
    space = Column(String(20))
    publisher = Column(String(200))
    doi = Column(String(200))
    year = Column(Integer)
    authors = Column(Text)
    abstract = Column(Text)
    citation = Column(Text)
    pubmed_metadata = Column(JsonString)
    created_at =  Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow,
                           onupdate=datetime.datetime.utcnow)

    tables = relationship('Table', cascade="all, delete-orphan",
                          backref='article')
    activations = relationship('Activation', cascade="all, delete-orphan",
                                backref='article')
    features = association_proxy('tags', 'feature')

    def __init__(self, text, pmid=None, doi=None, metadata=None):
        self.id = int(pmid)
        self.text = text
        self.space = extract.guess_space(text)
        self.doi = doi
        self.pubmed_metadata = metadata
        self.update_from_metadata()

    def update_from_metadata(self):
        if self.pubmed_metadata is not None:
            pmd = self.pubmed_metadata
            self.title = pmd['title']
            self.journal = pmd['journal']
            self.pubmed_metadata = pmd
            self.year = pmd['year']
            self.authors = pmd['authors']
            self.abstract = pmd['abstract']
            self.citation = pmd['citation']


class Table(Base):

    __tablename__ = 'tables'

    id = Column(Integer, primary_key=True)
    article_id = Column(Integer, ForeignKey('articles.id'))
    activations = relationship('Activation', cascade="all, delete-orphan",
                                backref='table')
    position = Column(Integer)   # The serial position of occurrence
    number = Column(String(10))   # The stated table ID (e.g., 1, 2b)
    label = Column(String(200))  # The full label (e.g., Table 1, Table 2b)
    caption = Column(Text)
    notes = Column(Text)
    n_activations = Column(Integer)
    n_columns = Column(Integer)

    def finalize(self):
        ''' Any cleanup and updating operations we need to do before saving. '''

        # # Remove duplicate activations--most commonly produced by problems with
        # # the grouping code.
        # act_defs = set()
        # to_keep = []
        # for a in self.activations:
        #     definition = json.dumps([a.x, a.y, a.z, a.groups])
        #     if definition not in act_defs:
        #         act_defs.add(definition)
        #         to_keep.append(a)
        # self.activations = to_keep

        self.n_activations = len(self.activations)


class Activation(Base):

    __tablename__ = 'activations'

    id = Column(Integer, primary_key=True)

    article_id = Column(Integer, ForeignKey('articles.id'))
    table_id = Column(Integer, ForeignKey('tables.id'))
    columns = Column(JsonString)
    groups = Column(JsonString)
    problems = Column(JsonString)
    x = Column(Float)
    y = Column(Float)
    z = Column(Float)
    number = Column(Integer)
    region = Column(String(100))
    hemisphere = Column(String(100))
    ba = Column(String(100))
    size = Column(String(100))
    statistic = Column(String(100))
    p_value = Column(String(100))

    def __init__(self):
        self.problems = []
        self.columns = {}

    def set_coords(self, x, y, z):
        self.x, self.y, self.z = [float(e) for e in [x, y, z]]

    def add_col(self, key, val):
        self.columns[key] = val

    # Validates Peak. Considers peak invalid if:
    # * At least one of X, Y, Z is nil or missing
    # * Any |coordinate| > 100
    # * Two or more columns are zeroes (most of the time this
    #   will indicate a problem, but occasionally a real coordinate)
    # Depending on config, either excludes peak, or allows it through
    # but flags potential problems for later inspection.
    def validate(self):

        for c in [self.x, self.y, self.z]:
            if c == '' or c is None:
                logger.debug('Missing x, y, or z coordinate information: [%s, %s, %s]' % tuple(
                    [str(e) for e in [self.x, self.y, self.z]]))
                return False
            try:
                if abs(c) >= 100:
                    logger.debug(
                        'Invalid coordinates: at least one dimension (x,y,z) >= 100.')
                    return False
            except:
                print(c)
                print(sys.exc_info()[0])
                raise

        sorted_xyz = sorted([abs(self.x), abs(self.y), abs(self.z)])
        if sorted_xyz[0] == 0 and sorted_xyz[1] == 0:
            logger.debug(
                "At least two dimensions have value == 0; coordinate is probably not real.")
            return False

        return True


# class Feature(Base):

#     __tablename__ = 'features'

#     id = Column(String, primary_key=True)
#     name = Column(String)


# class Tag(Base):

#     __tablename__ = 'tags'

#     feature_id = Column(Integer, ForeignKey('features.id'), primary_key=True)
#     article_id = Column(Integer, ForeignKey('articles.id'), primary_key=True)
#     weight = Column(Float)

#     article = relationship(Article, backref=backref(
#         "tags", cascade="all, delete-orphan"))
#     feature = relationship("Feature")
