# Database stuff and models

from sqlalchemy import (TypeDecorator, Table, Column, Integer, Float, String, Boolean,
                        ForeignKey, DateTime, Text)
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.sql import exists
from datetime import datetime
import simplejson as json
import logging
import sys
from os import path
import datetime

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

        engine = create_engine(db_uri, echo=False, connect_args={'timeout': 15})

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
            # except Exception as err:
            #     print(err)

    def delete_article(self, pmid):
        article = self.session.query(Article).filter_by(id=pmid).first()
        self.session.delete(article)
        self.session.commit()

    def print_stats(self):
        ''' Summarize the current state of the DB. '''
        n_articles = self.session.query(Article).count()
        n_articles_with_coordinates = self.session.query(Article).join(Table).filter(Table.n_activations>0).distinct('article_id').count()
        n_tables = self.session.query(Table).count()
        n_activations = self.session.query(Activation).count()
        n_links = self.session.query(NeurovaultLink).count()
        n_articles_with_links = self.session.query(NeurovaultLink).distinct('article_id').count()
        print(f"The database currently contains: {n_articles} articles.\n"
        f"{n_articles_with_coordinates} have coordinates, and {n_articles_with_links} have NeuroVault links.\n"
        f"Total of {n_tables} tables, {n_activations} activations and {n_links} NeuroVault links.")

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

    neurovault_links = relationship('NeurovaultLink', cascade="all, delete-orphan",
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
            self.doi = pmd['doi']


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

    missing_source = Column(Boolean, default=False)

    def __init__(self):
        self.problems = []
        self.columns = {}

    def set_coords(self, x, y, z):
        new_xyz = []
        for c in [x, y, z]:
            if c == '' or c is None:
                c = None
            else:
                c = c.replace(' ', '').replace('--', '-').rstrip('.')
                c = float(c)
            new_xyz.append(c)

        self.x, self.y, self.z = new_xyz

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
        self.is_valid = True
        
        for c in [self.x, self.y, self.z]:
            if c == '' or c is None:
                logger.debug('Missing x, y, or z coordinate information: [%s, %s, %s]' % tuple(
                    [str(e) for e in [self.x, self.y, self.z]]))
                self.is_valid = False
                return False
            try:
                if abs(c) >= 100:
                    logger.debug(
                        'Invalid coordinates: at least one dimension (x,y,z) >= 100.')
                    self.is_valid = False
                    return False
            except:
                print(c)
                print(sys.exc_info()[0])
                raise

        sorted_xyz = sorted([abs(self.x), abs(self.y), abs(self.z)])
        if sorted_xyz[0] == 0 and sorted_xyz[1] == 0:
            logger.debug(
                "At least two dimensions have value == 0; coordinate is probably not real.")
            self.is_valid = False
            return False

        return True

class NeurovaultLink(Base):
    
    __tablename__ = 'Neurovaultlinks'

    id = Column(Integer, primary_key=True, autoincrement=True)
    neurovault_id = Column(Integer)
    url = Column(String(100))
    type = Column(String(100))

    article_id = Column(Integer, ForeignKey('articles.id'))
