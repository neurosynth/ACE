# Database stuff and models

from sqlalchemy import TypeDecorator, Table, Column, Text, Integer, Float, String, ForeignKey, Boolean, DateTime
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.associationproxy import association_proxy
from datetime import datetime
import simplejson as json
import logging
import sys
import config, sources, scrapers

logger = logging.getLogger('ace')

Base = declarative_base()


# Handles all Database loading/saving stuff
class Database:

    def __init__(self, filename=None):
        ''' Connect to DB and initialize instance. '''

        if filename is None:
            filename = config.DATABASE_FILE
        engine = create_engine('sqlite:///%s' % filename, echo=False)
        Session = sessionmaker(bind=engine)
        Base.metadata.create_all(engine)
        self.session = Session()

    def add(self, record):
        ''' Add a record to the DB. '''
        self.session.add(record)

    def save(self):
        ''' Commit all stored records to file. '''
        self.session.commit()

    def add_articles(self, files, commit=True):
        ''' Process articles and add their data to the DB.
        Args:
            files: The path to the article(s) to process. Can be a single 
                filename (string), a list of filenames, or a path to pass 
                to glob (e.g., "article_dir/NIMG*html")
            commit: Whether or not to save records to DB file after adding them.
        '''

        manager = sources.SourceManager()

        if isinstance(files, basestring):
            from glob import glob
            files = glob(files)

        for f in files:
            logger.info("Processing article %s..." % f)
            html = open(f).read()
            source = manager.identify_source(html)
            article = source.parse_article(html)
            self.add(article)

        if commit:
            self.save()



# Create a JSONString column type for convenience
class JsonString(TypeDecorator):
    impl = String
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
    title = Column(String)
    journal = Column(String)
    publisher = Column(String)
    doi  = Column(String)
    year = Column(Integer)
    abstract = Column(Text)
    citation = Column(Text)
    pubmed_metadata = Column(JsonString)

    tables = relationship('Table', backref='article')
    activations = relationship('Activation', backref='article')
    features = association_proxy('tags', 'feature')

    def __init__(self, pmid=None):
        if pmid is not None:
            self.update_metadata_from_pubmed(pmid)

    def update_metadata_from_pubmed(self, pmid):
        pmd = scrapers.get_pubmed_metadata(pmid)
        self.id = int(pmid)
        self.title = pmd['title']
        self.journal = pmd['journal']
        self.pubmed_metadata = pmd
        self.year = pmd['year']
        self.abstract = pmd['abstract']
        self.citation = pmd['citation']


class Table(Base):

    __tablename__ = 'tables'

    id = Column(Integer, primary_key=True)
    article_id = Column(Integer, ForeignKey('articles.id'))
    activations = relationship('Activation', backref='table')
    number = Column(Integer)
    title = Column(String)
    caption = Column(Text)
    notes = Column(Text)
    n_activations = Column(Integer)
    n_columns = Column(Integer)

    def finalize(self):
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
    region = Column(String)
    hemisphere = Column(String)
    ba = Column(String)
    size = Column(String)
    statistic = Column(String)
    p_value = Column(String)


    def __init__(self):
        self.problems = []
        self.columns = {}

    def set_coords(self, x, y, z):
        self.x, self.y, self.z = [float(e) for e in [x,y,z]]
    
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
                logger.debug('Missing x, y, or z coordinate information: [%s, %s, %s]' % tuple([str(e) for e in [self.x, self.y, self.z]]))
                return False
            try:
                if abs(c) >= 100:
                    logger.debug('Invalid coordinates: at least one dimension (x,y,z) >= 100.')
                    return False
            except:
                print c
                print sys.exc_info()[0]
                raise

        sorted_xyz = sorted([abs(self.x), abs(self.y), abs(self.z)])
        if sorted_xyz[0] == 0 and sorted_xyz[1] == 0:
            logger.debug("At least two dimensions have value == 0; coordinate is probably not real.")
            return False

        return True


class Feature(Base):

    __tablename__ = 'features'

    id = Column(String, primary_key=True)
    name = Column(String)


class Tag(Base):

    __tablename__ = 'tags'

    feature_id = Column(Integer, ForeignKey('features.id'), primary_key=True)
    article_id = Column(Integer, ForeignKey('articles.id'), primary_key=True)
    weight = Column(Float)

    article = relationship(Article, backref=backref(
        "tags", cascade="all, delete-orphan"))
    feature = relationship("Feature")
