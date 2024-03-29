import os
import shutil
from os.path import dirname, join, exists, sep as pathsep

import pytest

from ace import sources, database, export, scrape, ingest



@pytest.fixture(scope="module")
def test_data_path():
    """Returns the path to test datasets, terminated with separator (/ vs \)"""
    return join(dirname(__file__), 'data') + pathsep


@pytest.fixture(scope="module")
def db():
    db = database.Database(adapter='sqlite', db_name='sqlite:///ace_test_database.tmp')
    yield db
    os.remove('ace_test_database.tmp')


@pytest.fixture(scope="module")
def source_manager(db):
    return sources.SourceManager(db)


@pytest.mark.vcr(record_mode="once")
def test_frontiers_source(test_data_path, source_manager):
    filename = join(test_data_path, 'frontiers.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    article = source.parse_article(html)
    tables = article.tables
    assert len(tables) == 3
    t = tables[2]
    assert t.number == '5'
    assert t.caption is not None
    assert t.n_activations == 13


@pytest.mark.vcr(record_mode="once")
def test_science_direct_source(test_data_path, source_manager):
    filename = join(test_data_path, 'cognition.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    article = source.parse_article(html)
    tables = article.tables
    assert len(tables) == 1
    t = tables[0]
    assert t.number == '1'
    assert t.caption is not None
    assert t.n_activations == 2


@pytest.mark.vcr(record_mode="once")
def test_plos_source(test_data_path, source_manager):
    filename = join(test_data_path, 'plosone.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    article = source.parse_article(html)
    tables = article.tables
    assert len(tables) == 1
    t = tables[0]
    assert t.number == '1'
    assert t.caption is not None
    assert t.n_activations == 24  # Since there are data for 2 experiments


@pytest.mark.vcr(record_mode="once")
def test_database_processing_stream(db, test_data_path):
    ingest.add_articles(db, test_data_path + '*.html')
    assert len(db.articles) == 4  # cannot find pmid for some articles
    export.export_database(db, 'exported_db')
    assert exists('exported_db')
    shutil.rmtree('exported_db')

@pytest.mark.vcr(record_mode="rewrite")
def test_journal_scraping(test_data_path):
    scrape_path = join(test_data_path, 'scrape_test')
    os.makedirs(scrape_path, exist_ok=True)
    # Test with PLoS ONE because it's OA
    scraper = scrape.Scraper(scrape_path)
    scraper.retrieve_articles('PLoS ONE', delay=5.0, mode='requests', search='fmri', limit=2, skip_pubmed_central=False)
    # For now just check to make sure we have expected number of files in the directory
    plos_dir = join(scrape_path, 'html/PLoS ONE/')
    n_files = len([name for name in os.listdir(plos_dir) if os.path.isfile(plos_dir + name)])
    assert n_files == 2
    shutil.rmtree(scrape_path)
