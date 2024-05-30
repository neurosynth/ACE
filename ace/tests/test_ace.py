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
def test_weird_data_path():
    """Returns the path to test datasets, terminated with separator (/ vs \)"""
    return join(dirname(__file__), 'weird_data') + pathsep


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
def test_springer_source(test_data_path, source_manager):
    filename = join(test_data_path, 'springer.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    article = source.parse_article(html)
    tables = article.tables
    assert len(tables) == 1
    t = tables[0]
    assert t.number == '1'
    assert t.caption is not None
    assert t.n_activations == 12


@pytest.mark.vcr(record_mode="once")
def test_database_processing_stream(db, test_data_path):
    ingest.add_articles(db, test_data_path + '*.html')
    assert len(db.articles) == 5  # cannot find pmid for some articles
    export.export_database(db, 'exported_db')
    assert exists('exported_db')
    shutil.rmtree('exported_db')


@pytest.mark.vcr(record_mode="once")
def test_journal_scraping(test_data_path):
    scrape_path = join(test_data_path, 'scrape_test')
    os.makedirs(scrape_path, exist_ok=True)
    # Test with PLoS ONE because it's OA
    scraper = scrape.Scraper(scrape_path)
    scraper.retrieve_articles(
        'PLoS ONE',
        delay=5.0,
        mode='requests',
        search='fmri',
        limit=2,
        skip_pubmed_central=False,
        invalid_article_log_file=join(scrape_path, 'invalid_articles.log'),
        prefer_pmc_source=True,
    )
    # For now just check to make sure we have expected number of files in the directory
    plos_dir = join(scrape_path, 'html/PLoS ONE/')
    n_files = len([name for name in os.listdir(plos_dir) if os.path.isfile(plos_dir + name)])
    assert n_files == 2
    shutil.rmtree(scrape_path)


@pytest.mark.vcr(record_mode="once")
def test_cerebral_cortex_source(test_weird_data_path, source_manager):
    pmid = '11532885'
    filename = join(test_weird_data_path, pmid + '.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    article = source.parse_article(html, pmid=pmid)
    tables = article.tables
    assert len(tables) == 5
    total_activations = sum([t.n_activations for t in tables])
    assert total_activations == 44  # across 5 tables


@pytest.mark.vcr(record_mode="once")
def test_neuropsychologia_source(test_weird_data_path, source_manager):
    pmid = '29366950'
    filename = join(test_weird_data_path, pmid + '.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    article = source.parse_article(html, pmid=pmid)
    tables = article.tables
    assert len(tables) == 1
    assert tables[0].n_activations == 10


# this paper grabs brodmann areas as coordinates, but is not a priority to fix.
@pytest.mark.vcr(record_mode="once")
def test_brain_research_source(test_weird_data_path, source_manager):
    pmid = '18760263'
    filename = join(test_weird_data_path, pmid + '.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    article = source.parse_article(html, pmid=pmid)
    tables = article.tables
    assert len(tables) == 2
    total_activations = sum([t.n_activations for t in tables])
    assert total_activations == 26  # across 2 tables
