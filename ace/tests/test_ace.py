import gzip
import json
import os
import shutil
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from os.path import dirname, join, exists, sep as pathsep

import pytest
import requests
from bs4 import BeautifulSoup
from vcr import VCR

from ace import sources, database, export, scrape, ingest
from ace import tableparser

from .conftest import API_KEY_PLACEHOLDER, FAKE_API_KEY


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
    return sources.SourceManager()


@pytest.mark.vcr(record_mode="once")
def test_pmc_source(test_data_path, source_manager):
    filename = join(test_data_path, 'pmc.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    source.__class__.__name__ == 'PmcSource'
    article = source.parse_article(html)
    tables = article.tables
    assert len(tables) == 1
    t = tables[0]
    assert t.number == '3'
    assert t.caption is not None
    assert t.n_activations == 11


def test_candidate_gate_retains_table_without_ace_activations(source_manager):
    html = """
    <html><head><meta name="citation_pmid" content="999001"></head><body>
      <div class="table-wrap">
        <h3>Table 1</h3>
        <table>
          <tr><th>Region</th><th>x</th><th>y</th><th>z</th></tr>
          <tr><td>Midline</td><td>0</td><td>0</td><td>10</td></tr>
        </table>
      </div>
    </body></html>
    """
    article = source_manager.sources["PMC"].parse_article(
        html, pmid="999001", skip_metadata=True
    )
    assert len(article.tables) == 1
    assert article.tables[0].n_activations == 0


def test_candidate_gate_handles_combined_coordinates_and_unicode_minus():
    decision = tableparser.classify_coordinate_table(rows=[
        ["Region", "MNI coordinates", "Peak Z"],
        ["Amygdala", "−20, -4, -18", "4.2"],
    ])
    assert decision.candidate
    assert "coordinate_context" in decision.reasons


def test_leaf_table_fallback_ignores_layout_and_behavior_tables(source_manager):
    html = """
    <html><body>
      <table class="layout"><tr><td>
        <table><tr><th>Group</th><th>Age</th></tr>
          <tr><td>Control</td><td>24</td></tr></table>
        <table><tr><th>Region</th><th>x</th><th>y</th><th>z</th></tr>
          <tr><td>Insula</td><td>-32</td><td>18</td><td>4</td></tr></table>
      </td></tr></table>
    </body></html>
    """
    article = source_manager.default_source.parse_article(
        html, pmid="999002", skip_metadata=True
    )
    assert len(article.tables) == 1
    assert article.tables[0].n_activations == 1


def test_extract_and_export_public_api(tmp_path):
    article_path = tmp_path / "999003.html"
    article_path.write_text(
        """
        <html><body><table>
          <tr><th>Region</th><th>x</th><th>y</th><th>z</th></tr>
          <tr><td>Midline</td><td>0</td><td>0</td><td>10</td></tr>
        </table></body></html>
        """,
        encoding="utf-8",
    )
    output_dir = tmp_path / "processed"
    summary = ingest.extract_and_export([article_path], output_dir)
    assert summary["articles"] == 1
    assert summary["tables"] == 1
    assert summary["coordinates"] == 0
    assert (output_dir / "coordinates.csv").exists()
    assert (output_dir / "tables.csv").exists()
    assert len(list((output_dir / "tables" / "999003").glob("*.html"))) == 1


@pytest.mark.vcr(record_mode="once")
def test_frontiers_source(test_data_path, source_manager):
    filename = join(test_data_path, 'frontiers.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    source.__class__.__name__ == 'FrontiersSource'
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
    assert source.__class__.__name__ == 'ScienceDirectSource'
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
    assert source.__class__.__name__ == 'PlosSource'
    article = source.parse_article(html)
    tables = article.tables
    assert len(tables) == 1
    t = tables[0]
    assert t.number == '1'
    assert t.caption is not None
    assert t.n_activations == 24  # Since there are data for 2 experiments

@pytest.mark.vcr(record_mode="once")
def test_springer_table_links_resolve_against_page_url(
        test_data_path, source_manager, attempted_urls):
    # This Nature page has no citation_fulltext_html_url; its table links are
    # root-relative and the only statement of its own URL is the canonical link.
    html = open(join(test_data_path, 'springer-nature-relative-tables.html')).read()
    source = source_manager.identify_source(html)
    assert source.__class__.__name__ == 'SpringerSource'
    source.parse_article(html, pmid='42401715', skip_metadata=True)
    # The first request is the table page; the rest are the publisher's
    # cookie-check redirects.
    assert attempted_urls[0] == (
        'https://www.nature.com/articles/s41380-026-03719-9/tables/1')


@pytest.mark.vcr(record_mode="once")
def test_springer_source(test_data_path, source_manager):
    filename = join(test_data_path, 'springer.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    assert source.__class__.__name__ == 'SpringerSource'
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
    assert len(db.articles) == 12  # cannot find pmid for some articles
    export.export_database(db, 'exported_db')
    assert exists('exported_db')
    shutil.rmtree('exported_db')


@pytest.mark.vcr(record_mode="once")
def test_journal_scraping(test_data_path, source_manager, attempted_urls):
    scrape_path = join(test_data_path, 'scrape_test')
    # Start clean: index_pmids makes the scraper skip anything already on disk,
    # so articles left behind by an earlier failed run would send it past the
    # PMIDs the cassette covers.
    shutil.rmtree(scrape_path, ignore_errors=True)
    os.makedirs(scrape_path)
    # Test with PLoS ONE because it's OA
    scraper = scrape.Scraper(scrape_path)
    try:
        scraper.retrieve_articles(
            'PLoS ONE',
            delay=5.0,
            mode='requests',
            search='fmri',
            limit=2,
            index_pmids=True,
            skip_pubmed_central=False,
            invalid_article_log_file=join(scrape_path, 'invalid_articles.log'),
            prefer_pmc_source=True,
        )
        plos_dir = join(scrape_path, 'html/PLoS ONE/')
        names = [name for name in os.listdir(plos_dir) if os.path.isfile(plos_dir + name)]
        docs = [open(join(plos_dir, name)).read() for name in names]
    finally:
        shutil.rmtree(scrape_path, ignore_errors=True)

    assert len(names) == 2
    assert not [url for url in attempted_urls if 'pubmed.ncbi.nlm.nih.gov' in url]
    # Each file must be the PLoS NLM XML. Neither article had a LinkOut record
    # when this was recorded, so both are addressed by their esummary DOI.
    for name, doc in zip(names, docs):
        assert '<article' in doc, name
        assert 'pubmed.ncbi.nlm.nih.gov' not in doc, name
        assert source_manager.identify_source(doc).__class__.__name__ == 'PlosSource', name
   


@pytest.mark.vcr(record_mode="once")
def test_cerebral_cortex_source(test_weird_data_path, source_manager):
    pmid = '11532885'
    filename = join(test_weird_data_path, pmid + '.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    assert source.__class__.__name__ == 'OUPSource'
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
    assert source.__class__.__name__ == 'ScienceDirectSource'
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
    assert source.__class__.__name__ == 'ScienceDirectSource'
    article = source.parse_article(html, pmid=pmid)
    tables = article.tables
    assert len(tables) == 2
    total_activations = sum([t.n_activations for t in tables])
    assert total_activations == 26  # across 2 tables


def test_pmc_embedded_table(test_weird_data_path, source_manager):
    pmid = '20159144'
    filename = join(test_weird_data_path, pmid + '.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    article = source.parse_article(html, pmid=pmid)
    tables = article.tables
    assert len(tables) == 0


def test_wiley_label(test_weird_data_path, source_manager):
    pmid = '36196770'
    filename = join(test_weird_data_path, pmid + '.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    article = source.parse_article(html, pmid=pmid)
    tables = article.tables
    assert len(tables) == 1


def test_elsivier_table_parse(test_weird_data_path, source_manager):
    pmid = '12417470'
    filename = join(test_weird_data_path, pmid + '.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    article = source.parse_article(html, pmid=pmid)
    tables = article.tables
    # The candidate gate retains the two malformed coordinate tables even though
    # deterministic parsing cannot recover activations from them.
    assert len(tables) == 6
    assert sum(table.n_activations == 0 for table in tables) == 2


def test_multi_column_float_conversion(test_weird_data_path, source_manager):
    pmid = '26021218'
    filename = join(test_weird_data_path, pmid + '.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    article = source.parse_article(html, pmid=pmid)
    tables = article.tables
    assert len(tables) == 2  # should be 0, but not removing innacurate tables yet.


def test_frontier_table_identification(test_weird_data_path, source_manager):
    pmid = '26696806'
    filename = join(test_weird_data_path, pmid + '.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    article = source.parse_article(html, pmid=pmid)
    tables = article.tables
    assert len(tables) == 0


def test_schizophrenia_research_source(test_weird_data_path, source_manager):
    pmid = '18439804'
    filename = join(test_weird_data_path, pmid + '.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    article = source.parse_article(html, pmid=pmid)
    tables = article.tables
    assert len(tables) == 2
    assert all(table.n_activations == 0 for table in tables)


def test_find_tables_in_old_sciencedirect(test_weird_data_path, source_manager):
    pmid = '22695256'
    filename = join(test_weird_data_path, pmid + '.html')
    filename = join(test_weird_data_path, pmid + '.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    article = source.parse_article(html, pmid=pmid)
    tables = article.tables
    assert len(tables) == 3
    extracted_coordinates = set([(a.x, a.y, a.z) for a in tables[-1].activations])
    table_5_coordinates = set([(-18, -80, 20), (20, 16, 30)])
    assert extracted_coordinates == table_5_coordinates


def test_old_springer_source(test_weird_data_path, source_manager):
    pmid = '23813017'
    filename = join(test_weird_data_path, pmid + '.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    article = source.parse_article(html, pmid=pmid)
    tables = article.tables
    assert len(tables) == 0


def test_old_table_parser(test_weird_data_path, source_manager):
    pmid = '15716157'
    filename = join(test_weird_data_path, pmid + '.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    article = source.parse_article(html, pmid=pmid)
    tables = article.tables
    assert len(tables) == 3


def test_empty_columns(test_weird_data_path, source_manager):
    pmid = '28432782'
    filename = join(test_weird_data_path, pmid + '.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    article = source.parse_article(html, pmid=pmid)
    tables = article.tables
    assert len(tables) == 2

@pytest.mark.vcr(record_mode="once")
def test_stroke_table(test_weird_data_path, source_manager):
    pmid = '38990127'
    filename = join(test_weird_data_path, pmid + '.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    article = source.parse_article(html, pmid=pmid)
    tables = article.tables
    assert len(tables) == 2


@pytest.mark.vcr(record_mode="once")
def test_taylor_and_francis_source(test_data_path, source_manager):
    filename = join(test_data_path, 'tandfonline.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    assert source is not None
    assert source.__class__.__name__ == 'TaylorAndFrancisSource'
    article = source.parse_article(html, pmid='12345678')
    tables = article.tables
    assert len(tables) == 2
    # Check first table
    t1 = tables[0]
    assert t1.number == '1'
    assert t1.label == 'Table 1'
    assert 'Talairach coordinates' in t1.caption
    assert t1.n_activations >= 2
    # Check second table
    t2 = tables[1]
    assert t2.number == '2'
    assert t2.label == 'Table 2'
    assert 'Talairach coordinates' in t2.caption
    assert t2.n_activations >= 2


@pytest.mark.vcr(record_mode="once")
def test_ampsych_source(test_data_path, source_manager):
    filename = join(test_data_path, 'ampsych.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    assert source.__class__.__name__ == 'AmPsychSource'
    article = source.parse_article(html)
    tables = article.tables
    assert len(tables) == 1
    t = tables[0]
    assert t.number == '2'
    assert "Brain Regions Demonstrating Differential BOLD" in t.caption
    assert t.n_activations > 20

@pytest.mark.vcr(record_mode="once")
def test_mdpi_source(test_data_path, source_manager):
    filename = join(test_data_path, 'mdpi.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    assert source.__class__.__name__ == 'MDPISource'
    article = source.parse_article(html)
    tables = article.tables
    assert len(tables) == 2
    t = tables[0]
    assert t.number == '1'
    assert "Brain activation regions when gripping each stress ball" in t.caption
    assert t.n_activations > 20

@pytest.mark.vcr(record_mode="once")
def test_sage_source(test_data_path, source_manager):
    filename = join(test_data_path, 'sage.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    assert source.__class__.__name__ == 'SageSource'
    article = source.parse_article(html)
    tables = article.tables
    assert len(tables) == 2
    t = tables[0]
    assert t.number == '2'
    assert "Brain regions showing hypometabolism correlated with ADL at the whole-brain level in patients with PCA" == t.caption
    assert t.n_activations == 6

@pytest.mark.vcr(record_mode="once")
def test_springer_nature_source(test_data_path, source_manager):
    filename = join(test_data_path, 'springer-nature.html')
    html = open(filename).read()
    source = source_manager.identify_source(html)
    assert source.__class__.__name__ == 'SpringerSource'
    article = source.parse_article(html)
    tables = article.tables
    assert len(tables) == 1
    t = tables[0]
    assert t.number == '2'
    assert "fMRI results across all participants" in t.caption
    assert t.n_activations  == 9


def _count_valid_activations(tables):
    return sum(
        1 for t in tables for a in t.activations
        if a.x is not None and a.y is not None and a.z is not None
    )


def test_pmc_modern_table_wrapper_source(test_weird_data_path, source_manager):
    pmid = '16085533'
    html = open(join(test_weird_data_path, pmid + '.html')).read()
    source = source_manager.identify_source(html)
    assert source is not None
    assert source.__class__.__name__ == 'PMCSource'
    article = source.parse_article(html, pmid=pmid, skip_metadata=True)
    assert article is not None
    assert len(article.tables) >= 1
    assert _count_valid_activations(article.tables) >= 1


def test_oup_table_wrap_fallback_source(test_weird_data_path, source_manager):
    pmid = '24700584'
    html = open(join(test_weird_data_path, pmid + '.html')).read()
    source = source_manager.identify_source(html)
    assert source is not None
    assert source.__class__.__name__ == 'OUPSource'
    article = source.parse_article(html, pmid=pmid, skip_metadata=True)
    assert article is not None
    assert len(article.tables) >= 1
    assert _count_valid_activations(article.tables) >= 1


def test_jcn_thumbnail_placeholder_is_not_exported_as_a_table(test_weird_data_path, source_manager):
    pmid = '24666131'
    html = open(join(test_weird_data_path, pmid + '.html')).read()
    source = source_manager.identify_source(html)
    assert source is not None
    assert source.__class__.__name__ == 'JournalOfCognitiveNeuroscienceSource'
    article = source.parse_article(html, pmid=pmid, skip_metadata=True)
    assert article is not None
    # This legacy page has only a JavaScript thumbnail link. Treating the
    # surrounding article layout as a table creates reference-list coordinates.
    assert article.tables == []


def test_jcn_thumbnail_placeholder_expands_linked_article_tables(
    test_weird_data_path,
    source_manager,
    monkeypatch,
):
    pmid = '24666131'
    html = open(join(test_weird_data_path, pmid + '.html')).read()
    source = source_manager.identify_source(html)
    linked_html = """
    <html><body><figure class="table">
      <figcaption>Table 1. Peak MNI coordinates</figcaption>
      <table>
        <tr><th>Region</th><th>x</th><th>y</th><th>z</th></tr>
        <tr><td>Insula</td><td>-32</td><td>18</td><td>4</td></tr>
      </table>
    </figure></body></html>
    """
    monkeypatch.setattr(scrape, "get_dynamic_html", lambda url: linked_html)

    article = source.parse_article(
        html,
        pmid=pmid,
        skip_metadata=True,
        expand_linked_tables=True,
    )

    assert len(article.tables) == 1
    assert article.tables[0].n_activations == 1


def test_sciencedirect_combined_coordinate_column_source(test_weird_data_path, source_manager):
    pmid = '15327927'
    html = open(join(test_weird_data_path, pmid + '.html')).read()
    source = source_manager.identify_source(html)
    assert source is not None
    assert source.__class__.__name__ == 'ScienceDirectSource'
    article = source.parse_article(html, pmid=pmid, skip_metadata=True)
    assert article is not None
    assert len(article.tables) >= 1
    assert _count_valid_activations(article.tables) >= 1


def test_springer_inline_table_fallback_source(test_weird_data_path, source_manager):
    pmid = '27007121'
    html = open(join(test_weird_data_path, pmid + '.html')).read()
    source = source_manager.identify_source(html)
    assert source is not None
    assert source.__class__.__name__ == 'SpringerSource'
    article = source.parse_article(html, pmid=pmid, skip_metadata=True)
    assert article is not None
    assert len(article.tables) >= 1
    assert _count_valid_activations(article.tables) >= 1


def test_unknown_source_coordinate_table_with_force_ingest(test_weird_data_path, tmp_path):
    pmid = '11296095'
    src_file = join(test_weird_data_path, pmid + '.html')
    target_file = tmp_path / f"{pmid}.html"
    shutil.copy(src_file, target_file)

    db_path_no_force = f"sqlite:///{(tmp_path / 'ace_no_force.db').as_posix()}"
    db_no_force = database.Database(adapter='sqlite', db_name=db_path_no_force)
    missing_sources = ingest.add_articles(
        db_no_force,
        [str(target_file)],
        pmid_filenames=True,
        force_ingest=False,
        num_workers=1,
        skip_metadata=True,
    )
    assert str(target_file) in missing_sources
    assert len(db_no_force.articles) == 0

    db_path_force = f"sqlite:///{(tmp_path / 'ace_force.db').as_posix()}"
    db_force = database.Database(adapter='sqlite', db_name=db_path_force)
    missing_sources_force = ingest.add_articles(
        db_force,
        [str(target_file)],
        pmid_filenames=True,
        force_ingest=True,
        num_workers=1,
        skip_metadata=True,
    )
    assert str(target_file) not in missing_sources_force
    assert len(db_force.articles) >= 1
    assert len(db_force.articles[0].tables) >= 1
    assert _count_valid_activations(db_force.articles[0].tables) >= 1


def test_ingest_prefers_best_duplicate_pmid_file(test_weird_data_path, tmp_path):
    pmid = "17913474"
    bad_src = join(test_weird_data_path, "17913474_recaptcha.html")
    good_src = join(test_weird_data_path, "17913474_pond.html")

    bad_dir = tmp_path / "a_bad"
    good_dir = tmp_path / "b_good"
    bad_dir.mkdir()
    good_dir.mkdir()

    # Same PMID filename in two different source folders
    bad_target = bad_dir / f"{pmid}.html"
    good_target = good_dir / f"{pmid}.html"
    shutil.copy(bad_src, bad_target)
    shutil.copy(good_src, good_target)

    db_path = f"sqlite:///{(tmp_path / 'ace_dupe_pick_best.db').as_posix()}"
    db = database.Database(adapter='sqlite', db_name=db_path)

    # Intentionally place blocked/challenge page first.
    ingest.add_articles(
        db,
        [str(bad_target), str(good_target)],
        pmid_filenames=True,
        force_ingest=True,
        num_workers=1,
        skip_metadata=True,
    )

    assert len(db.articles) == 1
    assert len(db.articles[0].tables) >= 1
    assert _count_valid_activations(db.articles[0].tables) >= 1


def test_validate_scrape_flags_recaptcha_challenge_page(test_weird_data_path):
    html = open(join(test_weird_data_path, "17913474_recaptcha.html")).read()
    assert scrape._validate_scrape(html) is False


def test_validate_scrape_allows_embedded_recaptcha_widget(test_weird_data_path):
    # PubMed embeds a reCAPTCHA widget in the "Email" form of every abstract
    # page, so the widget alone must not condemn an otherwise fine scrape.
    html = open(join(test_weird_data_path, "38814901_pubmed_recaptcha_widget.html")).read()
    assert "g-recaptcha" in html
    assert scrape._validate_scrape(html) is True


def test_xml_url_of_a_plos_article_is_its_manuscript():
    assert scrape._xml_url(
        "https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0303969"
    ) == (
        "https://journals.plos.org/plosone/article/file"
        "?id=10.1371/journal.pone.0303969&type=manuscript"
    )


def test_xml_url_takes_the_plos_site_slug_from_the_url():
    # A journal that is not in any name map still gets its own site.
    url = scrape._xml_url(
        "https://journals.plos.org/mentalhealth/article?id=10.1371/journal.pmen.0000749")
    assert url.startswith("https://journals.plos.org/mentalhealth/article/file?id=10.1371/journal.pmen.0000749")


def test_xml_url_is_none_for_other_publishers():
    assert scrape._xml_url("https://pubmed.ncbi.nlm.nih.gov/12345678/") is None


def test_check_for_substitute_url_leaves_a_plos_article_alone(tmp_path):
    # The manuscript is a download, which a browser saves to disk, so browser
    # mode never opens it.
    scraper = scrape.Scraper(str(tmp_path))
    landing = "https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0303969"
    assert scraper.check_for_substitute_url(landing) == landing


@pytest.mark.parametrize('url, expected', [
    ("https://www.frontiersin.org/articles/10.3389/fnhum.2014.01073/full",
     "https://www.frontiersin.org/articles/10.3389/fnhum.2014.01073/xml/nlm"),
    ("https://www.frontiersin.org/articles/10.3389/fnhum.2014.01073/abstract/",
     "https://www.frontiersin.org/articles/10.3389/fnhum.2014.01073/xml/nlm"),
    ("https://www.frontiersin.org/articles/10.3389/fnhum.2014.01073", None),
])
def test_xml_url_of_a_frontiers_article_is_its_nlm_xml(url, expected):
    # A literal backslash in the replacement made the old rewrite a 404.
    assert scrape._xml_url(url) == expected


def test_check_for_substitute_url_leaves_a_frontiers_article_alone(tmp_path):
    # A browser wraps the XML in its viewer, so browser mode reads the HTML page.
    scraper = scrape.Scraper(str(tmp_path))
    page = "https://www.frontiersin.org/articles/10.3389/fnhum.2014.01073/full"
    assert scraper.check_for_substitute_url(page) == page


def test_validate_scrape_flags_contentless_recaptcha_page():
    html = (
        '<html><head><title>Just a moment</title></head><body>'
        '<div class="g-recaptcha" data-sitekey="abc"></div>'
        '</body></html>'
    )
    assert scrape._validate_scrape(html) is False


@pytest.mark.parametrize(
    "pmid,expected_source",
    [
        ("17088334", "PMCSource"),
        ("26342221", "OUPSource"),
        ("27623361", "ScienceDirectSource"),
        ("27319001", "SpringerSource"),
        ("12860777", None),  # Unknown source -> DefaultSource fallback
    ],
)
def test_additional_missed_in_main_text_regressions(test_weird_data_path, source_manager, pmid, expected_source):
    html = open(join(test_weird_data_path, pmid + ".html")).read()
    source = source_manager.identify_source(html)

    if expected_source is None:
        assert source is None
        parser = source_manager.default_source
        assert parser is not None
    else:
        assert source is not None
        assert source.__class__.__name__ == expected_source
        parser = source

    article = parser.parse_article(html, pmid=pmid, skip_metadata=True)
    assert article is not None
    assert len(article.tables) >= 1
    assert _count_valid_activations(article.tables) >= 1


def test_table_text_repeats_spanned_cells():
    table = BeautifulSoup(
        "<table><caption>Peaks</caption>"
        "<tr><th rowspan=2>Region</th><th colspan=3>MNI</th></tr>"
        "<tr><td>x</td><td>y</td><td>z</td></tr>"
        "<tr><td>IFG</td><td>-42</td><td>18</td><td>6</td></tr></table>",
        "lxml",
    ).table
    assert sources.table_text(table) == (
        "Peaks\nRegion\tMNI\tMNI\tMNI\nRegion\tx\ty\tz\nIFG\t-42\t18\t6"
    )


def test_keep_tables_puts_tables_in_the_text(test_data_path, source_manager):
    html = open(join(test_data_path, 'pmc.html')).read()
    source = source_manager.identify_source(html)
    plain = source.parse_article(html, pmid='1', skip_metadata=True).text
    kept = source.parse_article(
        html, pmid='1', skip_metadata=True, keep_tables=True).text
    rows = [line for line in kept.splitlines() if line.count('\t') >= 2]
    assert rows
    assert not any(line.count('\t') >= 2 for line in plain.splitlines())
    assert '[ace-table-' not in kept


def test_a_table_readability_drops_is_appended(source_manager):
    source = source_manager.sources['Default'] if 'Default' in source_manager.sources \
        else next(iter(source_manager.sources.values()))
    source._clean_html_with_readability = lambda html: "Body text."
    soup = BeautifulSoup(
        "<html><body><p>Body text.</p>"
        "<table><tr><td>a</td><td>b</td></tr></table>"
        "<div hidden><table><tr><td>a</td><td>b</td></tr></table></div>"
        "</body></html>", "lxml")
    assert source._text_with_tables(soup) == "Body text.\n\na\tb"


ELINK_PRLINKS = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?cmd=prlinks&dbfrom=pubmed&id={}&retmode=json'
ELINK_PMC = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&id={}&linkname=pubmed_pmc&retmode=json'
CASSETTES = join(dirname(__file__), 'cassettes', 'test_ace')
PROVIDER_CASSETTE = join(CASSETTES, 'test_get_html_by_pmid_opens_the_provider_url_from_the_keyed_lookup.yaml')
PREFERS_PMC_CASSETTE = join(CASSETTES, 'test_get_html_by_pmid_prefers_pmc.yaml')
PMCID_LOOKUP_400_CASSETTE = join(CASSETTES, 'test_pmcid_lookup_failure_is_logged_without_the_api_key.yaml')
NO_LINKOUT_CASSETTE = join(CASSETTES, 'test_get_html_by_pmid_without_linkout_opens_nothing_for_other_journals[book-chapter].yaml')
ESUMMARY = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id={}&retmode=json'


def _played_urls(cassette):
    # cassette.requests is everything the cassette file holds; play_counts is
    # what the code actually asked for, so a test that uses this fails on the
    # run that records its cassette. Cassette URIs are stored with api_key
    # filtered out and the query sorted.
    return [request.uri for index, request in enumerate(cassette.requests) if cassette.play_counts[index]]


def _without_retry_backoff(scraper):
    # PubMedAPI retries a 400 five times with exponential backoff, 30 s in
    # all; keep the retries and skip the sleeps.
    scraper._client.session.get_adapter('https://').max_retries.backoff_factor = 0
    return scraper


@pytest.mark.vcr(record_mode="once")
def test_get_html_by_pmid_opens_the_provider_url_from_the_keyed_lookup(vcr, tmp_path):
    # PMC is listed before Frontiers in the elink JSON, but prlinks sends us to
    # Frontiers, so that is the page we must open, and without the unkeyed
    # cmd=prlinks&retmode=ref hop through E-utilities.
    scraper = scrape.Scraper(str(tmp_path))

    html = scraper.get_html_by_pmid(
        '25709573', mode='requests', prefer_pmc_source=False)

    played = _played_urls(vcr)
    assert vcr.all_played
    assert played[:2] == [ELINK_PRLINKS.format('25709573'), 'https://doi.org/10.3389/fnhum.2014.01073']
    # The article's full-text page redirects are followed first; the NLM XML of
    # that page is the last request.
    assert played[-1] == 'https://www.frontiersin.org/journals/human-neuroscience/articles/10.3389/fnhum.2014.01073/xml/nlm'
    assert [url for url in played if 'eutils.ncbi.nlm.nih.gov' in url] == [ELINK_PRLINKS.format('25709573')]
    # The NLM XML, not the article page, is what is saved.
    assert '<article-title>' in html


@pytest.mark.vcr(record_mode="once")
@pytest.mark.parametrize('pmid, doi, article_page', [
    ('42868208', '10.1371/journal.pone.0360047', 'https://journals.plos.org/plosone/article/file?id='),
    ('42868163', '10.1371/journal.pmen.0000749', 'https://journals.plos.org/mentalhealth/article/file?id='),
], ids=['plos-one', 'plos-mental-health'])
def test_get_html_by_pmid_without_linkout_opens_the_plos_article_by_doi(
        vcr, tmp_path, attempted_urls, pmid, doi, article_page):
    # Freshly indexed PLoS articles have no LinkOut record yet. The journal
    # passed in is a placeholder, so PubMed's esummary has to say it is PLoS.
    scraper = scrape.Scraper(str(tmp_path))

    html = scraper.get_html_by_pmid(pmid, mode='requests')

    played = _played_urls(vcr)
    assert vcr.all_played
    assert set(attempted_urls) == set(played)
    assert played[:3] == [ELINK_PRLINKS.format(pmid), ESUMMARY.format(pmid), f'https://doi.org/{doi}']
    # The article page is opened, then its NLM XML is fetched; the manuscript
    # redirects to a signed storage URL.
    assert [url for url in played if 'type=manuscript' in url and url.startswith(article_page)]
    assert not [url for url in played if 'pubmed.ncbi.nlm.nih.gov' in url]
    assert f'<article-id pub-id-type="doi">{doi}</article-id>' in html
    assert scrape._validate_scrape(html)


@pytest.mark.vcr(record_mode="once")
@pytest.mark.parametrize('pmid', ['28520367', '42869849'], ids=['book-chapter', 'article-with-a-doi'])
def test_get_html_by_pmid_without_linkout_opens_nothing_for_other_journals(
        vcr, tmp_path, caplog, attempted_urls, pmid):
    # The chapter has no DOI at all; the article has one that isn't PLoS's.
    scraper = scrape.Scraper(str(tmp_path))

    with caplog.at_level('INFO', logger='ace.scrape'):
        html = scraper.get_html_by_pmid(pmid, mode='requests')

    assert html is None
    assert vcr.all_played
    assert _played_urls(vcr) == [ELINK_PRLINKS.format(pmid), ESUMMARY.format(pmid)]
    assert set(attempted_urls) == set(_played_urls(vcr))
    assert f'No LinkOut record for PMID {pmid} and not a PLoS article' in caplog.text
    assert 'ERROR' not in caplog.text


@pytest.mark.vcr(record_mode="once")
def test_get_html_by_pmid_logs_an_esummary_error_reply_as_a_failed_lookup(vcr, tmp_path, caplog, attempted_urls):
    # NCBI answers an unknown PMID with HTTP 200 and a per-UID error instead
    # of a summary. That is a failed lookup, not an article that isn't PLoS.
    scraper = scrape.Scraper(str(tmp_path))

    with caplog.at_level('INFO', logger='ace.scrape'):
        html = scraper.get_html_by_pmid('99999999', mode='requests')

    assert html is None
    assert vcr.all_played
    assert _played_urls(vcr) == [ELINK_PRLINKS.format('99999999'), ESUMMARY.format('99999999')]
    assert set(attempted_urls) == set(_played_urls(vcr))
    assert 'E-utilities summary failed for PMID 99999999: cannot get document summary' in caplog.text
    assert 'Skipping' not in caplog.text


@pytest.mark.vcr(NO_LINKOUT_CASSETTE, record_mode="none")
def test_process_article_without_linkout_reports_an_invalid_article(vcr, tmp_path, attempted_urls):
    scraper = scrape.Scraper(str(tmp_path))

    filename, valid = scraper.process_article('28520367', 'Other journal', mode='requests')

    assert valid is None
    assert not filename.exists()
    assert vcr.all_played
    assert set(attempted_urls) == set(_played_urls(vcr))


@pytest.mark.vcr(record_mode="once")
def test_plos_summary_failure_is_logged_without_the_api_key(vcr, tmp_path, caplog, attempted_urls):
    scraper = _without_retry_backoff(scrape.Scraper(str(tmp_path), api_key=FAKE_API_KEY))

    with caplog.at_level('INFO', logger='ace.scrape'):
        url = scraper._plos_url_without_linkout('42868208')

    assert url is None
    assert vcr.all_played
    assert set(attempted_urls) == {ESUMMARY.format('42868208')}
    assert 'E-utilities summary failed for PMID 42868208' in caplog.text
    assert 'Skipping' not in caplog.text
    assert '<api_key>' in caplog.text
    assert FAKE_API_KEY not in caplog.text


@pytest.mark.vcr(record_mode="once")
def test_get_html_by_pmid_prefers_pmc(vcr, tmp_path):
    scraper = scrape.Scraper(str(tmp_path))

    scraper.get_html_by_pmid('25709573', mode='requests', prefer_pmc_source=True)

    played = _played_urls(vcr)
    assert vcr.all_played
    assert played[:3] == [
        ELINK_PRLINKS.format('25709573'),
        ELINK_PMC.format('25709573'),
        'https://pmc.ncbi.nlm.nih.gov/articles/PMC4325919',
    ]
    assert not [url for url in played if 'doi.org' in url or 'frontiersin' in url]


@pytest.mark.vcr(record_mode="once")
def test_get_html_by_pmid_opens_pmc_when_it_is_the_only_provider(vcr, tmp_path):
    scraper = scrape.Scraper(str(tmp_path))

    scraper.get_html_by_pmid('20311739', mode='requests', prefer_pmc_source=False)

    played = _played_urls(vcr)
    assert vcr.all_played
    assert played[:2] == [
        ELINK_PRLINKS.format('20311739'),
        'https://pmc.ncbi.nlm.nih.gov/articles/pmid/20311739/',
    ]


@pytest.mark.vcr(record_mode="once")
def test_get_html_by_pmid_pmc_only_skips_articles_without_pmc(vcr, tmp_path):
    scraper = scrape.Scraper(str(tmp_path))

    html = scraper.get_html_by_pmid('28520367', mode='requests', prefer_pmc_source='only')

    assert html is None
    assert _played_urls(vcr) == [ELINK_PRLINKS.format('28520367')]


@pytest.mark.vcr(PREFERS_PMC_CASSETTE, record_mode="none")
def test_get_html_by_pmid_pmc_only_opens_pmc(vcr, tmp_path):
    scraper = scrape.Scraper(str(tmp_path))

    html = scraper.get_html_by_pmid('25709573', mode='requests', prefer_pmc_source='only')

    assert html is not None
    assert vcr.all_played
    assert _played_urls(vcr)[:3] == [
        ELINK_PRLINKS.format('25709573'),
        ELINK_PMC.format('25709573'),
        'https://pmc.ncbi.nlm.nih.gov/articles/PMC4325919',
    ]


# No PMID we tried lists PMC in LinkOut without a pubmed_pmc link, so this
# pairs the recorded prlinks JSON for 25709573 with the recorded 400s NCBI
# gave its PMCID lookup under an invalid key.
@pytest.mark.vcr(PROVIDER_CASSETTE, PMCID_LOOKUP_400_CASSETTE, record_mode="none")
def test_get_html_by_pmid_pmc_only_skips_when_the_pmcid_lookup_fails(vcr, tmp_path, caplog):
    scraper = _without_retry_backoff(scrape.Scraper(str(tmp_path), api_key=FAKE_API_KEY))

    with caplog.at_level('INFO', logger='ace.scrape'):
        html = scraper.get_html_by_pmid('25709573', mode='requests', prefer_pmc_source='only')

    assert html is None
    assert sorted(_played_urls(vcr)) == sorted([ELINK_PRLINKS.format('25709573')] + [ELINK_PMC.format('25709573')] * 6)
    assert 'Failed to resolve PMCID for PMID 25709573' in caplog.text
    assert 'PMC source detected, but PMCID lookup failed' in caplog.text


@pytest.mark.vcr(record_mode="once")
def test_get_html_by_pmid_treats_an_elink_reply_without_linksets_as_a_failed_lookup(vcr, tmp_path):
    # NCBI answers every PMID it can parse with linksets[0].idurllist, even
    # with no LinkOut; an empty id gets a 200 with "linksets": [].
    scraper = scrape.Scraper(str(tmp_path))

    html = scraper.get_html_by_pmid('', mode='requests', prefer_pmc_source=False)

    assert html is None
    # Filtering api_key re-encodes the stored query without the blank id.
    assert _played_urls(vcr) == [
        'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?cmd=prlinks&dbfrom=pubmed&retmode=json']


@pytest.mark.vcr(record_mode="once")
def test_get_html_by_pmid_skips_a_blocked_provider_it_would_not_open(vcr, tmp_path):
    # HighWire is listed first and is the page prlinks opens; PMC, listed
    # second, is blocked, and any blocked provider skips the article.
    scraper = scrape.Scraper(str(tmp_path))

    html = scraper.get_html_by_pmid(
        '21159981', mode='requests', prefer_pmc_source=False,
        skip_url_substrings=['pmc.ncbi.nlm.nih.gov'])

    assert html is None
    assert scraper._skip_article_due_to_url
    assert _played_urls(vcr) == [ELINK_PRLINKS.format('21159981')]


@pytest.mark.vcr(record_mode="once")
def test_get_html_by_pmid_returns_none_when_the_keyed_lookup_fails(vcr, tmp_path, caplog):
    # NCBI answers an invalid key with 400 "API key invalid". There is no
    # fallback to the unkeyed prlinks hop, and the key stays out of the log.
    scraper = _without_retry_backoff(scrape.Scraper(str(tmp_path), api_key=FAKE_API_KEY))

    with caplog.at_level('ERROR', logger='ace.scrape'):
        html = scraper.get_html_by_pmid('25709573', mode='requests', prefer_pmc_source=False)

    assert html is None
    assert vcr.all_played
    assert set(_played_urls(vcr)) == {ELINK_PRLINKS.format('25709573')}
    assert {response['status']['code'] for response in vcr.responses} == {400}
    assert '<api_key>' in caplog.text
    assert FAKE_API_KEY not in caplog.text


@pytest.mark.vcr(record_mode="once")
def test_pmcid_lookup_failure_is_logged_without_the_api_key(vcr, tmp_path, caplog):
    scraper = _without_retry_backoff(scrape.Scraper(str(tmp_path), api_key=FAKE_API_KEY))

    with caplog.at_level('ERROR', logger='ace.scrape'):
        pmc_id = scraper._get_pmcid_from_pmid('25709573')

    assert pmc_id is None
    assert vcr.all_played
    assert '<api_key>' in caplog.text
    assert FAKE_API_KEY not in caplog.text


def test_recorded_cassettes_do_not_store_the_api_key(vcr_config, tmp_path):
    # NCBI echoes a rejected key in its error body. A local server stands in
    # for it, so the scrub is checked on every run, not only when a cassette
    # is re-recorded.
    body = gzip.compress(json.dumps({"error": "API key invalid", "api-key": FAKE_API_KEY}).encode())

    class EchoesTheKey(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(400)
            self.send_header('Content-Encoding', 'gzip')
            self.send_header('Content-Length', str(len(body)))
            self.send_header('X-Echoed-Key', FAKE_API_KEY)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = HTTPServer(('127.0.0.1', 0), EchoesTheKey)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    cassette = tmp_path / 'echo.yaml'
    try:
        with VCR(**vcr_config).use_cassette(str(cassette), record_mode='all'):
            requests.get(f'http://127.0.0.1:{server.server_port}/elink.fcgi', params={'api_key': FAKE_API_KEY})
    finally:
        server.shutdown()
        server.server_close()

    recorded = cassette.read_text()
    assert FAKE_API_KEY not in recorded
    assert recorded.count(API_KEY_PLACEHOLDER) == 2
