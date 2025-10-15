from os import path
import logging
from . import sources, config
from .scrape import _validate_scrape
import multiprocessing as mp
from functools import partial

logger = logging.getLogger(__name__)

def _process_file(f):
    """Helper function to read and validate a single file."""
    logger.info("Processing article %s..." % f)
    try:
        html = open(f).read()
    except Exception as e:
        logger.warning("Failed to read file %s: %s" % (f, str(e)))
        return f, None

    if not _validate_scrape(html):
        logger.warning("Invalid HTML for %s" % f)
        return f, None

    return f, html


def add_articles(db, files, commit=True, table_dir=None, limit=None,
    pmid_filenames=False, metadata_dir=None, force_ingest=True, parallel=True, num_workers=None, **kwargs):
    ''' Process articles and add their data to the DB.
    Args:
        files: The path to the article(s) to process. Can be a single
            filename (string), a list of filenames, or a path to pass
            to glob (e.g., "article_ls  dir/NIMG*html")
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
        force_ingest: Ingest even if no source is identified.
        parallel: Whether to process articles in parallel (default: True).
        num_workers: Number of worker processes to use when processing in parallel.
            If None (default), uses the number of CPUs available on the system.
        kwargs: Additional keyword arguments to pass to parse_article.
    '''

    manager = sources.SourceManager(db, table_dir)

    if isinstance(files, str):
        from glob import glob
        files = glob(files)
        if limit is not None:
            from random import shuffle
            shuffle(files)
            files = files[:limit]

    missing_sources = []
    
    if parallel:
        # Process files in parallel to extract HTML content
        with mp.Pool(processes=num_workers) as pool:
            file_html_pairs = pool.map(_process_file, files)
    else:
        # Process files sequentially
        file_html_pairs = []
        for f in files:
            file_html_pairs.append(_process_file(f))
    
    # Process each file's HTML content
    for i, (f, html) in enumerate(file_html_pairs):
        if html is None:
            # File reading or validation failed
            missing_sources.append(f)
            continue

        source = manager.identify_source(html)
        if source is None:
            logger.warning("Could not identify source for %s" % f)
            missing_sources.append(f)
            if not force_ingest:
                continue
            else:
                source = sources.DefaultSource(db)

        pmid = path.splitext(path.basename(f))[0] if pmid_filenames else None
        article = source.parse_article(html, pmid, metadata_dir=metadata_dir, **kwargs)
        if article and (config.SAVE_ARTICLES_WITHOUT_ACTIVATIONS or article.tables):
            db.add(article)
            if commit and (i % 100 == 0 or i == len(file_html_pairs) - 1):
                db.save()
    db.save()

    return missing_sources
