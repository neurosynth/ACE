from os import path
import logging
import re
from . import sources, config
from .scrape import _validate_scrape
import multiprocessing as mp
from functools import partial
from tqdm import tqdm

logger = logging.getLogger(__name__)

def _process_file_with_source(args):
    """Helper function to read, validate, and identify source for a single file."""
    f, source_configs = args
    try:
        html = open(f).read()
    except Exception as e:
        logger.warning("Failed to read file %s: %s" % (f, str(e)))
        return f, None, None

    if not _validate_scrape(html):
        logger.warning("Invalid HTML for %s" % f)
        return f, None, None

    # Identify source from HTML using regex patterns
    source_name = None
    for name, identifiers in source_configs.items():
        for patt in identifiers:
            if re.search(patt, html):
                logger.debug('Matched article to Source: %s' % name)
                source_name = name
                break
        if source_name:
            break

    return f, html, source_name


def _parse_article(args):
    """Helper function to parse an article from HTML content."""
    # Unpack arguments
    f, html, source_name, pmid, manager, metadata_dir, force_ingest, kwargs = args
    
    try:
        # Get the actual source object
        if source_name:
            source = manager.sources[source_name]
        else:
            # Fallback to original source identification
            source = manager.identify_source(html)
            if source is None:
                logger.warning("Could not identify source for %s" % f)
                return f, None

        article = source.parse_article(html, pmid, metadata_dir=metadata_dir, **kwargs)
        return f, article
    except Exception as e:
        logger.warning("Error parsing article %s: %s" % (f, str(e)))
        return f, None


def add_articles(db, files, commit=True, table_dir=None, limit=None,
    pmid_filenames=False, metadata_dir=None, force_ingest=True, num_workers=None, use_readability=None, **kwargs):
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
        num_workers: Number of worker processes to use when processing in parallel.
            If None (default), uses the number of CPUs available on the system.
        use_readability: When True, use readability.py for HTML cleaning if available.
            When False, use fallback HTML processing by default. If None (default),
            uses the value from config.USE_READABILITY.
        kwargs: Additional keyword arguments to pass to parse_article.
    '''

    manager = sources.SourceManager(table_dir, use_readability=use_readability if use_readability is not None else config.USE_READABILITY)
    
    # Prepare source configurations for parallel processing
    source_configs = {name: source.identifiers for name, source in manager.sources.items()}

    if isinstance(files, str):
        from glob import glob
        files = glob(files)
        if limit is not None:
            from random import shuffle
            shuffle(files)
            files = files[:limit]

    missing_sources = []
    
    # Step 1: Process files in parallel to extract HTML content and identify sources
    if num_workers is not None and num_workers != 1:
        # Process files in parallel to extract HTML content and identify sources
        process_args = [(f, source_configs) for f in files]
        with mp.Pool(processes=num_workers) as pool:
            file_html_source_tuples = list(tqdm(pool.imap_unordered(_process_file_with_source, process_args), total=len(process_args), desc="Processing files"))
    else:
        # Process files sequentially
        file_html_source_tuples = []
        for f in tqdm(files, desc="Processing files"):
            result = _process_file_with_source((f, source_configs))
            file_html_source_tuples.append(result)

    # Step 2: In serial mode, use the db object to skip articles that have been already added
    # Filter out files with reading/validation errors
    valid_files = []
    for f, html, source_name in file_html_source_tuples:
        if html is not None:
            valid_files.append((f, html, source_name))
        # We'll handle missing sources later when we actually parse the articles

    # Filter out articles that already exist in the database
    files_to_process = []
    missing_sources = []

    for f, html, source_name in valid_files:
        pmid = path.splitext(path.basename(f))[0] if pmid_filenames else None
        
        # Check if article already exists
        if pmid is not None and db.article_exists(pmid) and not config.OVERWRITE_EXISTING_ROWS:
            continue
            
        files_to_process.append((f, html, source_name, pmid))

    # Step 3: Process remaining articles in parallel
    # Prepare arguments for _parse_article
    parse_args = [(f, html, source_name, pmid, manager, metadata_dir, force_ingest, kwargs)
                  for f, html, source_name, pmid in files_to_process]

    if num_workers is not None and num_workers != 1 and parse_args:
        # Parse articles in parallel
        with mp.Pool(processes=num_workers) as pool:
            parsed_articles = list(tqdm(pool.imap_unordered(_parse_article, parse_args), total=len(parse_args), desc="Parsing articles"))
    else:
        # Parse articles sequentially
        parsed_articles = []
        for args in tqdm(parse_args, desc="Parsing articles"):
            parsed_articles.append(_parse_article(args))

    # Add successfully parsed articles to database
    for i, (f, article) in enumerate(parsed_articles):
        if article is None:
            missing_sources.append(f)
            continue
            
        if config.SAVE_ARTICLES_WITHOUT_ACTIVATIONS or article.tables:
            # Check again if article exists and handle overwrite
            pmid = path.splitext(path.basename(f))[0] if pmid_filenames else None
            if pmid is not None and db.article_exists(pmid):
                if config.OVERWRITE_EXISTING_ROWS:
                    db.delete_article(pmid)
                else:
                    continue
                    
            db.add(article)
            if commit and (i % 100 == 0 or i == len(parsed_articles) - 1):
                db.save()
                
    db.save()

    return missing_sources
