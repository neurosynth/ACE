from os import path
import logging
import re
from . import sources
from .config import get_config
from .scrape import _validate_scrape
import multiprocessing as mp
from functools import partial
from tqdm import tqdm

logger = logging.getLogger(__name__)

def _process_file_with_source(args):
    """Helper function to read, validate, and identify source for a file."""
    f, source_configs = args
    try:
        html = open(f).read()
    except Exception as e:
        logger.warning("Failed to read file %s: %s", f, str(e))
        return f, None, None

    if not _validate_scrape(html):
        logger.warning("Invalid HTML for %s", f)
        return f, None, None

    # Identify source from HTML using regex patterns
    source_name = None
    for name, identifiers in source_configs.items():
        for patt in identifiers:
            if re.search(patt, html):
                logger.debug('Matched article to Source: %s', name)
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
                logger.info("Could not identify source for %s", f)
                return f, None

        article = source.parse_article(html, pmid, metadata_dir=metadata_dir, **kwargs)
        return f, article
    except Exception as e:
        logger.info("Error parsing article %s: %s", f, str(e))
        return f, None


def add_articles(db, files, commit=True, table_dir=None, limit=None,
    pmid_filenames=False, metadata_dir=None, force_ingest=True, num_workers=None, 
    use_readability=None, **kwargs):
    ''' Process articles and add their data to the DB. '''
    manager = sources.SourceManager(
        table_dir, 
        use_readability=use_readability if use_readability is not None 
            else get_config('USE_READABILITY')
    )
    
    # Prepare source configurations for parallel processing
    source_configs = {name: source.identifiers for name, source in manager.sources.items()}

    if isinstance(files, str):
        from glob import glob
        files = glob(files)
        if limit is not None:
            from random import shuffle
            shuffle(files)
            files = files[:limit]

    # Step 1: Process files to extract HTML and identify sources
    if num_workers is not None and num_workers != 1:
        process_args = [(f, source_configs) for f in files]
        with mp.Pool(processes=num_workers) as pool:
            results = list(tqdm(
                pool.imap_unordered(_process_file_with_source, process_args), 
                total=len(process_args),
                desc="Processing files"
            ))
    else:
        results = []
        for f in tqdm(files, desc="Processing files"):
            results.append(_process_file_with_source((f, source_configs)))

    # Filter valid files
    valid_files = [(f, html, src) for f, html, src in results if html is not None]
    
    # Filter out existing articles if not overwriting
    files_to_process = []
    for f, html, source_name in valid_files:
        pmid = path.splitext(path.basename(f))[0] if pmid_filenames else None
        
        # Check if article exists and should be skipped
        if pmid and db.article_exists(pmid) and not get_config('OVERWRITE_EXISTING_ROWS'):
            continue
            
        files_to_process.append((f, html, source_name, pmid))

    # Step 2: Parse articles
    parse_args = [
        (f, html, source_name, pmid, manager, metadata_dir, force_ingest, kwargs)
        for f, html, source_name, pmid in files_to_process
    ]

    if num_workers is not None and num_workers != 1 and parse_args:
        with mp.Pool(processes=num_workers) as pool:
            parsed_articles = list(tqdm(
                pool.imap_unordered(_parse_article, parse_args),
                total=len(parse_args),
                desc="Parsing articles"
            ))
    else:
        parsed_articles = []
        for args in tqdm(parse_args, desc="Parsing articles"):
            parsed_articles.append(_parse_article(args))

    # Add successfully parsed articles to database
    missing_sources = []
    for i, (f, article) in enumerate(parsed_articles):
        if article is None:
            missing_sources.append(f)
            continue
            
        if get_config('SAVE_ARTICLES_WITHOUT_ACTIVATIONS') or article.tables:
            pmid = path.splitext(path.basename(f))[0] if pmid_filenames else None
            if pmid and db.article_exists(pmid):
                if get_config('OVERWRITE_EXISTING_ROWS'):
                    db.delete_article(pmid)
                else:
                    continue
                    
            db.add(article)
            if commit and (i % 100 == 0 or i == len(parsed_articles) - 1):
                db.save()
                
    db.save()

    return missing_sources
