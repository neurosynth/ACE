# coding: utf-8
from __future__ import unicode_literals  # use unicode everywhere
import re
import requests
from time import sleep
import config
from bs4 import BeautifulSoup
import logging
import os
from random import random, shuffle
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

logger = logging.getLogger(__name__)

def get_url(url, delay=0.0, verbose=False):
    headers = {'User-Agent': config.USER_AGENT_STRING}
    sleep(delay)
    r = requests.get(url, headers=headers, timeout=5.0)
    return r.text


def get_pmid_from_doi(doi):
    ''' Query PubMed for the PMID of a paper based on its doi. We need this
    for some Sources that don't contain the PMID anywhere in the artice HTML.
    '''
    data = get_url(
        'http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=%s[aid]' % doi)
    pmid = re.search('\<Id\>(\d+)\<\/Id\>', data).group(1)
    return pmid


def get_pubmed_metadata(pmid, parse=True, store=None, save=True):
    ''' Get PubMed metadata for article.
    Args:
        pmid: The article's PubMed ID
        parse: if True, parses the text and returns a dictionary. if False,
            returns raw text.
        store: optional string path to PubMed metadata files. If passed, first
            checks the passed folder for the corresponding ID, and only queries
            PubMed if not found.
        save: if store is passed, save is True, and the file does not already
            exist, will save the result of the new PubMed query to the store.
    '''
    if store is not None:
        md_file = os.path.join(store, pmid)

    if store is not None and os.path.exists(md_file):
        logger.info("Retrieving metadata from file %s..." %
                    os.path.join(store, pmid))
        text = open(md_file).read()
    else:
        logger.info("Retrieving metadata for PubMed article %s..." % str(pmid))
        text = get_url(
            'http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=%s&retmode=text&rettype=medline' % pmid)
        if store is not None and save and text is not None:
            if not os.path.exists(store):
                os.makedirs(store)
            open(md_file, 'w').write(text)

    return parse_PMID_text(text) if (parse and text is not None) else text


def parse_PMID_text(text, doi=None):
    ''' Take text-format PubMed metadata and convert it to a dictionary
    with standardized field names. '''
    data = {}
    text = re.sub('\n\s+', ' ', text)
    patt = re.compile(ur'([A-Z]+)\s*-\s+(.*)')
    for m in patt.finditer(text):
        field, val = m.group(1), m.group(2)
        if field in data:
            data[field] += ('; %s' % val)
        else:
            data[field] = val

    # Extra processing
    if doi is None:
        if 'AID' in data and '[doi]' in data['AID']:
            doi = filter(lambda x: 'doi' in x, data['AID'].split('; '))[
                0].split(' ')[0]
        else:
            doi = ''
    year = data['DP'].split(' ')[0]
    authors = data['AU'].replace(';', ',')
    for field in ['MH', 'AB', 'JT']:
        if field not in data:
            data[field] = ''

    metadata = {
        'authors': authors,
        'citation': data['SO'],
        'comment': data['AB'],
        'doi': doi,
        'keywords': '',
        'mesh': data['MH'],
        'pmid': data['PMID'],
        'title': data['TI'],
        'abstract': data['AB'],
        'journal': data['JT'],
        'year': year
    }
    return metadata


''' Class for journal Scraping. The above free-floating methods should 
probably be refactored into this class eventually. '''


class Scraper:
    '''
    Args:
        output_dir: Directory to save all article contents/metadata to.
        journal: The name of the journal (as it appears in PubMed).
        delay: Mean delay between requests.
        mode: When 'browser', use selenium to load articles in Chrome.
            When 'direct', attempts to fetch the HTML directly via requests
            module.
        search: An optional search string to append to the PubMed query.
            Primarily useful for journals that are not specific to
            neuroimaging.
        limit: Optional max number of articles to fetch. Note that only new
            articles are counted against this limit; e.g., if limit = 100
            and 2,000 articles are found in PubMed, retrieval will continue
            until 100 new articles have been added.
        overwrite: When True, all articles returned from PubMed query will
            be fetched, irrespective of whether or not they already exist
            on disk.
        min_pmid: When a PMID is provided, only articles with PMIDs greater
            than this will be processed. Primarily useful for excluding
            older articles that aren't available in full-text HTML format.
        save_metadata: When True, retrieves metadata from PubMed and saves
            it to the pubmed/ folder below the root storage folder.
    '''

    def __init__(self, output_dir, delay=None, mode='browser', search=None,
                 limit=None, overwrite=False, min_pmid=None,
                 save_metadata=True):

        self.output_dir = output_dir
        self.delay = delay
        self.mode = mode
        self.search = search
        self.limit = limit
        self.overwrite = overwrite
        self.min_pmid = min_pmid
        self.save_metadata = save_metadata

    def search_pubmed(self, journal, retmax=10000, search=None, savelist=None):
        journal = journal.replace(' ', '+')
        search = '+%s' % search if search is not None else ''
        query = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=(%s[Journal]+journal+article[pt]%s)&retmax=%s" % (
            journal, search, str(retmax))
        logger.info("Query: %s" % query)
        doc = get_url(query)
        if savelist is not None:
            outf = open(savelist, 'w')
            outf.write(doc)
            outf.close()
        return doc

    def get_html(self, url, mode):
        ''' Get HTML of full-text article. Uses either browser automation (if
        mode == 'browser') or just gets the URL directly. '''

        if mode == 'browser':
            driver = webdriver.Chrome()
            driver.get(url)
            url = driver.current_url
            driver.get(url)

            # Check for URL substitution and get the new one if it's changed
            url = driver.current_url  # After the redirect from PubMed
            html = driver.page_source
            new_url = self.check_for_substitute_url(url, html)

            if url != new_url:
                driver.get(new_url)
                if 'onlinelibrary.wiley' in url:
                    sleep(0.5 + random() * 1)
                    try:
                        WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located(
                                (By.ID, 'crossmark-widget')))
                    except TimeoutException:
                        print "Loading page took too much time!"

                # Sometimes we get annoying alerts (e.g., Flash animation
                # timeouts), so we dismiss them if present.
                try:
                    alert = driver.switch_to_alert()
                    alert.dismiss()
                except:
                    pass

                html = driver.page_source

            # This next line helps minimize the number of blank articles saved
            # from ScienceDirect, which loads content via Ajax requests only
            # after the page is done loading. There's probably a better way to
            # do this...
            sleep(1.5)
            driver.quit()
            return html

        else:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1464.0 Safari/537.36'}
            r = requests.get(url, headers=headers)
            # For some journals, we can do better than the returned HTML, so
            # get the final URL and substitute a better one.
            url = self.check_for_substitute_url(r.url, r.text)
            if url != r.url:
                r = requests.get(url, headers=headers)
                # XML content is usually misidentified as ISO-8859-1, so we
                # need to manually set utf-8. Unfortunately this can break
                # other documents. Need to eventually change this to inspect
                # the encoding attribute of the document header.
                r.encoding = 'utf-8'
            return r.text

    def get_html_by_pmid(self, pmid, mode, retmode='ref'):

        query = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&id=%s&cmd=prlinks&retmode=%s" % (
            pmid, retmode)
        logger.info(query)
        return self.get_html(query, mode)

    def check_for_substitute_url(self, url, html):
        ''' For some journals/publishers, we can get a better document version
        by modifying the URL passed from PubMed. E.g., we can get XML with
        embedded tables from PLoS ONE instead of the standard HTML, which
        displays tables as images. For some journals (e.g., Frontiers), it's
        easier to get the URL by searching the source, so pass the html in as
        well. '''
        j = self.journal.lower()
        try:
            if j == 'plos one':
                doi_part = re.search('article\?id\=(.*)', url).group(1)
                return 'http://journals.plos.org/plosone/article/asset?id=%s.XML' % doi_part
            # elif j in ['human brain mapping',
            #            'european journal of neuroscience',
            #            'brain and behavior', 'epilepsia',
            #            'journal of neuroimaging']:
            #     return url.replace('abstract', 'full').split(';')[0]
            elif 'doi/abs/' in url:
                return url.replace('doi/abs', 'doi/full')
            elif j.startswith('frontiers in'):
                return re.sub('(full|abstract)\/*$', 'xml\/nlm', url)
            elif 'sciencedirect' in url:
                return url + '?np=y'
            elif 'springer.com' in url:
                return url + '/fulltext.html'
            else:
                return url
        except Exception:
            return url

    def retrieve_journal_articles(self, journal, **kwargs):
        ''' Try to retrieve all PubMed articles for a single journal that don't
        already exist in the storage directory.

        '''
        self.journal = journal

        delay = kwargs.get('delay', self.delay)
        mode = kwargs.get('mode', self.mode)
        min_pmid = kwargs.get('min_pmid', self.min_pmid)
        overwrite = kwargs.get('overwrite', self.overwrite)
        limit = kwargs.get('limit', self.limit)
        search = kwargs.get('search', self.search)
        query = self.search_pubmed(journal, search=search)
        soup = BeautifulSoup(query)
        ids = [t.string for t in soup.find_all('id')]
        shuffle(ids)
        logger.info("Found %d records.\n" % len(ids))

        # Make directory if it doesn't exist
        journal_path = os.path.join(self.output_dir, 'html', journal)
        if not os.path.exists(journal_path):
            os.makedirs(journal_path)

        articles_found = 0

        for id in ids:

            if min_pmid is not None and int(id) < min_pmid:
                continue
            if limit is not None and articles_found >= limit:
                break

            logger.info("Processing %s..." % id)
            filename = '%s/%s.html' % (journal_path, id)

            if not overwrite and os.path.isfile(filename):
                logger.info("\tAlready exists! Skipping...")
                continue

            # Save the HTML
            doc = self.get_html_by_pmid(id, mode=mode)
            if doc:
                outf = open(filename, 'w')
                # Still having encoding issues with some journals (e.g.,
                # PLoS ONE). Why???
                outf.write(doc.encode('utf-8'))
                outf.close()
                articles_found += 1

                # Insert random delay until next request.
                if delay is not None:
                    sleep_time = random() * float(delay*2)
                    sleep(sleep_time)
