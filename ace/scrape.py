# coding: utf-8
from __future__ import unicode_literals  # use unicode everywhere
import re
import requests
from time import sleep
import config
import simplejson as json
from bs4 import BeautifulSoup
import logging
import os
from random import random, shuffle
from selenium import webdriver
import selenium.webdriver.support.ui as ui

logger = logging.getLogger('ace')


def get_url(url, delay=0.0, verbose=False):
    headers = {'User-Agent': config.USER_AGENT_STRING}
    sleep(delay)
    r = requests.get(url, headers=headers)
    return r.text


def get_pmid_from_doi(doi):
    ''' Query PubMed for the PMID of a paper based on its doi. We need this
    for some Sources that don't contain the PMID anywhere in the artice HTML.
    '''
    data = get_url(
        'http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=%s[aid]' % doi)
    pmid = re.search('\<Id\>(\d+)\<\/Id\>', data).group(1)
    return pmid


def get_pubmed_metadata(pmid):
    ''' Get metadata for article from PubMed '''
    logger.info("Retrieving metadata for PubMed article %s..." % str(pmid))
    text = get_url(
        'http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=%s&retmode=text&rettype=medline' % pmid)
    return parse_PMID_text(text)


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
        if 'AID' in data:
            doi = filter(lambda x: 'doi' in x, data[
                         'AID'].split('; '))[0].split(' ')[0]
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

    def __init__(self, store):

        self.store = store


    def search_pubmed(self, journal, retmax=10000, savelist=None):
        # query = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=(%s[Journal])+AND+(%s[Date+-+Publication])&retmax=%s" % (journal, str(year), str(retmax))
        journal = journal.replace(' ', '+')
        search = '+%s' % self.search if self.search is not None else ''
        query = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=(%s[Journal]+journal+article[pt]%s)&retmax=%s" % (journal, search, str(retmax))
        logger.info("Query: %s" % query)
        doc = get_url(query)
        if savelist is not None:
            outf = open(savelist, 'w')
            outf.write(doc)
            outf.close()
        return doc


    def get_html(self, url, ):

        ''' Get HTML of full-text article. Uses either browser automation (if mode == 'browser')
        or just gets the URL directly. '''

        if self.mode == 'browser':
            driver = webdriver.Chrome()
            driver.get(url)
            # The ?np=y bit is only useful for ScienceDirect, but doesn't hurt elsewhere.
            url = driver.current_url + '?np=y'
            driver.get(url)
            ## Uncomment this next line to scroll to end. Doesn't seem to actually help.
            # driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            ## Uncomment next line and insert ID to search for specific element.
            # driver.findElement(By.Id()).sendKeys(Keys.Tab) 
            # This next line helps minimize the number of blank articles saved from ScienceDirect,
            # which loads content via Ajax requests only after the page is done loading. There is 
            # probably a better way to do this...
            sleep(3.0)
            html = driver.page_source
            driver.quit()
            return html

        else:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1464.0 Safari/537.36'}
            r = requests.get(url, headers=headers)
            # For some journals, we can do better than the returned HTML, so get the final URL and 
            # substitute a better one.
            if self.journal.lower() in ['plos one']:
                url = self.check_for_substitute_url(r.url)
                r = requests.get(url, headers=headers)
            return r.text


    def get_html_by_pmid(self, pmid, retmode='ref'):

        query = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&id=%s&cmd=prlinks&retmode=%s" % (pmid, retmode)
        logger.info(query)
        return self.get_html(query)


    def check_for_substitute_url(self, url):
        ''' For some journals/publishers, we can get a better document version by modifying the 
        URL passed from PubMed. E.g., we can get XML with embedded tables from PLoS ONE instead of 
        the standard HTML, which displays tables as images. '''

        if self.journal.lower() == 'plos one':
            doi_part = re.search('article\/(info.*)', url).group(1)
            return 'http://www.plosone.org/article/fetchObjectAttachment.action?uri=%s&representation=XML' % doi_part
        else:
            return url


    def retrieve_journal_articles(self, journal, delay=None, mode='browser', search=None,
                                limit=None, overwrite=False):

        ''' Try to retrieve all PubMed articles for a single journal that don't 
        already exist in the storage directory.
        Args:
            journal: The name of the journal (as it appears in PubMed).
            delay: Mean delay between requests.
            mode: When 'browser', use selenium to load articles in Chrome. When 
                'direct', attempts to fetch the HTML directly via requests module.
            search: An optional search string to append to the PubMed query.
                Primarily useful for journals that are not specific to neuroimaging.
            limit: Optional max number of articles to fetch. Note that only new articles 
                are counted against this limit; e.g., if limit = 100 and 2,000 articles 
                are found in PubMed, retrieval will continue until 100 new articles 
                have been added.
            overwrite: When True, all articles returned from PubMed query will be 
                fetched, irrespective of whether or not they already exist on disk.
        '''
        self.journal = journal
        self.delay = delay
        self.mode = mode
        self.search = search
        query = self.search_pubmed(journal)
        soup = BeautifulSoup(query)
        ids = [t.string for t in soup.find_all('id')]
        shuffle(ids)
        logger.info("Found %d records.\n" % len(ids))

        # Make directory if it doesn't exist
        journal_path = '%s/%s' % (self.store, journal)
        if not os.path.exists(journal_path):
            os.mkdir(journal_path)

        articles_found = 0

        for id in ids:

            if limit is not None and articles_found >= limit: break
            
            logger.info("Processing %s..." % id)
            filename = '%s/%s.html' % (journal_path, id)

            if not overwrite and os.path.isfile(filename): 
                logger.info("\tAlready exists! Skipping...")
                continue

            doc = self.get_html_by_pmid(id)
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

