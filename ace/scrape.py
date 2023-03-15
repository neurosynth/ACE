# coding: utf-8
  # use unicode everywhere
import re
from pathlib import Path
import requests
from time import sleep
from . import config
from bs4 import BeautifulSoup
import logging
import os
import random
from selenium import webdriver
import selenium.webdriver.support.ui as ui
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

logger = logging.getLogger(__name__)


class PubMedAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
        self.headers = {'User-Agent': config.USER_AGENT_STRING}

        
    def esearch(self, query, retmax=100000):
        url = f"{self.base_url}/esearch.fcgi"
        params = {
            "db": "pubmed",
            "api_key": self.api_key,
            "term": query,
            "retmax": str(retmax)
        }
        response = requests.get(url, params=params, timeout=5, headers=self.headers)
        return response.content
    
    def efetch(self, pmids, retmode='txt', rettype='medline'):
        url = f"{self.base_url}/efetch.fcgi"
        params = {
            "db": "pubmed",
            "api_key": self.api_key,
            "id": ",".join(pmids),
            "retmode": retmode,
            "rettype": rettype
        }
        response = requests.get(url, params=params, headers=self.headers)
        return response.content
    
    def elink(self, pmid, retmode='ref'):
        url = f"{self.base_url}/elink.fcgi"
        params = {
            "dbfrom": "pubmed",
            "api_key": self.api_key,
            "id": pmid,
            "cmd": "prlinks",
            "retmode": retmode
        }
        response = requests.get(url, params=params, headers=self.headers)
        return response.content


def get_pmid_from_doi(doi, api_key=None):
    ''' Query PubMed for the PMID of a paper based on its doi. We need this
    for some Sources that don't contain the PMID anywhere in the artice HTML.
    '''
    query = f"{doi}[aid]"
    data = PubMedAPI(api_key=api_key).esearch(query=query)
    pmid = re.search('\<Id\>(\d+)\<\/Id\>', data).group(1)
    return pmid


def get_pubmed_metadata(pmid, parse=True, store=None, save=True, api_key=None):
    ''' Get PubMed metadata for article.
    Args:
        pmid: The article's PubMed ID
        parse: if True, parses the text and returns a dictionary. if False, returns raw text.
        store: optional string path to PubMed metadata files. If passed, first checks the passed
            folder for the corresponding ID, and only queries PubMed if not found.
        save: if store is passed, save is True, and the file does not already exist, 
            will save the result of the new PubMed query to the store.
    '''
    if store is not None:
        md_file = os.path.join(store, pmid)

    if store is not None and os.path.exists(md_file):
        logger.info("Retrieving metadata from file %s..." % os.path.join(store, pmid))
        text = open(md_file).read()
    else:
        logger.info("Retrieving metadata for PubMed article %s..." % str(pmid))
        text = PubMedAPI(api_key=api_key).efetch(pmid, retmode='text', rettype='medline')
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
    patt = re.compile(r'([A-Z]+)\s*-\s+(.*)')
    for m in patt.finditer(text):
        field, val = m.group(1), m.group(2)
        if field in data:
            data[field] += ('; %s' % val)
        else:
            data[field] = val

    # Extra processing
    if doi is None:
        if 'AID' in data and '[doi]' in data['AID']:
            doi = [x for x in data['AID'].split('; ') if 'doi' in x][0].split(' ')[0]
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

    def __init__(self, store, api_key=None):
        self.store = Path(store)
        self._client = PubMedAPI(api_key=api_key)


    def search_pubmed(self, journal, search, retmax=10000, savelist=None,):
        journal = journal.replace(' ', '+')
        search = '+%s' % search
        query = "({journal}[Journal]+journal+article[pt]{search})"
        logger.info("Query: %s" % query)

        doc = self._client.esearch(query, retmax=retmax)

        if savelist is not None:
            outf = open(savelist, 'w')
            outf.write(doc)
            outf.close()
        return doc


    def get_html(self, url, journal, mode='browser'):

        ''' Get HTML of full-text article. Uses either browser automation (if mode == 'browser')
        or just gets the URL directly. '''

        if mode == 'browser':
            driver = webdriver.Chrome()
            driver.get(url)
            url = driver.current_url
            driver.get(url)

            # Check for URL substitution and get the new one if it's changed
            url = driver.current_url  # After the redirect from PubMed

            html = driver.page_source
            new_url = self.check_for_substitute_url(url, html, journal)

            if url != new_url:
                driver.get(new_url)
                sleep(3
                )
                if journal.lower() in ['human brain mapping',
                                            'european journal of neuroscience',
                                            'brain and behavior','epilepsia']:
                    sleep(0.5 + random() * 1)
                    try:
                        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, 'relatedArticles')))
                    except TimeoutException:
                        print("Loading Wiley page took too much time!")

                # Sometimes we get annoying alerts (e.g., Flash animation
                # timeouts), so we dismiss them if present.
                try:
                    alert = driver.switch_to_alert()
                    alert.dismiss()
                except:
                    pass
                            
            logger.info(journal.lower())
            if journal.lower() in ['journal of neuroscience', 'j neurosci']:
                ## Find links with class data-table-url, and click on them
                ## to load the table data.
                table_links = driver.find_elements(By.CLASS_NAME, 'table-expand-inline')

                if len(table_links):         
                    for link in table_links:
                        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((
                            By.CLASS_NAME, 'table-expand-inline')))    
                        driver.execute_script("arguments[0].scrollIntoView();", link)
                        link.click()
                        sleep(0.5 + random.random() * 1)

            ## Uncomment this next line to scroll to end. Doesn't seem to actually help.
            # driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            ## Uncomment next line and insert ID to search for specific element.
            # driver.find_element_by_id('relatedArticles').send_keys('\t')
            # This next line helps minimize the number of blank articles saved from ScienceDirect,
            # which loads content via Ajax requests only after the page is done loading. There is 
            # probably a better way to do this...
            html = driver.page_source
            driver.quit()
            return html

        elif mode == 'requests':
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1464.0 Safari/537.36'}
            r = requests.get(url, headers=headers)
            # For some journals, we can do better than the returned HTML, so get the final URL and 
            # substitute a better one.
            url = self.check_for_substitute_url(r.url, r.text, journal)
            if url != r.url:
                r = requests.get(url, headers=headers)
                # XML content is usually misidentified as ISO-8859-1, so we need to manually set utf-8.
                # Unfortunately this can break other documents. Need to eventually change this to inspect the 
                # encoding attribute of the document header.
                r.encoding = 'utf-8'
            return r.text


    def get_html_by_pmid(self, pmid, journal, mode='browser', retmode='ref'):
        query = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&id=%s&cmd=prlinks&retmode=%s" % (pmid, retmode)
        logger.info(query)
        return self.get_html(query, journal, mode=mode)


    def check_for_substitute_url(self, url, html, journal):
        ''' For some journals/publishers, we can get a better document version by modifying the 
        URL passed from PubMed. E.g., we can get XML with embedded tables from PLoS ONE instead of 
        the standard HTML, which displays tables as images. For some journals (e.g., Frontiers),  
        it's easier to get the URL by searching the source, so pass the html in as well. '''

        j = journal.lower()
        try:
            if j == 'plos one':
                doi_part = re.search('article\?id\=(.*)', url).group(1)
                return 'http://journals.plos.org/plosone/article/asset?id=%s.XML' % doi_part
            elif j in ['human brain mapping', 'european journal of neuroscience',
                       'brain and behavior', 'epilepsia', 'journal of neuroimaging']:
                return url.replace('abstract', 'full').split(';')[0]
            elif j == 'journal of cognitive neuroscience':
                return url.replace('doi/abs', 'doi/full')
            elif j.startswith('frontiers in'):
                return re.sub('(full|abstract)\/*$', 'xml\/nlm', url)
            elif 'sciencedirect' in url:
                return url + '?np=y'
            elif 'springer.com' in url:
                return url + '/fulltext.html'
            else:
                return url
        except Exception as err:
            return url

    def has_pmc_entry(self, pmid):
        ''' Check if a PubMed Central entry exists for a given PubMed ID. '''
        content = self._client.efetch(pmid, retmode='xml')
        return '<ArticleId IdType="pmc">' in str(content)

    def process_article(self, id, journal, delay=None, mode='browser', overwrite=False):

        logger.info("Processing %s..." % id)
        journal_path = (self.store / 'html' / journal)
        filename = journal_path / f"{id}.html"

        if not overwrite and os.path.isfile(filename): 
            logger.info("\tAlready exists! Skipping...")
            
            return None

        # Save the HTML
        doc = self.get_html_by_pmid(id, journal, mode=mode)
        if doc:
            with filename.open('w') as f:
                f.write(doc)

            # Insert random delay until next request.
            if delay is not None:
                sleep_time = random.random() * float(delay*2)
                sleep(sleep_time)
        
        return filename

    def retrieve_journal_articles(self, journal, delay=None, mode='browser', search=None,
                                limit=None, overwrite=False, min_pmid=None, shuffle=False,
                                skip_pubmed_central=True, save_metadata=True):

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
            min_pmid: When a PMID is provided, only articles with PMIDs greater than 
                this will be processed. Primarily useful for excluding older articles 
                that aren't available in full-text HTML format.
            shuffle: When True, articles are retrieved in random order.
            skip_pubmed_central: When True, skips articles that are available from
                PubMed Central. 
            save_metadata: When True, retrieves metadata from PubMed and saves it to 
                the pubmed/ folder below the root storage folder.
        '''
        query = self.search_pubmed(journal, search)
        soup = BeautifulSoup(query)
        ids = [t.string for t in soup.find_all('id')]
        if shuffle:
            random.shuffle(ids)
        else:
            ids.sort()

        logger.info("Found %d records.\n" % len(ids))

        # Make directory if it doesn't exist
        (self.store / 'html' / journal).mkdir(parents=True, exist_ok=True)

        articles_found = 0

        for id in ids:
            if min_pmid is not None and int(id) < min_pmid: continue
            if limit is not None and articles_found >= limit: break

            if skip_pubmed_central and self.has_pmc_entry(id):
                logger.info(f"\tPubMed Central entry found! Skipping {id}...")
                continue

            filename = self.process_article(id, journal, delay, mode, overwrite)
            if filename:
                articles_found += 1