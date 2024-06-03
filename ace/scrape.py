# coding: utf-8
  # use unicode everywhere
import re
import sys
from pathlib import Path
from collections import Mapping
import requests
from time import sleep
import logging
import os
import random
import xmltodict
from seleniumbase import Driver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from tqdm import tqdm

from ace.utils import PubMedAPI
from ace.config import USER_AGENTS

logger = logging.getLogger(__name__)


def get_url(url, n_retries=5, timeout=10.0, verbose=False):
    headers = {'User-Agent': random.choice(USER_AGENTS)}

    def exponential_backoff(retries):
        return 2 ** retries

    retries = 0
    while retries < n_retries:

        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            return r.text
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed: {e}")
            sleep_time = exponential_backoff(retries)
            logger.info(f"Retrying in {sleep_time} seconds...")
            sleep(sleep_time)
            retries += 1
    logger.error("Exceeded maximum number of retries.")
    return None

def _convert_pmid_to_pmc(pmids):
    url_template = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?ids="
    logger.info("Converting PMIDs to PMCIDs...")

    # Chunk the PMIDs into groups of 200
    pmids = [str(p) for p in pmids]
    pmid_chunks = [pmids[i:i + 200] for i in range(0, len(pmids), 200)]

    pmc_ids = []
    for chunk in tqdm(pmid_chunks):
        pmid_str = ','.join(chunk)
        url = url_template + pmid_str
        response = get_url(url)
        # Respionse <record requested-id="23193288" pmcid="PMC3531191" pmid="23193288" doi="10.1093/nar/gks1163">
        pmc_ids += re.findall(r'<record requested-id="[^"]+" pmcid="([^"]+)" pmid="([^"]+)" doi="[^"]+">', response)

    logger.info(f"Found {len(pmc_ids)} PMCIDs from {len(pmids)} PMIDs.")

    pmids_found = set([p[1] for p in pmc_ids])
    missing_pmids = [(None, p) for p in pmids if p not in pmids_found]

    pmc_ids = pmc_ids + missing_pmids
        
    return pmc_ids


def get_pmid_from_doi(doi, api_key=None):
    ''' Query PubMed for the PMID of a paper based on its doi. We need this
    for some Sources that don't contain the PMID anywhere in the artice HTML.
    '''
    query = f"{doi}[aid]"
    data = PubMedAPI(api_key=api_key).esearch(query=query)
    if data:
        data = data[0]
    else:
        data = None
    return data


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
        with open(md_file, 'rb') as f:
            xml = f.read()

    else:
        logger.info("Retrieving metadata for PubMed article %s..." % str(pmid))
        xml = PubMedAPI(api_key=api_key).efetch(input_id=pmid,  retmode='xml', rettype='medline', db='pubmed')
        if store is not None and save and xml is not None:
            if not os.path.exists(store):
                os.makedirs(store)
            with open(md_file, 'wb') as f:
                f.write(xml)

    return parse_PMID_xml(xml) if (parse and xml is not None) else xml


def parse_PMID_xml(xml):
    ''' Take XML-format PubMed metadata and convert it to a dictionary
    with standardized field names. '''

    di = xmltodict.parse(xml).get('PubmedArticleSet')
    if not di:
        return None
    
    di = di['PubmedArticle']
    article = di['MedlineCitation']['Article']

    if 'ArticleDate' in article:
        date = article['ArticleDate']
    elif 'Journal' in article:
        date = article['Journal']['JournalIssue']['PubDate']
    else:
        date = None
    
    if date:
        year = date.get('Year', None)
    else:   
        year = None

    doi = None
    doi_source = article.get('ELocationID', None)
    if doi_source is not None and isinstance(doi_source, list):
        doi_source = [d for d in doi_source if d['@EIdType'] == 'doi'][0]

    if doi_source is not None and doi_source['@EIdType'] == 'doi':
        doi = doi_source['#text']

    authors = article.get('AuthorList', None)
    
    if authors:
        authors = authors['Author']

        try:
            _get_author = lambda a: a['LastName'] + ', ' + a['ForeName']
            if isinstance(authors, list):
                authors = [_get_author(a) for a in authors if 'ForeName' in a]
            else:
                authors = [_get_author(authors)]
            authors = ';'.join(authors)
        except:
            authors = None

    if 'MeshHeadingList' in di['MedlineCitation']:
        mesh = di['MedlineCitation']['MeshHeadingList']['MeshHeading']
    else:
        mesh = []

    abstract = article.get('Abstract', '')
    if abstract != '':
        abstract = abstract.get('AbstractText', '')

    cit = di['PubmedData']['ArticleIdList']['ArticleId']
    if isinstance(cit, list):
        cit = cit[1]

    metadata = {
        'authors': authors,
        'citation': cit['#text'],
        'comment': abstract,
        'doi': doi,
        'keywords': '',
        'mesh': mesh,
        'pmid': di['MedlineCitation']['PMID'],
        'title': article['ArticleTitle'],
        'abstract': abstract,
        'journal': article['Journal']['Title'],
        'year': year
    }

    # Clean up nested Dicts
    for k, v in metadata.items():
        if isinstance(v, list):
            to_join = []
            for a in v:
                if 'DescriptorName' in a:
                    a = a['DescriptorName']
                a = a['#text']
                
                to_join.append(a)
            v = ' | '.join(to_join)
        elif isinstance(v, Mapping):
            v = v.get('#text', '')
        metadata[k] = v

    return metadata

def _validate_scrape(html):
    """ Checks to see if scraping was successful. 
    For example, checks to see if Cloudfare interfered """

    patterns = ['Checking if you are a human',
    'Please turn JavaScript on and reload the page',
    'Checking if the site connection is secure',
    'Enable JavaScript and cookies to continue',
    'There was a problem providing the content you requested',
    '<title>Redirecting</title>',
    '<title>Page not available - PMC</title>',
    'Your request cannot be processed at this time. Please try again later',
    '403 Forbidden',
    'Page not found — ScienceDirect',
    'This site can’t be reached',
    'used Cloudflare to restrict access',
    '502 Bad Gateway',
    ]

    for pattern in patterns:
        if pattern in html:
            return False

    return True

''' Class for journal Scraping. The above free-floating methods should 
probably be refactored into this class eventually. '''
class Scraper:

    def __init__(self, store, api_key=None):
        self.store = Path(store)
        self._client = PubMedAPI(api_key=api_key)


    def search_pubmed(self, journal, search, retmax=10000, savelist=None,):
        journal = journal.replace(' ', '+')
        search = '+%s' % search
        query = f"({journal}[Journal]+journal+article[pt]{search})"
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
            driver = Driver(
                uc=True,
                headless2=True,
                agent=random.choice(USER_AGENTS),
            )
            for attempt in range(15):
                try:
                    driver.set_page_load_timeout(10)
                    driver.get(url)
                    url = driver.current_url
                except:
                    driver.quit()
                    logger.info(f"Timeout exception #{attempt}. Retrying...")
                    sleep(5)
                    continue
                else:
                    break
            else:
                logger.info("Timeout exception. Giving up.")
                return None
            for attempt in range(10):
                try:
                    html = driver.page_source
                except:
                    logger.info(f"Source Page #{attempt}. Retrying...")
                    driver.quit()
                    driver = Driver(
                        uc=True,
                        headless2=True,
                        agent=random.choice(USER_AGENTS),
                    )
                    driver.get(url)
                    sleep(2)
                else:
                    break
    
            new_url = self.check_for_substitute_url(url, html, journal)

            if url != new_url:
                driver = Driver(
                    uc=True,
                    headless2=True,
                    agent=random.choice(USER_AGENTS),
                )
                driver.get(new_url)
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
            timeout = 5
            for attempt in range(10):
                try:
                    html = driver.page_source
                except:
                    logger.info(f"Source Page #{attempt}. Retrying...")
                    driver.quit()
                    driver = Driver(
                        uc=True,
                        headless2=True,
                        agent=random.choice(USER_AGENTS),
                    )
                    driver.get(url)
                    sleep(2)
                else:
                    break
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

            # If title has ScienceDirect in in title
            elif ' - ScienceDirect' in html:
                try:
                    element_present = EC.presence_of_element_located((By.ID, 'abstracts'))
                    WebDriverWait(driver, timeout).until(element_present)
                except TimeoutException:
                    pass
            elif 'Wiley Online Library</title>' in html:
                try:
                    element_present = EC.presence_of_element_located((By.ID, 'article__content'))
                    WebDriverWait(driver, timeout).until(element_present)
                except TimeoutException:
                    pass

            ## Uncomment this next line to scroll to end. Doesn't seem to actually help.
            # driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            ## Uncomment next line and insert ID to search for specific element.
            # driver.find_element_by_id('relatedArticles').send_keys('\t')
            # This next line helps minimize the number of blank articles saved from ScienceDirect,
            # which loads content via Ajax requests only after the page is done loading. There is 
            # probably a better way to do this...
            
            driver.quit()
            return html

        elif mode == 'requests':
            headers = {'User-Agent': random.choice(USER_AGENTS)}
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


    def get_html_by_pmid(self, pmid, journal, mode='browser', retmode='ref', prefer_pmc_source=True):
        base_url = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi"
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

        if prefer_pmc_source:
            try:
                response = self._client.elink(pmid, retmode='json', return_content=False)
                response.raise_for_status()  # Raise an HTTPError for bad responses
                json_content = response.json()

                providers = {obj['provider']['nameabbr']: obj["url"]["value"] for obj in json_content['linksets'][0]['idurllist'][0]['objurls']}
                pmc_url = providers.get('PMC')

                if pmc_url:
                    return self.get_html(pmc_url, journal, mode='requests')
                elif prefer_pmc_source == "only":
                    logger.info("\tNo PMC source found! Skipping...")
                    return
            except requests.RequestException as e:
                logger.error(f"Request failed: {e}")
            except KeyError as e:
                logger.error(f"Key error: {e} - JSON content: {json_content}")
        else:
            query = f"{base_url}?dbfrom=pubmed&id={pmid}&cmd=prlinks&retmode={retmode}"
            logger.info(query)
            return self.get_html(query, journal, mode=mode)

        if prefer_pmc_source == "only":
            logger.info("\tNo PMC source found!! Skipping...")
            return

        # Fallback if no PMC link found
        query = f"{base_url}?dbfrom=pubmed&id={pmid}&cmd=prlinks&retmode={retmode}"
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

    
    def is_pmc_open_acess(self, pmcid):
        oa_url = "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id="

        response = get_url(oa_url + pmcid)
    
        return 'idIsNotOpenAccess' not in response

    def process_article(self, id, journal, delay=None, mode='browser', overwrite=False, prefer_pmc_source=True):

        logger.info("Processing %s..." % id)
        journal_path = (self.store / 'html' / journal)
        journal_path.mkdir(parents=True, exist_ok=True)
        filename = journal_path / f"{id}.html"

        if not overwrite and os.path.isfile(filename): 
            logger.info("\tAlready exists! Skipping...")
            
            return None, None

        # Save the HTML 
        doc = self.get_html_by_pmid(id, journal, mode=mode, prefer_pmc_source=prefer_pmc_source)
        valid = None
        if doc:
            valid = _validate_scrape(doc)
            if valid:
                with filename.open('w') as f:
                    f.write(doc)
            if not valid:
                logger.info("\tScrape failed! Skipping...")

            # Insert random delay until next request.
            if delay is not None:
                sleep_time = random.random() * float(delay*2)
                sleep(sleep_time)

        return filename, valid

    def retrieve_articles(self, journal=None, pmids=None, dois=None, delay=None, mode='browser', search=None,
                                limit=None, overwrite=False, min_pmid=None, max_pmid=None, shuffle=False,
                                skip_pubmed_central=True, invalid_article_log_file=None, prefer_pmc_source=True):

        ''' Try to retrieve all PubMed articles for a single journal that don't 
        already exist in the storage directory.
        Args:
            journal: The name of the journal (as it appears in PubMed).
            pmids: A list of PMIDs to retrieve.
            dois: A list of DOIs to retrieve. 
            delay: Mean delay between requests.
            mode: When 'browser', use selenium to load articles in Chrome. When 
                'requests', attempts to fetch the HTML directly via requests module.
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
            max_pmid: When a PMID is provided, only articles with PMIDs less than
                this will be processed. 
            shuffle: When True, articles are retrieved in random order.
            skip_pubmed_central: When True, skips articles that are available from
                PubMed Central.
            invalid_article_log_file: Optional path to a file to log files where scraping failed.
            prefer_pmc_source: Optional
                When True, preferentially retrieve articles from PubMed Central, using requests instead of browser
                (regardless of mode). This is useful for journals that have full-text articles available on PMC,
                but are not open-access. If set to "only", will only retrieve articles from PMC, and
                skip articles it cannot retrieve from PMC.
        '''
        articles_found = 0
        if journal is None and dois is None and pmids is None:
            raise ValueError("Either journal, pmids, or dois must be provided.")

        if journal is not None:
            logger.info("Getting PMIDs for articles from %s..." % journal)
            pmids = self.search_pubmed(journal, search)

        if dois is not None:
            logger.info("Retrieving articles from %s..." % ', '.join(dois))
            pmids = [get_pmid_from_doi(doi) for doi in dois]

            # Remove None values and log missing DOIs
            pmids = [pmid for pmid in pmids if pmid is not None]
            missing_dois = [doi for doi, pmid in zip(dois, pmids) if pmid is None]
            if len(missing_dois) > 0:
                logger.info("Missing DOIs: %s" % ', '.join(missing_dois))

        if shuffle:
            random.shuffle(pmids)

        logger.info("Found %d records.\n" % len(pmids))

        # If journal is provided, check for existing articles
        if journal is not None:
            logger.info("Retrieving articles from %s..." % journal)
            journal_path = (self.store / 'html' / journal)
            if journal_path.exists():
                existing = journal_path.glob('*.html')
                existing = [int(f.stem) for f in existing]
                n_existing = len(existing)
                pmids = [pmid for pmid in pmids if int(pmid) not in existing]
                logger.info(f"Found {n_existing} existing articles.")

        # Filter out articles that are outside the PMID range
        pmids = [
            pmid
            for pmid in pmids 
            if (min_pmid is None or int(pmid) >= min_pmid) and (max_pmid is None or int(pmid) <= max_pmid)
            ]
    
        logger.info(f"Retrieving {len(pmids)} articles...")
        
        if skip_pubmed_central:
            all_ids = _convert_pmid_to_pmc(pmids)
        else:
            all_ids = [(None, pmid) for pmid in pmids]

        invalid_articles = []
        for pmcid, pmid in all_ids:
            if journal is None:
                # Get the journal name
                metadata = get_pubmed_metadata(pmid)
                journal = metadata['journal']

            if limit is not None and articles_found >= limit: break

            if skip_pubmed_central and pmcid and self.is_pmc_open_acess(pmcid):
                logger.info(f"\tPubMed Central OpenAccess entry found! Skipping {pmid}...")
                continue

            filename, valid = self.process_article(pmid, journal, delay, mode, overwrite, prefer_pmc_source)

            if not valid:
                invalid_articles.append(filename)
                if invalid_article_log_file is not None:
                    with open(invalid_article_log_file, 'a') as f:
                        f.write(f"{pmid}\n")
            else:
                articles_found += 1

        return invalid_articles
