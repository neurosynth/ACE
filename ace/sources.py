# coding: utf-8
from __future__ import unicode_literals  # use unicode everywhere
from bs4 import BeautifulSoup
import re
import os
import json
import abc
import importlib
from glob import glob
import datatable
import tableparser
import scrape
import config
import database
import logging

logger = logging.getLogger('ace')


class SourceManager:

    ''' Loads all the available Source subclasses from this module and the
    associated directory of JSON config files and uses them to determine which parser
    to call when a new HTML file is passed. '''

    def __init__(self, database, table_dir=None):
        ''' SourceManager constructor.
        Args:
            database: A Database instance to use with all Sources.
            table_dir: An optional directory name to save any downloaded tables to.
                When table_dir is None, nothing will be saved (requiring new scraping
                each time the article is processed).
        '''
        module = importlib.import_module('ace.sources')
        self.sources = {}
        source_dir = os.path.join(os.path.dirname(__file__), 'sources')
        for config_file in glob('%s/*json' % source_dir):
            class_name = config_file.split('/')[-1].split('.')[0]
            cls = getattr(module, class_name + 'Source')(config_file, database, table_dir)
            self.sources[class_name] = cls

    def identify_source(self, html):
        ''' Identify the source of the article and return the corresponding Source object. '''
        for source in self.sources.values():
            for patt in source.identifiers:
                if re.search(patt, html):
                    logger.debug('Matched article to Source: %s' % source.__class__.__name__)
                    return source


# A single source of articles--i.e., a publisher or journal
class Source:

    __metaclass__ = abc.ABCMeta

    # Core set of HTML entities and unicode characters to replace.
    # BeautifulSoup converts HTML entities to unicode, so we could
    # potentially do the replacement only for unicode chars after
    # soupifying the HTML. But this way we only have to do one pass
    # through the entire file, so it should be faster to do it up front.
    ENTITIES = {
        '&nbsp;': ' ',
        '&minus;': '-',
        # '&kappa;': 'kappa',
        '\xa0': ' ',        # Unicode non-breaking space
        # '\x3e': ' ',
        '\u2212': '-',      # Various unicode dashes
        '\u2012': '-',
        '\u2013': '-',
        '\u2014': '-',
        '\u2015': '-',
        '\u8211': '-',
        '\u0150': '-',
        '\u0177': '',
        '\u0160': '',
        '\u0145': "'",
        '\u0146': "'",

    }

    def __init__(self, config, database, table_dir=None):

        config = json.load(open(config, 'rb'))
        self.database = database
        self.table_dir = table_dir

        valid_keys = ['name', 'identifiers', 'entities', 'delay']

        for k, v in config.items():
            if k in valid_keys:
                setattr(self, k, v)

        # Append any source-specific entities found in the config file to
        # the standard list
        if self.entities is None:
            self.entities = Source.ENTITIES
        else:
            self.entities.update(Source.ENTITIES)

    @abc.abstractmethod
    def parse_article(self, html):
        ''' Takes HTML article as input and returns an Article. '''
        html = html.decode('utf-8')   # Make sure we're working with unicode
        html = self.decode_html_entities(html)
        soup = BeautifulSoup(html)
        doi = self.extract_doi(soup)
        pmid = self.extract_pmid(soup)
        text = soup.get_text()
        if self.database.article_exists(pmid):
            if config.OVERWRITE_EXISTING_ROWS:
                self.database.delete_article(pmid)
            else:
                return False
        self.article = database.Article(text, pmid=pmid, doi=doi)
        return soup

    @abc.abstractmethod
    def parse_table(self, table):
        ''' Takes HTML for a single table and returns a Table. '''

        # Formatting issues sometimes prevent table extraction, so just return
        if table is None:
            return False

        # Count columns. Check either just one row, or all of them.
        def n_cols_in_row(row):
            return sum([int(td['colspan']) if td.has_attr('colspan') else 1 for td in row.find_all('td')])

        if config.CAREFUL_PARSING:
            n_cols = max([n_cols_in_row(
                row) for row in table.find('tbody').find_all('tr')])
        else:
            n_cols = n_cols_in_row(table.find('tbody').find('tr'))

        # Initialize grid and populate
        data = datatable.DataTable(0, n_cols)
        rows = table.find_all('tr')
        for (j, r) in enumerate(rows):
            try:
                cols = r.find_all(['td', 'th'])
                cols_found_in_row = 0
                n_cells = len(cols)
                # Assign number of rows and columns this cell fills. We use these rules:
                # * If a rowspan/colspan is explicitly provided, use it
                # * If not, initially assume span == 1 for both rows and columns.
                for (i, c) in enumerate(cols):
                    r_num = int(c['rowspan']) if c.has_attr('rowspan') else 1
                    c_num = int(c['colspan']) if c.has_attr('colspan') else 1
                    cols_found_in_row += c_num
                    # * Check to make sure that we don't have unaccounted-for columns in the
                    #   row after including the current cell. If we do, adjust the colspan
                    #   to take up all of the remaining columns. This is necessary because
                    #   some tables have malformed HTML, and BeautifulSoup can also
                    #   cause problems in its efforts to fix bad tables. The most common
                    #   problem is deletion or omission of enough <td> tags to fill all
                    #   columns, hence our adjustment. Note that in some cases the order of
                    #   filling is not sequential--e.g., when a previous row has cells with
                    #   rowspan > 1. So we have to check if there are None values left over
                    # in the DataTable's current row after we finish filling
                    # it.
                    if i + 1 == n_cells and cols_found_in_row < n_cols and data[j].count(None) > c_num:
                        c_num += n_cols - cols_found_in_row
                    data.add_val(c.get_text(), r_num, c_num)
            except Exception as e:
                if not config.SILENT_ERRORS:
                    logger.error(e.message)
                if not config.IGNORE_BAD_ROWS:
                    raise
        return tableparser.parse_table(data)

    @abc.abstractmethod
    def extract_doi(self, soup):
        ''' Every Source subclass must be able to extract its doi. '''
        return

    @abc.abstractmethod
    def extract_pmid(self, soup):
        ''' Every Source subclass must be able to extract its PMID. '''
        return

    def decode_html_entities(self, html):
        ''' Re-encode HTML entities as innocuous little Unicode characters. '''
        # Any entities BeautifulSoup passes through thatwe don't like, e.g.,
        # &nbsp/x0a
        patterns = re.compile('(' + '|'.join(re.escape(
            k) for k in self.entities.iterkeys()) + ')')
        replacements = lambda m: self.entities[m.group(0)]
        return patterns.sub(replacements, html)
        # return html

    def _download_table(self, url):
        ''' For Sources that have tables in separate files, a helper for 
        downloading and extracting the table data. Also saves to file if desired.
        '''

        delay = self.delay if hasattr(self, 'delay') else 0

        if self.table_dir is not None:
            filename = '%s/%s' % (self.table_dir, url.replace('/', '_'))
            if os.path.exists(filename):
                table_html = open(filename).read().decode('utf-8')
            else:
                table_html = scrape.get_url(url, delay=delay)
                open(filename, 'w').write(table_html.encode('utf-8'))
        else:
            table_html = scrape.get_url(url, delay=delay)

        table_html = self.decode_html_entities(table_html)
        return(BeautifulSoup(table_html))



class HighWireSource(Source):

    def parse_article(self, html):
        soup = super(HighWireSource, self).parse_article(html)
        if not soup:
            return False

        # To download tables, we need the content URL and the number of tables
        content_url = soup.find('meta', {
                                'name': 'citation_public_url'})['content']

        n_tables = len(soup.find_all('span', class_='table-label'))

        # Now download each table and parse it
        tables = []
        for i in range(n_tables):
            t_num = i + 1
            url = '%s/T%d.expansion.html' % (content_url, t_num)
            table_soup = self._download_table(url)
            tc = table_soup.find(class_='table-expansion')
            t = tc.find('table', {'id': 'table-%d' % (t_num)})
            t = self.parse_table(t)
            if t:
                t.position = t_num
                t.label = tc.find(class_='table-label').text
                t.number = t.label.split(' ')[-1]
                try:
                    t.caption = tc.find(class_='table-caption').get_text()
                except:
                    pass
                try:
                    t.notes = tc.find(class_='table-footnotes').get_text()
                except:
                    pass
                tables.append(t)

        self.article.tables = tables
        return self.article

    def parse_table(self, table):
        return super(HighWireSource, self).parse_table(table)

    def extract_doi(self, soup):
        return soup.find('meta', {'name': 'citation_doi'})['content']

    def extract_pmid(self, soup):
        return soup.find('meta', {'name': 'citation_pmid'})['content']


class ScienceDirectSource(Source):

    def parse_article(self, html):
        soup = super(ScienceDirectSource, self).parse_article(
            html)  # Do some preprocessing
        if not soup:
            return False

        # Extract tables
        tables = []
        for (i, tc) in enumerate(soup.find_all('dl', {'class': 'table '})):
            table_html = tc.find('table')
            t = self.parse_table(table_html)
            if t:
                t.position = i + 1
                t.number = tc['data-label'].split(' ')[-1]
                t.label = tc.find('span', class_='label').text.strip()
                try:
                    t.caption = tc.find('p', class_='caption').get_text()
                except:
                    pass
                try:
                    t.notes = tc.find(class_='tblFootnote').get_text()
                except:
                    pass
                tables.append(t)

        self.article.tables = tables
        return self.article

    def parse_table(self, table):
        return super(ScienceDirectSource, self).parse_table(table)

    def extract_doi(self, soup):
        return soup.find('a', {'id': 'ddDoi'})['href'].replace('http://dx.doi.org/', '')

    def extract_pmid(self, soup):
        return scrape.get_pmid_from_doi(self.extract_doi(soup))


class PlosSource(Source):

    def parse_article(self, html):
        soup = super(PlosSource, self).parse_article(
            html)  # Do some preprocessing
        if not soup:
            return False

        # Extract tables
        tables = []
        for (i, tc) in enumerate(soup.find_all('table-wrap')):
            table_html = tc.find('table')
            t = self.parse_table(table_html)
            if t:
                t.position = i + 1
                t.label = tc.find('label').text
                t.number = t.label.split(' ')[-1]
                try:
                    t.caption = tc.find('title').get_text()
                except:
                    pass
                try:
                    t.notes = tc.find('table-wrap-foot').get_text()
                except:
                    pass
                tables.append(t)

        self.article.tables = tables
        return self.article

    def parse_table(self, table):
        return super(PlosSource, self).parse_table(table)

    def extract_doi(self, soup):
        return soup.find('article-id', {'pub-id-type': 'doi'}).text

    def extract_pmid(self, soup):
        return scrape.get_pmid_from_doi(self.extract_doi(soup))


class FrontiersSource(Source):

    def parse_article(self, html):

        soup = super(FrontiersSource, self).parse_article(
            html)  # Do some preprocessing
        if not soup:
            return False

        # Extract tables
        tables = []
        table_containers = soup.findAll(
            'table-wrap', {'id': re.compile('^T\d+$')})
        for (i, tc) in enumerate(table_containers):
            table_html = tc.find('table')
            t = self.parse_table(table_html)
            # If Table instance is returned, add other properties
            if t:
                t.position = i + 1
                t.number = tc['id'][1::]
                t.label = tc.find('label').get_text()
                try:
                    t.caption = tc.find('caption').get_text()
                except:
                    pass
                try:
                    t.notes = tc.find('table-wrap-foot').get_text()
                except:
                    pass
                tables.append(t)

        self.article.tables = tables
        return self.article

    def parse_table(self, table):
        return super(FrontiersSource, self).parse_table(table)

    def extract_doi(self, soup):
        return soup.find('article-id', {'pub-id-type': 'doi'}).text

    def extract_pmid(self, soup):
        return scrape.get_pmid_from_doi(self.extract_doi(soup))


class JournalOfCognitiveNeuroscienceSource(Source):

    def parse_article(self, html):
        soup = super(
            JournalOfCognitiveNeuroscienceSource, self).parse_article(html)
        if not soup:
            return False

        # To download tables, we need the DOI and the number of tables
        m = re.search(
            '\<meta.*content="http://dx.doi.org/(10.1162/jocn_a_00371)["\s]+', html)
        doi = m.group(1)

        pattern = re.compile('^T\d+$')
        n_tables = len(soup.find_all('table', {'id': pattern}))

        tables = []

        # Now download each table and parse it
        for i in range(n_tables):
            url = 'http://www.mitpressjournals.org/action/showPopup?citid=citart1&id=T%d&doi=%s' % (
                i + 1, doi)
            table_soup = self._download_table(url)
            t = table_soup.find('table').find(
                'table')  # JCogNeuro nests tables 2-deep
            t = self.parse_table(t)
            if t:
                tables.append(t)

        self.article.tables = tables
        return self.article

    def parse_table(self, table):
        return super(JournalOfCognitiveNeuroscienceSource, self).parse_table(table)

    def extract_doi(self, soup):
        return soup.find('meta', {'name': 'dc.Identifier', 'scheme': 'doi'})['content']

    def extract_pmid(self, soup):
        return scrape.get_pmid_from_doi(self.extract_doi(soup))


class WileySource(Source):

    def parse_article(self, html):

        soup = super(WileySource, self).parse_article(
            html)  # Do some preprocessing
        if not soup:
            return False

        # Extract tables
        tables = []
        table_containers = soup.findAll('div', {
                                        'class': 'table', 'id': lambda x: x.startswith('tbl')})
        for (i, tc) in enumerate(table_containers):
            table_html = tc.find('table')
            try:
                table_html.tfoot.extract(
                )  # Remove footer, which appears inside table
            except:
                pass
            t = self.parse_table(table_html)
            # If Table instance is returned, add other properties
            if t:
                t.position = i + 1
                t.number = tc['id'][3::]
                t.label = tc.find('span', class_='label').get_text()
                t.caption = tc.find('caption').get_text()
                try:
                    t.notes = tc.find('tfoot').get_text()
                except:
                    pass
                tables.append(t)

        self.article.tables = tables
        return self.article

    def parse_table(self, table):
        return super(WileySource, self).parse_table(table)

    def extract_doi(self, soup):
        return soup.find('meta', {'name': 'citation_doi'})['content']

    def extract_pmid(self, soup):
        return scrape.get_pmid_from_doi(self.extract_doi(soup))

# Note: the SageSource is largely useless and untested because Sage renders tables
# as images.


class SageSource(Source):

    def parse_article(self, html):

        soup = super(SageSource, self).parse_article(
            html)  # Do some preprocessing
        if not soup:
            return False

        # To download tables, we need the content URL and the number of tables
        content_url = soup.find('meta', {
                                'name': 'citation_public_url'})['content']

        n_tables = len(soup.find_all('span', class_='table-label'))

        # Now download each table and parse it
        tables = []
        for i in range(n_tables):
            t_num = i + 1
            url = '%s/T%d.expansion.html' % (content_url, t_num)
            table_soup = self._download_table(url)
            tc = table_soup.find(class_='table-expansion')
            t = tc.find('table', {'id': 'table-%d' % (t_num)})
            t = self.parse_table(t)
            if t:
                t.position = t_num
                t.label = tc.find(class_='table-label').text
                t.number = t.label.split(' ')[-1]
                try:
                    t.caption = tc.find(class_='table-caption').get_text()
                except:
                    pass
                try:
                    t.notes = tc.find(class_='table-footnotes').get_text()
                except:
                    pass
                tables.append(t)

        self.article.tables = tables
        return self.article

    def parse_table(self, table):
        return super(SageSource, self).parse_table(table)

    def extract_doi(self, soup):
        return soup.find('meta', {'name': 'citation_doi'})['content']

    def extract_pmid(self, soup):
        return soup.find('meta', {'name': 'citation_pmid'})['content']
