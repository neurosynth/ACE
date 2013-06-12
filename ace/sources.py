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
import scrapers
import config
import database


class SourceManager:
    ''' Loads all the available Source subclasses from this module and the 
    associated directory of JSON config files and uses them to determine which parser
    to call when a new HTML file is passed. '''

    def __init__(self):
        module = importlib.import_module('ace.sources')
        self.sources = {}
        source_dir = os.path.join(os.path.dirname(__file__), 'sources')
        for config_file in glob('%s/*json' % source_dir):
            class_name = config_file.split('/')[-1].split('.')[0]
            cls = getattr(module, class_name + 'Source')(config_file)
            self.sources[class_name] = cls

    def identify_source(self, html):
        ''' Identify the source of the article and return the corresponding Source object. '''
        for source in self.sources.values():
            for patt in source.identifiers:
                if re.search(patt, html):
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

    def __init__(self, config):

        config = json.load(open(config, 'rb'))

        valid_keys = ['name', 'identifiers', 'entities']

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
        self.article = database.Article()
        html = html.decode('utf-8')   # Make sure we're working with unicode
        html = self.decode_html_entities(html)
        return html

    @abc.abstractmethod
    def parse_table(self, table):
        ''' Takes HTML for a single table and returns a Table. '''
        
        # Count columns. Check either just one row, or all of them.
        def n_cols_in_row(row):
            return sum([int(td['colspan']) if td.has_key('colspan') else 1 for td in row.find_all('td')])

        if config.CAREFUL_PARSING:
            n_cols = max([n_cols_in_row(row) for row in table.find('tbody').find_all('tr')])
        else:
            n_cols = n_cols_in_row(table.find('tbody').find('tr'))
        
        # Initialize grid and populate
        data = datatable.DataTable(0, n_cols)
        rows = table.find_all('tr')
        for r in rows:
            try:
                cols = r.find_all(['td', 'th'])
                cols_found_in_row = 0
                n_cells = len(cols)
                # Assign number of rows and columns this cell fills. We use these rules:
                # * If a rowspan/colspan is explicitly provided, use it
                # * If not, initially assume span == 1 for both rows and columns.
                # * Check to make sure that we don't have unaccounted-for columns in the 
                #   row after including the current cell. If we do, adjust the colspan 
                #   to take up all of the remaining columns. This is necessary because 
                #   some tables have malformed HTML, and BeautifulSoup can also 
                #   cause problems in its efforts to fix bad tables. The most common 
                #   problem is deletion or omission of enough <td> tags to fill all 
                #   columns, hence our adjustment.
                for (i,c) in enumerate(cols):
                    r_num = int(c['rowspan']) if c.has_key('rowspan') else 1
                    c_num = int(c['colspan']) if c.has_key('colspan') else 1
                    cols_found_in_row += c_num
                    if i+1 == n_cells and cols_found_in_row < n_cols:
                        c_num += n_cols - cols_found_in_row
                    data.add_val(c.get_text(), r_num, c_num)
            except Exception as e:
                if not config.SILENT_ERRORS: print e.message
                if not config.IGNORE_BAD_ROWS: raise
        return tableparser.parse_table(data)

    def decode_html_entities(self, html):
        ''' Re-encode HTML entities as innocuous little Unicode characters. '''
        # Any entities BeautifulSoup passes through thatwe don't like, e.g., &nbsp/x0a
        patterns = re.compile('(' + '|'.join(re.escape(k) for k in self.entities.iterkeys()) + ')')
        replacements = lambda m: self.entities[m.group(0)]
        return patterns.sub(replacements, html)
        # return html



class HighWireSource(Source):

    def parse_article(self, html):
        html = super(HighWireSource, self).parse_article(html)
        soup = BeautifulSoup(html)

        # To download tables, we need the content URL and the number of tables
        content_url = soup.find('meta', {'name': 'citation_public_url'})['content']

        n_tables = len(soup.find_all('span', class_='table-label'))

        # Now download each table and parse it
        tables = []
        for i in range(n_tables):
            t_num = i+1
            url = '%s/T%d.expansion.html' % (content_url, t_num)
            table_html = parse_utils.get_url(url)
            table_html = self.decode_html_entities(table_html)
            table_soup = BeautifulSoup(table_html)
            tc = table_soup.find(class_='table-expansion')
            t = tc.find('table', {'id': 'table-%d' % (t_num)})
            t = self.parse_table(t)
            if t: 
                t.number = t_num
                t.title = tc.find(class_='table-label').text
                try:
                    t.caption = tc.find(class_='table-caption').get_text()
                except: pass
                try:
                    t.notes = tc.find(class_='table-footnotes').get_text()
                except: pass
                tables.append(t)

        self.article.tables = tables
        return self.article

    def parse_table(self, table):
        return super(HighWireSource, self).parse_table(table)



class ScienceDirectSource(Source):

    def parse_article(self, html):
        html = super(ScienceDirectSource, self).parse_article(html)  # Do some preprocessing
        soup = BeautifulSoup(html)

        # Extract tables
        tables = []
        for tc in soup.find_all('dl', {'class': 'table '}):
            table_html = tc.find('table')
            t = self.parse_table(table_html)
            if t:
                t.number = int(tc['data-label'].split(' ')[-1])
                t.title = tc.find('span', class_='label').text.strip()
                try:
                    t.caption = tc.find('p', class_='caption').get_text()
                except: pass
                try:
                    t.notes = tc.find(class_='tblFootnote').get_text()
                except: pass
                tables.append(t)

        self.article.tables = tables
        return self.article

    def parse_table(self, table):
        return super(ScienceDirectSource, self).parse_table(table)



class PlosSource(Source):

    def parse_article(self, html):
        html = super(PlosSource, self).parse_article(html)  # Do some preprocessing
        soup = BeautifulSoup(html)

        # Extract tables
        tables = []
        for tc in soup.find_all('table-wrap'):
            table_html = tc.find('table')
            t = self.parse_table(table_html)
            if t:
                t.title = tc.find('label').text
                t.number = int(t.title.split(' ')[-1])
                try:
                    t.caption = tc.find('title').get_text()
                except: pass
                try:
                    t.notes = tc.find('table-wrap-foot').get_text()
                except: pass
                tables.append(t)

        self.article.tables = tables
        return self.article

    def parse_table(self, table):
        return super(PlosSource, self).parse_table(table)



class FrontiersSource(Source):

    def parse_article(self, html):

        html = super(FrontiersSource, self).parse_article(html)  # Do some preprocessing
        soup = BeautifulSoup(html)

        # Extract tables
        tables = []
        table_containers = soup.findAll('table-wrap', {'id': re.compile('^T\d+$')})
        for tc in table_containers:
            table_html = tc.find('table')
            t = self.parse_table(table_html)
            # If Table instance is returned, add other properties
            if t:
                t.number = int(tc['id'][1::])
                t.title = tc.find('label').get_text()
                try:
                    t.caption = tc.find('caption').get_text()
                except: pass
                try:
                    t.notes = tc.find('table-wrap-foot').get_text()
                except: pass
                tables.append(t)

        self.article.tables = tables
        return self.article

    def parse_table(self, table):
        return super(FrontiersSource, self).parse_table(table)



class JournalOfCognitiveNeuroscienceSource(Source):

    def parse_article(self, html):
        html = super(JournalOfCognitiveNeuroscienceSource, self).parse_article(html)
        soup = BeautifulSoup(html)

        # To download tables, we need the DOI and the number of tables
        m = re.search('\<meta.*content="http://dx.doi.org/(10.1162/jocn_a_00371)["\s]+', html)
        doi = m.group(1)

        pattern = re.compile('^T\d+$')
        n_tables = len(soup.find_all('table', {'id': pattern }))

        tables = []

        # Now download each table and parse it
        for i in range(n_tables):
            url = 'http://www.mitpressjournals.org/action/showPopup?citid=citart1&id=T%d&doi=%s' % (i+1, doi)
            table_html = parse_utils.get_url(url)
            table_html = self.decode_html_entities(table_html)
            table_soup = BeautifulSoup(table_html)
            t = table_soup.find('table').find('table')  # JCogNeuro nests tables 2-deep
            t = self.parse_table(t)
            if t: tables.append(t)

        self.article.tables = tables
        return self.article
            
    def parse_table(self, table):
        return super(JournalOfCognitiveNeuroscienceSource, self).parse_table(table)

