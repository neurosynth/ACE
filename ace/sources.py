# coding: utf-8
# use unicode everywhere
from bs4 import BeautifulSoup
import re
import os
import json
import abc
import importlib
from glob import glob
from ace import datatable
from ace import tableparser
from ace import scrape
from ace import config
from ace import database
import logging

logger = logging.getLogger(__name__)

# Try to import readabilipy for enhanced HTML cleaning
try:
    from readabilipy import simple_json_from_html_string
    READABILITY_AVAILABLE = True
except ImportError:
    READABILITY_AVAILABLE = False
    logger.warning("readabilipy not installed. Install with 'pip install readabilipy' for enhanced HTML cleaning. "
                     "Note: Node.js is also required for readabilipy to work.")


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
            cls = getattr(module, class_name + 'Source')(database, config=config_file, table_dir=table_dir)
            self.sources[class_name] = cls

    def identify_source(self, html):
        ''' Identify the source of the article and return the corresponding Source object. '''
        for source in list(self.sources.values()):
            for patt in source.identifiers:
                if re.search(patt, html):
                    logger.debug('Matched article to Source: %s' % source.__class__.__name__)
                    return source


# A single source of articles--i.e., a publisher or journal
class Source(metaclass=abc.ABCMeta):
    # need to include the \\u2009 which is the thin space to which the table is being invalidated due to those characters
    # -\\u2009int
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
        '\u2009': "",     # Various whitespaces within tables
        '\u2007': "",

    }

    def _clean_html_with_readability(self, html):
        """
        Clean HTML content using Mozilla's readability algorithm via readabilipy.
        
        Falls back to basic BeautifulSoup cleaning if readabilipy is not available or fails.
        
        Args:
            html: The HTML content to clean
            
        Returns:
            The cleaned text content
        """
        global READABILITY_AVAILABLE
        
        # If readabilipy is not available, fall back to basic BeautifulSoup cleaning
        if not READABILITY_AVAILABLE:
            logger.warning("Falling back to basic HTML cleaning as readabilipy is not available")
            return self._safe_clean_html(html)
        
        try:
            # Use readabilipy with Mozilla's readability algorithm
            article = simple_json_from_html_string(html, use_readability=True)
            if article and 'content' in article and article['content']:
                # Extract text content from the HTML
                soup = BeautifulSoup(article['content'], "lxml")
                # Get text content, preserving some structure
                text_parts = []
                for element in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                    text = element.get_text(strip=False)
                    if text.strip():
                        text_parts.append(text.strip())
                return '\n\n'.join(text_parts) if text_parts else soup.get_text()
            else:
                # If readability failed to extract content, fall back to safe cleaning
                logger.warning("Readability failed to extract content, falling back to basic HTML cleaning")
                return self._safe_clean_html(html)
        except Exception as e:
            # If any error occurs, fall back to safe cleaning
            logger.warning(f"Error using readabilipy, falling back to basic HTML cleaning: {e}")
            return self._safe_clean_html(html)
    
    def _safe_clean_html(self, html):
        """
        Clean HTML content using BeautifulSoup as a fallback.
        
        Args:
            html: The HTML content to clean
            
        Returns:
            The cleaned text content
        """
        soup = BeautifulSoup(html, "lxml")

        # 1. Remove non-text tags
        for tag in soup(["script", "style", "noscript", "iframe", "svg", "canvas"]):
            tag.decompose()

        # 2. Remove comments
        from bs4 import Comment
        for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
            comment.extract()

        # 3. Strip heavy attributes but keep the tags/text
        for tag in soup.find_all(True):
            for attr in list(tag.attrs):
                if attr in ["style", "onclick", "class", "id", "aria-hidden", "aria-label"]:
                    del tag[attr]

        # Extract text content
        text_parts = []
        for element in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            text = element.get_text(strip=False)
            if text.strip():
                text_parts.append(text.strip())
        return '\n\n'.join(text_parts) if text_parts else soup.get_text()

    def __init__(self, database, config=None, table_dir=None):
        self.database = database
        self.table_dir = table_dir
        self.entities = {}

        if config is not None:
            config = json.load(open(config, 'rb'))
            valid_keys = ['name', 'identifiers', 'entities', 'delay']

            for k, v in list(config.items()):
                if k in valid_keys:
                    setattr(self, k, v)

            # Append any source-specific entities found in the config file to
            # the standard list
            if self.entities is None:
                self.entities = Source.ENTITIES
            else:
                self.entities.update(Source.ENTITIES)

    @abc.abstractmethod
    def parse_article(self, html, pmid=None, metadata_dir=None):
        ''' Takes HTML article as input and returns an Article. PMID Can also be 
        passed, which prevents having to scrape it from the article and/or look it 
        up in PubMed. '''

        # Skip rest of processing if this record already exists
        if pmid is not None and self.database.article_exists(pmid) and not config.OVERWRITE_EXISTING_ROWS:
            return False

        html = self.decode_html_entities(html)
        soup = BeautifulSoup(html)
        if pmid is None:
            pmid = self.extract_pmid(soup)

        # did our best to find PMID, but failed
        if not pmid:
            return False

        metadata = scrape.get_pubmed_metadata(pmid, store=metadata_dir, save=True)

        # Remove all scripts and styles
        for script in soup(["script", "style"]):
            script.extract()
        
        # Get text using readability
        text = self._clean_html_with_readability(str(soup))
        if self.database.article_exists(pmid):
            if config.OVERWRITE_EXISTING_ROWS:
                self.database.delete_article(pmid)
            else:
                return False

        self.article = database.Article(text, pmid=pmid, metadata=metadata)
        self.extract_neurovault(soup)
        return soup

    def extract_neurovault(self, soup):
        ''' Look through all links, and use regex to identify NeuroVault links. '''
        image_regexes = ['identifiers.org/neurovault.image:(\d*)',
                     'neurovault.org/images/(\d*)']

        image_regexes = re.compile( '|'.join( image_regexes) )

        collection_regexes = ['identifiers.org/neurovault.collection:(\w*)',
                     'neurovault.org/collections/(\w*)']

        collection_regexes = re.compile( '|'.join( collection_regexes) )


        nv_links = []
        for link in soup.find_all('a'):
            if link.has_attr('href'):
                href = link['href']

                img_m = image_regexes.search(href)
                col_m = collection_regexes.search(href)
                if not (img_m or col_m):
                    continue

                if img_m:
                    type = 'image'
                    val =  img_m.groups()[0] or img_m.groups()[1]
                elif col_m:
                    type = 'collection'
                    val =  col_m.groups()[0] or col_m.groups()[1]

                nv_links.append(
                    database.NeurovaultLink(
                        type=type,
                        neurovault_id=val,
                        url=href
                    )
                )

        self.article.neurovault_links = nv_links


    def parse_table(self, table):
        ''' Takes HTML for a single table and returns a Table. '''
        # Formatting issues sometimes prevent table extraction, so just return
        if table is None:
            return False

        logger.debug("\t\tFound a table...")

        # change <br/> to \n
        for br in table.find_all("br"):
            br.replace_with("\n")

        # Count columns. Check either just one row, or all of them.
        def n_cols_in_row(row):
            return sum([
                int(td['colspan'])
                if td.has_attr('colspan') and td['colspan'] != "NaN" else 1
                for td in row.find_all(['th', 'td'])
                ])

        search_table = table.find("tbody")
        if search_table is None:
            search_table = table

        all_trs = search_table.find_all('tr')
        if all_trs is None or len(all_trs) == 0:
            return False

        if config.CAREFUL_PARSING:
            n_cols = max([n_cols_in_row(
                row) for row in all_trs])
        else:
            n_cols = n_cols_in_row(search_table.find('tr'))

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
                    r_num = (
                        int(c['rowspan'])
                        if c.has_attr('rowspan') and c['rowspan'] != "NaN" else 1
                    )
                    c_num = (
                        int(c['colspan'])
                        if c.has_attr('colspan') and c['colspan'] != "NaN" else 1
                    )
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
                    if i + 1 == n_cells and cols_found_in_row < n_cols and (len(data.data) == j+1) and data[j].count(None) > c_num:
                        c_num += n_cols - cols_found_in_row
                    data.add_val(c.get_text(), r_num, c_num)
            except Exception as err:
                if not config.SILENT_ERRORS:
                    logger.error(str(err))
                if not config.IGNORE_BAD_ROWS:
                    raise

        if data.data[data.n_rows- 1].count(None) == data.n_cols:
            data.data.pop()
        logger.debug("\t\tTrying to parse table...")
        return tableparser.parse_table(data)

    def extract_doi(self, soup):
        ''' Every Source subclass must be able to extract its doi. '''
        return

    def extract_pmid(self, soup):
        ''' Every Source subclass must be able to extract its PMID. '''
        return

    def decode_html_entities(self, html):
        ''' Re-encode HTML entities as innocuous little Unicode characters. '''
        # Any entities BeautifulSoup passes through thatwe don't like, e.g.,
        # &nbsp/x0a
        if self.entities:
            patterns = re.compile('(' + '|'.join(re.escape(
                k) for k in list(self.entities.keys())) + ')')
            replacements = lambda m: self.entities[m.group(0)]
            return patterns.sub(replacements, html)
        else:
            return html

    def _download_table(self, url):
        ''' For Sources that have tables in separate files, a helper for 
        downloading and extracting the table data. Also saves to file if desired.
        '''

        delay = self.delay if hasattr(self, 'delay') else 0

        if self.table_dir is not None:
            filename = '%s/%s' % (self.table_dir, url.replace('/', '_'))
            if os.path.exists(filename):
                table_html = open(filename).read()
            else:
                table_html = scrape.get_url(url)
                open(filename, 'w').write(table_html.encode('utf-8'))
        else:
            table_html = scrape.get_url(url)

        if table_html:
            table_html = self.decode_html_entities(table_html)
            return BeautifulSoup(table_html)

        return None


class DefaultSource(Source):
    def parse_article(self, html, pmid=None, **kwargs):
        soup = super(DefaultSource, self).parse_article(html, pmid, **kwargs)
        if not soup:
            return False

        # Extract tables using multi-strategy detection system
        tables = []
        
        # Strategy 1: Publisher-agnostic container detection
        table_containers = self._detect_table_containers_strategy_1(soup)
        
        # Strategy 2: Semantic HTML analysis
        if not table_containers:
            table_containers = self._detect_table_containers_strategy_2(soup)
            
        # Strategy 3: Content-based detection
        if not table_containers:
            table_containers = self._detect_table_containers_strategy_3(soup)
            
        # Strategy 4: Generic fallback
        if not table_containers:
            table_containers = self._detect_table_containers_strategy_4(soup)

        logger.info(f"Found {len(table_containers)} potential table containers.")
        
        for (i, tc) in enumerate(table_containers):
            table_html = self._extract_table_from_container(tc)
            if not table_html:
                continue
                
            t = self.parse_table(table_html)
            if t:
                t.position = i + 1
                
                # Extract metadata using multiple fallback approaches
                metadata = self._extract_table_metadata(tc, table_html, i + 1)
                t.number = metadata.get('number')
                t.label = metadata.get('label')
                t.caption = metadata.get('caption')
                t.notes = metadata.get('notes')
                
                # Validate table quality
                if self._validate_table(t, tc):
                    tables.append(t)

        self.article.tables = tables
        if not tables:
            self.article.missing_source = True
        return self.article
    
    def _detect_table_containers_strategy_1(self, soup):
        """Strategy 1: Publisher-agnostic container detection"""
        containers = []
        
        # Common table container patterns observed across publishers
        selectors = [
            # Oxford Academic style
            'div.table-full-width-wrap:not(.table-modal)',
            'div[class*="table-wrap"]',
            'div[class*="table-container"]',
            
            # Science Direct style
            'div.tables',
            'dl.table',
            'div[class*="table"][class*="content"]',
            
            # XML-style tags (PLoS, Frontiers)
            'table-wrap',
            
            # PMC style
            'div.table-wrap',
            
            # Figure-based table containers
            'figure[id*="table"]',
            'figure[id*="tbl"]',
            'figure[class*="table"]',
            
            # Generic table containers with IDs
            'div[id*="table"]',
            'div[id*="tbl"]',
            'section[id*="table"]',
            'section[id*="tbl"]',
        ]
        
        for selector in selectors:
            try:
                found = soup.select(selector)
                if found:
                    # Prioritize containers that actually contain table elements
                    valid_containers = [tc for tc in found if tc.find('table')]
                    if valid_containers:
                        logger.debug(f"Strategy 1: Found {len(valid_containers)} containers with selector: {selector}")
                        return valid_containers
                    containers.extend(found)
            except Exception as e:
                logger.debug(f"Strategy 1: Selector '{selector}' failed: {e}")
                continue
                
        return containers

    def _detect_table_containers_strategy_2(self, soup):
        """Strategy 2: Semantic HTML analysis"""
        containers = []
        
        # Look for tables with semantic context
        tables_with_context = []
        
        # Find tables with captions
        tables = soup.find_all('table')
        for table in tables:
            # Check for caption element
            if table.find('caption'):
                containers.append(table.parent if table.parent else table)
                continue
                
            # Check for preceding headings with "Table" or "Tab"
            prev_elements = table.find_all_previous(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p'], limit=3)
            for elem in prev_elements:
                if elem and re.search(r'\btable?\s*\d+', elem.get_text(), re.IGNORECASE):
                    containers.append(table.parent if table.parent else table)
                    break
                    
            # Check for role="table" attribute
            if table.get('role') == 'table':
                containers.append(table.parent if table.parent else table)
                
        return containers

    def _detect_table_containers_strategy_3(self, soup):
        """Strategy 3: Content-based detection using heuristics"""
        containers = []
        
        # Find tables containing coordinate-like data
        tables = soup.find_all('table')
        for table in tables:
            text_content = table.get_text()
            
            # Look for coordinate patterns (numbers that could be x,y,z coordinates)
            coord_patterns = [
                r'-?\d+\.?\d*\s*,\s*-?\d+\.?\d*\s*,\s*-?\d+\.?\d*',  # x,y,z format
                r'-?\d+\s+-?\d+\s+-?\d+',  # space-separated coordinates
                r'MNI|Talairach|coordinates',  # neuroimaging coordinate systems
            ]
            
            has_coords = any(re.search(pattern, text_content, re.IGNORECASE) for pattern in coord_patterns)
            
            # Look for neuroimaging keywords in headers
            header_text = ' '.join([th.get_text() for th in table.find_all(['th', 'thead'])])
            neuro_keywords = ['region', 'area', 'activation', 'volume', 'voxel', 'brain', 'cortex',
                            'significance', 'p-value', 'z-score', 't-value']
            has_neuro_keywords = any(keyword in header_text.lower() for keyword in neuro_keywords)
            
            # Look for statistical data patterns
            has_stats = bool(re.search(r'p\s*[<>=]\s*0\.\d+|[zpt]\s*=\s*\d+\.\d+', text_content, re.IGNORECASE))
            
            if has_coords or (has_neuro_keywords and has_stats):
                containers.append(table.parent if table.parent else table)
                
        return containers

    def _detect_table_containers_strategy_4(self, soup):
        """Strategy 4: Generic fallback - extract all tables with filtering"""
        containers = []
        
        # Get all table elements
        tables = soup.find_all('table')
        
        for table in tables:
            # Filter out navigation/layout tables
            if self._is_navigation_table(table):
                continue
                
            # Must have reasonable content
            rows = table.find_all('tr')
            if len(rows) < 2:  # Need at least header + 1 data row
                continue
                
            containers.append(table.parent if table.parent else table)
            
        return containers

    def _is_navigation_table(self, table):
        """Check if table is likely for navigation/layout rather than data"""
        # Check for navigation indicators
        table_text = table.get_text().lower()
        nav_indicators = ['menu', 'navigation', 'nav', 'login', 'search', 'footer', 'header']
        
        if any(indicator in table_text for indicator in nav_indicators):
            return True
            
        # Check table structure - navigation tables often have many links
        links = table.find_all('a')
        cells = table.find_all(['td', 'th'])
        if cells and len(links) / len(cells) > 0.5:  # More than 50% of cells have links
            return True
            
        # Check for CSS classes that suggest navigation
        table_classes = ' '.join(table.get('class', []))
        nav_classes = ['nav', 'menu', 'footer', 'header', 'sidebar']
        if any(cls in table_classes.lower() for cls in nav_classes):
            return True
            
        return False

    def _extract_table_from_container(self, container):
        """Extract the actual table element from a container"""
        # If container is already a table, return it
        if container.name == 'table':
            return container
            
        # Look for table within container
        table = container.find('table')
        return table

    def _extract_table_metadata(self, container, table_html, position):
        """Extract table metadata using multiple fallback approaches"""
        metadata = {
            'number': None,
            'label': None,
            'caption': None,
            'notes': None
        }
        
        # Strategy 1: XML-style metadata (PLoS, Frontiers style)
        if container.name in ['table-wrap', 'fig']:
            metadata.update(self._extract_xml_style_metadata(container, position))
            
        # Strategy 2: HTML container metadata (OUP, ScienceDirect style)
        if not metadata['label']:
            metadata.update(self._extract_html_container_metadata(container, position))
            
        # Strategy 3: Table-level metadata (caption, etc.)
        if not metadata['caption'] and table_html:
            metadata.update(self._extract_table_level_metadata(table_html, position))
            
        # Strategy 4: Context-based metadata (look around the table)
        if not metadata['label']:
            metadata.update(self._extract_context_metadata(container, position))
            
        return metadata

    def _extract_xml_style_metadata(self, container, position):
        """Extract metadata from XML-style containers (PLoS/Frontiers pattern)"""
        metadata = {}
        
        try:
            # Label from XML tags
            label_elem = container.find('label')
            if label_elem:
                metadata['label'] = label_elem.get_text().strip()
                # Extract number from label
                number_match = re.search(r'(\d+)', metadata['label'])
                if number_match:
                    metadata['number'] = number_match.group(1)
                    
            # Caption from title or caption tags
            caption_elem = container.find(['title', 'caption'])
            if caption_elem:
                metadata['caption'] = caption_elem.get_text().strip()
                
            # Notes from footer
            footer_elem = container.find(['table-wrap-foot', 'fig-foot'])
            if footer_elem:
                metadata['notes'] = footer_elem.get_text().strip()
                
        except Exception as e:
            logger.debug(f"XML metadata extraction failed: {e}")
            
        return metadata

    def _extract_html_container_metadata(self, container, position):
        """Extract metadata from HTML containers (OUP/ScienceDirect pattern)"""
        metadata = {}
        
        try:
            # Look for label spans (OUP style)
            label_elem = container.find('span', class_='label')
            if not label_elem:
                label_elem = container.find(['span', 'div'], string=re.compile(r'Table\s*\d+', re.IGNORECASE))
            if label_elem:
                metadata['label'] = label_elem.get_text().strip()
                number_match = re.search(r'(\d+)', metadata['label'])
                if number_match:
                    metadata['number'] = number_match.group(1)
                    
            # Look for captions (multiple possible locations)
            caption_elem = container.find(['span', 'div', 'p'], class_='caption')
            if not caption_elem:
                caption_elem = container.find(['span', 'div', 'p'], class_=re.compile(r'caption|title'))
            if caption_elem:
                metadata['caption'] = caption_elem.get_text().strip()
                
            # Look for footnotes/notes
            notes_elem = container.find(['span', 'div'], class_=re.compile(r'fn|footnote|note'))
            if notes_elem:
                metadata['notes'] = notes_elem.get_text().strip()
                
        except Exception as e:
            logger.debug(f"HTML container metadata extraction failed: {e}")
            
        return metadata

    def _extract_table_level_metadata(self, table, position):
        """Extract metadata from table element itself"""
        metadata = {}
        
        try:
            # Look for caption element
            caption_elem = table.find('caption')
            if caption_elem:
                caption_text = caption_elem.get_text().strip()
                metadata['caption'] = caption_text
                
                # Try to extract label/number from caption
                label_match = re.search(r'(Table\s*\d+)', caption_text, re.IGNORECASE)
                if label_match:
                    metadata['label'] = label_match.group(1)
                    number_match = re.search(r'(\d+)', metadata['label'])
                    if number_match:
                        metadata['number'] = number_match.group(1)
                        
            # Look for footer notes
            footer_elem = table.find('tfoot')
            if footer_elem:
                metadata['notes'] = footer_elem.get_text().strip()
                
        except Exception as e:
            logger.debug(f"Table-level metadata extraction failed: {e}")
            
        return metadata

    def _extract_context_metadata(self, container, position):
        """Extract metadata by looking at context around the table"""
        metadata = {}
        
        try:
            # Look for headings before the table
            current = container
            for _ in range(5):  # Look up to 5 elements back
                prev = current.find_previous_sibling()
                if not prev:
                    break
                current = prev
                
                if prev.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                    heading_text = prev.get_text().strip()
                    if re.search(r'\btable?\s*\d+', heading_text, re.IGNORECASE):
                        metadata['label'] = heading_text
                        number_match = re.search(r'(\d+)', heading_text)
                        if number_match:
                            metadata['number'] = number_match.group(1)
                        break
                        
            # If no number found, use position
            if not metadata['number']:
                metadata['number'] = str(position)
                
            # Default label if none found
            if not metadata['label']:
                metadata['label'] = f"Table {position}"
                
        except Exception as e:
            logger.debug(f"Context metadata extraction failed: {e}")
            # Fallback defaults
            metadata['number'] = str(position)
            metadata['label'] = f"Table {position}"
            
        return metadata

    def _validate_table(self, table, container):
        """Validate table quality to ensure it meets standards"""
        try:
            # Basic structure validation
            if not table or not hasattr(table, 'activations'):
                return False
                
            # Must have some activations
            if not table.activations or len(table.activations) == 0:
                return False
                
            # Content validation - check for meaningful data
            has_meaningful_content = False
            for activation in table.activations:
                # Look for coordinate data or meaningful regions
                if (hasattr(activation, 'x') and activation.x is not None) or \
                   (hasattr(activation, 'region') and activation.region and
                    not activation.region.lower() in ['', 'empty', 'n/a', 'none']):
                    has_meaningful_content = True
                    break
                    
            if not has_meaningful_content:
                logger.debug("Table validation failed: no meaningful content found")
                return False
                
            # Context validation - ensure it's within article content
            if container:
                container_text = container.get_text().lower()
                # Reject if likely to be navigation/advertisement
                reject_indicators = ['advertisement', 'sponsored', 'related articles',
                                   'journal menu', 'issue contents', 'navigation']
                if any(indicator in container_text for indicator in reject_indicators):
                    logger.debug("Table validation failed: appears to be navigation/ads")
                    return False
                    
            return True
            
        except Exception as e:
            logger.debug(f"Table validation failed with exception: {e}")
            return False


class HighWireSource(Source):

    def parse_article(self, html, pmid=None, **kwargs):
        soup = super(HighWireSource, self).parse_article(html, pmid, **kwargs)
        if not soup:
            return False

        # To download tables, we need the content URL and the number of tables
        content_url = soup.find('meta', {
                                'name': 'citation_public_url'})['content']

        n_tables = len(soup.find_all('span', class_='table-label'))

        # Now download each table and parse it
        tables = []
        logger.info(f"Found {n_tables} tables.")
        for i in range(n_tables):
            t_num = i + 1
            url = '%s/T%d.expansion.html' % (content_url, t_num)
            table_soup = self._download_table(url)
            if not table_soup:
                continue
            tc = table_soup.find(class_='table-expansion')
            if tc:
                t = tc.find('table', {'id': 'table-%d' % (t_num)})
                t = self.parse_table(t)
                if t:
                    t.position = t_num
                    t.label = tc.find(class_='table-label').text
                    t.number = t.label.split(' ')[-1].strip()
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
        try:
            return soup.find('meta', {'name': 'citation_doi'})['content']
        except:
            return ''

    def extract_pmid(self, soup):
        return soup.find('meta', {'name': 'citation_pmid'})['content']



class OUPSource(Source):

    def parse_article(self, html, pmid=None, **kwargs):
        soup = super(OUPSource, self).parse_article(html, pmid, **kwargs)
        if not soup:
            return False

        # Extract tables
        tables = []

        # Exclude modal tables to prevent duplicates
        all_tables = set(soup.select('div.table-full-width-wrap'))
        modal_tables = set(soup.select('div.table-full-width-wrap.table-modal'))
        table_containers = all_tables - modal_tables
        logger.info(f"Found {len(table_containers)} tables.")
        for (i, tc) in enumerate(table_containers):
            table_html = tc.find('table')
            t = self.parse_table(table_html)
            if t:
                t.position = i + 1
                try:
                    t.number =  tc.find('span', class_='label').text.split(' ')[-1].strip()
                    t.label = tc.find('span', class_='label').text.strip()
                except:
                    pass
                try:
                    t.caption = tc.find('span', class_='caption').get_text()
                except:
                    pass
                try:
                    t.notes = tc.find('span', class_='fn').get_text()
                except:
                    pass
                tables.append(t)

        self.article.tables = tables
        return self.article

    def parse_table(self, table):
        return super(OUPSource, self).parse_table(table)

    def extract_doi(self, soup):
        try:
            return soup.find('meta', {'name': 'citation_doi'})['content']
        except:
            return ''

    def extract_pmid(self, soup):
        pmid = soup.find('meta', {'name': 'citation_pmid'})
        if pmid:
            return pmid['content']
        else:
            return None



class ScienceDirectSource(Source):

    def parse_article(self, html, pmid=None, **kwargs):
        soup = super(ScienceDirectSource, self).parse_article(html, pmid, **kwargs)
        if not soup:
            return False

        # Extract tables
        tables = []
        table_containers = soup.find_all('div', {'class': 'tables'})
        if len(table_containers) == 0:
            # try old method
            table_containers = soup.find_all('dl', {'class': 'table'})

        logger.info(f"Found {len(table_containers)} tables.")
        for (i, tc) in enumerate(table_containers):
            table_html = tc.find('table')
            t = self.parse_table(table_html)
            if t:
                t.position = i + 1
                try:
                    t.number = tc.find('span', class_='label').text.split(' ')[-1].strip() or tc['data-label'].split(' ')[-1].strip()
                    t.label = tc.find('span', class_='label').text.strip()
                except:
                    pass
                try:
                    t.caption = tc.find('p').contents[-1].strip()
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
        try:
            return list(soup.find('div', {'id': 'article-identifier-links'}).children)[0]['href'].replace('https://doi.org/', '')
        except:
            return ''

    def extract_pmid(self, soup):
        return scrape.get_pmid_from_doi(self.extract_doi(soup))


class PlosSource(Source):

    def parse_article(self, html, pmid=None, **kwargs):
        soup = super(PlosSource, self).parse_article(html, pmid, **kwargs)  # Do some preprocessing
        if not soup:
            return False

        # Extract tables
        tables = []
        table_containers = soup.find_all('table-wrap')
        logger.info(f"Found {len(table_containers)} tables.")
        for (i, tc) in enumerate(table_containers):
            table_html = tc.find('table')
            t = self.parse_table(table_html)
            if t:
                t.position = i + 1
                t.label = tc.find('label').text
                t.number = t.label.split(' ')[-1].strip()
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
        try:
            return soup.find('article-id', {'pub-id-type': 'doi'}).text
        except:
            return ''
        
    def extract_pmid(self, soup):
        return scrape.get_pmid_from_doi(self.extract_doi(soup))


class FrontiersSource(Source):

    def parse_article(self, html, pmid=None, **kwargs):

        soup = super(FrontiersSource, self).parse_article(html, pmid, **kwargs)
        if not soup:
            return False

        # Extract tables
        tables = []
        table_containers = soup.findAll(
            'table-wrap', {'id': re.compile('^T\d+$')})
        logger.info(f"Found {len(table_containers)} tables.")
        for (i, tc) in enumerate(table_containers):
            table_html = tc.find('table')
            t = self.parse_table(table_html)
            # If Table instance is returned, add other properties
            if t:
                t.position = i + 1
                t.number = tc['id'][1::].strip()
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
        try:
            return soup.find('article-id', {'pub-id-type': 'doi'}).text
        except:
            return ''

    def extract_pmid(self, soup):
        return scrape.get_pmid_from_doi(self.extract_doi(soup))


class JournalOfCognitiveNeuroscienceSource(Source):

    def parse_article(self, html, pmid=None, **kwargs):
        soup = super(
            JournalOfCognitiveNeuroscienceSource, self).parse_article(html, pmid, **kwargs)
        if not soup:
            return False

        # To download tables, we need the DOI and the number of tables
        doi = self.article.doi or self.extract_doi(soup)
        tables = []

        # Now download each table and parse it
        table_containers = soup.find_all('div', {'class': 'table-wrap'})
        logger.info(f"Found {len(table_containers)} tables.")
        for i, tc in enumerate(table_containers):
            table_html = tc.find('table', {'role': 'table'})
            if not table_html:
                continue

            t = self.parse_table(table_html)

            if t:
                t.position = i + 1
                s = re.search('T(\d+).+$', tc['content-id'])
                if s:
                    t.number = s.group(1)
                caption = tc.find('div', class_='caption')
                if caption:
                    t.label = caption.get_text()
                    t.caption = caption.get_text()
                try:
                    t.notes = tc.find('div', class_="fn").p.get_text()
                except:
                    pass
                tables.append(t)

        self.article.tables = tables
        return self.article

    def parse_table(self, table):
        return super(JournalOfCognitiveNeuroscienceSource, self).parse_table(table)

    def extract_doi(self, soup):
        try:
            return soup.find('meta', {'name': 'dc.Identifier', 'scheme': 'doi'})['content']
        except:
            return ''

    def extract_pmid(self, soup):
        return scrape.get_pmid_from_doi(self.extract_doi(soup))


class WileySource(Source):

    def parse_article(self, html, pmid=None, **kwargs):

        soup = super(WileySource, self).parse_article(html, pmid, **kwargs)  # Do some preprocessing
        if not soup:
            return False

        # Extract tables
        tables = []
        table_containers = soup.findAll('div', {
                                        'class': re.compile('article-table-content|table'), 'id': re.compile('^(.*?)\-tbl\-\d+$|^t(bl)*\d+$')})
        logger.info(f"Found {len(table_containers)} tables.")
        for (i, tc) in enumerate(table_containers):
            table_html = tc.find('table')
            footer = None
            try:
                # Remove footer, which appears inside table
                footer = table_html.tfoot.extract()
            except:
                pass
            t = self.parse_table(table_html)
            # If Table instance is returned, add other properties
            if t:
                t.position = i + 1
                # t.number = tc['id'][3::].strip()
                t.number = re.search('t[bl0\-]*(\d+)$', tc['id']).group(1)
                try:
                    t.label = tc.find('span', class_='label').get_text()
                except:
                    pass
                try:
                    t.caption = tc.find('caption').get_text()
                except AttributeError:
                    caption = tc.find('div', {'header': 'article-table-caption'})
                    t.caption = caption.get_text() if caption else None
                try:
                    t.notes = footer.get_text() if footer else None
                except AttributeError:
                    notes = tc.find('div', {'class': 'article-section__table-footnotes'})
                    t.notes = notes.get_text() if caption else None
                    pass
                tables.append(t)

        self.article.tables = tables
        return self.article

    def parse_table(self, table):
        return super(WileySource, self).parse_table(table)

    def extract_doi(self, soup):
        try:
            return soup.find('meta', {'name': 'citation_doi'})['content']
        except:
            return ''
    
    def extract_pmid(self, soup):
        return scrape.get_pmid_from_doi(self.extract_doi(soup))

# Note: the SageSource is largely useless and untested because Sage renders tables
# as images.


class SageSource(Source):

    def parse_article(self, html, pmid=None, **kwargs):

        soup = super(SageSource, self).parse_article(html, pmid, **kwargs)
        if not soup:
            return False

        # To download tables, we need the content URL and the number of tables
        content_url = soup.find('meta', {
                                'name': 'citation_public_url'})['content']

        n_tables = len(soup.find_all('span', class_='table-label'))
        logger.info(f"Found {n_tables} tables.")
        # Now download each table and parse it
        tables = []
        for i in range(n_tables):
            t_num = i + 1
            url = '%s/T%d.expansion.html' % (content_url, t_num)
            table_soup = self._download_table(url)
            if not table_soup:
                continue
            tc = table_soup.find(class_='table-expansion')
            if tc:
                t = tc.find('table', {'id': 'table-%d' % (t_num)})
                t = self.parse_table(t)
                if t:
                    t.position = t_num
                    t.label = tc.find(class_='table-label').text
                    t.number = t.label.split(' ')[-1].strip()
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
        try:
            return soup.find('meta', {'name': 'citation_doi'})['content']
        except: 
            return ''
        
    def extract_pmid(self, soup):
        return soup.find('meta', {'name': 'citation_pmid'})['content']


class OldSpringerSource(Source):

    def parse_article(self, html, pmid=None, **kwargs):

        soup = super(OldSpringerSource, self).parse_article(html, pmid, **kwargs)
        if not soup:
            return False

        # Extract tables
        tables = []
        table_containers = soup.findAll(
            'figure', {'id': re.compile('^Tab\d+$')})
        for (i, tc) in enumerate(table_containers):
            table_html = tc.find('table')
            t = self.parse_table(table_html)
            # If Table instance is returned, add other properties
            if t:
                t.position = i + 1
                t.number = tc['id'][3::].strip()
                t.label = tc.find('span', class_='CaptionNumber').get_text()
                try:
                    t.caption = tc.find(class_='CaptionContent').p.get_text()
                except:
                    pass
                try:
                    t.notes = tc.find(class_='TableFooter').p.get_text()
                except:
                    pass
                tables.append(t)

        self.article.tables = tables
        return self.article

    def parse_table(self, table):
        return super(OldSpringerSource, self).parse_table(table)

    def extract_doi(self, soup):
        content = soup.find('p', class_='ArticleDOI').get_text()
        return content.split(' ')[1]

    def extract_pmid(self, soup):
        return scrape.get_pmid_from_doi(self.extract_doi(soup))


class SpringerSource(Source):

    def parse_article(self, html, pmid=None, **kwargs):

        soup = super(SpringerSource, self).parse_article(html, pmid, **kwargs)
        if not soup:
            return False

        # Extract table; going to take the approach of opening and parsing the table via links
        # To download tables, we need the content URL and the number of tables
        content_url = soup.find('meta', {'name': 'citation_fulltext_html_url'})['content']

        n_tables = len(soup.find_all('span', string='Full size table'))
        logger.info(f"Found {n_tables} tables.")
        # Now download each table and parse it
        tables = []
        for i in range(n_tables):
            t_num = i + 1
            url = '%s/tables/%d' % (content_url, t_num)
            table_soup = self._download_table(url)
            if not table_soup:
                continue
            tc = table_soup.find(class_='data last-table')
            t = self.parse_table(tc)
            if t:
                t.position = t_num

                # id_name is the id HTML element that cotains the title, label and table number that needs to be parse
                # temp_title sets it up to where the title can be parsed and then categorized
                id_name = f"table-{t_num}-title"
                temp_title = table_soup.find('h1', attrs={'id': id_name}).get_text().split()

                # grabbing the first two elements for the label and then making them a string object
                t.label = " ".join(temp_title[:2])
                t.number = str(temp_title[1])
                try:
                    # grabbing the rest of the element for the caption/title of the table and then making them a string object
                    t.caption =  " ".join(temp_title[2:])
                except:
                    pass
                try:
                    t.notes = table_soup.find(class_='c-article-table-footer').get_text()
                except:
                    pass
                tables.append(t)

        self.article.tables = tables
        return self.article

    def parse_table(self, table):
        return super(SpringerSource, self).parse_table(table)

    def extract_doi(self, soup):
        try: 
            return soup.find('meta', attrs={'name': "citation_doi"})['content']
        except:
            return ''
  
    def extract_pmid(self, soup):
        return scrape.get_pmid_from_doi(self.extract_doi(soup))


class PMCSource(Source):
    def parse_article(self, html, pmid=None, **kwargs):
        soup = super(PMCSource, self).parse_article(html, pmid, **kwargs)
        if not soup:
            return False

        tables = []
        table_containers = soup.findAll('div', {'class': 'table-wrap'})
        logger.info(f"Found {len(table_containers)} tables.")
        for (i, tc) in enumerate(table_containers):
            sub_tables = tc.findAll('div', {'class': 'xtable'})
            for st in sub_tables:
                t = self.parse_table(st)
                if t:
                    t.position = i + 1
                    t.label = tc.find('h3').text if tc.find('h3') else None
                    t.number = t.label.split(' ')[-1].strip() if t.label else None
                    try:
                        t.caption = tc.find({"div": {"class": "caption"}}).text
                    except:
                        pass
                    try:
                        t.notes = tc.find('div', class_='tblwrap-foot').text
                    except:
                        pass
                    tables.append(t)

        self.article.tables = tables
        return self.article

    def extract_pmid(self, soup):
        return soup.find('meta', {'name': 'citation_pmid'})['content']

    def extract_doi(self, soup):
        return soup.find('meta', {'name': 'citation_doi'})['content']
