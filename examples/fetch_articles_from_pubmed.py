""" Query PubMed for results from several journals, and save to file.
The resulting directory can then be passed to the Database instance for 
extraction, as in the create_db_and_add_articles example.
NOTE: selenium must be installed and working properly for this to work. 
Code has only been tested with the Chrome driver. """

from ace.scrape import *
import ace
import os


journals = {
    'Neuroimage': {
        'delay': 20,  # Mean delay between article downloads--prevents the banhammer
        'mode': 'browser',  # ScienceDirect journals require selenium to work properly
        'search': 'fmri',  # Only retrieve articles with this string in abstract
        'min_pmid': 20000000   # Start from this PMID--can run incrementally
    },

    'PLoS ONE': {
        'delay': 10,
        'search': 'fmri',
        'mode': 'direct',  # PLoS sends nice usable XML directly
        'min_pmid': None
    },
    'Journal of Neuroscience': {
        'delay': 20,
        'mode': 'browser',
        'search': 'fmri',
        'min_pmid': None,
        'limit': 100  # We can limit to only N new articles
    }
}

# Verbose output
ace.set_logging_level('debug')

# Create temporary output dir
output_dir = '/tmp/articles'
if not os.path.exists(output_dir):
	os.makedirs(output_dir)

# Initialize Scraper
scraper = Scraper('/tmp/articles')

# Loop through journals and 
for j, settings in list(journals.items()):
    scraper.retrieve_journal_articles(j, **settings)



