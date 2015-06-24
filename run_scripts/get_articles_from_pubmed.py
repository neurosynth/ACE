""" Query PubMed for results from several journals, and save to file.
The resulting directory can then be passed to the Database instance for 
extraction, as in the create_db_and_add_articles example.
NOTE: selenium must be installed and working properly for this to work. 
Code has only been tested with the Chrome driver. """

from ace.scrape import *
import ace
import os


search_query = '(cell OR neuron OR glia OR astrocyte OR channel OR neurotransmitter OR "LTP" OR "synaptic plasticity" OR physiology OR biophysics) AND ("1997/01/01"[PDAT] : "2015/12/31"[PDAT])'
pmc_query = '((neuron electrophysiology) OR (neurophysiology) OR ("input resistance") OR ("resting potential" OR "resting membrane potential") OR "LTP" OR "synaptic plasticity" OR "LTD")',
search_ret_limit = 100000
get_delay = 10
journals = {
    'Brain Research': {
        'delay': get_delay,
        'mode': 'browser',
        'search': search_query,
        'min_pmid': None,
        'limit': search_ret_limit,  # We can limit to only N new articles
        'search_db': 'pubmed',
    },
    'Neuroscience': {
        'delay': get_delay,
        'mode': 'browser',
        'search': search_query,
        'min_pmid': None,
        'limit': search_ret_limit,  # We can limit to only N new articles
        'search_db': 'pubmed',
    },
    'Neurobiol Dis': {
        'delay': get_delay,
        'mode': 'browser',
        'search': search_query,
        'min_pmid': None,
        'limit': search_ret_limit,  # We can limit to only N new articles
        'search_db': 'pubmed',
    },
    'Neuroscience Letters': {
        'delay': get_delay,
        'mode': 'browser',
        'search': search_query,
        'min_pmid': None,
        'limit': search_ret_limit,  # We can limit to only N new articles
        'search_db': 'pubmed',
    },
    'Eur J Neurosci': {
        'delay': get_delay,
        'mode': 'browser',
        'search': search_query,
        'min_pmid': None,
        'limit': search_ret_limit,  # We can limit to only N new articles
        'search_db': 'pubmed',
    },
    'J Neurosci': {
        'delay': get_delay,
        'mode': 'browser',
        'search': search_query,
        'min_pmid': None,
        'limit': search_ret_limit,  # We can limit to only N new articles
        'search_db': 'pubmed',
    },
    'J Physiol': {
        'delay': get_delay,
        'mode': 'browser',
        'search': search_query,
        'min_pmid': None,
        'limit': search_ret_limit,  # We can limit to only N new articles
        'search_db': 'pubmed',
    },
    'J Neurophysiol': {
        'delay': get_delay,
        'mode': 'browser',
        'search': search_query,
        'min_pmid': None,
        'limit': search_ret_limit,  # We can limit to only N new articles
        'search_db': 'pubmed',
    },
    'Cereb Cortex': {
        'delay': get_delay,
        'mode': 'browser',
        'search': search_query,
        'min_pmid': None,
        'limit': search_ret_limit,  # We can limit to only N new articles
        'search_db': 'pubmed',
    },
    'Glia': {
        'delay': get_delay,
        'mode': 'browser',
        'search': search_query,
        'min_pmid': None,
        'limit': search_ret_limit,  # We can limit to only N new articles
        'search_db': 'pubmed',
    },
    'Hippocampus': {
        'delay': get_delay,
        'mode': 'browser',
        'search': search_query,
        'min_pmid': None,
        'limit': search_ret_limit,  # We can limit to only N new articles
        'search_db': 'pubmed',
    },
    'Neuropsychopharmacology': {
        'delay': get_delay,
        'mode': 'browser',
        'search': search_query,
        'min_pmid': None,
        'limit': search_ret_limit,  # We can limit to only N new articles
        'search_db': 'pubmed',
    },
    'J Comp Neurol': {
        'delay': get_delay,
        'mode': 'browser',
        'search': search_query,
        'min_pmid': None,
        'limit': search_ret_limit,  # We can limit to only N new articles
        'search_db': 'pubmed',
    },
    'J Neurosci Res': {
        'delay': get_delay,
        'mode': 'browser',
        'search': search_query,
        'min_pmid': None,
        'limit': search_ret_limit,  # We can limit to only N new articles
        'search_db': 'pubmed',
    },
    # 'Biochem Biophys Res Commun': {
        # 'delay': get_delay,
        # 'mode': 'browser',
        # 'search': search_query,
        # 'min_pmid': None,
        # 'limit': search_ret_limit,  # We can limit to only N new articles
        # 'search_db': 'pubmed',
    # },
    # 'Proc Natl Acad Sci U S A': {
        # 'delay': get_delay,
        # 'mode': 'browser',
        # 'search': search_query,
        # 'min_pmid': None,
        # 'limit': search_ret_limit,  # We can limit to only N new articles
        # 'search_db': 'pubmed',
    # },
    'Synapse': {
        'delay': get_delay,
        'mode': 'browser',
        'search': search_query,
        'min_pmid': None,
        'limit': search_ret_limit,  # We can limit to only N new articles
        'search_db': 'pubmed',
    },
    'Biochim Biophys Acta': {
        'delay': get_delay,
        'mode': 'browser',
        'search': search_query,
        'min_pmid': None,
        'limit': search_ret_limit,  # We can limit to only N new articles
        'search_db': 'pubmed',
    },
}


# journals = {
#         'Frontiers in Behavioral Neuroscience': {
#         'delay': 10,
#         #'search':'neuron',
#         'search': '((neuron electrophysiology) OR (neurophysiology) OR ("input resistance") OR ("resting potential" OR "resting membrane potential") OR "LTP" OR "synaptic plasticity" OR "LTD")',
#         'mode': 'direct',  # PLoS sends nice usable XML directly
#         'min_pmid': None,
#         'limit': 20000,  # We can limit to only N new articles
#         'search_db': 'pmc'
#     },
    # 'Neuroimage': {
    #     'delay': 20,  # Mean delay between article downloads--prevents the banhammer
    #     'mode': 'browser',  # ScienceDirect journals require selenium to work properly
    #     'search': 'fmri',  # Only retrieve articles with this string in abstract
    #     'min_pmid': 20000000   # Start from this PMID--can run incrementally
    # },

    # 'PLoS ONE': {
    #     'delay': 10,
    #     'search': 'fmri',
    #     'mode': 'direct',  # PLoS sends nice usable XML directly
    #     'min_pmid': None,
    #     'limit': 5  # We can limit to only N new articles
    # },
#         'Frontiers in Cellular Neuroscience': {
#         'delay': 10,
#         #'search':'neuron',
#         'search': '((neuron electrophysiology) OR (neurophysiology) OR ("input resistance") OR ("resting potential" OR "resting membrane potential") OR "LTP" OR "synaptic plasticity" OR "LTD")',
#         'mode': 'direct',  # PLoS sends nice usable XML directly
#         'min_pmid': None,
#         'limit': 20000,  # We can limit to only N new articles
#         'search_db': 'pmc'
#     },
#         'Frontiers in Computational Neuroscience': {
#         'delay': 10,
#         #'search':'neuron',
#         'search': '((neuron electrophysiology) OR (neurophysiology) OR ("input resistance") OR ("resting potential" OR "resting membrane potential") OR "LTP" OR "synaptic plasticity" OR "LTD")',
#         'mode': 'direct',  # PLoS sends nice usable XML directly
#         'min_pmid': None,
#         'limit': 20000,  # We can limit to only N new articles
#         'search_db': 'pmc'
#     },
#         'Frontiers in Human Neuroscience': {
#         'delay': 10,
#         #'search':'neuron',
#         'search': '((neuron electrophysiology) OR (neurophysiology) OR ("input resistance") OR ("resting potential" OR "resting membrane potential") OR "LTP" OR "synaptic plasticity" OR "LTD")',
#         'mode': 'direct',  # PLoS sends nice usable XML directly
#         'min_pmid': None,
#         'limit': 20000,  # We can limit to only N new articles
#         'search_db': 'pmc'
#     },
#         'Frontiers in Integrative Neuroscience': {
#         'delay': 10,
#         #'search':'neuron',
#         'search': '((neuron electrophysiology) OR (neurophysiology) OR ("input resistance") OR ("resting potential" OR "resting membrane potential") OR "LTP" OR "synaptic plasticity" OR "LTD")',
#         'mode': 'direct',  # PLoS sends nice usable XML directly
#         'min_pmid': None,
#         'limit': 20000,  # We can limit to only N new articles
#         'search_db': 'pmc'
#     },
#         'Frontiers in Molecular Neuroscience': {
#         'delay': 10,
#         #'search':'neuron',
#         'search': '((neuron electrophysiology) OR (neurophysiology) OR ("input resistance") OR ("resting potential" OR "resting membrane potential") OR "LTP" OR "synaptic plasticity" OR "LTD")',
#         'mode': 'direct',  # PLoS sends nice usable XML directly
#         'min_pmid': None,
#         'limit': 20000,  # We can limit to only N new articles
#         'search_db': 'pmc'
#     },
#         'Frontiers in Neural Circuits': {
#         'delay': 10,
#         #'search':'neuron',
#         'search': '((neuron electrophysiology) OR (neurophysiology) OR ("input resistance") OR ("resting potential" OR "resting membrane potential") OR "LTP" OR "synaptic plasticity" OR "LTD")',
#         'mode': 'direct',  # PLoS sends nice usable XML directly
#         'min_pmid': None,
#         'limit': 20000,  # We can limit to only N new articles
#         'search_db': 'pmc'
#     },
#         'Frontiers in Neuroanatomy': {
#         'delay': 10,
#         #'search':'neuron',
#         'search': '((neuron electrophysiology) OR (neurophysiology) OR ("input resistance") OR ("resting potential" OR "resting membrane potential") OR "LTP" OR "synaptic plasticity" OR "LTD")',
#         'mode': 'direct',  # PLoS sends nice usable XML directly
#         'min_pmid': None,
#         'limit': 20000,  # We can limit to only N new articles
#         'search_db': 'pmc'
#     },
#         'Frontiers in Neuroscience': {
#         'delay': 10,
#         #'search':'neuron',
#         'search': '((neuron electrophysiology) OR (neurophysiology) OR ("input resistance") OR ("resting potential" OR "resting membrane potential") OR "LTP" OR "synaptic plasticity" OR "LTD")',
#         'mode': 'direct',  # PLoS sends nice usable XML directly
#         'min_pmid': None,
#         'limit': 20000,  # We can limit to only N new articles
#         'search_db': 'pmc'
#     },
#         'Frontiers in Systems Neuroscience': {
#         'delay': 10,
#         #'search':'neuron',
#         'search': '((neuron electrophysiology) OR (neurophysiology) OR ("input resistance") OR ("resting potential" OR "resting membrane potential") OR "LTP" OR "synaptic plasticity" OR "LTD")',
#         'mode': 'direct',  # PLoS sends nice usable XML directly
#         'min_pmid': None,
#         'limit': 20000,  # We can limit to only N new articles
#         'search_db': 'pmc'
#     },

# }

# Verbose output
ace.set_logging_level('debug')

save_dir = "E:\downloaded"
#save_dir = '/home/stripathy/downloaded_html'
output_dir = save_dir

# Create temporary output dir
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Initialize Scraper
scraper = Scraper(output_dir)

# Loop through journals and 
for j, settings in journals.items():
    scraper.retrieve_journal_articles(j, **settings)



