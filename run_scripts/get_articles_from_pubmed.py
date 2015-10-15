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

attrib_dict_oa = {}
attrib_dict_oa['search'] = search_query
attrib_dict_oa['mode'] = 'direct'
attrib_dict_oa['delay'] = 3
attrib_dict_oa['limit'] = 100000
attrib_dict_oa['search_db'] = 'pubmed'

oa_journal_list = ['Frontiers in Behavioral Neuroscience', 'Frontiers in Cellular Neuroscience', 'Frontiers in Computational Neuroscience',
'Frontiers in Human Neuroscience', 'Frontiers in Integrative Neuroscience', 'Frontiers in Molecular Neuroscience',
'Frontiers in Molecular Neuroscience', 'Frontiers in Neural Circuits', 'Frontiers in Neuroanatomy', 'Frontiers in Neuroscience',
'Frontiers in Systems Neuroscience', 'PLoS ONE', 'PLoS Biol', 'PLoS Comput Biol']

# non_oa_journal_list = ['Brain Research', 'Neuroscience', 'Neurobiol Dis', 'Neuroscience Letters', 'Eur J Neurosci',
# 'J Neurosci', 'J Physiol', 'J Neurophysiol', 'Cereb Cortex', 'Glia', 'Hippocampus', 'J Comp Neurol', 
# 'J Neurosci Res', 'Biochem Biophys Res Commun', 'Synapse', 'Biochim Biophys Acta']

# attrib_dict_non_oa = attrib_dict_oa
# attrib_dict_non_oa['mode'] = 'browser'
# attrib_dict_non_oa['delay'] = 30

def create_journal_dict(journal_list, attrib_dict):
    journal_dict = {}
    for j in journal_list:
        journal_dict[j] = attrib_dict
    return journal_dict

journals = create_journal_dict(oa_journal_list, attrib_dict_oa)

# Verbose output
ace.set_logging_level('debug')

#save_dir = "E:\downloaded"
save_dir = '/home/stripathy/downloaded_html'
output_dir = save_dir

# Create temporary output dir
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Initialize Scraper
scraper = Scraper(output_dir)

# Loop through journals and 
for j, settings in journals.items():
    scraper.retrieve_journal_articles(j, **settings)



