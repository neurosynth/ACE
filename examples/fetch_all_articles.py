""" Query PubMed for results from several journals, and save to file.
The resulting directory can then be passed to the Database instance for 
extraction, as in the create_db_and_add_articles example.
NOTE: selenium must be installed and working properly for this to work. 
Code has only been tested with the Chrome driver. """

from ace.scrape import *
import ace
import os


all_journals = ['The Journal of neuroscience : the official journal of the Society for Neuroscience',
       'Journal of neurophysiology',
       'Proceedings of the National Academy of Sciences of the United States of America',
       'Neuron', 'Brain research. Cognitive brain research',
       'Journal of neurology, neurosurgery, and psychiatry',
       'Cerebral cortex (New York, N.Y. : 1991)', 'Brain research',
       'Neuropsychologia', 'Schizophrenia research',
       'Biological psychiatry', 'Brain : a journal of neurology',
       'Neuroscience letters', 'Pain', 'Human brain mapping',
       'The European journal of neuroscience',
       'Clinical neurophysiology : official journal of the International Federation of Clinical Neurophysiology',
       'Psychiatry research', 'Vision research', 'Neuroscience research',
       'Journal of affective disorders', 'Epilepsia',
       'Journal of neuroscience methods', 'Behavioural brain research',
       'Brain and language', 'NeuroImage', 'Brain and cognition',
       'Journal of the neurological sciences',
       'Experimental brain research',
       'International journal of psychophysiology : official journal of the International Organization of Psychophysiology',
       'Brain research. Brain research reviews', 'Psychopharmacology',
       'Drug and alcohol dependence', 'Neurobiology of aging',
       'Biological psychology', 'Progress in brain research',
       'Journal of autism and developmental disorders',
       'European neuropsychopharmacology : the journal of the European College of Neuropsychopharmacology',
       'European archives of psychiatry and clinical neuroscience',
       'PloS one', 'Social cognitive and affective neuroscience',
       'Brain structure & function',
       'Neurophysiologie clinique = Clinical neurophysiology',
       'Journal of neuroimaging : official journal of the American Society of Neuroimaging',
       'Cortex; a journal devoted to the study of the nervous system and behavior',
       'Journal of cognitive neuroscience', 'Brain topography',
       'Brain imaging and behavior', 'Frontiers in human neuroscience',
       'Frontiers in systems neuroscience', 'Frontiers in neuroscience',
       'Cognitive, affective & behavioral neuroscience',
       'Frontiers in behavioral neuroscience', 'Brain and behavior',
       'Developmental cognitive neuroscience', 'Hormones and behavior',
       'NeuroImage. Clinical', 'Aging clinical and experimental research',
       'Psychiatry research. Neuroimaging',
       'Schizophrenia research. Cognition']

journals = {
    'Neuroimage': {
        'delay': 20,  # Mean delay between article downloads--prevents the banhammer
        'mode': 'browser',  # ScienceDirect journals require selenium to work properly
        'search': 'fmri',  # Only retrieve articles with this string in abstract
        'min_pmid': 29845006   # Start from this PMID--can run incrementally
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