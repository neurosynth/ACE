import re
import requests
from time import sleep
import config
import simplejson as json
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger('ace')


def get_url(url, delay=0.0, verbose=False):
	headers = { 'User-Agent': config.USER_AGENT_STRING }
	sleep(delay)
	r = requests.get(url, headers=headers)
	return r.text


def get_pmid_from_doi(doi):
	''' Query PubMed for the PMID of a paper based on its doi. We need this 
	for some Sources that don't contain the PMID anywhere in the artice HTML.
	'''
	data = get_url('http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=%s[aid]' % doi)
	pmid = re.search('\<Id\>(\d+)\<\/Id\>', data).group(1)
	return pmid


def get_pubmed_metadata(pmid):
	''' Get metadata for article from PubMed '''
	logger.info("Retrieving metadata for PubMed article %s..." % str(pmid))
	text = get_url('http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=%s&retmode=text&rettype=medline' % pmid)
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
		if  'AID' in data:
			doi = filter(lambda x: 'doi' in x, data['AID'].split('; '))[0].split(' ')[0]
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