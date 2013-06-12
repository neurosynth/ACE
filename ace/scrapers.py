import re
import requests
from time import sleep
import config

def get_url(url, delay=0.5, verbose=False):
	if verbose:
		print "\nRetrieving %s..." % url
	headers = { 'User-Agent': config.USER_AGENT_STRING }
	sleep(delay)
	r = requests.get(url, headers=headers)
	return r.text