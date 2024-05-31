import os
import requests
import random

from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

from ace.config import USER_AGENTS

class PubMedAPI:
    def __init__(self, api_key=None):
        if api_key is None:
            # Look for api key in environment variable
            api_key = os.environ.get('PUBMED_API_KEY')
        self.api_key = api_key
        self.base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
        self.headers = {'User-Agent': random.choice(USER_AGENTS)}

        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504, 400])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))


    def get(self, util, params=None, return_content=True):
        url = f"{self.base_url}/{util}.fcgi"
        if self.api_key:
            params['api_key'] = self.api_key
            
        response = self.session.get(url, params=params, headers=self.headers, timeout=10)

        if response.status_code != 200:
            raise Exception(f"PubMed API returned status code {response.status_code} for {url}")

        if return_content:
            response = response.content

        return response
        
    def esearch(self, query, retstart=None, retmax=10000, extract_ids=True, **kwargs):
        params = {
            "db": "pubmed",
            "term": query,
            "retmax": str(retmax),
        }
        if retstart is not None:
            params["retstart"] = str(retstart)
            
        response = self.get("esearch", params=params, **kwargs)
        if extract_ids:
            soup = BeautifulSoup(response)
            response = [t.string for t in soup.find_all('id')]
        return response
    
    def efetch(self, input_id, retmode='txt', rettype='medline', db = 'pubmed', **kwargs):
        params = {
            "db": db,
            "id": input_id,
            "retmode": retmode,
            "rettype": rettype
        }
        
        
        response = self.get("efetch", params=params, **kwargs)
        return response
    
    def elink(self, pmid, retmode='ref', access_db = 'pubmed', **kwargs):
        params = {
            "dbfrom": "pubmed",
            "id": pmid,
            "retmode": retmode
        }
        if access_db == "pmc":
            params["linkname"] = "pubmed_pmc"
        else:
            params["cmd"] = "prlinks"
        
        response = self.get("elink", params=params, **kwargs)
        return response
