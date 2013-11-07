# from nltk import *
import re
from collections import Counter

class Labeler:
	''' Labels articles with features. '''

	def __init__(self):
		pass

	def extract_word_features(self, text, pattern=r'[^\sa-z\-]+', normalize=None):
		''' Takes text from an article as input and returns a dict of 
		features --> weights.
		'''
		patt = re.compile(pattern)
		clean = patt.sub('', text.lower())
		tokens = re.split('\s+', clean)
		n_words = len(tokens)
		
