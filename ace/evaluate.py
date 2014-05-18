""" Tools for evaluating the quality of extracted coordinates. """

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

def plot_xyz_histogram(database, bins=50):
	''' Takes a database file as input and plots histograms for X/Y/Z coords. '''
	data = pd.read_csv(database,sep='\t')
	data[['x','y','z']].hist(bins=bins)
	plt.show()


def proportion_integer_values(database):
	''' Reports the proportion of integer values in X/Y/Z columns of database file. 
	This should generally be close to 0--typically around 0.02 or so if everything 
	is working properly. '''
	data = pd.read_csv(database,sep='\t')

	print 1 - data[['x','y','z']].apply(lambda x: np.mean(x == x.round()))
