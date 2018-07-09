""" Tools for evaluating the quality of extracted coordinates. """

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

def _read_database(database):
    ''' For convenience; loads pandas DF from tsv if needed. '''
    if isinstance(database, basestring):
        database = pd.read_csv(database, sep='\t')
    return database

def plot_xyz_histogram(database, bins=50):
    ''' Takes a database file as input and plots histograms for X/Y/Z coords.
    '''
    data = _read_database(database)
    data[['x', 'y', 'z']].hist(bins=bins)
    plt.show()


def proportion_integer_values(database):
    ''' Reports the proportion of integer values in X/Y/Z columns of database
    file. This should generally be close to 1--typically around 0.98 or so if
    everything is working properly. '''
    data = _read_database(database)
    return data[['x', 'y', 'z']].apply(lambda x: np.mean(x == x.round()))
