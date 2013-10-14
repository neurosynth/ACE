# Miscellaneous methods for extracting information from text/html

import bs4 as BeautifulSoup
import re


def guess_space(text):
    ''' Take article text as input and return a guess about the image space. '''

    targets = ['mni', 'talairach', 'afni', 'flirt',
               '711-2', 'spm', 'brainvoyager', 'fsl']
    n_targ = len(targets)
    text = text.lower()
    res = [0] * n_targ
    matches = []
    for i in range(n_targ):
        res[i] = len(re.findall(
            r'\b(.{30,40}\b%s.{30,40})\b' % targets[i], text))

    # Sum up diagnostic strings...
    mni = res[5] + res[7]
    t88 = res[2] + res[6]
    software = mni + t88

# Assign label
# 1. If only one of MNI or T88 is implied, classify as that
    if (mni and not t88) or (not software and res[0] and not res[1]):
        label = 'MNI'
    elif (t88 and not mni) or (not software and res[1] and not res[0]):
        label = 'TAL'
    else:
        label = 'UNKNOWN'

    return label
