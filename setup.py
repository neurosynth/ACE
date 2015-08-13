import os
import sys

# Borrowing a trick from nibabel
if len(set(('test', 'easy_install', 'develop')).intersection(sys.argv)) > 0:
    import setuptools

from distutils.core import setup

extra_setuptools_args = {}
if 'setuptools' in sys.modules:
    extra_setuptools_args = dict(
        tests_require=['nose'],
        test_suite='nose.collector',
        extras_require=dict(
            test='nose>=0.10.1')
    )

# fetch version from within ACE module
with open(os.path.join('ace', 'version.py')) as f:
    exec(f.read())

setup(name="ace",
      version=__version__,
      description="Automated Coordinate Extraction",
      maintainer='Tal Yarkoni',
      maintainer_email='tyarkoni@gmail.com',
      url='http://github.com/neurosynth/ace',
      packages=["ace",
                  "ace.tests"],
      package_data={'ace': ['sources/*'],
                    'ace.tests': ['data/*']
                    },
      **extra_setuptools_args
      )
