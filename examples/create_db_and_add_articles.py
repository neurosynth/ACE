# In this example we create a new DB file and process a bunch of
# articles. Note that due to copyright restrictions, articles can't
# be included in this package, so you'll need to replace PATH_TO_FILES
# with something that works.

import ace
from ace import database

# Uncomment the next line to seem more information
# ace.set_logging_level('info')

# Change this to a valid path to a set of html files.
PATH_TO_FILES = "/home/zorro/neurosynth_scrape/articles/html/Neuroimage/*"

db = database.Database(adapter='sqlite', db_name='sqlite:///example_db.db')
db.add_articles(PATH_TO_FILES, pmid_filenames=True)
db.print_stats()
