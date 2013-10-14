# In this example we create a new DB file and process a bunch of
# articles. Note that due to copyright restrictions, articles can't
# be included in this package, so you'll need to replace PATH_TO_FILES
# with something that works.

import ace
from ace import database

# Uncomment the next line to seem more information
ace.set_logging_level('info')

# Change this to a valid path to a set of html files.
PATH_TO_FILES = "/Users/tal/tmp/html/*.html"

db = database.Database('example_db.sqlite')
db.add_articles(PATH_TO_FILES)
db.print_stats()
