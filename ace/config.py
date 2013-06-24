''' GLOBAL SETTINGS '''

# When True, all Exceptions will be suppressed. When False, Exception 
# messages will be printed out.
SILENT_ERRORS = False


''' DATABASE SETTINGS '''
# The filename to save the SQLite database to.
DATABASE_FILE = 'ACE_db.sqlite'


''' SOURCE PROCESSING SETTINGS '''

# If True, will exercise greater care when parsing (e.g., when estimating 
# number of columns in table, will check every row in the table and take the 
# max instead of just checking the first row). This is generally desirable,
# but will result in slower processing.
CAREFUL_PARSING = True

# Sometimes tables have rows that can't be processed--usually because of malformed
# HTML or XML (e.g., failure to close a <td> tag). Such problems will always be 
# logged, but if IGNORE_BAD_ROWS is True, the row will be skipped and execution
# will continue gracefully. When False, any errors will be re-raised,
# terminating execution.
IGNORE_BAD_ROWS = True

# Whether or not to ignore tables that appear to be missing a label for at 
# least one column. This doesn't happen much, and in practice most tables with 
# missing labels appear to genuinely have empty columns that are ignored
# anyway, so this should be left off unless problems arise.
EXCLUDE_TABLES_WITH_MISSING_LABELS = False




''' SCRAPING/PARSING SETTINGS '''

USER_AGENT_STRING = 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1464.0 Safari/537.36'