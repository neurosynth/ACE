''' GLOBAL SETTINGS '''

# When True, all Exceptions will be suppressed. When False, Exception 
# messages will be printed out.
SILENT_ERRORS = False


''' DATABASE SETTINGS '''
# Adapter to use--either 'mysql' or 'sqlite'
SQL_ADAPTER = 'mysql'

# SQLite path (when using sqlite adapter)
SQLITE_URI = 'sqlite:///ace.db'

# MySQL configuration
MYSQL_USER = 'ace'
MYSQL_PASSWORD = 'CHANGEME'
MYSQL_DB = 'ace_test'

# When True, any processed articles will be saved to DB, whether or not they 
# contain any extracted activations. When False, only articles from which 
# at least one activation was extracted will be saved. Note that if this is set
# to False, processing will be much slower, since every article not already in
# the DB will be parsed, even if it contains no activations and has been
# previously processed.
SAVE_ARTICLES_WITHOUT_ACTIVATIONS = True

# By default, ACE will ignore any articles that already exist in the DB 
# when processing new HTML files. If OVERWRITE is set to True, ACE will 
# always overwrite existing records. This is useful when the extraction 
# code has improved substantially and you want to re-extract all data, 
# but should otherwise be left off for the sake of efficiency.
OVERWRITE_EXISTING_ROWS = False


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

# Whether to use readability.py for HTML cleaning when available.
# When False, will use fallback HTML processing by default.
USE_READABILITY = True

# Whether to save the original HTML of the table in the Table object
SAVE_ORIGINAL_HTML = False


''' SCRAPING/PARSING SETTINGS '''
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.155 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.155 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.155 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.122 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.201 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.78 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.78 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.118 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.201 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.78 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.91 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.118 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.91 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.207 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.60 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.122 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.60 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.122 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.86 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.86 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.58 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.58 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.128 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.207 Safari/537.36',  # noqa: E501
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.60 Safari/537.36'  # noqa: E501
]


class ConfigManager:
    """Manages runtime configuration settings for ACE"""
    
    _defaults = {}
    _overrides = {}
    
    def __init__(self):
        # Capture initial defaults
        self._defaults = {
            'SILENT_ERRORS': SILENT_ERRORS,
            'SQL_ADAPTER': SQL_ADAPTER,
            'SQLITE_URI': SQLITE_URI,
            'MYSQL_USER': MYSQL_USER,
            'MYSQL_PASSWORD': MYSQL_PASSWORD,
            'MYSQL_DB': MYSQL_DB,
            'SAVE_ARTICLES_WITHOUT_ACTIVATIONS': 
                SAVE_ARTICLES_WITHOUT_ACTIVATIONS,
            'OVERWRITE_EXISTING_ROWS': OVERWRITE_EXISTING_ROWS,
            'CAREFUL_PARSING': CAREFUL_PARSING,
            'IGNORE_BAD_ROWS': IGNORE_BAD_ROWS,
            'EXCLUDE_TABLES_WITH_MISSING_LABELS': 
                EXCLUDE_TABLES_WITH_MISSING_LABELS,
            'USE_READABILITY': USE_READABILITY,
            'SAVE_ORIGINAL_HTML': SAVE_ORIGINAL_HTML
        }
    
    def update(self, **kwargs):
        """Update configuration settings at runtime
        
        Args:
            **kwargs: Key-value pairs of configuration settings to update
        """
        valid_keys = self._defaults.keys()
        for key, value in kwargs.items():
            if key in valid_keys:
                self._overrides[key] = value
            else:
                raise ValueError(f"Invalid config key: {key}")
    
    def reset(self, key=None):
        """Reset configuration to default values
        
        Args:
            key (str): Specific key to reset (reset all if None)
        """
        if key:
            if key in self._overrides:
                del self._overrides[key]
        else:
            self._overrides = {}
    
    def __getattr__(self, name):
        if name in self._overrides:
            return self._overrides[name]
        elif name in self._defaults:
            return self._defaults[name]
        else:
            raise AttributeError(f"Config setting {name} does not exist")


# Create global config manager instance
config_manager = ConfigManager()


# Proxy functions for easier access
def update_config(**kwargs):
    """Update configuration settings at runtime"""
    config_manager.update(**kwargs)


def reset_config(key=None):
    """Reset configuration to default values"""
    config_manager.reset(key)


def get_config(key):
    """Get current configuration value"""
    return getattr(config_manager, key)


# Expose config settings through the manager
SILENT_ERRORS = config_manager.SILENT_ERRORS
SQL_ADAPTER = config_manager.SQL_ADAPTER
SQLITE_URI = config_manager.SQLITE_URI
MYSQL_USER = config_manager.MYSQL_USER
MYSQL_PASSWORD = config_manager.MYSQL_PASSWORD
MYSQL_DB = config_manager.MYSQL_DB
SAVE_ARTICLES_WITHOUT_ACTIVATIONS = \
    config_manager.SAVE_ARTICLES_WITHOUT_ACTIVATIONS
OVERWRITE_EXISTING_ROWS = config_manager.OVERWRITE_EXISTING_ROWS
CAREFUL_PARSING = config_manager.CAREFUL_PARSING
IGNORE_BAD_ROWS = config_manager.IGNORE_BAD_ROWS
EXCLUDE_TABLES_WITH_MISSING_LABELS = \
    config_manager.EXCLUDE_TABLES_WITH_MISSING_LABELS
USE_READABILITY = config_manager.USE_READABILITY
SAVE_ORIGINAL_HTML = config_manager.SAVE_ORIGINAL_HTML
