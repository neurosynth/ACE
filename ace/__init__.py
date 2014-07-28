# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 et:
"""ACE -- Automated Coordinate Extraction.
"""
__all__ = ["config", "database", "datatable", "exporter", "set_logging_level", "scrape", "sources", "tableparser", "tests", "__version__"]

import logging
import sys
import os

from version import __version__

def set_logging_level(level=None):
    """Set package-wide logging level

    Args
        level : Logging level constant from logging module (warning, error, info, etc.)
    """
    if level is None:
        level = os.environ.get('ACE_LOGLEVEL', 'warn')
    logger.setLevel(getattr(logging, level.upper()))
    return logger.getEffectiveLevel()

def _setup_logger(logger):
    # Basic logging setup
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter("%(levelname)-6s %(module)-7s %(message)s"))
    logger.addHandler(console)
    set_logging_level()

# Set up logger
logger = logging.getLogger("ace")
_setup_logger(logger)