import os

import pytest

# The key the tests hand to Scraper when they need NCBI to reject one.
FAKE_API_KEY = 'not-a-real-key'
API_KEY_PLACEHOLDER = '<api_key>'


def _scrub_api_keys(response):
    # NCBI echoes the key in its error bodies ("api-key":"<key>"), which
    # filter_query_parameters doesn't reach.
    for key in filter(None, (os.environ.get('PUBMED_API_KEY'), FAKE_API_KEY)):
        response['body']['string'] = response['body']['string'].replace(
            key.encode(), API_KEY_PLACEHOLDER.encode())
        response['headers'] = {
            name: [value.replace(key, API_KEY_PLACEHOLDER) for value in values]
            for name, values in response['headers'].items()
        }
    return response


@pytest.fixture(scope="module")
def vcr_config():
    # Cassettes recorded with PUBMED_API_KEY set must not store the key, and
    # must still play back whether or not the key is set. Bodies are stored
    # decoded so the key can be scrubbed from them.
    return {
        "filter_query_parameters": ["api_key"],
        "decode_compressed_response": True,
        "before_record_response": _scrub_api_keys,
    }
