import pytest


@pytest.fixture(scope="module")
def vcr_config():
    # Cassettes recorded with PUBMED_API_KEY set must not store the key, and
    # must still play back whether or not the key is set.
    return {"filter_query_parameters": ["api_key"]}
