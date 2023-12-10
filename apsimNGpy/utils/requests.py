"""
Author: Stephen Odara
Date: Dec 3rd 2023

Includes functions for downloading information from various sources.
Since downloading is a network based process, it makes sense to isolate these functions separately
This allows for possible future optimizations of processes that use these functions.
Also allows for graceful error handling. e.g timeouts, connection errors. The above
is particularly important to be a well behaved API.
"""
import requests
import logging

logger = logging.getLogger(__name__)


def download_data(url, args, **kwargs):
    """
    """
    result = None
    try:
        response = requests.get(url)
    except requests.RequestException as e:
        logger.exception(repr(e))
    else:
        data = response.context


