"""Bounded GET-only retries using Requests and urllib3's maintained transport."""
import requests
from requests.adapters import HTTPAdapter
from urllib3.exceptions import InvalidHeader
from urllib3.util import Retry


class ReadRetry(Retry):
    def get_retry_after(self, response):
        delay = super().get_retry_after(response)
        if delay is not None and delay > 60:
            # Do not retry earlier than the provider permits, or hold a worker
            # indefinitely. Fail this read and preserve incomplete-data status.
            raise InvalidHeader("Provider retry delay exceeds 60 second budget")
        return delay


class ReadOnlySession(requests.Session):
    def __init__(self):
        super().__init__()
        retry = ReadRetry(total=2, connect=2, read=2, status=2, other=0,
                          allowed_methods=frozenset({'GET'}),
                          status_forcelist=(429, 500, 502, 503, 504),
                          backoff_factor=2, raise_on_status=False)
        self.mount('https://', HTTPAdapter(max_retries=retry))
        self.mount('http://', HTTPAdapter(max_retries=retry))

    def request(self, method, url, **kwargs):
        if method.upper() != 'GET':
            raise ValueError('Provider read session only permits GET')
        kwargs['allow_redirects'] = False
        return super().request(method, url, **kwargs)
