
import sys
sys.path.append('/users/ngoyal')

import errno
import sys
import time
import requests as rq
from src.utils.interfaces.stream_interface import Stream

class GbifStream(Stream):
    def __init__(self, page_size=1000, retry_delay=2, timeout=30):
        self.PAGE_SIZE = page_size
        self.RETRY_DELAY_IN_SECONDS = retry_delay
        self.TIMEOUT_IN_SECONDS = timeout
        self.page = 0

    def make_query(self):
        return {
            "rq": {},
            "limit": self.PAGE_SIZE,
            "offset": self.page
        }

    def start_stream(self):
        while True:
            try:
                query = self.make_query()
                response = rq.post("https://api.gbif.org/v1/occurrence/search", json=query, timeout=self.TIMEOUT_IN_SECONDS)
            except rq.exceptions.Timeout as e:
                print(e, file=sys.stderr)
                print(f"Retrying in {self.RETRY_DELAY_IN_SECONDS} seconds...", file=sys.stderr)
                time.sleep(self.RETRY_DELAY_IN_SECONDS)
                continue

            if not response:
                print(f"Received unexpected response status code {response.status_code}", file=sys.stderr)
                time.sleep(self.RETRY_DELAY_IN_SECONDS)
                print(f"Retrying in {self.RETRY_DELAY_IN_SECONDS} seconds...", file=sys.stderr)
                continue

            try:
                try:
                    response_data = response.json(strict=False)
                    # print(f'############# {response_data.keys()}')
                    # dict_keys(['offset', 'limit', 'endOfRecords', 'count', 'results', 'facets'])
                    records = response_data["results"]
                    
                    # yield records
                    for record in records:
                        yield record
                    # for record in records:
                    #    print(json.dumps(record), file=sys.stdout)

                    # If this is the last page of records
                    if response_data["endOfRecords"]:
                        break

                    self.page += 1
                except ValueError as e:
                    print(e, file=sys.stderr)
                    continue

            except IOError as e:
                if e.errno != errno.EPIPE:
                    print(e, file=sys.stderr)
                break
