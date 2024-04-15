
import sys
sys.path.append('/users/ngoyal')
import errno
import sys
import time
import requests as rq
from src.utils.interfaces.stream_interface import Stream

class ObisStream(Stream):
    def __init__(self, page_size=1000, retry_delay=2, timeout=30):
        self.PAGE_SIZE = page_size
        self.RETRY_DELAY_IN_SECONDS = retry_delay
        self.TIMEOUT_IN_SECONDS = timeout
        self.after = "-1"

    # not required for obis
    def make_query(self):
        pass

    def start_stream(self):
        while True:
            try:
                response = rq.get(f"https://api.obis.org/v3/occurrence?size={self.PAGE_SIZE}&after={self.after}", timeout=self.TIMEOUT_IN_SECONDS)
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
                    records = response_data["results"]
                    # yield records
                    for record in records:
                        yield record

                    # If this is the last page of records
                    if len(records) == 0:
                        break
                    
                    self.after = records[-1]["id"]

                except ValueError as e:
                    print(e, file=sys.stderr)
                    continue

            except IOError as e:
                if e.errno != errno.EPIPE:
                    print(e, file=sys.stderr)
                break
