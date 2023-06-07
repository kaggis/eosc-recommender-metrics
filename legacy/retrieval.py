#!/usr/bin/env python3
import requests
from urllib.parse import urljoin

# with open('pages','r') as f:
#    lines=f.readlines()


def retrieve_id(page_id,
                base_url="https://marketplace.eosc-portal.eu/services/"):

    url = urljoin(base_url, page_id.strip())
    # print(page_id, base_url, url)

    try:
        r = requests.get(url)
        return r.text.split("favourite-")[1].split('" value=')[0]

    except IndexError:
        return None
