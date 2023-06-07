#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup
import sys
import csv
import argparse


def get_eosc_marketplace_url(num_of_items=1000):
    """Constructs the EOSC Marketplace URL to grab the complete service catalog
    (list of available services) in one request.

    Args:
        num_of_items (int, optional): Number of items per page to be used as an
        url argument when contacting EOSC Marketplace webpage to grab all
        services in one take. Defaults to 1000.

    Returns:
        string: EOSC marketplace url along with the neccessary url parameters
        to grab the list of all available services
    """
    url = (
        "https://marketplace.eosc-portal.eu/services?page=1&per_page={}"
        .format(str(num_of_items))
    )
    return url


# Contacts eosc marketplace page to retrieve the complete list of items in a
# single take
def get_service_catalog_page_content(url):
    """Returns the HTML Page content of EOSC Marketplace Service Page catalog

    Args:
        url (string): url to EOSC Marketplace Service list

    Returns:
        bytes: html content of the eosc marketplace service list page
    """
    page = requests.get(url)
    return page.content


def get_service_catalog_items(content):
    """Parses EOSC Marketplace service list html page and extracts all active
    services.
    Each service is described by a list of three items:
    [service_id, service_name, service_path]

    Args:
        content (bytes): Html content of EOSC Marketplace page containing the
        complete list of available services

    Returns:
        list of lists: A list of service entries. Each service entry is a
        three-item list containing:
        [service_id, service_name, service_path]
    """
    rows = []
    soup = BeautifulSoup(content, "html.parser")
    results = soup.findAll("h2", {"data-e2e": "service-id"})
    for item in results:
        a = item.findChildren("a", recursive=False)[0]
        row = [int(item.attrs["data-service-id"]), a.text.strip(), a["href"]]
        rows.append(row)
    # sort rows by id
    rows = sorted(rows, key=lambda x: x[0])
    return rows


def save_service_items_to_csv(items, output):
    with open(output, "w") as f:
        writer = csv.writer(f)
        writer.writerows(items)


# Main logic
def main(args=None):
    # call eosc marketplace with ample number of services per page
    # default = 1000
    url = get_eosc_marketplace_url(args.items)
    print(
        "Retrieving page: marketplace list of services... \nGrabbing url: {0}"
        .format(url)
    )
    page_content = get_service_catalog_page_content(url)
    print("Page retrieved!\nGenerating results...")
    results = get_service_catalog_items(page_content)
    # output to csv
    save_service_items_to_csv(results, args.output)
    print("File written to {}".format(args.output))


# Parse arguments and call main
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Retrieve service catalog from eosc marketplace"
    )
    parser.add_argument(
        "-n",
        "--num-of-items",
        metavar="STRING",
        help="Number of items per page",
        required=False,
        dest="items",
        default="1000",
    )
    parser.add_argument(
        "-o",
        "--output",
        metavar="STRING",
        help="Output csv file",
        required=False,
        dest="output",
        default="./service_catalog.csv",
    )

    # Parse the arguments
    sys.exit(main(parser.parse_args()))
