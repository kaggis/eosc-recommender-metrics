#!/usr/bin/env python3
import requests
import sys
import csv
import argparse


def get_services_from_search(endpoint_url, batch=100):
    """Given an eosc search service endpoint url and a batch number the
    function tries to call iteratively the json endpoint and retrive the
    list of services in the search service. Finally it produces a list of
    triplets [service_id, service_name, service_path]

    Args:
        endpoint (string): A valid EOSC search service endpoint
        batch (int): number of how many items to iterate. Max = 100

    Returns:
        list of lists: A list of service entries. Each service entry is a
        three-item list containing:
        [service_id, service_name, service_path]
    """

    # cap batch at 100 items due to the search service max row limit per page
    if batch > 100:
        batch = 100

    # here we will store all the collected services
    services = []
    # needed POST parameters on each request
    payload = {
        "facets": {
            "title": {
                "field": "title",
                "type": "terms",
                "limit": 0
                }
            }
        }
    # needed headers on each request
    headers = {"Accept": "application/json"}
    # keep track of next page cursor
    cursor = ""
    index = 0
    while True:
        index += 1
        url = "{}?rows={}&collection=service&q=*&qf=title".format(
            endpoint_url, batch
        )
        if cursor:
            url = "{}&cursor={}".format(url, cursor)
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code != 200:
            # if request error break and return empty
            return None
        # else parse the details
        data = response.json()

        # if results are empty we are finished
        if len(data.get("results")) == 0:
            break

        services.extend(
            [
                [
                    int(service.get("id")),
                    service.get("title")[0],
                    "/services/" + service.get("slug"),
                ]
                for service in data.get("results")
            ]
        )
        print("Retrieving up to {}...".format(index * batch))
        cursor = data.get("nextCursorMark")

    # sort by service id
    services = sorted(services, key=lambda x: x[0])

    return services


def output_services_to_csv(items, output):
    with open(output, "w") as f:
        writer = csv.writer(f)
        writer.writerows(items)


# Main logic
def main(args=None):
    # begin collecting services from url per batch number
    print("Connecting to: {}...".format(args.url))
    services = get_services_from_search(args.url, args.batch)
    # output to csv
    output_services_to_csv(services, args.output)
    print("File written to {}".format(args.output))


# Parse arguments and call main
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Retrieve service catalog from eosc marketplace"
    )
    parser.add_argument(
        "-u",
        "--url",
        metavar="STRING",
        help="service list endpoint url",
        required=False,
        dest="url",
        default=100,
    )
    parser.add_argument(
        "-n",
        "--num-of-items",
        metavar="INTEGER",
        help="Number of items per page",
        required=False,
        dest="batch",
        default=100,
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
