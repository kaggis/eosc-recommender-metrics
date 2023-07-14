#!/usr/bin/env python3
import requests
import sys
import csv
import argparse


def prep_url(category, item):
    if category == "service":
        return "services/{}".format(item["slug"])
    elif category == "data_source":
        return "services/{}".format(item["id"])
    elif category == "training":
        return "trainings/{}".format(item["id"])
    elif category == "guideline":
        return "guidelines/{}".format(item["id"])
    elif category == "bundle":
        return "services/{}".format(item["service_id"])
    else:
        # signifies external link
        return "search/result"


def get_items_from_search(endpoint_url, category, batch=100, limit=-1):
    """Given an eosc search service endpoint url and an item category and batch
    number the function tries to call iteratively the json endpoint and
    retrive the list of items of this category in the search service.
    Finally it produces a list of triplets [item_id, item_name, item_path]

    Args:
        endpoint (string): A valid EOSC search endpoint
        category (string): A category item name (e.g. service, traning)
        batch (int): number of how many items to iterate. Max = 100

    Returns:
        list of lists: A list of item entries. Each item entry is a
        three-item list containing:
        [item_id, item_name, item_path]
    """

    # cap batch at 100 items due to the search service max row limit per page
    if batch > 100:
        batch = 100

    # here we will store all the collected items
    items = []
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

    while True:
        url = "{}?rows={}&collection={}&q=*&qf=title".format(
            endpoint_url, batch, category
        )

        if cursor:
            url = "{}&cursor={}".format(url, cursor)
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code != 200:
            # if request error break and return empty
            return None
        # else parse the details
        data = response.json()

        num_of_results = len(data.get("results"))
        # if results are empty we are finished
        if num_of_results == 0:
            break

        items.extend(
            [
                [
                    item.get("id"),
                    item.get("title")[0],
                    prep_url(category, item),
                ]
                for item in data.get("results")
            ]
        )

        # if user enforced a maximum limit on the retrieved check
        if limit > 0:
            # if current num of items extends the limit
            if len(items) > limit:
                # return only the first part of the list equal to the limit
                items = items[:limit]
                print("Items Retrieved till now... {}".format(len(items)))
                return items

        print("Items Retrieved till now... {}".format(len(items)))
        cursor = data.get("nextCursorMark")

    # sort by item id
    items = sorted(items, key=lambda x: x[0])

    return items


def output_items_to_csv(items, output):
    with open(output, "w") as f:
        writer = csv.writer(f)
        writer.writerows(items)


# Main logic
def main(args=None):
    # begin collecting items from url per batch number
    print("Connecting to: {}... and retrieving {} items".format(
        args.url, args.category
        ))
    items = get_items_from_search(
        args.url, args.category, int(args.batch), int(args.limit)
    )
    # output to csv
    output_items_to_csv(items, args.output)
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
        help="endpoint url",
        required=True,
        dest="url"
    )
    parser.add_argument(
        "-c",
        "--category",
        metavar="STRING",
        help="category of item (service, training etc)",
        required=False,
        dest="category",
        default="service",
    )
    parser.add_argument(
        "-b",
        "--batch",
        metavar="INTEGER",
        help="Number of items to retrieve per batch",
        required=False,
        dest="batch",
        default=100,
    )
    parser.add_argument(
        "-l",
        "--limit",
        metavar="INTEGER",
        help="Maximum number of items to retrieve",
        required=False,
        dest="limit",
        default=-1,
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
