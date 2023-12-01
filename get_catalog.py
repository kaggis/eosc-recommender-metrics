#!/usr/bin/env python3
import requests
import sys
import csv
import argparse
from pymongo import MongoClient
from datetime import datetime


def prep_url(category, item):
    if category == "service":
        return "services/{}".format(item["pid"])
    elif category == "data_source":
        return "services/{}".format(item["pid"])
    elif category == "training":
        return "trainings/{}".format(item["id"])
    elif category == "guideline":
        return "guidelines/{}".format(item["id"])
    elif category == "bundle":
        return "services/{}".format(item["service_id"])
    else:
        # signifies external link
        return "search/result"


def get_items_from_search(endpoint_url, category, provider, batch=100,
                          limit=-1):
    """Given an eosc search service endpoint url and an item category and batch
    number the function tries to call iteratively the json endpoint and
    retrive the list of items of this category in the search service.
    Finally it produces a list of item objects

    Args:
        endpoint (string): A valid EOSC search endpoint
        category (string): A category item name (e.g. service, traning)
        provider (string): A provider handling the items
        batch (int): number of how many items to iterate. Max = 100


    Returns:
        list of lists: A list of item entries. Each item entry is a
        three-item list containing:
        [item_id, item_name, item_path]
    """

    # save items with timestamp
    timestamp = datetime.utcnow()

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
        url = "{}?rows={}&collection={}&q=*&qf=title&exact=false".format(
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

        for item in data.get("results"):
            result = {
                "id": item.get("id"),
                "name": item.get("title")[0],
                "path": prep_url(category, item),
                "created_on": None,
                "deleted_on": None,
                "type": category,
                "ingestion": "batch",
                "timestamp": timestamp
            }

            result["provider"] = provider

            if "scientific_domains" in item:
                result["scientific_domain"] = item["scientific_domains"]

            if "categories" in item:
                result["category"] = item["categories"]

            items.append(result)

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

    return items


def output_items_to_csv(items, output):
    """Given a list of items save them to a csv file

    Args:
        items (list): list of items
        output (string): filename to csv file
    """

    # from a list of items create a list of three item arrays
    # [item_id, item_name, item_path]

    csv_friendly_list = [
        [item["id"], item["name"], item["path"]] for item in items
    ]

    # save list of three tuples in csv
    with open(output, "w") as f:
        writer = csv.writer(f)
        writer.writerows(csv_friendly_list)


def ouput_items_to_mongo(items, mongo_uri, category, provider,
                         clear_prev=True):
    """Gets a list of items and stores them to a mongodb database under
    collection: resources

    Args:
        items (list): the list of items
        mongo_uri (string): the mongodb service uri containing the database
        host and the database name
        category: the category of items to be stored
        provider: the provider of that is handling the items
        clear_prev (boolean): clear previous results

    Returns:
        (integer, integer): a tuple with number of items inserted into the
        database and previous items cleared
    """

    # initialize db client
    mc = MongoClient(mongo_uri)
    # get the db name from uri that client parsed
    db_name = mc.get_default_database().name

    # always save to a collection named resources
    col_name = "resources"

    # get database and collection objects
    db = mc[db_name]
    col = db[col_name]

    result_del = []

    # by default NOT clear previous results
    if clear_prev:
        result_del = col.delete_many(
            {
                "provider": provider,
                "type": category,
                "ingestion": "batch",
            }
        )

    result = col.insert_many(items)
    # close connection to the datastore
    mc.close()

    # return number of inserted items
    return (len(result.inserted_ids),
            result_del.deleted_count if result_del else 0)


# Main logic
def main(args=None):

    if ',' in args.provider:
        print("Only one provider is accepted per request")
        raise

    # begin collecting items from url per batch number
    print("Connecting to: {}... and retrieving {} items".format(
        args.url, args.category
        ))

    items = get_items_from_search(
       args.url, args.category, args.provider, int(args.batch), int(args.limit)
    )
    # output to csv on if argument given
    if args.output:
        output_items_to_csv(items, args.output)
        print("File written to {}".format(args.output))

    # output to mongodb if argument given
    if args.datastore:

        result_num, result_clear = ouput_items_to_mongo(
            items, args.datastore, args.category, args.provider,
            clear_prev=False
        )

        # if previous items have been cleared display message
        if result_clear:
            print(
                "{} previous items cleared from the datastore: {}".format(
                    result_num, args.datastore
                )
            )

        print("{} items stored at datastore: {}".format(
            result_num, args.datastore
        ))


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
        default="",
    )
    parser.add_argument(
        "-p",
        "--provider",
        metavar="STRING",
        help="Designate which provider handles the items",
        required=True,
        dest="provider",
        default="",
    )
    parser.add_argument(
        "-d",
        "--datastore",
        metavar="STRING",
        help="mongo datastore uri - e.g. mongodb://localhost:27017/rsmetrics",
        required=False,
        dest="datastore",
        default="",
    )

    # Parse the arguments
    sys.exit(main(parser.parse_args()))
