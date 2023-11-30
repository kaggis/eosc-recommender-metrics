#!/usr/bin/env python3
import sys
import argparse
import pymongo
import logging
from datetime import datetime

# establish basic logging
logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format="[%(asctime)s] %(levelname)s %(message)s",
)


def main(args):

    try:
        # connect to the datastore
        mongo = pymongo.MongoClient(args.datastore,
                                    uuidRepresentation="pythonLegacy")
        rsmetrics_db = mongo[args.datastore.split("/")[-1]]

        # check if datastore is alive
        if mongo.rsmetrics_db.command('ping') == {u'ok': 1.0}:
            logging.info("Connected succesfully to {}".format(args.datastore))

    except Exception as e:
        logging.error("Cannot connect to {}: {}".format(args.datastore, e))
        return

    time_filter = {}

    if args.starttime:
        args.starttime = datetime.fromisoformat(args.starttime)
        if "timestamp" not in time_filter:
            time_filter["timestamp"] = {}
            time_filter["timestamp"]["$gte"] = args.starttime

    if args.endtime:
        edt = datetime.fromisoformat(args.endtime)
        args.endtime = datetime.combine(edt, datetime.max.time())
        if "timestamp" not in time_filter:
            time_filter["timestamp"] = {}
            time_filter["timestamp"]["$lte"] = args.endtime

    if args.starttime and args.endtime:
        if args.endtime < args.starttime:
            logging.error("End date must be older than start date")
            return

    logging.info("Searching for the period {} - {}".format(args.starttime,
                                                           args.endtime))

    for col in args.collection:
        try:
            doc_count = rsmetrics_db[col].count_documents(time_filter)

            logging.info("> Collection '{}' has {} entries".format(col,
                                                                   doc_count))

            for item_type in rsmetrics_db[col].distinct('type', time_filter):
                if col != 'resources':
                    count = rsmetrics_db[col].count_documents({**time_filter,
                                                               "type":
                                                               item_type})
                else:
                    count = len(rsmetrics_db[col].distinct('id',
                                                           {**time_filter,
                                                               "type":
                                                               item_type}))

                logging.info("\t* '{}'\thas {} entries".format(item_type,
                                                               count))

        except Exception as e:
            logging.error("Cannot retrieve entries from collection '{}'\n{}"
                          .format(col, e))


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="RS Monitor")

    parser.add_argument(
        "-c",
        "--collection",
        metavar="STRING",
        nargs="+",
        help=("collection to monitor, default all "
              "(i.e., user_actions recommendations resources)"),
        default=["user_actions", "recommendations", "resources"],
    )
    parser.add_argument(
        "-d",
        "--datastore",
        metavar="STRING",
        help="datastore uri",
        required=True,
        dest="datastore",
    )
    parser.add_argument(
        "-s",
        "--starttime",
        metavar=("DATETIME"),
        help=("filter search from given datetime in ISO format (UTC) "
              "e.g. YYYY-MM-DD"),
        nargs="?",
        default=None,
    )
    parser.add_argument(
        "-e",
        "--endtime",
        metavar=("DATETIME"),
        help=("filter search to given datetime in ISO format (UTC) "
              "e.g. YYYY-MM-DD"),
        nargs="?",
        default=None,
    )

    # Pass the arguments to main method
    sys.exit(main(parser.parse_args()))
