#!/usr/bin/env python3
import sys
import argparse
import yaml
import pymongo
from datetime import datetime
import os
import logging

logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format="[%(asctime)s] %(levelname)s %(message)s",
)

__copyright__ = (
    "© "
    + str(datetime.utcnow().year)
    + ", National Infrastructures for Research and Technology (GRNET)"
)
__status__ = "Production"
__version__ = "1.0.2"


def print_help(func):
    def inner():
        print("RS Metrics Preprocessor")
        print("Version: " + __version__)
        print(__copyright__ + "\n")
        func()

    return inner


def remove_service_prefix(text):
    """Removes '/service/' prefix from eosc service paths

    Args:
        text (string): string containing a service path

    Returns:
        string: service path without the /service/ prefix
    """
    if text.startswith("/service/"):
        return text[len("/service/"):]
    return text


parser = argparse.ArgumentParser(
    prog="preprocessor",
    description="Prepare data for the EOSC Marketplace RS metrics calculation",
    add_help=False,
)
parser.print_help = print_help(parser.print_help)
parser._action_groups.pop()
required = parser.add_argument_group("required arguments")
optional = parser.add_argument_group("optional arguments")

optional.add_argument(
    "-c",
    "--config",
    metavar=("FILEPATH"),
    help="override default configuration file (./config.yaml)",
    nargs="?",
    default="./config.yaml",
    type=str,
)
optional.add_argument(
    "-o",
    "--output",
    metavar=("DIRPATH"),
    help="override default output dir path (./data)",
    nargs="?",
    default="./data",
    type=str,
)
optional.add_argument(
    "-p",
    "--provider",
    metavar=("DIRPATH"),
    help=("source of the data based on providers specified in the "
          "configuration file"),
    nargs="?",
    default="cyfronet",
    type=str,
)
optional.add_argument(
    "-s",
    "--starttime",
    metavar=("DATETIME"),
    help=("process data starting from given datetime in ISO format (UTC) "
          "e.g. YYYY-MM-DD"),
    nargs="?",
    default=None,
)
optional.add_argument(
    "-e",
    "--endtime",
    metavar=("DATETIME"),
    help=("process data ending to given datetime in ISO format (UTC) "
          "e.g. YYYY-MM-DD"),
    nargs="?",
    default=None,
)

optional.add_argument(
    "-h", "--help", action="help", help="show this help message and exit"
)
optional.add_argument(
    "-V", "--version", action="version", version="%(prog)s v" + __version__
)

# args=parser.parse_args(args=None if sys.argv[1:] else ['--help'])

args = parser.parse_args()

query = {"timestamp": {"$lt": datetime.utcnow()}}

if args.starttime:
    args.starttime = datetime.fromisoformat(args.starttime)
    query["timestamp"]["$gte"] = args.starttime

if args.endtime:
    args.endtime = datetime.fromisoformat(args.endtime)
    query["timestamp"]["$lt"] = args.endtime

if args.starttime and args.endtime:
    if args.endtime < args.starttime:
        print("End date must be older than start date")
        sys.exit(0)


with open(args.config, "r") as _f:
    config = yaml.load(_f, Loader=yaml.FullLoader)


os.makedirs(args.output, exist_ok=True)

provider = None
for p in config["providers"]:
    if args.provider == p["name"]:
        provider = p

if not provider:
    print("Given provider not in configuration")
    sys.exit(0)

# connect to internal db server for reading users
datastore = pymongo.MongoClient(config["datastore"],
                                uuidRepresentation="pythonLegacy")
# use db
rsmetrics_db = datastore[config["datastore"].split("/")[-1]]

# connect to external db server for reading recommendations
myclient = pymongo.MongoClient(provider["db"],
                               uuidRepresentation="pythonLegacy")
# use db
recdb = myclient[provider["db"].split("/")[-1]]


recs = []

if provider["name"] == "cyfronet":
    for rec in recdb["recommendation"].find(query).sort("user"):
        # in legacy mode the non-existance of user_id equals to
        # anonynoums action,
        # which in rs metrics (legacy mode) is indicated with -1
        user_id = -1
        aai_uid = None
        unique_id = None
        if "user" in rec:
            user_id = rec["user"]

        if "aai_uid" in rec:
            aai_uid = rec["aai_uid"]

        if "unique_id" in rec:
            unique_id = str(rec["unique_id"])

        recs.append(
            {
                "user_id": user_id,
                "aai_uid": aai_uid,
                "unique_id": unique_id,
                "resource_ids": rec["services"],
                "timestamp": rec["timestamp"],
                "type": "service",  # currently, static
                "provider": provider["name"],  # currently, static
                "ingestion": "batch",  # currently, static
            }
        )

elif provider["name"] == "athena":
    _query = query.copy()
    _query["date"] = _query.pop("timestamp")
    for rec in recdb["recommendation"].find(_query).sort("user_id"):
        # in legacy mode the non-existance of user_id equals to
        # anonynoums action,
        # which in rs metrics (legacy mode) is indicated with -1
        user_id = -1
        aai_uid = None
        unique_id = None
        if "user_id" in rec:
            user_id = rec["user_id"]

        if "aai_uid" in rec:
            aai_uid = rec["aai_uid"]

        if "unique_id" in rec:
            unique_id = str(rec["unique_id"])

        recs.append(
            {
                "user_id": user_id,
                "aai_uid": aai_uid,
                "unique_id": unique_id,
                "resource_ids": list(
                    map(lambda x: x["service_id"], rec["recommendation"])
                ),
                "resource_scores": list(
                    map(lambda x: x["score"], rec["recommendation"])
                ),
                "timestamp": rec["date"],
                "type": "service",  # currently, static
                "provider": provider["name"],  # currently, static
                "ingestion": "batch",  # currently, static
            }
        )

# store data to Mongo DB

rsmetrics_db["recommendations"].delete_many(
    {"provider": provider["name"], "ingestion": "batch"}
)
if len(recs) > 0:
    rsmetrics_db["recommendations"].insert_many(recs)

logging.info("Recommendation collection for {} stored...".format(
    provider["name"]))
