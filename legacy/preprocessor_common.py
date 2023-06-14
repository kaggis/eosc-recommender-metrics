#!/usr/bin/env python3
import sys
import argparse
import yaml
import pymongo
from datetime import datetime
import natsort as ns
from natsort import natsorted
import logging
import pandas as pd
import os

# local lib
import reward_mapping as rm

from get_service_catalog import (
    get_eosc_marketplace_url,
    get_service_catalog_items,
    get_service_catalog_page_content,
    save_service_items_to_csv,
)

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
        print("RS Metrics Preprocessor Common")
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
    prog="preprocessor_common",
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
    "-p",
    "--provider",
    metavar=("DIRPATH"),
    help=("source of the data based on providers specified "
          "in the configuration file"),
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
    "--use-cache",
    help=("Use the specified file in configuration as the file to read "
          "resources"),
    action="store_true",
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

provider = None
for p in config["providers"]:
    if args.provider == p["name"]:
        provider = p

if not provider:
    print("Given provider not in configuration")
    sys.exit(0)

# connect to Connector DB
connector = pymongo.MongoClient(provider["db"],
                                uuidRepresentation="pythonLegacy")

# use db
recdb = connector[provider["db"].split("/")[-1]]

# connect to Datastore DB

# connect to db server
datastore = pymongo.MongoClient(config["datastore"],
                                uuidRepresentation="pythonLegacy")

# use db
rsmetrics_db = datastore[config["datastore"].split("/")[-1]]

# automatically associate page ids to service ids
# default to no caching
if not args.use_cache:
    eosc_url = get_eosc_marketplace_url()
    print(
        "Retrieving page: marketplace list of services... \nGrabbing url: {0}"
        .format(eosc_url)
    )
    eosc_page_content = get_service_catalog_page_content(eosc_url)
    print("Page retrieved!\nGenerating results...")
    eosc_service_results = get_service_catalog_items(eosc_page_content)

    if config["service"]["store"]:
        # output to csv
        save_service_items_to_csv(eosc_service_results,
                                  config["service"]["store"])
        print("File written to {}".format(config["service"]["store"]))

# if cache file is used
else:
    with open(config["service"]["store"], "r") as f:
        lines = f.readlines()

    eosc_service_results = list(map(lambda x: x.split(","), lines))

# read map file and save in dict
keys = list(map(lambda x: remove_service_prefix(x[-1]).strip(),
                eosc_service_results))
ids = list(map(lambda x: str(x[0]), eosc_service_results))
names = list(map(lambda x: x[1], eosc_service_results))

rdmap = dict(zip(ids, zip(keys, names)))

# A. Working on scientific domains and categories

for col in ['scientific_domain', 'category']:
    data = recdb[col].find({})
    data = list(
        map(
            lambda x: {
                "id": int(str(x["_id"])),
                "name": str(x["name"]),
            },
            data,
        )
    )

    rsmetrics_db[col].drop()
    rsmetrics_db[col].insert_many(data)

    logging.info("{} collection stored...".format(col))

# B. Working on users

# 1) produce users csv with each user id along with the user's accessed
# services
# 2) query users from database for fields _id and accessed_services then create
# a list of rows
# 3) each row contains two elements:
# first: user_id in string format and
# second: a space separated sorted list of accessed services
users = recdb["user"].find({}, {"accessed_services": 1})
users = list(
    map(
        lambda x: {
            "id": int(str(x["_id"])),
            "accessed_resources": sorted(set(x["accessed_services"])),
            "created_on": None,
            "deleted_on": None,
            "provider": ["cyfronet", "athena"],  # currently, static
            "ingestion": "batch",  # currently, static
        },
        users,
    )
)

rsmetrics_db["users"].delete_many(
    {
        "provider": {"$in": ["cyfronet", "athena"]},
        "ingestion": "batch",
    }
)
rsmetrics_db["users"].insert_many(users)

logging.info("Users collection stored...")

# D. Working on resources

remote_resources = {}
for d in recdb["service"].find({}, {'_id': 1, 'categories': 1,
                                    'scientific_domains': 1}):
    remote_resources[d['_id']] = (d['scientific_domains'], d['categories'])

_ss = natsorted(list(set(list(map(lambda x: x + "\n", ids)))),
                alg=ns.ns.SIGNED)

resources = []
for s in _ss:
    try:
        resources.append({
            "id": int(s.strip()),
            "name": rdmap[s.strip()][1],
            "path": rdmap[s.strip()][0],
            "created_on": None,
            "deleted_on": None,
            "scientific_domain": remote_resources.setdefault(int(s.strip()),
                                                             (None, None))[0],
            "category": remote_resources.setdefault(int(s.strip()),
                                                    (None, None))[1],
            "type": "service",  # currently, static
            "provider": ["cyfronet", "athena"],  # currently, static
            "ingestion": "batch",  # currently, static
        })
    except Exception as e:
        logging.error('Could not collect resource with id {}'.format(e))

rsmetrics_db["resources"].delete_many(
    {
        "provider": {"$in": ["cyfronet", "athena"]},
        "ingestion": "batch",
    }
)
rsmetrics_db["resources"].insert_many(resources)

logging.info("Resources collection stored...")

# E. Working on user_actions


class Mock:
    pass


class User_Action:
    def __init__(self, source_page_id, target_page_id, order):
        self.source = Mock()
        self.target = Mock()
        self.action = Mock()
        self.source.page_id = source_page_id
        self.target.page_id = target_page_id
        self.action.order = order


reward_mapping = {
    "order": 1.0,
    "interest": 0.7,
    "mild_interest": 0.3,
    "simple_transition": 0.0,
    "unknown_transition": 0.0,
    "exit": 0.0,
}

# reward_mapping.py is modified so that the function
# reads the Transition rewards csv file once
# consequently, one argument has been added to the
# called function
ROOT_DIR = "./"

TRANSITION_REWARDS_CSV_PATH = os.path.join(
    ROOT_DIR, "resources", "transition_rewards.csv"
)
transition_rewards_df = pd.read_csv(TRANSITION_REWARDS_CSV_PATH,
                                    index_col="source")

# reading resources to be used for filtering user_actions
resources = pd.DataFrame(
    list(rsmetrics_db["resources"]
         .find({"provider": {"$in": [args.provider]}}))
).iloc[:, 1:]

resources.columns = [
    "Service",
    "Name",
    "Page",
    "Created_on",
    "Deleted_on",
    "Scientific_domain",
    "Category",
    "Type",
    "Provider",
    "Ingestion",
]
resources = pd.Series(resources["Service"].values,
                      index=resources["Page"]).to_dict()

luas = []
col = "user_actions" if provider["name"] == "athena" else "user_action"
for ua in recdb[col].find(query).sort("user"):
    # set -1 to anonymous users
    user = -1
    if "user" in ua:
        user = ua["user"]

    # process data that map from page id to service id exist
    # for both source and target page ids
    # if not set service id to -1
    try:
        _pageid = "/" + "/".join(ua["source"]["page_id"].split("/")[1:3])
        source_service_id = resources[_pageid]
    except (KeyError, IndexError):
        source_service_id = -1

    try:
        _pageid = "/" + "/".join(ua["target"]["page_id"].split("/")[1:3])
        target_service_id = resources[_pageid]
    except KeyError:
        target_service_id = -1

    # function has been modified where one more argument is given
    # in order to avoid time-consuming processing of reading csv file
    # for every func call
    symbolic_reward = rm.ua_to_reward_id(
        transition_rewards_df,
        User_Action(
            ua["source"]["page_id"],
            ua["target"]["page_id"],
            ua["action"]["order"]
        ),
    )

    reward = reward_mapping[symbolic_reward]

    luas.append(
        {
            "user_id": int(user),
            "source_resource_id": int(source_service_id),
            "target_resource_id": int(target_service_id),
            "reward": float(reward),
            "panel": ua["source"]["root"]["type"],
            "timestamp": ua["timestamp"],
            "source_path": ua["source"]["page_id"],
            "target_path": ua["target"]["page_id"],
            "type": "service",  # currently, static
            "provider": ["cyfronet", "athena"],  # currently, static
            "ingestion": "batch",  # currently, static
        }
    )
rsmetrics_db["user_actions"].delete_many(
    {"provider": provider["name"], "ingestion": "batch"}
)
if len(luas) > 0:
    rsmetrics_db["user_actions"].insert_many(luas)

logging.info("User_actions collection stored...")