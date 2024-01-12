#!/usr/bin/env python3
import sys
import traceback
import argparse
import json
import yaml
import pymongo
from datetime import datetime
import pandas as pd
import numpy as np
from inspect import getmembers, isfunction
from pymongoarrow.api import aggregate_pandas_all
import logging

# local lib
import metrics as m
import get_catalog


__copyright__ = (
    "© "
    + str(datetime.utcnow().year)
    + ", National Infrastructures for Research and Technology (GRNET)"
)

__status__ = "Production"
__version__ = "0.2.2"


logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format="[%(asctime)s] %(levelname)s %(message)s",
)


def print_help(func):
    def inner():
        print("RS Metrics Evaluator")
        print("Version: " + __version__)
        print(__copyright__ + "\n")
        func()

    return inner


# based on the schema it returns a pandas Series (self)
# with registered users only, accordingly
def find_registered(self, schema):
    if schema == 'current':
        return self.notnull()
    else:
        return self != -1


# function is attached to a pandas Series (self)
pd.Series.find_registered = find_registered

parser = argparse.ArgumentParser(
    prog="rsmetrics",
    description="Calculate metrics for the EOSC Marketplace RS",
    add_help=False,
)
parser.print_help = print_help(parser.print_help)
parser._action_groups.pop()
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
    metavar=("STRING"),
    help=("name of the provider-rs specified "
          "in the configuration file"),
    nargs="?",
    default="marketplace_rs",
    type=str,
)
optional.add_argument(
    "-t",
    "--tag",
    metavar=("STRING"),
    help=("tag results e.g. year-month"),
    nargs="?",
    default="",
    type=str,
)
optional.add_argument(
    "-s",
    "--starttime",
    metavar=("DATETIME"),
    help=("calculate metrics starting from given datetime in ISO format (UTC) "
          "e.g. YYYY-MM-DD"),
    nargs="?",
    default=None,
)
optional.add_argument(
    "-e",
    "--endtime",
    metavar=("DATETIME"),
    help=("calculate metrics ending to given datetime in ISO format (UTC) "
          "e.g. YYYY-MM-DD"),
    nargs="?",
    default=None,
)
optional.add_argument("--legacy", help=("enable in order to run calculation \
based on legacy schema"), action="store_true")

optional.add_argument(
    "--use-cache",
    help=("Enable to perform calculations without resources' retrieval"),
    action="store_true",
)

optional.add_argument(
    "--ignore-timestamp",
    help=("Enable to perform calculations without checking if items are \
within the requested datetime range"),
    action="store_true",
)

optional.add_argument(
    "-h", "--help", action="help", help="show this help message and exit"
)
optional.add_argument(
    "-V", "--version", action="version", version="%(prog)s v" + __version__
)
optional.add_argument("-v", action="store_true")

args = parser.parse_args()
logging.disable = not args.v

run = m.Runtime(args.legacy)

if args.starttime:
    args.starttime = datetime.fromisoformat(args.starttime)

if args.endtime:
    edt = datetime.fromisoformat(args.endtime)
    args.endtime = datetime.combine(edt, datetime.max.time())

if args.starttime and args.endtime:
    if args.endtime < args.starttime:
        print("End date must be older than start date")
        sys.exit(0)

# read configuration file
with open(args.config, "r") as _f:
    config = yaml.load(_f, Loader=yaml.FullLoader)

if args.provider not in [p["name"] for p in config["providers"]]:
    print("Provider must be in the configuration")
    sys.exit(0)

# if no cache, retrieve resources
# using the get_catalog tool
if not args.use_cache:

    # call get_catalog
    class GetCatalogArgs:
        pass

    _args = GetCatalogArgs()
    _args.output = False
    _args.batch = 100
    _args.limit = -1
    _args.datastore = config["datastore"]
    _args.url = config['service']['service_list_url']
    _args.provider = args.provider

    try:
        for cat in config['service']['category'][_args.provider]:
            _args.category = cat
            get_catalog.main(_args)
    except Exception as e:
        print("Error: Could not retrieve {} items from {}. See: {}".format(
            _args.category, _args.url, e))
        raise

# read data
# connect to db server
datastore = pymongo.MongoClient(config["datastore"],
                                uuidRepresentation="pythonLegacy")

# use db
rsmetrics_db = datastore[config["datastore"].split("/")[-1]]

# establish a matching query to select data for correct provider
match_query = {}

# schema decision table based on which data to calculate metrics upon
# metrics computations consider both registered and anonymous users
# schema   | registered     | anonymous
# current | aai_uid=!None  | aai_uid=null and user_id=null
# legacy  | user_id=!None  | user_id == -1
if not args.legacy:
    match_query = {
        "$or": [
            {"aai_uid": {"$ne": None}},
            {"$and": [
                {"aai_uid": {"$eq": None}},
                {"user_id": {"$eq": None}}
            ]}
         ]}
else:
    match_query = {
        "$or": [
            {"user_id": {"$ne": None}},
            {"$and": [
                {"user_uid": {"$eq": -1}}
            ]}
         ]}

# start/end date of request
if args.starttime is not None:
    if "timestamp" not in match_query:
        match_query["timestamp"] = {}
    match_query["timestamp"]["$gte"] = args.starttime

if args.endtime is not None:
    if "timestamp" not in match_query:
        match_query["timestamp"] = {}
    match_query["timestamp"]["$lte"] = args.endtime

# merge dictionaries to create two seperate match queries (one for user
# actions and one for rec)
match_ua = {**match_query}
match_rs = {**match_query, "provider": args.provider}

# first column (_id) ignored, where iloc is used
# pymongoarrow lib provides efficient and direct load of query results into
# panda data frames using functions such as find_pandas_all and
# aggregate_pandas_all
logging.info("Reading user actions...")

run.user_actions_all = pd.DataFrame(
    list(rsmetrics_db["user_actions"].find(match_ua,
                                           {"_id": 0}))
)

# it seems that pymongoarrow returns pandas dataframe with
# convert_dtypes=True, therefore None values are treated as np.nan
# keep only None for further processing
run.user_actions_all = run.user_actions_all.replace(np.nan, None)

for _col_id in ['aai_uid', 'user_id', 'source_resource_id',
                'target_resource_id']:
    if _col_id not in run.user_actions_all.columns:
        # Create a new column with None values
        run.user_actions_all[_col_id] = None

# if aai_uid is null then anonymous.
# If anonymous copy the unique_id to aai_uid.
# Thus, all entries have aai_uid (both registered and anonymous)
if not args.legacy:
    run.user_actions_all['registered'] = run.user_actions_all.apply(
                                         lambda row:
                                         False if pd.isnull(row['aai_uid'])
                                         else True, axis=1)
    run.user_actions_all['aai_uid'] = run.user_actions_all.apply(
                                      lambda row:
                                      row['unique_id'] if not row['registered']
                                      else row['aai_uid'], axis=1)

# Same logic but for legacy mode:
# if user_id == -1 then anonymous.
# If anonymous copy 0 to user_id.
# Thus, all entries have user_id >= 0 (both registered and anonymous)
else:
    run.user_actions_all['registered'] = run.user_actions_all.apply(
                                         lambda row:
                                         False if row['user_id'] == -1
                                         else True, axis=1)
    run.user_actions_all['user_id'] = run.user_actions_all.apply(
                                      lambda row:
                                      0 if not row['registered']
                                      else row['user_id'], axis=1)

logging.info("Reading recommendations...")
if args.provider == "athena":
    # aggregate_pandas_all directly returns a pandas dataframe
    run.recommendations = aggregate_pandas_all(
        rsmetrics_db["recommendations"],
        [
            {"$match":  match_rs},
            {
                "$addFields": {
                    "x": {"$zip": {"inputs": ["$resource_ids",
                                   "$resource_scores"]}}
                }
            },
            {"$unwind": "$x"},
            {
                "$addFields": {
                    "resource_ids": {"$first": "$x"},
                    "resource_scores": {"$last": "$x"},
                }
            },
        ],
    ).iloc[:, 1:-1]

else:

    run.recommendations = \
        pd.DataFrame(list(
            rsmetrics_db["recommendations"].aggregate([
                {"$match": match_rs},
                {"$unwind": "$resource_ids"}])))

# it seems that pymongoarrow returns pandas dataframe with
# convert_dtypes=True, therefore None values are treated as np.nan
# keep only None for further processing
run.recommendations = run.recommendations.replace(np.nan, None)

run.recommendations.rename(columns={'resource_ids': 'resource_id'},
                           inplace=True)

for _col_id in ['aai_uid', 'user_id']:
    if _col_id not in run.recommendations.columns:
        # Create a new column with None values
        run.recommendations[_col_id] = None

# if aai_uid is null then anonymous.
# If anonymous copy the unique_id to aai_uid.
# Thus, all entries have aai_uid (both registered and anonymous)
if not args.legacy:
    run.recommendations['registered'] = run.recommendations.apply(
                                        lambda row: False if
                                        pd.isnull(row['aai_uid'])
                                        else True, axis=1)
    run.recommendations['aai_uid'] = run.recommendations.apply(
                                        lambda row: row['unique_id']
                                        if not row['registered']
                                        else row['aai_uid'], axis=1)
# Same logic but for legacy mode:
# if user_id == -1 then anonymous.
# If anonymous copy 0 to user_id.
# Thus, all entries have user_id >= 0 (both registered and anonymous)
else:
    run.recommendations['registered'] = run.recommendations.apply(
                                        lambda row: False if
                                        row['aai_uid'] == -1
                                        else True, axis=1)
    run.recommendations['user_id'] = run.recommendations.apply(
                                        lambda row: 0
                                        if not row['registered']
                                        else row['user_id'], axis=1)

logging.info("Reading items...")
run.items = pd.DataFrame(
    list(rsmetrics_db["resources"].find(
        {
            "$and": [
                {"provider": args.provider},
                {"type": {"$in":
                          config['service']['category'][args.provider]}},
                {"$or": [{"created_on": {"$lte": args.endtime}},
                         {"created_on": None}]},
                {"$or": [{"deleted_on": {"$gte": args.starttime}},
                         {"deleted_on": None}]},
                {"timestamp": {"$lte": args.endtime}}
                if (args.endtime and not args.ignore_timestamp) else {},
                {"timestamp": {"$gte": args.starttime}}
                if (args.starttime and not args.ignore_timestamp) else {},
            ]
        },
        {"_id": 0}
    ))
)

# from duplicates keep the latest entry
run.items = run.items.sort_values(by='timestamp', ascending=False)
run.items = run.items.drop_duplicates(subset='id', keep='first')


if not args.legacy:
    run.items['id'] = run.items['id'].astype(str)

for _col_id in ['category', 'scientific_domain']:
    if _col_id not in run.items.columns:
        # Create a new column with None values
        run.items[_col_id] = None

# The users dataframe is the users found in the user actions
# that have been matched based on the query filters
# users dataframe is table of two columns (id and accessed resources)
# Accessed resources is all unique service ids (apart from -1, i.e. not known)
# found in both source_resource_id or target_resource_id lists
logging.info("Reading users...")

# aggregate_pandas_all directly returns a pandas dataframe

# get both registered and anonynmous
users_ids = {"$ifNull": ["$aai_uid", "$unique_id"]}

run.users = pd.DataFrame(list(rsmetrics_db["user_actions"].aggregate(
    [
        {"$match":  match_ua},
        {"$group": {
            "_id": '$'+run.id_field if args.legacy else users_ids,
            "source_ids": {"$addToSet": "$source_resource_id"},
            "target_ids": {"$addToSet": "$target_resource_id"}
         }},
        {"$project": {
            "accessed_resources": {
                "$setUnion": [
                    {"$filter": {
                        "input": {"$setUnion": ["$source_ids", "$target_ids"]},
                        "as": "resource_id",
                        "cond": {
                            "$and": [
                                {"$ne": ["$$resource_id", -1]},
                                {"$ne": ["$$resource_id", None]}
                            ]
                        }
                     }}, []]
                }
            }}
        ])))

run.users = run.users.rename(columns={'_id': 'id'})

if args.legacy:
    logging.info("Reading categories...")
    run.categories = pd.DataFrame(
                              list(rsmetrics_db["category"].find({},
                                   {"_id": 0}))
    )

    logging.info("Reading scientific domains...")
    run.scientific_domains = pd.DataFrame(
                              list(rsmetrics_db["scientific_domain"].find({},
                                   {"_id": 0}))
    )

# Filtering collections
try:
    # keeps only registered users
    run.users = run.users[run.users['id'].find_registered(run.schema)]

    # convert timestamp column to datetime object
    run.user_actions_all["timestamp"] = (
        pd.to_datetime(run.user_actions_all["timestamp"])
    )

    # services ids are returned as float with trailing .0,
    # so they are converted to str(int))
    # also ignore None values by assign them '0' and then back to None
    if not args.legacy:
        for res_id_type in ['source_resource_id', 'target_resource_id']:
            run.user_actions_all[res_id_type] = \
                run.user_actions_all[res_id_type].fillna(0).astype(str)
            run.user_actions_all[res_id_type] = \
                run.user_actions_all[res_id_type].apply(lambda x: x[:-2] if
                                                        x[-2:] == '.0' else x)
            run.user_actions_all[res_id_type] = \
                run.user_actions_all[res_id_type].replace('0', None)

    # remove user actions when item does not exist in items' catalog
    # not-known items (i.e. -1 or None) are not excluded
    # (there is no need to do this for users, since users are already
    # built upon user actions)
    # also a source_resource_id or target_resource_id will always be
    # an string (However, [int] -1 or None indicates not known)
    run.user_actions = run.user_actions_all[
        (run.user_actions_all["source_resource_id"]
         .isin(run.items["id"].tolist() + ['-1', -1, None]))
    ]

    run.user_actions = run.user_actions[
        (run.user_actions["target_resource_id"]
         .isin(run.items["id"].tolist() + ['-1', -1, None]))
    ]

    run.recommendations["timestamp"] = (
        pd.to_datetime(run.recommendations["timestamp"])
    )

    # remove recommendations when user or service does not exist in users' or
    # items' catalogs
    # anonymous users (i.e. -1 or None in legacy or current mode respectively)
    # are not excluded
    # not-known items (i.e. -1 or None) are not excluded
    # (having both -1 and None cover both schemas -current or legacy-)
    # meanwhile, current schema can not have -1 while legacy None,
    # so there is no issue to filter both entries concurrently
    run.recommendations = run.recommendations[
        run.recommendations[run.id_field].isin(run.users["id"].tolist() +
                                               [-1, None])
    ]

    run.recommendations["resource_id"] =\
        run.recommendations["resource_id"].astype(str)
    # we have added None which is the new state of unkown resources
    # and -1 for backward compatibility
    run.recommendations = run.recommendations[
        run.recommendations["resource_id"]
        .isin(run.items["id"].tolist() + ['-1', -1, None])
    ]

except Exception as e:
    print(''.join(traceback.format_exception(None, e, e.__traceback__)))
    pass

data_errors = []
if len(run.user_actions) == 0:
    data_errors.append("No user actions found")

if len(run.recommendations) == 0:
    data_errors.append("No recommendations found")

if len(run.items) == 0:
    data_errors.append("No services found")

if len(run.users) == 0:
    data_errors.append("No users found")

if data_errors:
    for data_error in data_errors:
        logging.error(data_error)
    logging.error("Not enough data. Skipping computations!")
    sys.exit(1)

run.provider = args.provider
output = {"timestamp": str(datetime.utcnow())}
metrics = []
statistics = []

# get all function names in metrics module
func_names = list(map(lambda x: x[0], getmembers(m, isfunction)))
# keep all function names except decorators such as metric and statistic
func_names = list(filter(lambda x: not (x == "metric" or x == "statistic"),
                         func_names))

for func_name in func_names:
    # get function based on function name

    func = getattr(m, func_name)
    # if function has attribute kind
    # (which means that evaluates a metric or a static)
    if hasattr(func, "kind"):
        kind = getattr(func, "kind")
        logging.info("Evaluating {}: {}...".format(kind, func_name))
        # execute and get value
        value = func(run)
        documentation = ""
        # if has documentation ge it
        if hasattr(func, "doc"):
            documentation = func.doc
        # prepare json output object with function name, execution result
        # and optional documentation
        item = {"name": func_name, "value": value, "doc": documentation}
        # if metric add it to the metrics list else to the statistics list
        if kind == "metric":
            metrics.append(item)
        elif kind == "statistic":
            statistics.append(item)

# Add the two lists to the final output onject
output["metrics"] = metrics
output["statistics"] = statistics
output["provider"] = args.provider
output["schema"] = run.schema

if args.tag is not None:
    output["name"] = args.provider + " - " + args.tag
else:
    output["name"] = args.provider

output["errors"] = run.errors

# this line is necessary in order to store the output to MongoDB
jsonstr = json.dumps(output, indent=4)

# keep one metrics collection per schema
rsmetrics_db["metrics"].delete_many({"name": output["name"]})

rsmetrics_db["metrics"].insert_one(output)

# result in stdout console (not in logs)
print(jsonstr)
logging.info("Metrics computation finished for {}...".format(
    args.provider))
