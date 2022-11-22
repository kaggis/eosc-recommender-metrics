#!/usr/bin/env python3
import sys, argparse
import yaml
import pymongo
from datetime import datetime
import os
import pandas as pd
from inspect import getmembers, isfunction

# local lib
import reward_mapping as rm

__copyright__ = "© "+str(datetime.utcnow().year)+", National Infrastructures for Research and Technology (GRNET)"
__status__ = "Production"
__version__ = "1.0.2"


os.environ['COLUMNS'] = "90"

def print_help(func):
    def inner():
        print("""
  _____                                                       
 |  __ \                                                      
 | |__) | __ ___ _ __  _ __ ___   ___ ___  ___ ___  ___  _ __ 
 |  ___/ '__/ _ \ '_ \| '__/ _ \ / __/ _ \/ __/ __|/ _ \| '__|
 | |   | | |  __/ |_) | | | (_) | (_|  __/\__ \__ \ (_) | |   
 |_|   |_|  \___| .__/|_|  \___/ \___\___||___/___/\___/|_|   
                | |                                           
                |_|                                           
""")
        print('Version: ' + __version__)
        print( __copyright__+'\n')
        func()
    return inner

def remove_service_prefix(text):
    """Removes '/service/' prefix from eosc service paths

    Args:
        text (string): string containing a service path

    Returns:
        string: service path without the /service/ prefix
    """
    if text.startswith('/service/'):
        return text[len('/service/'):]
    return text

parser = argparse.ArgumentParser(prog='preprocessor', description='Prepare data for the EOSC Marketplace RS metrics calculation', add_help=False)
parser.print_help=print_help(parser.print_help)
parser._action_groups.pop()
required = parser.add_argument_group('required arguments')
optional = parser.add_argument_group('optional arguments')

optional.add_argument('-c', '--config', metavar=('FILEPATH'), help='override default configuration file (./config.yaml)', nargs='?', default='./config.yaml', type=str)
optional.add_argument('-o', '--output', metavar=('DIRPATH'), help='override default output dir path (./data)', nargs='?', default='./data', type=str)
optional.add_argument('-p', '--provider', metavar=('DIRPATH'), help='source of the data based on providers specified in the configuration file', nargs='?', default='cyfronet', type=str)
optional.add_argument('-s', '--starttime', metavar=('DATETIME'), help='process data starting from given datetime in ISO format (UTC) e.g. YYYY-MM-DD', nargs='?', default=None)
optional.add_argument('-e', '--endtime', metavar=('DATETIME'), help='process data ending to given datetime in ISO format (UTC) e.g. YYYY-MM-DD', nargs='?', default=None)

optional.add_argument('-h', '--help', action='help', help='show this help message and exit')
optional.add_argument('-V', '--version', action='version', version='%(prog)s v'+__version__)

#args=parser.parse_args(args=None if sys.argv[1:] else ['--help'])

args=parser.parse_args()

class Mock:
    pass

class User_Action:
    def __init__(self, source_page_id, target_page_id, order):
        self.source=Mock()
        self.target=Mock()
        self.action=Mock()
        self.source.page_id=source_page_id
        self.target.page_id=target_page_id
        self.action.order=order

reward_mapping = {
        "order": 1.0,
        "interest": 0.7,
        "mild_interest": 0.3,
        "simple_transition": 0.0,
        "unknown_transition": 0.0,
        "exit": 0.0,
    }

query={'timestamp':{'$lt':datetime.utcnow()}}

if args.starttime:
    args.starttime=datetime.fromisoformat(args.starttime)
    query['timestamp']['$gte']=args.starttime

if args.endtime:
    args.endtime=datetime.fromisoformat(args.endtime)
    query['timestamp']['$lt']=args.endtime

if args.starttime and args.endtime:
    if args.endtime<args.starttime:
        print('End date must be older than start date')
        sys.exit(0)


with open(args.config, 'r') as _f:
    config=yaml.load(_f, Loader=yaml.FullLoader)


os.makedirs(args.output, exist_ok=True)

provider=None
for p in config["providers"]:
    if args.provider == p['name']:
        provider=p

if not provider:
    print('Given provider not in configuration')
    sys.exit(0)

# connect to internal db server for reading users and resources
datastore = pymongo.MongoClient(config["datastore"], uuidRepresentation='pythonLegacy')
# use db
rsmetrics_db = datastore[config["datastore"].split('/')[-1]]

# reading resources to be used for filtering user_actions
resources=pd.DataFrame(list(rsmetrics_db["resources"].find({"provider": {"$in": [args.provider]}}))).iloc[:,1:]
resources.columns=["Service", "Name", "Page", "Created_on", "Deleted_on", "Type", "Provider", "Ingestion"]
resources=pd.Series(resources['Service'].values,index=resources['Page']).to_dict()

# connect to external db server for reading user_actions and recommendations
myclient = pymongo.MongoClient(provider['db'], uuidRepresentation='pythonLegacy')
# use db
recdb = myclient[provider['db'].split('/')[-1]]

# reward_mapping.py is modified so that the function
# reads the Transition rewards csv file once
# consequently, one argument has been added to the
# called function
ROOT_DIR='./'

TRANSITION_REWARDS_CSV_PATH = os.path.join(
    ROOT_DIR, "resources", "transition_rewards.csv"
)
transition_rewards_df = pd.read_csv(TRANSITION_REWARDS_CSV_PATH, index_col="source")

luas=[]
col='user_actions' if provider['name'] == 'athena' else 'user_action'
for ua in recdb[col].find(query).sort("user"):
    # set -1 to anonymous users
    try:
        user=ua['user']
    except:
        user=-1

    # process data that map from page id to service id exist
    # for both source and target page ids
    # if not set service id to -1
    try:
        _pageid="/"+"/".join(ua['source']['page_id'].split('/')[1:3])
        source_service_id=resources[_pageid]
    except:
        source_service_id=-1

    try:
        _pageid="/"+"/".join(ua['target']['page_id'].split('/')[1:3])
        target_service_id=resources[_pageid]
    except:
        target_service_id=-1

    # function has been modified where one more argument is given
    # in order to avoid time-consuming processing of reading csv file
    # for every func call
    symbolic_reward=rm.ua_to_reward_id(transition_rewards_df,
                                       User_Action(ua['source']['page_id'],
                                                   ua['target']['page_id'],
                                                   ua['action']['order']))

    reward=reward_mapping[symbolic_reward]

    luas.append({'user_id':int(user),
                 'source_resource_id':int(source_service_id), 
                 'target_resource_id':int(target_service_id), 
                 'reward':float(reward), 
                 'panel':ua['source']['root']['type'], 
                 'timestamp':ua['timestamp'], 
                 'source_path':ua['source']['page_id'], 
                 'target_path':ua['target']['page_id'],
                 'type': 'service', # currently, static
                 'provider': provider['name'], # currently, static
                 'ingestion': 'batch', # currently, static 
                })

recs=[]

if provider['name'] == 'cyfronet':
    for rec in recdb["recommendation"].find(query).sort("user"):
        try:
            user=rec['user']
        except:
            user=-1

        recs.append({'user_id':int(user),
                 'resource_ids': rec['services'],
                 'timestamp':rec['timestamp'], 
                 'type': 'service', # currently, static
                 'provider': provider['name'], # currently, static
                 'ingestion': 'batch', # currently, static 
                 })

elif provider['name'] == 'athena':
    _query=query.copy()
    _query['date'] = _query.pop('timestamp')
    for rec in recdb["recommendation"].find(_query).sort("user_id"):
        recs.append({'user_id':int(rec['user_id']),
                 'resource_ids': list(map(lambda x: x['service_id'],rec['recommendation'])),
                 'resource_scores': list(map(lambda x: x['score'],rec['recommendation'])),
                 'timestamp':rec['date'], 
                 'type': 'service', # currently, static
                 'provider': provider['name'], # currently, static
                 'ingestion': 'batch', # currently, static 
                 })

# store data to Mongo DB

rsmetrics_db["user_actions"].delete_many({"provider":provider['name'], "ingestion":'batch'})
rsmetrics_db["user_actions"].insert_many(luas)

rsmetrics_db["recommendations"].delete_many({"provider":provider['name'], "ingestion":'batch'})
rsmetrics_db["recommendations"].insert_many(recs)

