#!/usr/bin/env python3
import os, sys, argparse
import yaml
import pymongo
from datetime import datetime
import natsort as ns
from natsort import natsorted
from inspect import getmembers, isfunction

# local lib
from get_service_catalog import get_eosc_marketplace_url, get_service_catalog_items, get_service_catalog_page_content, save_service_items_to_csv

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
optional.add_argument('-p', '--provider', metavar=('DIRPATH'), help='source of the data based on providers specified in the configuration file', nargs='?', default='cyfronet', type=str)
optional.add_argument('-s', '--starttime', metavar=('DATETIME'), help='process data starting from given datetime in ISO format (UTC) e.g. YYYY-MM-DD', nargs='?', default=None)
optional.add_argument('-e', '--endtime', metavar=('DATETIME'), help='process data ending to given datetime in ISO format (UTC) e.g. YYYY-MM-DD', nargs='?', default=None)
optional.add_argument('--use-cache', help='Use the specified file in configuration as the file to read resources', action='store_true')
optional.add_argument('-h', '--help', action='help', help='show this help message and exit')
optional.add_argument('-V', '--version', action='version', version='%(prog)s v'+__version__)

#args=parser.parse_args(args=None if sys.argv[1:] else ['--help'])

args=parser.parse_args()

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

provider=None
for p in config["providers"]:
    if args.provider == p['name']:
        provider=p

if not provider:
    print('Given provider not in configuration')
    sys.exit(0)

# connect to db server
myclient = pymongo.MongoClient(provider['db'], uuidRepresentation='pythonLegacy')

# use db
recdb = myclient[provider['db'].split('/')[-1]]

# automatically associate page ids to service ids
# default to no caching
if not args.use_cache:
    eosc_url = get_eosc_marketplace_url()
    print("Retrieving page: marketplace list of services... \nGrabbing url: {0}".format(eosc_url))
    eosc_page_content = get_service_catalog_page_content(eosc_url)
    print("Page retrieved!\nGenerating results...")
    eosc_service_results = get_service_catalog_items(eosc_page_content)
 
    if config['service']['store']:
        # output to csv
        save_service_items_to_csv(eosc_service_results, config['service']['store'])
        print("File written to {}".format(config['service']['store']))

# if cache file is used
else:
    with open(config['service']['store'], 'r') as f:
        lines=f.readlines()

    eosc_service_results=list(map(lambda x: x.split(','),lines))

# read map file and save in dict
keys=list(map(lambda x: remove_service_prefix(x[-1]).strip(), eosc_service_results))
ids=list(map(lambda x: str(x[0]),eosc_service_results))
names=list(map(lambda x: x[1],eosc_service_results))

rdmap=dict(zip(ids,zip(keys,names)))

# produce users csv with each user id along with the user's accessed services
# query users from database for fields _id and accessed_services then create a list of rows
# each rows contains two elements, first: user_id in string format and second: a space separated sorted list of accessed services 
users = recdb['user'].find({},{'accessed_services':1})
users = list(map(lambda x: {'id':int(str(x['_id'])),
                            'accessed_resources': sorted(set(x['accessed_services'])),
                            'created_on': None,
                            'deleted_on': None,
                            'provider': ['cyfronet', 'athena'], # currently, static
                            'ingestion': 'batch', # currently, static 
                           }, users))

if config['service']['from']=='page_map':

    _ss=natsorted(list(set(list(map(lambda x: x+'\n',ids)))),alg=ns.ns.SIGNED)
    resources=[]
    for s in _ss:
        try:
            #ss.append(s.strip()+',"'+rdmap[s.strip()][1]+'",'+rdmap[s.strip()][0]+'\n')
            resources.append({'id':int(s.strip()),
                       'name':rdmap[s.strip()][1],
                       'path':rdmap[s.strip()][0],
                       'created_on': None,
                       'deleted_on': None,
                       'type': 'service', # currently, static
                       'provider': ['cyfronet', 'athena'], # currently, static
                       'ingestion': 'batch', # currently, static
                       })
        except:
            continue

else: # 'source'
    _query=""
    if config['service']['published']:
        _query={"status":"published"}
 
    _ss=natsorted(list(set(list(map(lambda x: str(x['_id'])+',"'+str(x['name'])+'"\n',recdb["service"].find(_query))))),alg=ns.ns.SIGNED)
    resources=[]
    for s in _ss:
        try:
            #ss.append(s.strip()+','+rdmap[s.split(',')[0]]+'\n')
            resources.append({'id':int(s.split(',')[0]),
                       'name':rdmap[s.split(',')[0]][1],
                       'path':rdmap[s.split(',')[0]][0],
                       'created_on': None,
                       'deleted_on': None,
                       'type': 'service', # currently, static
                       'provider': ['cyfronet', 'athena'], # currently, static
                       'ingestion': 'batch', # currently, static
                       })

        except:
            continue

# store data to Mongo DB

# connect to db server
datastore = pymongo.MongoClient(config["datastore"], uuidRepresentation='pythonLegacy')

# use db
rsmetrics_db = datastore[config["datastore"].split('/')[-1]]

rsmetrics_db["users"].delete_many({"provider": {"$in": ["cyfronet"]}, "provider": {"$in": ["athena"]}, "ingestion":'batch'})
rsmetrics_db["users"].insert_many(users)

rsmetrics_db["resources"].delete_many({"provider": {"$in": ["cyfronet"]}, "provider": {"$in": ["athena"]}, "ingestion":'batch'})
rsmetrics_db["resources"].insert_many(resources)

