#!/usr/bin/env python3

import time
import stomp
import argparse
import sys
import json
import logging
import ssl
import pymongo
import dateutil.parser
from datetime import datetime
import re
import pandas as pd
import reward_mapping as rm

# mapping recommendations
rec_map = {'publications': 'publication', 'datasets': 'dataset',
           'software': 'software', 'services': 'service',
           'trainings': 'training', 'other_research_product': 'other'}


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


# Streaming connector using stomp protocol to ingest data from rs databus

# establish basic logging
logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format="[%(asctime)s] %(levelname)s %(message)s",
)


def connect_subscribe(msg_queue, username, password, topic):
    msg_queue.connect(username, password, wait=True)
    # subscribe to the topic
    msg_queue.subscribe(destination="/topic/" + topic, id=1,
                        ack="auto")


def main(args):

    # reward_mapping.py is modified so that the function
    # reads the Transition rewards csv file once
    # consequently, one argument has been added to the
    # called function
    transition_rewards_df = pd.read_csv(args.rewards,
                                        index_col="source")

    # extract provider arg
    provider = args.provider

    # init resource_lookup dictionary where we store service paths to
    # corresponding ids
    resource_lookup = {}
    reverse_resource_lookup = {}

    # Create a listener class to react on each message received from the queue
    class UserActionsListener(stomp.ConnectionListener):
        def __init__(self, conn):
            self.conn = conn

        # In case of error log it along with the message
        def on_error(self, frame):
            logging.error("error occured {}".format(frame.body))

        def on_disconnect(self):
            logging.warning("disconnected ...trying to reconnect")
            connect_subscribe(self.conn)

        def on_message(self, frame):
            # process the message
            message = json.loads(json.loads(frame.body))

            panel = "other"
            target_path = ""
            source_path = ""
            target_resource_id = -1
            source_resource_id = -1
            # in current mode there should be not found user_id=-1
            # a -1 indicates a legacy mode,
            # since it is current user_id is set with None
            user_id = None
            aai_uid = None
            unique_id = None
            if "user_id" in message:
                user_id = message["user_id"]

            if "aai_uid" in message:
                aai_uid = message["aai_uid"] if not message["aai_uid"] == "" \
                                             else None

            if "unique_id" in message:
                unique_id = str(message["unique_id"])

            if "source" in message:
                if "page_id" in message["source"]:
                    source_path = message["source"]["page_id"]

                if "root" in message["source"]:
                    if "type" in message["source"]["root"]:
                        panel = message["source"]["root"]["type"]

            if "target" in message:
                if "page_id" in message["target"]:
                    target_path = message["target"]["page_id"]

            # check if action comes from the search page
            # then correct the target_path which includes the EOSC Marketplace
            # website with the actual service's landing page based on the
            # resource_lookup (the reversed version)
            pattern = r"search%2F(?:all|dataset|software|service" + \
                      "|data-source|training|guideline|other)"
            if re.findall(pattern, source_path):
                if message["source"]["root"]["resource_type"] == 'service':
                    source_path = "/services"
                    _id = int(message["source"]["root"]["resource_id"])
                    target_path = reverse_resource_lookup[_id]

            # if path exist in resource lookup dictionary,
            # identify the resource id
            if target_path in resource_lookup:
                target_resource_id = resource_lookup[target_path]

            # if path exist in resource lookup dictionary,
            # identify the resource id
            if source_path in resource_lookup:
                source_resource_id = resource_lookup[source_path]

            # function has been modified where one more argument is given
            # in order to avoid time-consuming processing of reading csv file
            # for every func call
            symbolic_reward = rm.ua_to_reward_id(
                transition_rewards_df,
                User_Action(
                    source_path.rstrip('/'),
                    target_path.rstrip('/'),
                    message["action"]["order"]),
                )

            reward = reward_mapping[symbolic_reward]

            record = {
                "timestamp": dateutil.parser.isoparse(message["timestamp"]),
                "user_id": user_id,
                "aai_uid": aai_uid,
                "unique_id": unique_id,
                "panel": panel,
                "target_path": target_path,
                "source_path": source_path,
                "target_resource_id": target_resource_id,
                "source_resource_id": source_resource_id,
                "reward": float(reward),
                "type": "service",
                "ingestion": "stream",
                "provider": provider,
            }

            rsmetrics_db["user_actions"].insert_one(record)

    # Create a listener class to react on each message received from the queue
    class UserEventsListener(stomp.ConnectionListener):
        def __init__(self, conn):
            self.conn = conn

        # In case of error log it along with the message
        def on_error(self, frame):
            logging.error("error occured {}".format(frame.body))

        def on_disconnect(self):
            logging.warning("disconnected ...trying to reconnect")
            connect_subscribe(self.conn)

        def on_message(self, frame):
            # process the message
            message = json.loads(frame.body)

            # Handle user and update users collection
            if message['model'] == 'User':
                # retrieve user id
                user = int(message['record']['id'])

                # Update user info
                # if user is deleted, update entry as in update,
                # but also set deleted_on with timestamp
                if message['cud'] == 'update' or message['cud'] == 'delete':
                    record = {'accessed_resources':
                              sorted(set(
                                  message['record']["accessed_services"])),
                              'deleted_on': datetime.fromisoformat(
                                  message['timestamp'].replace('Z', '+00:00'))
                              if message['cud'] == 'delete' else None,
                              'provider': ['cyfronet', 'athena'],
                              'ingestion': 'stream'}

                    # a connection has already been established at main
                    result = rsmetrics_db['users'].update_one({'id': user},
                                                              {'$set': record})
                    if result.matched_count == 1:
                        logging.info("The user {} was successfully {}d".format(
                            user, message['cud']))

                # Create user record
                elif message['cud'] == 'create':
                    record = {'id': user,
                              'accessed_resources': sorted(set(
                                  message['record']["accessed_services"])),
                              'created_on': datetime.fromisoformat(
                                  message['timestamp'].replace('Z', '+00:00')),
                              'deleted_on': None,
                              'provider': ['cyfronet', 'athena'],
                              'ingestion': 'stream'}

                    # a connection has already been established at main
                    result = rsmetrics_db['users'].insert_one(record)
                    if result.acknowledged == 1:
                        logging.info("The user {} was successfully \
                            created".format(user))

                else:
                    logging.info("Unknown Type of resource's state")

                # add user info to streaming collection too
                rsmetrics_db['user_events_streaming'].insert_one(message)

            # Handle resource and update resources collection
            elif message['model'] == 'Service':
                # retrieve resource id
                resource = int(message['record']['id'])

                # Update resource info
                # if resource is deleted, update entry as in update,
                # but also set deleted_on with timestamp
                if message['cud'] == 'update' or message['cud'] == 'delete':
                    record = {'name': message['record']['name'],
                              'deleted_on': datetime.fromisoformat(
                                  message['timestamp'].replace('Z', '+00:00'))
                              if message['cud'] == 'delete' else None,
                              'scientific_domain':
                              message['record']['scientific_domains'],
                              'category':
                              message['record']['categories'],
                              'type': 'service',
                              'provider': ['cyfronet', 'athena'],
                              'ingestion': 'stream'}

                    # a connection has already been established at main
                    result = rsmetrics_db['resources'].update_one(
                                                              {'id': resource},
                                                              {'$set': record})
                    if result.matched_count == 1:
                        logging.info("The resource {} was successfully\
                            {}d".format(
                            resource, message['cud']))

                # Create resource record
                elif message['cud'] == 'create':
                    record = {'id': resource,
                              'name': message['record']['name'],
                              'created_on': datetime.fromisoformat(
                                  message['timestamp'].replace('Z', '+00:00')),
                              'deleted_on': None,
                              'scientific_domain':
                              message['record']['scientific_domains'],
                              'category': message['record']['categories'],
                              'type': 'service',
                              'provider': ['cyfronet', 'athena'],
                              'ingestion': 'stream'}

                    # a connection has already been established at main
                    result = rsmetrics_db['resources'].insert_one(record)
                    if result.acknowledged == 1:
                        logging.info("The resource {} was successfully \
                            created".format(resource))

                else:
                    logging.info("Unknown Type of resource's state")

                # add resource info to streaming collection too
                rsmetrics_db['service_events_streaming'].insert_one(message)

            else:
                rsmetrics_db['other_events_streaming'].insert_one(message)

    # Create a listener class to react on each message received from the queue
    class RecommendationsListener(stomp.ConnectionListener):
        def __init__(self, conn):
            self.conn = conn

        # In case of error log it along with the message
        def on_error(self, frame):
            logging.error("error occured {}".format(frame.body))

        def on_disconnect(self):
            logging.warning("disconnected ...trying to reconnect")
            connect_subscribe(self.conn)

        def on_message(self, frame):
            # process the message
            message = json.loads(frame.body)
            # in current mode there should be not found user_id=-1
            # a -1 indicates a legacy mode,
            # since it is current user_id is set with None
            user_id = None
            aai_uid = None
            unique_id = None
            if "user_id" in message["context"]:
                user_id = message["context"]["user_id"]

            if "aai_uid" in message["context"]:
                aai_uid = message["context"]["aai_uid"]
                aai_uid = message["context"]["aai_uid"] if not \
                    message["context"]["aai_uid"] == "" \
                    else None

            if "unique_id" in message["context"]:
                unique_id = str(message["context"]["unique_id"])

            # handle data accordingly
            if provider == "cyfronet":
                record = {
                    "timestamp": dateutil.parser.isoparse(
                                 message["context"]["timestamp"]),
                    "user_id": user_id,
                    "aai_uid": aai_uid,
                    "unique_id": unique_id,
                    "resource_ids": message["recommendations"],
                    "type": rec_map[message["panel_id"]],
                    "ingestion": "stream",
                    "provider": provider,
                }

            else:
                logging.info("Currently, only recommendations from \
                                 Cyfronet are supported")

            rsmetrics_db["recommendations"].insert_one(record)

    # connect to the datastore
    mongo = pymongo.MongoClient(args.datastore,
                                uuidRepresentation="pythonLegacy")

    rsmetrics_db = mongo[args.datastore.split("/")[-1]]

    username, password = args.auth.split(":")
    host, port = args.queue.split(":")

    # get resources (services for the time being)
    resources = rsmetrics_db["resources"].find({}, {"_id": 0,
                                               "path": 1, "id": 1})
    for item in resources:
        # if the service item has indeed an updated path (url) grab it
        if "path" in item:
            resource_lookup[item["path"]] = item["id"]
            reverse_resource_lookup[item["id"]] = item["path"]

    # create the connection to the queue
    msg_queue = stomp.Connection([(host, port)], heartbeats=(10000, 5000))
    msg_queue.set_ssl(for_hosts=[(host, port)], ssl_version=ssl.PROTOCOL_TLS)

    # Check what kind of resource_type is going to be used
    if args.data_type == "user_actions":
        msg_queue.set_listener("", UserActionsListener(msg_queue))
    elif args.data_type == "mp_db_events":
        msg_queue.set_listener('', UserEventsListener(msg_queue))
    elif args.data_type == "recommendations":
        msg_queue.set_listener('', RecommendationsListener(msg_queue))
    else:
        logging.error(
            "{} is not a supported ingestion data type".format(args.data_type)
        )
        sys.exit(1)

    connect_subscribe(msg_queue, username, password, args.data_type)

    while True:
        time.sleep(2)
        if not msg_queue.is_connected():
            logging.warning("disconnected ...trying to reconnect")
            connect_subscribe(msg_queue, username, password, args.data_type)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="RS Stream Connector")
    parser.add_argument(
        "-a",
        "--auth",
        metavar="STRING",
        help="authentication in the form username:password",
        required=True,
        dest="auth",
    )
    parser.add_argument(
        "-q",
        "--queue",
        metavar="STRING",
        help="queue in the form host:port",
        required=True,
        dest="queue",
    )
    parser.add_argument(
        "-t",
        "--type",
        metavar="STRING",
        help=("type of data to be ingested "
              "(e.g. user_actions, recommendations, mp_db_events)"),
        required=True,
        dest="data_type",
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
        "-p",
        "--provider",
        metavar="STRING",
        help="name of the provider",
        required=True,
        dest="provider",
    )

    parser.add_argument(
        "-r",
        "--rewards",
        metavar="STRING",
        help="path to transition rewards info file",
        required=True,
        dest="rewards",
    )

    # Pass the arguments to main method
    sys.exit(main(parser.parse_args()))
