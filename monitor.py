#!/usr/bin/env python3
import sys
import argparse
import pymongo
import logging
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.parse import urlparse
from dateutil.relativedelta import relativedelta
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter

# establish basic logging
logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format="[%(asctime)s] %(levelname)s %(message)s",
)


class Logger:
    def __init__(self):
        self.text = ""

    def info(self, message):
        logging.info(message)
        self.text += message + "\n"

    def error(self, message):
        logging.error(message)
        self.text += message + "\n"


def send_email(sender_email, recipients, subject, body, smtp_info):
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = ', '.join(recipients)
    message["Subject"] = subject
    message.attach(MIMEText(body, "plain"))

    with smtplib.SMTP(smtp_info['hostname'], smtp_info['port']) as server:
        server.starttls()

        if smtp_info['username'] and smtp_info['password']:
            server.login(smtp_info['username'], smtp_info['password'])

        server.sendmail(sender_email, recipients, message.as_string())

    print("Email sent successfully.")


def plot(df):

    df['date'] = pd.to_datetime(df[['year', 'month']].assign(day=1))

    # Create subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8), sharex=True)

    # Plot for User Actions
    ax1.plot(df['date'], df['user_actions'], marker='o', color='blue')
    ax1.set_ylabel('Count')
    ax1.grid(True)
    ax1.set_title('Total User Actions')

    # Plot for Recommendations
    ax2.plot(df['date'], df['recommendations'], marker='o', color='orange')
    ax2.set_ylabel('Count')
    ax2.grid(True)
    ax2.set_title('Total Recommendations')

    # Format x-axis ticks as YEAR-MONTH
    date_format = DateFormatter('%Y-%m')
    plt.gca().xaxis.set_major_formatter(date_format)
    plt.gca().xaxis.set_major_locator(plt.MaxNLocator(
                                      len(df['date'].unique())))

    # Rotate x-axis labels for better visibility
    plt.gcf().autofmt_xdate()

    min_date = df['date'].min().strftime('%Y_%m')
    max_date = df['date'].max().strftime('%Y_%m')

    # Show the plot
    plt.savefig('capacity_{}_{}.pdf'.format(min_date, max_date),
                bbox_inches='tight')


def capacity(args, db):

    data = []

    current = args.starttime
    while current <= args.endtime:
        current = current.replace(day=1)
        next_current = current + relativedelta(months=1)

        time_filter = {}
        time_filter["timestamp"] = {}
        time_filter["timestamp"]["$gte"] = current
        time_filter["timestamp"]["$lt"] = next_current

        data.append([current.year,
                     current.month,
                     db["user_actions"].count_documents(time_filter),
                     db["recommendations"].count_documents(time_filter)])

        # Move to the next month
        current = next_current

    df = pd.DataFrame(data, columns=['year', 'month', 'user_actions',
                                     'recommendations'])

    # Set display options to show all rows and columns
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    print(df)

    if args.plot:
        plot(df)


def main(args):

    # create the logger
    logger = Logger()

    try:
        # connect to the datastore
        mongo = pymongo.MongoClient(args.datastore,
                                    uuidRepresentation="pythonLegacy")
        rsmetrics_db = mongo[args.datastore.split("/")[-1]]

        # check if datastore is alive
        if mongo.rsmetrics_db.command('ping') == {u'ok': 1.0}:
            logger.info("Connected succesfully to {}".format(args.datastore))

    except Exception as e:
        logger.error("Cannot connect to {}: {}".format(args.datastore, e))
        return

    time_filter = {}

    if args.starttime:
        args.starttime = datetime.fromisoformat(args.starttime)
        if "timestamp" not in time_filter:
            time_filter["timestamp"] = {}
            time_filter["timestamp"]["$gte"] = args.starttime

    if args.endtime:
        edt = datetime.fromisoformat(args.endtime)
        args.endtime = datetime.combine(edt, datetime.min.time())
        if "timestamp" not in time_filter:
            time_filter["timestamp"] = {}
            time_filter["timestamp"]["$lt"] = args.endtime

    if args.starttime and args.endtime:
        if args.endtime < args.starttime:
            logger.error("End date must be older than start date")
            return

    logger.info("Searching for the period {} - {}".format(args.starttime,
                                                          args.endtime))

    if args.capacity:
        capacity(args, rsmetrics_db)
        return

    for col in args.collection:
        try:
            doc_count = rsmetrics_db[col].count_documents(time_filter)

            logger.info("> Collection '{}' has {} entries".format(col,
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

                logger.info("\t* '{}'\thas {} entries".format(item_type,
                                                              count))

        except Exception as e:
            logger.error("Cannot retrieve entries from collection '{}'\n{}"
                         .format(col, e))

    if args.email:
        smtp_info = parse_smtp_uri(args.smtp_uri)
        send_email(args.sender_email, args.recipients,
                   'RSeval Report for {}'.format(args.starttime
                                                 .strftime("%Y-%m-%d")),
                   logger.text, smtp_info)


def parse_smtp_uri(smtp_uri):
    parsed_uri = urlparse(smtp_uri)

    if parsed_uri.scheme != 'smtp':
        raise ValueError('Invalid SMTP URI. Scheme must be "smtp".')

    username, password = None, None
    if parsed_uri.username:
        username = parsed_uri.username
        if parsed_uri.password:
            password = parsed_uri.password

    return {
        'hostname': parsed_uri.hostname,
        'port': parsed_uri.port,
        'username': username,
        'password': password
    }


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

    # Add optional argument to enable email-related arguments
    parser.add_argument('--email', action='store_true',
                        help='Send email using SMTP URI')

    # Create a mutually exclusive group for email-related arguments
    email_group = parser.add_argument_group('Email Options')

    # Add email-related arguments to the group
    email_group.add_argument('smtp_uri', nargs='?',
                             help='SMTP URI for the mail server')
    email_group.add_argument('sender_email', nargs='?',
                             help='Sender email address')
    email_group.add_argument('recipients', nargs='*',
                             help='Recipient email addresses (at least one)')

    # Add optional argument to enable email-related arguments
    parser.add_argument('--capacity', action='store_true',
                        help='Export output for capacity info. CSV format of \
                              YEAR, MONTH, TOTAL USER ACTIONS, TOTAL \
                              RECOMMENDATIONS')

    capacity_group = parser.add_argument_group('Capacity Options')
    capacity_group.add_argument('--plot', action='store_true',
                                help='Plot to file')

    # Pass the arguments to main method
    sys.exit(main(parser.parse_args()))
