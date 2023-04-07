import os
import json
import boto3
import logging
import argparse

from shared.nodes import Account, Region
from shared.common import query_aws, get_regions, parse_arguments, get_parameter_file
from datetime import datetime, date, timedelta
from shared.common import get_account, custom_serializer
from commands.collect import get_filename_from_parameter
from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError

def make_directory(path):
    try:
        os.mkdir(path)
    except OSError:
        # Already exists
        pass

def snakecase(s):
    return s.replace("-", "_")

def get_sqs_queue_metrics(arguments, accounts, config):
    logging.getLogger("botocore").setLevel(logging.WARN)
    outputfile = "account-data/prod/us-west-2/sqs-list-queues.json"

    default_region = os.environ.get("AWS_REGION", "us-east-1")
    # regions_filter = None
    # if len(arguments.regions_filter) > 0:
    #     regions_filter = arguments.regions_filter.lower().split(",")
    #     # Force include of default region -- seems to be required
    #     if default_region not in regions_filter:
    #         regions_filter.append(default_region)

    session_data = {"region_name": default_region}

    if arguments.profile:
        session_data["profile_name"] = arguments.profile

    session = boto3.Session(**session_data)

    sts = session.client("sts")
    try:
        sts.get_caller_identity()
        logging.debug("Using AWS account: {}".format(sts.get_caller_identity()["Account"]))
    except ClientError as e:
        if "InvalidClientTokenId" in str(e):
            print(
                "ERROR: sts.get_caller_identity failed with InvalidClientTokenId. Likely cause is no AWS credentials are set.",
                flush=True,
            )
            exit(-1)
        else:
            print(
                "ERROR: Unknown exception when trying to call sts.get_caller_identity: {}".format(
                    e
                ),
                flush=True,
            )
            exit(-1)

    client = session.client('cloudwatch')
    parameters = {
        'MetricDataQueries': [{
            'Id': 'id_0',
            'MetricStat': {
                'Metric': {
                    'Namespace': 'AWS/SQS',
                    'MetricName': 'NumberOfMessagesReceived',
                    'Dimensions': [
                        {
                            'Name': 'QueueName',
                            #'Value': 'hsl-activities-migration'
                        },
                    ],
                },
                'Period': 86400,
                'Stat': 'Sum'
            },
            'ReturnData': True,
        }],
        'StartTime': datetime.utcnow() - timedelta(weeks=4),
        'EndTime': datetime.utcnow(),
    }

    #response = client.get_metric_data(**parameters)
    #print('Metric Total: ', sum(response['MetricDataResults'][0]['Values']))

    # parameter file is loaded into saved_sqs_list
    sqs_dynamic_param = "QueueUrl"

    for account in accounts:
        for region_json in get_regions(Account(None, account)):
            region = Region(Account(None, account), region_json)
            print(f"Region: {region.name}")
            saved_sqs_list = query_aws(region.account, "sqs-list-queues", region=region)
            sqs_attributes_filepath = "account-data/{}/{}/{}-{}".format(
                arguments.profile, region.name, "sqs", "get-queue-attributes"
            )

            if 'QueueUrls' in saved_sqs_list:
                #print(saved_sqs_list['QueueUrls'][0])
                queue_url = saved_sqs_list['QueueUrls'][0]
                saved_sqs_details = get_parameter_file(region, "sqs", "get-queue-attributes", queue_url)
                print(saved_sqs_details)

                queue_arn = saved_sqs_details['Attributes']['QueueArn']
                queue_name = saved_sqs_details['Attributes']['QueueArn'].split(':')[-1]
                print(f"Queue Name: {queue_name}")

                if "MessagesReceivedInLastMonth" not in saved_sqs_details['Attributes']:
                    parameters['MetricDataQueries'][0]['MetricStat']['Metric']['Dimensions'][0]['Value'] = queue_name
                    response = client.get_metric_data(**parameters)
                    messages_received = sum(response['MetricDataResults'][0]['Values'])
                    print('Messages Received in Last Month: ', messages_received)
                    saved_sqs_details['Attributes']['MessagesReceivedInLastMonth'] = messages_received

                    # This is the filename we want to save the data to
                    #print(get_filename_from_parameter(queue_url))
                    filename = get_filename_from_parameter(queue_url)
                    outputfile = "{}/{}".format(sqs_attributes_filepath, filename)
                    print(f"Output File: {outputfile}")
                    if saved_sqs_details is not None:
                        with open(outputfile, "w+") as f:
                            f.write(
                                json.dumps(saved_sqs_details, indent=4, sort_keys=True, default=custom_serializer)
                            )



def run(arguments):

    args, accounts, config = parse_arguments(arguments)
    get_sqs_queue_metrics(args, accounts, config)
