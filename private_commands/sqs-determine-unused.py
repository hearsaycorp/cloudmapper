import boto3
import json
import logging
import os

from botocore.exceptions import ClientError
from commands.collect import get_filename_from_parameter
from datetime import datetime, timedelta
from shared.common import query_aws, get_regions, parse_arguments, get_parameter_file, custom_serializer
from shared.nodes import Account, Region

def make_directory(path):
    try:
        os.mkdir(path)
    except OSError:
        # Already exists
        pass

def snakecase(s):
    return s.replace("-", "_")

def get_sqs_queue_metrics_and_tags(arguments, accounts, config):
    logging.getLogger("botocore").setLevel(logging.WARN)

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

    sqs = session.client('sqs')
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
                            'Name': 'QueueName'
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

    for account in accounts:
        # get_regions reads the file at account-data/{profile}/describe-regions.json
        for region_json in get_regions(Account(None, account)):
            region = Region(Account(None, account), region_json)
            print(f"Region: {region.name}")

            saved_sqs_list = query_aws(region.account, "sqs-list-queues", region=region)
            sqs_attributes_filepath = "account-data/{}/{}/{}-{}".format(
                arguments.profile, region.name, "sqs", "get-queue-attributes"
            )

            if 'QueueUrls' in saved_sqs_list:
                queue_url = saved_sqs_list['QueueUrls'][1]
                saved_sqs_details = get_parameter_file(region, "sqs", "get-queue-attributes", queue_url)

                queue_name = saved_sqs_details['Attributes']['QueueArn'].split(':')[-1]
                print(f"Queue Name: {queue_name}")

                parameters['MetricDataQueries'][0]['MetricStat']['Metric']['Dimensions'][0]['Value'] = queue_name
                response = client.get_metric_data(**parameters)
                messages_received = sum(response['MetricDataResults'][0]['Values'])
                print('Messages Received in Last Month: ', messages_received)
                saved_sqs_details['Attributes']['MessagesReceivedInLastMonth'] = messages_received

                queue_tags = sqs.list_queue_tags(QueueUrl=queue_url)
                if 'Tags' in queue_tags:
                    saved_sqs_details['Attributes']['Tags'] = queue_tags['Tags']

                filename = get_filename_from_parameter(queue_url)
                sqs_attributes_file = "{}/{}".format(sqs_attributes_filepath, filename)
                print(f"Output File: {sqs_attributes_file}")
                if saved_sqs_details is not None:
                    with open(sqs_attributes_file, "w+") as f:
                        f.write(
                            json.dumps(saved_sqs_details, indent=4, sort_keys=True, default=custom_serializer)
                        )



def run(arguments):

    args, accounts, config = parse_arguments(arguments)
    get_sqs_queue_metrics_and_tags(args, accounts, config)
