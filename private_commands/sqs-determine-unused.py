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


def fetch_queue_metrics(client, request_parameters, queue_name, queue_attributes_obj):
    request_parameters['MetricDataQueries'][0]['MetricStat']['Metric']['Dimensions'][0]['Value'] = queue_name
    response = client.get_metric_data(**request_parameters)
    queue_attributes_obj['Attributes']['MessagesReceivedInLastMonth'] = sum(response['MetricDataResults'][0]['Values'])

    return queue_attributes_obj


def fetch_queue_tags(client, queue_url, queue_attributes_obj):
    queue_tags = client.list_queue_tags(QueueUrl=queue_url)
    if 'Tags' in queue_tags:
        queue_attributes_obj['Attributes']['Tags'] = queue_tags['Tags']

    return queue_attributes_obj


def set_unused_flag(queue_attributes_obj):
    creation_time = int(queue_attributes_obj['Attributes']['CreatedTimestamp'])
    created_recently = datetime.utcnow() - timedelta(weeks=4) < datetime.fromtimestamp(creation_time)

    if int(queue_attributes_obj['Attributes']['MessagesReceivedInLastMonth']) == 0 and not created_recently:
        queue_attributes_obj['Attributes']['Unused'] = True
    else:
        queue_attributes_obj['Attributes']['Unused'] = False

    return queue_attributes_obj


def save_sqs_attributes_file(profile_name, region_name, queue_url, queue_attributes_obj):
    sqs_attributes_filepath = "account-data/{}/{}/{}-{}".format(
        profile_name, region_name, "sqs", "get-queue-attributes"
    )
    filename = get_filename_from_parameter(queue_url)
    sqs_attributes_file = "{}/{}".format(sqs_attributes_filepath, filename)
    print(f"Output File: {sqs_attributes_file}")
    if queue_attributes_obj is not None:
        with open(sqs_attributes_file, "w+") as f:
            f.write(
                json.dumps(queue_attributes_obj, indent=4, sort_keys=True, default=custom_serializer)
            )

    return


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
    base_parameters = {
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

            if 'QueueUrls' not in saved_sqs_list:
                continue

            queue_url = saved_sqs_list['QueueUrls'][1]
            saved_sqs_details = get_parameter_file(region, "sqs", "get-queue-attributes", queue_url)
            queue_name = saved_sqs_details['Attributes']['QueueArn'].split(':')[-1]
            #print(f"Queue Name: {queue_name}")

            saved_sqs_details = fetch_queue_metrics(client, base_parameters, queue_name, saved_sqs_details)
            saved_sqs_details = fetch_queue_tags(sqs, queue_url, saved_sqs_details)
            saved_sqs_details = set_unused_flag(saved_sqs_details)
            save_sqs_attributes_file(arguments.profile, region.name, queue_url, saved_sqs_details)

    return


def run(arguments):

    args, accounts, config = parse_arguments(arguments)
    get_sqs_queue_metrics_and_tags(args, accounts, config)
