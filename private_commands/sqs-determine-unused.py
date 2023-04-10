import boto3
import json
import logging
import os
import re
import copy

from botocore.exceptions import ClientError
from commands.collect import get_filename_from_parameter
from datetime import datetime, timedelta
from shared.common import query_aws, get_regions, parse_arguments, get_parameter_file, custom_serializer
from shared.nodes import Account, Region

def make_directory(path: str) -> None:
    try:
        os.mkdir(path)
    except OSError:
        # Already exists
        pass


def snakecase(s: str) -> str:
    return s.replace("-", "_")


def fetch_queue_metrics(client: boto3.Session.client, base_parameters: dict, inner_base_parameters: dict, queue_names: list, queue_attributes_objs: list) -> list:
    print(f'Fetching metrics for {len(queue_names)} queues')
    if len(base_parameters['MetricDataQueries']) > 0:
        base_parameters['MetricDataQueries'] = []
    full_params = base_parameters.copy()

    for i, queue_name in enumerate(queue_names):
        single_sub_params = inner_base_parameters.copy()
        single_sub_params['MetricStat']['Metric']['Dimensions'][0]['Value'] = queue_name
        single_sub_params['Id'] = f'queue{str(i)}'
        full_params['MetricDataQueries'].append(single_sub_params)

    response = client.get_metric_data(**full_params)
    metric_results = [(int(re.sub('[a-z]', '', x['Id'])), sum(x['Values'])) for x in response['MetricDataResults']]

    # paranoid about things being out of order, so sort by the id
    metric_results.sort(key = lambda x: x[0])
    metric_results = [x[1] for x in metric_results]

    if len(metric_results) != len(queue_names) or len(metric_results) != len(queue_attributes_objs):
        print("Error: Number of metric results don't match queue names or attributes")
        exit(-1)

    for i, queue_attributes_obj in enumerate(queue_attributes_objs):
        queue_attributes_obj['Attributes']['MessagesReceivedInLastMonth'] = metric_results[i]

    return queue_attributes_objs


def fetch_queue_tags(client: boto3.Session.client, queue_urls: list, queue_attributes_objs: list) -> list:
    print('Fetching tags')
    for i in range(len(queue_urls)):
        queue_tags = client.list_queue_tags(QueueUrl=queue_urls[i])
        if 'Tags' in queue_tags:
            queue_attributes_objs[i]['Attributes']['Tags'] = queue_tags['Tags']

    return queue_attributes_objs


def set_unused_flag(queue_attributes_objs: list) -> list:
    print('Setting the "Unused" flags')
    for queue_attributes_obj in queue_attributes_objs:
        if 'CreatedTimestamp' not in queue_attributes_obj['Attributes']:
            print("Error: CreatedTimestamp not found in queue attributes")
            continue
        creation_time = int(queue_attributes_obj['Attributes']['CreatedTimestamp'])
        created_recently = datetime.utcnow() - timedelta(weeks=4) < datetime.fromtimestamp(creation_time)

        if int(queue_attributes_obj['Attributes']['MessagesReceivedInLastMonth']) == 0 and not created_recently:
            queue_attributes_obj['Attributes']['Unused'] = True
        else:
            queue_attributes_obj['Attributes']['Unused'] = False

    return queue_attributes_objs


def save_sqs_attributes_file(profile_name: str, region_name: str, queue_urls: list, queue_attributes_objs: list) -> None:
    print('Saving SQS attributes files')
    for queue_url, queue_attributes_obj in zip(queue_urls, queue_attributes_objs):
        sqs_attributes_filepath = "account-data/{}/{}/{}-{}".format(
            profile_name, region_name, "sqs", "get-queue-attributes"
        )
        filename = get_filename_from_parameter(queue_url)
        sqs_attributes_file = "{}/{}".format(sqs_attributes_filepath, filename)
        if not os.path.exists(sqs_attributes_file):
            print(f"File does not exist: {sqs_attributes_file}")
            continue
        if queue_attributes_obj is not None:
            with open(sqs_attributes_file, "w+") as f:
                f.write(
                    json.dumps(queue_attributes_obj, indent=4, sort_keys=True, default=custom_serializer)
                )

    return


def get_sqs_queue_metrics_and_tags(arguments: dict, accounts: list, config: dict) -> None:
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
        'MetricDataQueries': [],
        'StartTime': datetime.utcnow() - timedelta(weeks=4),
        'EndTime': datetime.utcnow(),
    }
    inner_base_params = {
            #'Id': 'id_0',
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
        }

    for account in accounts:
        # get_regions reads the file at account-data/{profile}/describe-regions.json
        for region_json in get_regions(Account(None, account)):
            region = Region(Account(None, account), region_json)
            print(f"Processing SQS Queues for Region {region.name}")

            saved_sqs_list = query_aws(region.account, "sqs-list-queues", region=region)

            if 'QueueUrls' not in saved_sqs_list:
                print(f"No SQS queues found in {region.name}")
                continue

            print(f"Found {len(saved_sqs_list['QueueUrls'])} SQS queues in {region.name}")

            for i in range(len(saved_sqs_list['QueueUrls']) // 500 + 1):
                end_idx = (i+1)*500
                if end_idx > len(saved_sqs_list['QueueUrls']):
                    end_idx = len(saved_sqs_list['QueueUrls'])
                queue_urls = saved_sqs_list['QueueUrls'][i*500:end_idx]
                print(f"Processing URLs [{i*500}, {end_idx})")
                queue_details = [get_parameter_file(region, "sqs", "get-queue-attributes", queue_url) for queue_url in queue_urls]
                queue_names = [queue_detail['Attributes']['QueueArn'].split(':')[-1] for queue_detail in queue_details]

                queue_details = fetch_queue_metrics(client, base_parameters, inner_base_params, queue_names, queue_details)
                queue_details = fetch_queue_tags(sqs, queue_urls, queue_details)
                queue_details = set_unused_flag(queue_details)
                save_sqs_attributes_file(arguments.profile, region.name, queue_urls, queue_details)

    return


def run(arguments: list) -> None:

    args, accounts, config = parse_arguments(arguments)
    get_sqs_queue_metrics_and_tags(args, accounts, config)
