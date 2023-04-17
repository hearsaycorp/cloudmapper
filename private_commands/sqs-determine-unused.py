import boto3
import json
import logging
import os
import re

from botocore.exceptions import ClientError
from commands.collect import get_filename_from_parameter
from datetime import datetime, timedelta
from shared.common import get_regions, parse_arguments, custom_serializer
from shared.nodes import Account, Region
from shared.query import query_aws, get_parameter_file

def make_directory(path: str) -> None:
    try:
        os.mkdir(path)
    except OSError:
        # Already exists
        pass


def snakecase(s: str) -> str:
    return s.replace("-", "_")


def fetch_queue_metrics(client: boto3.Session.client, base_parameters: dict, inner_base_parameters: dict, metrics_to_fetch: list, queue_names: list, queue_attributes_objs: list) -> list:
    print(f'Fetching metrics for {len(queue_names)} queues')
    if len(base_parameters['MetricDataQueries']) > 0:
        base_parameters['MetricDataQueries'] = []
    full_params = base_parameters.copy()
    print(f'Using Metrics: {metrics_to_fetch}')

    for i, queue_name in enumerate(queue_names):
        for metric in metrics_to_fetch:
            single_sub_params = inner_base_parameters.copy()
            single_sub_params['MetricStat']['Metric']['Dimensions'][0]['Value'] = queue_name
            single_sub_params['MetricStat']['Metric']['MetricName'] = metric
            single_sub_params['Id'] = f'queue_{str(i)}_{metric}'
            full_params['MetricDataQueries'].append(single_sub_params)

    response = client.get_metric_data(**full_params)
    metric_results = [(int(x['Id'].split('_')[1]), x['Id'].split('_')[-1], sum(x['Values'])) for x in response['MetricDataResults']]

    if len(metric_results) != len(queue_names)*len(metrics_to_fetch) or \
        len(metric_results) != len(queue_attributes_objs)*len(metrics_to_fetch):
        print("Error: Number of metric results don't match queue names or attributes")
        exit(-1)

    for i, queue_attributes_obj in enumerate(queue_attributes_objs):
        for j, metric in enumerate(metrics_to_fetch):
            result_idx = len(metrics_to_fetch)*i + j
            if metric != metric_results[result_idx][1]:
                print(f"Error: Metric name {metric_results[result_idx][1]} doesn't match expected value {metric}")
                exit(-1)
            queue_attributes_obj['Attributes'][metric] = metric_results[result_idx][2]

    return queue_attributes_objs


def fetch_ec2_dependencies(client: boto3.Session.client, queue_names: list, queue_attributes_objs: list) -> list:
    print('Fetching EC2 dependencies')
    ec2_id_regex = 'i-[a-f0-9]{8}(?:[a-f0-9]{9})?'

    for i, queue_name in enumerate(queue_names):
        instance_id = re.findall(ec2_id_regex, queue_name)
        if instance_id:
            try:
                ec2_response = client.describe_instance_status(InstanceIds=instance_id, IncludeAllInstances=True)
                print(ec2_response)
                queue_attributes_objs[i]['Attributes']['HasEC2Dependency'] = True
            except ClientError as e:
                if e.response['Error']['Code'] == 'InvalidInstanceID.NotFound':
                    queue_attributes_objs[i]['Attributes']['HasEC2Dependency'] = False
                else:
                    raise e

    return queue_attributes_objs


def fetch_queue_tags(client: boto3.Session.client, queue_urls: list, queue_attributes_objs: list) -> list:
    print('Fetching tags')
    for i in range(len(queue_urls)):
        queue_tags = client.list_queue_tags(QueueUrl=queue_urls[i])
        if 'Tags' in queue_tags:
            queue_attributes_objs[i]['Attributes']['Tags'] = queue_tags['Tags']

    return queue_attributes_objs


def set_unused_flag(queue_attributes_objs: list, metrics: list) -> list:
    print('Setting the "Unused" flags')
    for queue_attributes_obj in queue_attributes_objs:
        if 'CreatedTimestamp' not in queue_attributes_obj['Attributes']:
            print("Error: CreatedTimestamp not found in queue attributes")
            continue
        creation_time = int(queue_attributes_obj['Attributes']['CreatedTimestamp'])
        created_recently = datetime.utcnow() - timedelta(weeks=13) < datetime.fromtimestamp(creation_time)

        unused_list = [int(queue_attributes_obj['Attributes'][metric]) == 0 for metric in metrics]
        unused_list.append(not created_recently)
        if 'HasEC2Dependency' in queue_attributes_obj['Attributes']:
            unused_list.append(not queue_attributes_obj['Attributes']['HasEC2Dependency'])

        queue_attributes_obj['Attributes']['Unused'] = all(unused_list)

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
    # if len(args.regions_filter) > 0:
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

    max_metric_queries = 500
    base_parameters = {
        'MetricDataQueries': [],
        'StartTime': datetime.utcnow() - timedelta(weeks=13),
        'EndTime': datetime.utcnow(),
    }
    inner_base_params = {
            #'Id': 'id_0',
            'MetricStat': {
                'Metric': {
                    'Namespace': 'AWS/SQS',
                    #'MetricName': 'NumberOfMessagesReceived',
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
    metrics = [
        'ApproximateAgeOfOldestMessage',
        'ApproximateNumberOfMessagesDelayed',
        'ApproximateNumberOfMessagesNotVisible',
        'ApproximateNumberOfMessagesVisible',
        'NumberOfEmptyReceives',
        'NumberOfMessagesDeleted',
        'NumberOfMessagesReceived',
        'NumberOfMessagesSent',
        'SentMessageSize'
    ]

    for account in accounts:
        # get_regions reads the file at account-data/{profile}/describe-regions.json
        for region_json in get_regions(Account(None, account)):
            region = Region(Account(None, account), region_json)
            sqs = session.client("sqs", region_name=region.name)
            cloudwatch = session.client('cloudwatch', region_name=region.name)
            ec2 = session.client('ec2', region_name=region.name)
            print(f"Processing SQS Queues for Region {region.name}")

            saved_sqs_list = query_aws(region.account, "sqs-list-queues", region=region)

            if 'QueueUrls' not in saved_sqs_list:
                print(f"No SQS queues found in {region.name}")
                continue

            print(f"Found {len(saved_sqs_list['QueueUrls'])} SQS queues in {region.name}")

            for i in range(len(saved_sqs_list['QueueUrls']) * len(metrics) // max_metric_queries + 1):

                start_idx = i*max_metric_queries//len(metrics)
                end_idx = (i+1)*max_metric_queries//len(metrics)
                if end_idx > len(saved_sqs_list['QueueUrls']):
                    end_idx = len(saved_sqs_list['QueueUrls'])
                print(f"Processing URLs [{start_idx}, {end_idx})")
                queue_urls = saved_sqs_list['QueueUrls'][start_idx:end_idx]
                queue_details = [get_parameter_file(region, "sqs", "get-queue-attributes", queue_url) for queue_url in queue_urls]
                queue_names = [queue_detail['Attributes']['QueueArn'].split(':')[-1] for queue_detail in queue_details]

                queue_details = fetch_queue_metrics(cloudwatch, base_parameters, inner_base_params, metrics, queue_names, queue_details)
                queue_details = fetch_queue_tags(sqs, queue_urls, queue_details)
                queue_details = fetch_ec2_dependencies(ec2, queue_urls, queue_details)
                queue_details = set_unused_flag(queue_details, metrics)
                save_sqs_attributes_file(arguments.profile, region.name, queue_urls, queue_details)

    return


def run(arguments: list) -> None:

    args, accounts, config = parse_arguments(arguments)
    get_sqs_queue_metrics_and_tags(args, accounts, config)
