import boto3
import json
import logging
import os
import re
import copy

from botocore.exceptions import ClientError
from commands.collect import get_filename_from_parameter
from datetime import datetime, timedelta
from shared.common import get_regions, parse_arguments, custom_serializer
from shared.nodes import Account, Region
from shared.query import query_aws, get_parameter_file

def filter_for_unused_queues(queue_urls: list, queue_attributes_objs: list) -> list:
    unused_queues = [queue_url for queue_url, queue_attributes_obj in zip(queue_urls, queue_attributes_objs) \
                     if queue_attributes_obj['Attributes']['Unused']] #and \
                     #('Tags' not in queue_attributes_obj['Attributes'] or 'EngTeam' not in queue_attributes_obj['Attributes']['Tags'])]
    print(f'Filtered {len(queue_urls)} queues down to {len(unused_queues)} unused queues')

    return unused_queues


def delete_queues(client: boto3.Session.client, queue_urls: list) -> None:
    print(f'Deleting {len(queue_urls)} unused queues')
    for queue_url in queue_urls:
        try:
            client.delete_queue(QueueUrl=queue_url)
        except ClientError as e:
            print(f'Error deleting queue {queue_url}: {e}')
    
    return


def clean_up_deleted_queue_files(unused_queues: list, saved_sqs_list: dict, account_name: str, profile_name: str, region_name: str) -> None:
    print(f'Cleaning up {len(unused_queues)} deleted queue files')
    original_queue_list_len = len(saved_sqs_list['QueueUrls'])
    sqs_attributes_filepath = "account-data/{}/{}/{}-{}".format(
        profile_name, region_name, "sqs", "get-queue-attributes"
    )

    for queue_url in unused_queues:
        saved_sqs_list['QueueUrls'].remove(queue_url)
        filename = get_filename_from_parameter(queue_url)
        sqs_attributes_file = "{}/{}".format(sqs_attributes_filepath, filename)
        if os.path.exists(sqs_attributes_file):
            os.remove(sqs_attributes_file)

    print(f'Reduced queue list from {original_queue_list_len} to {len(saved_sqs_list["QueueUrls"])}')
    sqs_queue_list_file = "account-data/{}/{}/{}.json".format(account_name, region_name, "sqs-list-queues")
    with open(sqs_queue_list_file, "w+") as f:
        f.write(
            json.dumps(saved_sqs_list, indent=4, sort_keys=True, default=custom_serializer)
        )

    return


def run(arguments: list) -> None:
    args, accounts, config = parse_arguments(arguments)
    logging.getLogger("botocore").setLevel(logging.WARN)

    default_region = os.environ.get("AWS_REGION", "us-east-1")

    session_data = {"region_name": default_region}

    if args.profile:
        session_data["profile_name"] = args.profile

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
    for account in accounts:
        # get_regions reads the file at account-data/{profile}/describe-regions.json
        for region_json in get_regions(Account(None, account)):
            region = Region(Account(None, account), region_json)
            print(f"Processing SQS Queues for Region {region.name}")

            saved_sqs_list = query_aws(region.account, "sqs-list-queues", region=region)

            if 'QueueUrls' not in saved_sqs_list:
                print(f"No SQS queues found in {region.name}")
                continue

            queue_urls = saved_sqs_list['QueueUrls']
            queue_details = [get_parameter_file(region, "sqs", "get-queue-attributes", queue_url) for queue_url in queue_urls]
            unused_queues = filter_for_unused_queues(queue_urls, queue_details)
            if not args.dry_run:
                delete_queues(sqs, unused_queues)
                clean_up_deleted_queue_files(unused_queues, saved_sqs_list, account['name'], args.profile, region.name)
            else:
                print("This operation would delete the following SQS queues:")
                print(unused_queues)
