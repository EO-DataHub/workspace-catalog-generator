import json
import logging
import os
import time
import traceback
from io import BytesIO
from typing import List

import boto3
import botocore.client
import botocore.exceptions
import pulsar
import pulsar.exceptions

# create logger for stac fastapi ingester
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(filename="create-workspace-catalogs.log", encoding="utf-8", level=logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# create formatter
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# set the time converter to UTC
formatter.converter = time.gmtime

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)


def create_catalogs(client: botocore.client, workspace_name: str, bucket_name: str) -> List[str]:
    """
    Create new catalog files in S3.
    """

    created_keys = []

    # Create new top-level catalog entry for workspace name
    with open("templates/workspace-catalog-template.json") as f:
        catalog_dict = json.load(f)

    new_id = {"id": catalog_dict["id"].replace("{workspace_name}", workspace_name)}
    new_title = {"title": catalog_dict["title"].replace("{workspace_name}", workspace_name)}
    new_description = {
        "description": catalog_dict["description"].replace("{workspace_name}", workspace_name)
    }
    catalog_dict.update(new_id)
    catalog_dict.update(new_title)
    catalog_dict.update(new_description)

    # Generate a unique filename for the catalog
    filename = f"{workspace_name}.json"

    # Convert the catalog dictionary to JSON string
    catalog_json = json.dumps(catalog_dict)

    # Upload the catalog JSON file to S3 bucket
    try:
        client.upload_fileobj(BytesIO(catalog_json.encode("utf-8")), bucket_name, filename)
        # Log the S3 upload details
        logger.info(
            f"Uploaded catalog '{filename}' to S3 bucket '{bucket_name}' with key '{filename}'"
        )
        created_keys.append(filename)
    except Exception as e:
        logger.error(
            f"Failed to upload catalog '{filename}' to S3 bucket '{bucket_name}' with key '{filename}'"
        )
        logger.error(e)

    # Create new top-level catalog entry for workspace name
    with open("templates/processing-results-catalog-template.json") as f:
        catalog_dict = json.load(f)

    new_description = {
        "description": catalog_dict["description"].replace("{workspace_name}", workspace_name)
    }
    catalog_dict.update(new_description)

    # Generate a unique filename for the catalog
    filename = f"{workspace_name}/processing-results.json"

    # Convert the catalog dictionary to JSON string
    catalog_json = json.dumps(catalog_dict)

    # Upload the catalog JSON file to S3 bucket
    try:
        client.upload_fileobj(BytesIO(catalog_json.encode("utf-8")), bucket_name, filename)
        # Log the S3 upload details
        logger.info(
            f"Uploaded catalog '{filename}' to S3 bucket '{bucket_name}' with key '{filename}'"
        )
        created_keys.append(filename)
    except Exception as e:
        logger.error(
            f"Failed to upload catalog '{filename}' to S3 bucket '{bucket_name}' with key '{filename}'"
        )
        logger.error(e)
    return created_keys


def delete_catalogs(client: botocore.client, workspace_name: str, bucket_name: str) -> List[str]:
    """
    Delete catalog files in S3.
    """
    deleted_keys = []
    files_to_delete = [f"{workspace_name}.json", f"{workspace_name}/processing-results.json"]

    for key in files_to_delete:
        try:
            client.delete_object(Bucket=bucket_name, Key=key)
            logger.info(f"Deleted catalog '{key}' from S3 bucket '{bucket_name}'")
            deleted_keys.append(key)
        except Exception as e:
            logger.error(f"Failed to delete catalog '{key}' from S3 bucket '{bucket_name}'")
            logger.error(e)
    return deleted_keys


def interpret_message(msg: str) -> tuple:
    """
    Interpret Pulsar message to create, update or delete catalogs in S3.
    """
    # Configure boto3 client ready for data access
    if os.getenv("AWS_ACCESS_KEY") and os.getenv("AWS_SECRET_ACCESS_KEY"):
        session = boto3.session.Session(
            aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )
        client = session.client("s3")

    else:
        client = boto3.client("s3")

    # Extract as JSON dictionary
    msg_json = json.loads(msg) 
    workspace_name = msg_json["Spec"]["aws"]["s3"]["buckets"][0]["path"]
    if workspace_name.endswith("/"):
        workspace_name = workspace_name.rstrip("/")
    bucket_name = msg_json["Spec"]["aws"]["s3"]["buckets"][0]["name"]
    workspace_status = msg_json["Event"]

    created_keys = []
    deleted_keys = []

    if workspace_status == "update":
        # This covers workspace creation and updates
        logger.info(f"Updating catalogs for the {workspace_name} workspace.")
        created_keys = create_catalogs(client, workspace_name, bucket_name)
    elif workspace_status == "delete":
        # This covers workspace deletion
        logger.info(f"Removing catalogs for the {workspace_name} workspace.")
        deleted_keys = delete_catalogs(client, workspace_name, bucket_name)

    return workspace_name, bucket_name, created_keys, deleted_keys


def publish_to_transformer(
    workspace_name: str, bucket_name: str, created_keys: List[str], deleted_keys: List[str]
):
    """
    Publish messages to a Pulsar broker.
    """
    # Generate output message for altered files
    output_msg = {
        "id": "workspaces/transform-new-workspace",
        "workspace": workspace_name,
        "branch": "",
        "bucket_name": bucket_name,
        "updated_keys": [],
        "added_keys": created_keys,
        "deleted_keys": deleted_keys,
        "source": workspace_name,
        "target": f"user-datasets/{workspace_name}",
    }

    # Send message to Pulsar
    producer.send((json.dumps(output_msg)).encode("utf-8"))
    logging.info(f"Sent transform message {output_msg}")


def main():
    """
    Accept messages from a Pulsar broker.
    """
    while True:
        try:
            logger.info("Waiting for message")
            msg = consumer.receive()
            logging.info(f"Parsing harvested message {msg.data()}")
            decoded_msg = msg.data().decode("utf-8")
            workspace_name, bucket_name, created_keys, deleted_keys = interpret_message(decoded_msg)
            publish_to_transformer(workspace_name, bucket_name, created_keys, deleted_keys)
            consumer.acknowledge(msg)
        except Exception as err:
            logger.debug(traceback.format_exc())
            logger.error(err)
            consumer.negative_acknowledge(msg)


if __name__ == "__main__":
    pulsar_url = os.environ.get("PULSAR_URL", "")
    client = pulsar.Client(pulsar_url)
    consumer = client.subscribe(topic="workspace-controller", subscription_name="workspace-catalog-generator")
    producer = client.create_producer(topic="harvested", producer_name="workspace-catalog-generator")
    main()
