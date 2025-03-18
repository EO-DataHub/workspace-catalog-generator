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

# Set Top-Level User Catalogue Name
USER_CATALOG_NAME = os.getenv("USER_CATALOG_NAME", "user")

# Load catalog template
with open("templates/catalog-template.json") as f:
    catalog_dict = json.load(f)

# Read list of expected catalogs
with open("config/required-catalogs.json") as f:
    catalogs_to_generate = json.load(f)


def upload_file_to_s3(client: botocore.client, file_name: str, bucket: str, data: dict) -> bool:
    """
    Upload a file to an S3 bucket
    """
    try:
        client.upload_fileobj(BytesIO(data.encode("utf-8")), bucket, file_name)
        # Log the S3 upload details
        logger.info(
            f"Uploaded catalog '{file_name}' to S3 bucket '{bucket}' with key '{file_name}'"
        )
        return True
    except Exception as e:
        logger.error(
            f"Failed to upload catalog '{file_name}' to S3 bucket '{bucket}' with key '{file_name}'"
        )
        logger.error(e)
        return False


def create_catalog(
    client: botocore.client,
    workspace_name: str,
    bucket_name: str,
    file_name_and_path: str,
    id: str,
    title: str,
    description: str,
) -> List[str]:
    """
    Create new catalog files in S3.
    """
    # Create a dictionary for the catalog
    catalog_dict["id"] = id.replace("{workspace_name}", workspace_name)
    catalog_dict["title"] = title.replace("{workspace_name}", workspace_name)
    catalog_dict["description"] = description.replace("{workspace_name}", workspace_name)

    # Generate a unique filename for the catalog
    filename = file_name_and_path.replace("{workspace_name}", workspace_name)
    harvested_filename = "harvested/" + filename.replace("/", "/catalogs/")

    # Convert the catalog dictionary to JSON string
    catalog_json = json.dumps(catalog_dict)

    # Upload the catalog JSON file to S3 bucket
    file_uploaded = upload_file_to_s3(client, filename, bucket_name, catalog_json)
    # Upload second file to harvested source
    if file_uploaded:
        harvested_file_uploaded = upload_file_to_s3(
            client, harvested_filename, bucket_name, catalog_json
        )
        if harvested_file_uploaded:
            return harvested_filename


def create_catalogs(client: botocore.client, workspace_name: str, bucket_name: str) -> List[str]:
    """
    Create new catalog files in S3.
    """
    created_files = []
    for file_to_create in catalogs_to_generate:
        uploaded_filename = create_catalog(
            client,
            workspace_name,
            bucket_name,
            file_to_create["file_name_and_path"],
            file_to_create["id"],
            file_to_create["title"],
            file_to_create["description"],
        )

        if uploaded_filename:
            created_files.append(uploaded_filename)

    return created_files


def delete_catalogs(client: botocore.client, workspace_name: str, bucket_name: str) -> List[str]:
    """
    Delete catalog files in S3.
    """
    deleted_keys = []
    files_to_delete = [
        file["file_name_and_path"].replace("{workspace_name}", workspace_name)
        for file in catalogs_to_generate
    ]

    for key in files_to_delete:
        try:
            client.delete_object(Bucket=bucket_name, Key=key)
            logger.info(f"Deleted catalog '{key}' from S3 bucket '{bucket_name}'")
            deleted_keys.append(key)
        except Exception as e:
            logger.error(f"Failed to delete catalog '{key}' from S3 bucket '{bucket_name}'")
            logger.error(e)
    return deleted_keys


def get_s3_client() -> boto3.client:
    """Generate boto3 client ready for S3 data access"""
    if os.getenv("AWS_ACCESS_KEY") and os.getenv("AWS_SECRET_ACCESS_KEY"):
        session = boto3.session.Session(
            aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )
        client = session.client("s3")

    else:
        client = boto3.client("s3")

    return client


def interpret_message(msg: str) -> tuple:
    """
    Interpret Pulsar message to create, update or delete catalogs in S3.
    """
    # Configure boto3 client ready for data access
    client = get_s3_client()

    # Extract as JSON dictionary
    msg_json = json.loads(msg)
    workspace_name = msg_json["spec"]["aws"]["s3"]["buckets"][0]["path"]
    if workspace_name.endswith("/"):
        workspace_name = workspace_name.rstrip("/")
    bucket_name = msg_json["spec"]["aws"]["s3"]["buckets"][0]["name"]
    workspace_status = msg_json["event"]

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


def send_pulsar_message(msg: str, producer: pulsar.Producer):
    """
    Send message via Pulsar.
    """
    producer.send((json.dumps(msg)).encode("utf-8"))
    logging.info(f"Sent transform message {msg}")


def generate_output_message(
    workspace_name: str, bucket_name: str, created_keys: List[str], deleted_keys: List[str]
):
    """
    Publish messages to a Pulsar broker.
    """
    # Generate output message for altered files
    output_msg = {
        "id": f"{workspace_name}-workspace-generator",
        "workspace": workspace_name,
        "bucket_name": bucket_name,
        "updated_keys": [],
        "added_keys": created_keys,
        "deleted_keys": deleted_keys,
        "source": workspace_name,
        "target": f"catalogs/{USER_CATALOG_NAME}/catalogs/{workspace_name}",
    }

    return output_msg


def main(consumer: pulsar.Consumer, producer: pulsar.Producer):
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
            output_msg = generate_output_message(
                workspace_name, bucket_name, created_keys, deleted_keys
            )
            send_pulsar_message(output_msg, producer)
            consumer.acknowledge(msg)
        except Exception as err:
            logger.debug(traceback.format_exc())
            logger.error(err)
            consumer.negative_acknowledge(msg)


if __name__ == "__main__":
    pulsar_url = os.environ.get("PULSAR_URL", "")
    client = pulsar.Client(pulsar_url)
    consumer = client.subscribe(
        topic="workspace-controller", subscription_name="workspace-catalog-generator"
    )
    producer = client.create_producer(
        topic="harvested", producer_name="workspace-catalog-generator"
    )
    main(consumer, producer)
