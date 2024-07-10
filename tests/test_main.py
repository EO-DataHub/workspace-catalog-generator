import json
from unittest import mock
from unittest.mock import patch

from catalog_generator.__main__ import (
    create_catalogs,
    delete_catalogs,
    generate_output_message,
    interpret_message,
    send_pulsar_message,
)


@patch("catalog_generator.__main__.send_pulsar_message")
def test_generate_output_message(mock_send_pulsar_message):
    workspace_name = "test_ws"
    bucket_name = "test-bucket"
    created_keys = ["test-datasets/test-catalog/test-collection.json"]
    deleted_keys = []

    test_msg = {
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

    output_msg = generate_output_message(workspace_name, bucket_name, created_keys, deleted_keys)

    assert output_msg == test_msg


@mock.patch("boto3.client")
@patch("catalog_generator.__main__.delete_catalogs")
@patch("catalog_generator.__main__.create_catalogs")
@patch("catalog_generator.__main__.get_s3_client")
def test_interpret_message_update(
    mock_get_s3_client, mock_create_catalogs, mock_delete_catalogs, mock_boto_client
):

    # Set test variables
    workspace_name = "test-ws"
    bucket_name = "test-bucket"

    # Create a mock S3 client
    mock_s3 = mock.Mock()
    mock_boto_client.return_value = mock_s3

    # Mock return value for mocked functions
    mock_get_s3_client.return_value = mock_s3
    mock_create_catalogs.return_value = ["test-datasets/test-catalog/test-collection.json"]
    mock_delete_catalogs.return_value = []

    # Create test message as if from Pulsar
    test_msg_json = {
        "spec": {"aws": {"s3": {"buckets": [{"path": workspace_name, "name": bucket_name}]}}},
        "event": "update",
    }

    test_msg = json.dumps(test_msg_json)

    result_list = interpret_message(test_msg)

    # Check functions called with expected parameters
    mock_create_catalogs.assert_called_once_with(mock_s3, workspace_name, bucket_name)

    # Assert that the delete_catalogs function was not called
    assert mock_delete_catalogs.call_count == 0

    # Check output results are as expected
    assert result_list[0] == workspace_name
    assert result_list[1] == bucket_name
    assert result_list[2] == ["test-datasets/test-catalog/test-collection.json"]
    assert result_list[3] == []


@mock.patch("boto3.client")
@patch("catalog_generator.__main__.delete_catalogs")
@patch("catalog_generator.__main__.create_catalogs")
@patch("catalog_generator.__main__.get_s3_client")
def test_interpret_message_delete(
    mock_get_s3_client, mock_create_catalogs, mock_delete_catalogs, mock_boto_client
):

    # Set test variables
    workspace_name = "test-ws"
    bucket_name = "test-bucket"

    # Create a mock S3 client
    mock_s3 = mock.Mock()
    mock_boto_client.return_value = mock_s3

    # Mock return value for mocked functions
    mock_get_s3_client.return_value = mock_s3
    mock_create_catalogs.return_value = []
    mock_delete_catalogs.return_value = ["test-datasets/test-catalog/test-collection.json"]

    # Create test message as if from Pulsar
    test_msg_json = {
        "spec": {"aws": {"s3": {"buckets": [{"path": workspace_name, "name": bucket_name}]}}},
        "event": "delete",
    }

    test_msg = json.dumps(test_msg_json)

    result_list = interpret_message(test_msg)

    # Check functions called with expected parameters
    mock_delete_catalogs.assert_called_once_with(mock_s3, workspace_name, bucket_name)

    # Assert that the create_catalogs function was not called
    assert mock_create_catalogs.call_count == 0

    # Check output results are as expected
    assert result_list[0] == workspace_name
    assert result_list[1] == bucket_name
    assert result_list[2] == []
    assert result_list[3] == ["test-datasets/test-catalog/test-collection.json"]


@mock.patch("io.BytesIO")
@mock.patch("boto3.client")
def test_create_catalogs(mock_boto_client, mock_bytes_io):

    # Set test variables
    workspace_name = "test-ws"
    bucket_name = "test-bucket"
    test_catalog_file_name = f"{workspace_name}.json"
    test_proc_results_file_name = f"{workspace_name}/processing-results.json"

    test_catalog_json = {
        "type": "Catalog",
        "id": f"{workspace_name}",
        "title": f"Catalog for workspace {workspace_name}",
        "description": f"Datasets in the {workspace_name} workspace",
        "stac_version": "1.0.0",
    }

    test_catalog_json_str = json.dumps(test_catalog_json)

    test_proc_results_json = {
        "type": "Catalog",
        "id": "processing-results",
        "title": "Workflow Output Catalogs",
        "description": f"Workflow outputs in the {workspace_name} workspace",
        "stac_version": "1.0.0",
    }

    test_proc_results_json_str = json.dumps(test_proc_results_json)

    # Create a mock S3 client
    mock_boto_client = mock.Mock()

    created_keys = create_catalogs(mock_boto_client, workspace_name, bucket_name)

    # Assert that the create_catalogs function was called twice
    assert mock_boto_client.upload_fileobj.call_count == 2

    # Check arguments were as expected
    calls = mock_boto_client.upload_fileobj.call_args_list
    # Compare arguments for first call
    first_call = calls[0]
    # Need to decode the BytesIO object to compare with the original JSON string
    assert first_call.args[0].getvalue().decode("utf-8") == test_catalog_json_str
    assert first_call.args[1] == bucket_name
    assert first_call.args[2] == test_catalog_file_name

    # Compare arguments for second call
    second_call = calls[1]
    # Need to decode the BytesIO object to compare with the original JSON string
    assert second_call.args[0].getvalue().decode("utf-8") == test_proc_results_json_str
    assert second_call.args[1] == bucket_name
    assert second_call.args[2] == test_proc_results_file_name

    # Check final output is as expected
    assert created_keys == [test_catalog_file_name, test_proc_results_file_name]


@mock.patch("boto3.client")
def test_delete_catalogs(mock_boto_client):

    # Set test variables
    workspace_name = "test-ws"
    bucket_name = "test-bucket"
    test_catalog_file_name = f"{workspace_name}.json"
    test_proc_results_file_name = f"{workspace_name}/processing-results.json"

    # Create a mock S3 client
    mock_boto_client = mock.Mock()

    deleted_keys = delete_catalogs(mock_boto_client, workspace_name, bucket_name)

    # Assert that the delete_catalogs function was called twice
    assert mock_boto_client.delete_object.call_count == 2

    # Check arguments were as expected
    calls = mock_boto_client.delete_object.call_args_list
    # Compare arguments for first call
    first_call = calls[0]
    assert first_call.kwargs == {"Bucket": bucket_name, "Key": test_catalog_file_name}

    # Compare arguments for second call
    second_call = calls[1]
    assert second_call.kwargs == {"Bucket": bucket_name, "Key": test_proc_results_file_name}

    assert deleted_keys == [test_catalog_file_name, test_proc_results_file_name]


@mock.patch("pulsar.Producer")
def test_send_pulsar_message(mock_pulsar_producer):

    # Define test variable
    workspace_name = "test_ws"
    bucket_name = "test-bucket"
    created_keys = ["test-datasets/test-catalog/test-collection.json"]
    deleted_keys = []

    # Generate test output message
    test_msg = {
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

    test_msg_str_enc = json.dumps(test_msg).encode("utf-8")

    # Create a mock pulsar producer
    mock_pulsar_producer = mock.Mock()

    send_pulsar_message(test_msg, mock_pulsar_producer)

    # Check that the producer send method was called with the expected argument
    mock_pulsar_producer.send.assert_called_once_with(test_msg_str_enc)
