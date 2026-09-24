import json
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

from catalog_generator.__main__ import (
    create_catalogs,
    create_files,
    delete_catalogs,
    delete_files,
    generate_output_message,
    interpret_message,
    send_pulsar_message,
    upload_file_to_s3,
    workspace_controller_topics,
)


@patch("catalog_generator.__main__.send_pulsar_message")
def test_generate_output_message(mock_send_pulsar_message: MagicMock) -> None:
    workspace_name = "test_ws"
    bucket_name = "test-bucket"
    created_keys = ["test-datasets/test-catalog/test-collection.json"]
    deleted_keys: list[str] = []

    test_msg = {
        "id": f"{workspace_name}-workspace-generator",
        "workspace": workspace_name,
        "bucket_name": bucket_name,
        "updated_keys": [],
        "added_keys": created_keys,
        "deleted_keys": deleted_keys,
        "source": workspace_name,
        "target": f"catalogs/user/catalogs/{workspace_name}",
    }

    output_msg = generate_output_message(workspace_name, bucket_name, created_keys, deleted_keys)

    assert output_msg == test_msg


@mock.patch("boto3.client")
@patch("catalog_generator.__main__.delete_catalogs")
@patch("catalog_generator.__main__.create_catalogs")
@patch("catalog_generator.__main__.get_s3_client")
def test_interpret_message_update(
    mock_get_s3_client: MagicMock,
    mock_create_catalogs: MagicMock,
    mock_delete_catalogs: MagicMock,
    mock_boto_client: MagicMock,
) -> None:
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
    mock_get_s3_client: MagicMock,
    mock_create_catalogs: MagicMock,
    mock_delete_catalogs: MagicMock,
    mock_boto_client: MagicMock,
) -> None:
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
def test_create_catalogs(mock_boto_client: MagicMock, mock_bytes_io: MagicMock) -> None:
    # Set test variables
    workspace_name = "test-ws"
    bucket_name = "test-bucket"

    # Read expected catalogs config
    with open("config/required-files.json") as file:
        catalogs_to_generate = json.load(file)["catalogs"]

    # Replace workspace name in the catalog dictionary
    for catalog in catalogs_to_generate:
        catalog["id"] = catalog["id"].replace("{workspace_name}", workspace_name)
        catalog["title"] = catalog["title"].replace("{workspace_name}", workspace_name)
        catalog["description"] = catalog["description"].replace("{workspace_name}", workspace_name)
        catalog["file_name_and_path"] = catalog["file_name_and_path"].replace("{workspace_name}", workspace_name)

    # Construct expected catalogs
    with open("templates/catalog-template.json") as f:
        catalog_dict = json.load(f)

    expected_catalogs = []
    for catalog in catalogs_to_generate:
        catalog_dict["id"] = catalog["id"]
        catalog_dict["title"] = catalog["title"]
        catalog_dict["description"] = catalog["description"]
        expected_catalogs.append(json.dumps(catalog_dict))
        # Add again for harvested upload
        expected_catalogs.append(json.dumps(catalog_dict))

    # Create a mock S3 client
    mock_boto_client = mock.Mock()

    created_keys = create_catalogs(mock_boto_client, workspace_name, bucket_name)

    # Assert that the create_catalogs function was called for each catalog in the config
    assert mock_boto_client.upload_fileobj.call_count == len(catalogs_to_generate) * 2

    # Check arguments were as expected
    calls = mock_boto_client.upload_fileobj.call_args_list
    # Compare arguments for first catalog first call
    call = calls[0]
    # Need to decode the BytesIO object to compare with the original JSON string
    assert call.args[0].getvalue().decode("utf-8") == expected_catalogs[0]
    assert call.args[1] == bucket_name
    assert call.args[2] == catalogs_to_generate[0]["file_name_and_path"]

    # Compare arguments for first catalog second call
    call = calls[1]
    # Need to decode the BytesIO object to compare with the original JSON string
    assert call.args[0].getvalue().decode("utf-8") == expected_catalogs[1]
    assert call.args[1] == bucket_name
    assert call.args[2] == "harvested/" + catalogs_to_generate[0]["file_name_and_path"].replace("/", "/catalogs/")

    # Compare arguments for second catalog first call
    call = calls[2]
    # Need to decode the BytesIO object to compare with the original JSON string
    assert call.args[0].getvalue().decode("utf-8") == expected_catalogs[2]
    assert call.args[1] == bucket_name
    assert call.args[2] == catalogs_to_generate[1]["file_name_and_path"]

    # Compare arguments for second catalog second call
    call = calls[3]
    # Need to decode the BytesIO object to compare with the original JSON string
    assert call.args[0].getvalue().decode("utf-8") == expected_catalogs[3]
    assert call.args[1] == bucket_name
    assert call.args[2] == "harvested/" + catalogs_to_generate[1]["file_name_and_path"].replace("/", "/catalogs/")

    # Compare arguments for third catalog first call
    call = calls[4]
    # Need to decode the BytesIO object to compare with the original JSON string
    assert call.args[0].getvalue().decode("utf-8") == expected_catalogs[4]
    assert call.args[1] == bucket_name
    assert call.args[2] == catalogs_to_generate[2]["file_name_and_path"]

    # Compare arguments for third catalog second call
    call = calls[5]
    # Need to decode the BytesIO object to compare with the original JSON string
    assert call.args[0].getvalue().decode("utf-8") == expected_catalogs[5]
    assert call.args[1] == bucket_name
    assert call.args[2] == "harvested/" + catalogs_to_generate[2]["file_name_and_path"].replace("/", "/catalogs/")

    # Check final output is as expected
    assert created_keys == [
        "harvested/" + file["file_name_and_path"].replace("/", "/catalogs/") for file in catalogs_to_generate
    ]


@mock.patch("boto3.client")
def test_delete_catalogs(mock_boto_client: MagicMock) -> None:
    # Set test variables
    workspace_name = "test-ws"
    bucket_name = "test-bucket"

    # Read expected catalogs
    with open("config/required-files.json") as file:
        catalogs_to_generate = json.load(file)["catalogs"]

    # Replace workspace name in the catalog dictionary
    for catalog in catalogs_to_generate:
        catalog["file_name_and_path"] = catalog["file_name_and_path"].replace("{workspace_name}", workspace_name)

    # Create a mock S3 client
    mock_boto_client = mock.Mock()

    deleted_keys = delete_catalogs(mock_boto_client, workspace_name, bucket_name)

    # Assert that the delete_catalogs function was called for each catalog in the config
    assert mock_boto_client.delete_object.call_count == len(catalogs_to_generate)

    # Check arguments were as expected
    calls = mock_boto_client.delete_object.call_args_list
    # Compare arguments for first call
    first_call = calls[0]
    assert first_call.kwargs == {
        "Bucket": bucket_name,
        "Key": catalogs_to_generate[0]["file_name_and_path"],
    }

    # Compare arguments for second call
    second_call = calls[1]
    assert second_call.kwargs == {
        "Bucket": bucket_name,
        "Key": catalogs_to_generate[1]["file_name_and_path"],
    }

    # Compare arguments for third call
    second_call = calls[2]
    assert second_call.kwargs == {
        "Bucket": bucket_name,
        "Key": catalogs_to_generate[2]["file_name_and_path"],
    }

    assert deleted_keys == [file["file_name_and_path"] for file in catalogs_to_generate]


@mock.patch("pulsar.Producer")
def test_send_pulsar_message(mock_pulsar_producer: MagicMock) -> None:
    # Define test variable
    workspace_name = "test_ws"
    bucket_name = "test-bucket"
    created_keys = ["test-datasets/test-catalog/test-collection.json"]
    deleted_keys: list[str] = []

    # Generate test output message
    test_msg: dict[str, str | list[str]] = {
        "id": f"{workspace_name}-workspace-generator",
        "workspace": workspace_name,
        "branch": "",
        "bucket_name": bucket_name,
        "updated_keys": [],
        "added_keys": created_keys,
        "deleted_keys": deleted_keys,
        "source": workspace_name,
        "target": f"catalogs/user/catalogs/{workspace_name}",
    }

    test_msg_str_enc = json.dumps(test_msg).encode("utf-8")

    # Create a mock pulsar producer
    mock_pulsar_producer = mock.Mock()

    send_pulsar_message(test_msg, mock_pulsar_producer)

    # Check that the producer send method was called with the expected argument
    mock_pulsar_producer.send.assert_called_once_with(test_msg_str_enc)


@mock.patch("io.BytesIO")
@mock.patch("boto3.client")
def test_create_files(mock_boto_client: MagicMock, mock_bytes_io: MagicMock) -> None:
    # Set test variables
    workspace_name = "test-ws"
    bucket_name = "test-bucket"

    # Read expected catalogs config
    with open("config/required-files.json") as file:
        files_to_generate = json.load(file)["files"]

    # Replace workspace name in the catalog dictionary
    for file in files_to_generate:
        file["file_name_and_path"] = file["file_name_and_path"].replace("{workspace_name}", workspace_name)

    # Construct expected catalogs
    with open("templates/public-readme.md") as f:
        file_template = f.read()

    # Create a mock S3 client
    mock_boto_client = mock.Mock()

    create_files(mock_boto_client, workspace_name, bucket_name)

    # Check arguments were as expected
    calls = mock_boto_client.upload_fileobj.call_args_list

    # Compare arguments for first call
    call = calls[0]
    assert call.args[0].getvalue().decode("utf-8") == file_template
    assert call.args[1] == bucket_name
    assert call.args[2] == files_to_generate[0]["file_name_and_path"]


@mock.patch("io.BytesIO")
@mock.patch("boto3.client")
def test_delete_files(mock_boto_client: MagicMock, mock_bytes_io: MagicMock) -> None:
    # Set test variables
    workspace_name = "test-ws"
    bucket_name = "test-bucket"

    # Read expected catalogs config
    with open("config/required-files.json") as file:
        files_to_generate = json.load(file)["files"]

    # Replace workspace name in the catalog dictionary
    for file in files_to_generate:
        file["file_name_and_path"] = file["file_name_and_path"].replace("{workspace_name}", workspace_name)

    # Create a mock S3 client
    mock_boto_client = mock.Mock()

    delete_files(mock_boto_client, workspace_name, bucket_name)

    # Check arguments were as expected
    calls = mock_boto_client.delete_object.call_args_list

    # Compare arguments for first call
    call = calls[0]
    assert call[1]["Key"] == files_to_generate[0]["file_name_and_path"]
    assert call[1]["Bucket"] == bucket_name


@patch("catalog_generator.__main__.logger")
@patch("catalog_generator.__main__.boto3.client")
def test_upload_file_to_s3_success(mock_boto_client: MagicMock, mock_logger: MagicMock) -> None:
    # Arrange
    mock_client = mock_boto_client.return_value
    mock_client.upload_fileobj.return_value = None
    file_name = "test_file.json"
    bucket = "test_bucket"
    data = '{"key": "value"}'
    file_type = "file"

    # Act
    result = upload_file_to_s3(mock_client, file_name, bucket, data, file_type)

    # Assert
    mock_client.upload_fileobj.assert_called_once()
    mock_logger.info.assert_any_call(
        f"Uploaded {file_type} '{file_name}' to S3 bucket '{bucket}' with key '{file_name}'"
    )
    assert result


@patch("catalog_generator.__main__.logger")
@patch("catalog_generator.__main__.boto3.client")
def test_upload_file_to_s3_boto3_failure(mock_boto_client: MagicMock, mock_logger: MagicMock) -> None:
    # Arrange
    mock_client = mock_boto_client.return_value
    mock_client.upload_fileobj.side_effect = Exception("Boto3 upload failed")
    file_name = "test_file.json"
    bucket = "test_bucket"
    data = '{"key": "value"}'
    file_type = "file"

    # Act
    result = upload_file_to_s3(mock_client, file_name, bucket, data, file_type)

    # Assert
    mock_client.upload_fileobj.assert_called_once()
    mock_logger.error.assert_any_call(
        f"Failed to upload {file_type} '{file_name}' to S3 bucket '{bucket}' with key '{file_name}'"
    )
    assert not result


@pytest.mark.parametrize(
    ("env_value", "expected"),
    [
        pytest.param(None, "workspace-controller", id="unset"),
        pytest.param("", "workspace-controller", id="empty"),
        pytest.param(
            "persistent://public/workspaces/workspace-controller",
            "persistent://public/workspaces/workspace-controller",
            id="single",
        ),
        pytest.param(
            "workspace-controller, persistent://public/workspaces/workspace-controller",
            ["workspace-controller", "persistent://public/workspaces/workspace-controller"],
            id="list",
        ),
    ],
)
def test_workspace_controller_topics(
    monkeypatch: pytest.MonkeyPatch, env_value: str | None, expected: str | list[str]
) -> None:
    if env_value is None:
        monkeypatch.delenv("PULSAR_TOPIC_WORKSPACE_CONTROLLER", raising=False)
    else:
        monkeypatch.setenv("PULSAR_TOPIC_WORKSPACE_CONTROLLER", env_value)

    assert workspace_controller_topics() == expected
