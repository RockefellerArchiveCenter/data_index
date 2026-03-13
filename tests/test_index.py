import json
from unittest import TestCase
from unittest.mock import ANY, MagicMock, call, patch

import boto3
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID

from src.indexer import DataIndexer, lambda_handler

DEFAULT_CONFIG = {
    "ELASTICSEARCH_HOSTS": ["elasticsearch:9200"],
    "ELASTICSEARCH_INDEX": "test-index",
    "AWS_SNS_TOPIC": "sns-topic",
}

OBJECT_1 = {"uri": "/repositories/2/archival_objects/1"}
OBJECT_2 = {"uri": "/repositories/2/archival_objects/2"}

records = [
    {
        "body": json.dumps(OBJECT_1),
        "messageAttributes": {
            "requested_action": {"stringValue": "index"},
            "es_id": {"stringValue": "1"},
            "object_type": {"stringValue": "collection"},
        }
    },
    {
        "body": json.dumps(OBJECT_2),
        "messageAttributes": {
            "requested_action": {"stringValue": "index"},
            "es_id": {"stringValue": "2"},
            "object_type": {"stringValue": "agent"},
        }
    },
    {
        "messageAttributes": {
            "requested_action": {"stringValue": "delete"},
            "es_id": {"stringValue": "3"},
            "object_type": {"stringValue": "collection"},
        }
    },
    {
        "messageAttributes": {
            "requested_action": {"stringValue": "delete"},
            "es_id": {"stringValue": "4"},
            "object_type": {"stringValue": "collection"},
        }
    },
]


class DataIndexerInitTests(TestCase):
    """Test that config is loaded and a missing Elasticsearch index raises an error."""

    @patch("src.indexer.get_config")
    @patch("src.indexer.Index")
    @patch("src.indexer.connections.create_connection")
    def test_init(self, mock_create_connection, mock_index_cls, mock_get_config):
        mock_get_config.return_value = DEFAULT_CONFIG
        mock_index_cls.return_value.exists.return_value = True

        indexer = DataIndexer()

        mock_get_config.assert_called_once()
        self.assertEqual(indexer.sns_topic, DEFAULT_CONFIG["AWS_SNS_TOPIC"])

        """Test missing index error handling."""
        mock_index_cls.return_value.exists.return_value = False
        with self.assertRaises(RuntimeError):
            DataIndexer()


class DataIndexerMethodTests(TestCase):

    @patch("src.indexer.get_config")
    @patch("src.indexer.Index")
    @patch("src.indexer.connections.create_connection")
    def setUp(self, mock_create_connection, mock_index_cls, mock_get_config):
        mock_get_config.return_value = DEFAULT_CONFIG
        mock_index_cls.return_value.exists.return_value = True
        self.indexer = DataIndexer()

    def set_up_sns(self, region_name="us-east-1"):
        client = boto3.client("sns", region_name=region_name)
        topic_arn = client.create_topic(
            Name="test-topic.fifo", Attributes={"FifoTopic": "true"}
        )["TopicArn"]
        self.indexer.sns_topic = topic_arn
        sqs_conn = boto3.resource("sqs", region_name=region_name)
        sqs_conn.create_queue(QueueName="test-queue")
        client.subscribe(
            TopicArn=topic_arn,
            Protocol="sqs",
            Endpoint=f"arn:aws:sqs:{region_name}:{DEFAULT_ACCOUNT_ID}:test-queue",
        )
        queue = sqs_conn.get_queue_by_name(QueueName="test-queue")
        return queue

    def test_parse_batch_groups_by_type_and_action(self):
        """Records are grouped by object_type, with index actions in 'add' and delete actions in 'delete'."""

        grouped = self.indexer.parse_batch({"Records": records})

        self.assertIn("collection", grouped)
        self.assertEqual(
            grouped["collection"]["add"],
            [{"es_id": "1", "data": OBJECT_1, "object_status": "updated"}]
        )
        self.assertIn("agent", grouped)
        self.assertEqual(
            grouped["agent"]["add"],
            [{"es_id": "2", "data": OBJECT_2, "object_status": "updated"}]
        )
        self.assertEqual(
            grouped["collection"]["delete"],
            [{"es_id": "3", "object_status": "deleted"}, {"es_id": "4", "object_status": "deleted"}]
        )

    def test_parse_batch_invalid_json_raises(self):
        """Test that invalid JSON in a batch record raises a ValueError."""

        records = [
            {
                "body": "{invalid-json",
                "messageAttributes": {
                    "requested_action": {"stringValue": "index"},
                    "es_id": {"stringValue": "1"},
                    "object_type": {"stringValue": "collection"}
                }
            }
        ]

        with self.assertRaises(ValueError):
            self.indexer.parse_batch({"Records": records})

    @patch("src.indexer.BaseDescriptionComponent")
    def test_prepare_deletes_skips_missing(self, mock_base_component):
        """Documents not found in the index are skipped when preparing deletes."""

        from elasticsearch.exceptions import NotFoundError

        # A fake Elasticsearch document standing in for a real BaseDescriptionComponent instance.
        # MagicMock is used here so we can control the return value of prepare_streaming_dict
        # without needing a real Elasticsearch connection.
        doc = MagicMock()
        doc.prepare_streaming_dict.return_value = {"_id": "3"}
        mock_base_component.get.side_effect = [doc, NotFoundError()]

        deletes = list(self.indexer.prepare_deletes(["3", "4"]))

        self.assertEqual(deletes, [{"_id": "3"}])
        mock_base_component.get.assert_has_calls([call(id="3"), call(id="4")])

    def test_add_calls_bulk_action(self):
        """Test that add prepares documents and calls bulk_action on the correct document class."""

        doc_cls = MagicMock()
        doc_cls.bulk_action.return_value = ["1"]
        self.indexer.prepare_updates = MagicMock(return_value=iter([{"_id": "1"}]))

        index_objects = [{"es_id": "1", "data": OBJECT_1, "object_status": "updated"}]

        with patch.dict("src.indexer.OBJECT_TYPES", {"collection": doc_cls}):
            output = self.indexer.add("collection", index_objects)

            doc_cls.bulk_action.assert_called_once_with(
                self.indexer.connection, self.indexer.prepare_updates(doc_cls, index_objects)
            )
            self.assertEqual(output, ["1"])

    @patch("src.indexer.BaseDescriptionComponent")
    def test_delete_calls_bulk_action(self, mock_base_component):
        """Test that delete prepares documents and calls bulk_action on BaseDescriptionComponent for deletes."""

        mock_base_component.bulk_action.return_value = ["1"]
        self.indexer.prepare_deletes = MagicMock(return_value=iter([{"_id": "1"}]))

        output = self.indexer.delete(["1"])

        mock_base_component.bulk_action.assert_called_once_with(
            self.indexer.connection, self.indexer.prepare_deletes(["1"])
        )
        self.assertEqual(output, ["1"])

    @mock_aws
    def test_deliver_success_notification(self):
        """Test that a success message is sent to SNS with the expected attributes when indexing succeeds."""

        queue = self.set_up_sns()

        self.indexer.deliver_success_notification("collection", ["1", "2"], ["3", "4"])

        messages = queue.receive_messages(MaxNumberOfMessages=1)
        message_body = json.loads(messages[0].body)
        self.assertIn("Successfully indexed 2 documents and deleted 2 documents", message_body["Message"])
        self.assertEqual(
            message_body["MessageAttributes"],
            {
                "service": {
                    "Type": "String",
                    "Value": "data_index"
                },
                "object_type": {
                    "Type": "String",
                    "Value": "collection"
                },
                "outcome": {
                    "Type": "String",
                    "Value": "SUCCESS"
                },
                "indexed_count": {
                    "Type": "Number",
                    "Value": "2"
                },
                "deleted_count": {
                    "Type": "Number",
                    "Value": "2"
                }})

    @mock_aws
    def test_deliver_failure_notification(self):
        """Test that a failure message is sent to SNS with the expected attributes when indexing fails."""

        queue = self.set_up_sns()

        self.indexer.deliver_failure_notification(
            "1", "collection", "updated", Exception("foo")
        )

        messages = queue.receive_messages(MaxNumberOfMessages=1)
        message_body = json.loads(messages[0].body)
        self.assertEqual(message_body["Message"], "")
        self.assertEqual(
            message_body["MessageAttributes"],
            {
                "service": {
                    "Type": "String",
                    "Value": "data_index"
                },
                "outcome": {
                    "Type": "String",
                    "Value": "FAILURE"
                },
                "object_type": {
                    "Type": "String",
                    "Value": "collection"
                },
                "object_status": {
                    "Type": "String",
                    "Value": "updated"
                },
                "object_id": {
                    "Type": "String",
                    "Value": "1"
                },
                "message": {
                    "Type": "String",
                    "Value": "foo"
                }})


class LambdaHandlerTests(TestCase):

    @patch("src.indexer.DataIndexer")
    def test_lambda_handler(self, mock_indexer_cls):
        """Test that the lambda handler parses the batch, calls add and delete, and delivers a success notification."""

        indexer = mock_indexer_cls.return_value
        indexer.parse_batch.return_value = {
            "collection": {
                "add": [{"es_id": "1", "data": OBJECT_1, "object_status": "updated"}],
                "delete": [{"es_id": "3", "object_status": "deleted"}, {"es_id": "4", "object_status": "deleted"}],
            },
            "agent": {
                "add": [{"es_id": "2", "data": OBJECT_2, "object_status": "updated"}],
                "delete": [],
            },
        }
        indexer.add.side_effect = [["1"], ["2"]]
        indexer.delete.side_effect = [["3"], ["4"]]

        lambda_handler({"Records": records}, None)

        mock_indexer_cls.assert_called_once()
        indexer.parse_batch.assert_called_once_with({"Records": records})
        indexer.add.assert_has_calls([
            call("collection", [{"es_id": "1", "data": OBJECT_1, "object_status": "updated"}]),
            call("agent", [{"es_id": "2", "data": OBJECT_2, "object_status": "updated"}])
        ])
        indexer.delete.assert_has_calls([
            call(["3"]),
            call(["4"])
        ])
        indexer.deliver_success_notification.assert_has_calls([
            call("collection", ["1"], ["3", "4"]),
            call("agent", ["2"], [])
        ])

    @patch("src.indexer.DataIndexer")
    def test_lambda_handler_handles_exceptions_per_object(self, mock_indexer_cls):
        """Test that exceptions raised during add/delete are handled by object and delivers a failure notification."""

        indexer = mock_indexer_cls.return_value
        indexer.parse_batch.return_value = {
            "collection": {
                "add": [{"es_id": "1", "data": OBJECT_1, "object_status": "updated"}],
                "delete": [{"es_id": "3", "object_status": "deleted"}, {"es_id": "4", "object_status": "deleted"}]
            },
            "agent": {
                "add": [{"es_id": "2", "data": OBJECT_2, "object_status": "updated"}],
                "delete": []
            },
        }
        indexer.add.side_effect = Exception("add-failure")
        indexer.delete.side_effect = Exception("delete-failure")

        lambda_handler({"Records": records}, None)

        indexer.deliver_failure_notification.assert_has_calls(
            [
                call("1", "collection", "updated", ANY),
                call("3", "collection", "deleted", ANY),
                call("4", "collection", "deleted", ANY),
                call("2", "agent", "updated", ANY),
            ]
        )
