import json
import logging
import traceback
from os import getenv

import boto3
from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl import Index, connections
from rac_es.documents import (Agent, BaseDescriptionComponent, Collection,
                              Object, Term)

# Configure Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Map object type strings to rac_es classes
OBJECT_TYPES = {
    "agent": Agent,
    "collection": Collection,
    "object": Object,
    "term": Term
}

# SSM path for configs
SERVICE_NAME = 'data_index'
FULL_CONFIG_PATH = f"/{getenv('ENV')}/{getenv('APP_CONFIG_PATH')}"


def get_config(ssm_parameter_path):
    """Fetch config values from AWS Parameter Store by path."""

    configuration = {}
    try:
        ssm_client = boto3.client('ssm', region_name=getenv('AWS_DEFAULT_REGION', 'us-east-1'))

        param_details = ssm_client.get_parameters_by_path(
            Path=ssm_parameter_path,
            Recursive=False,
            WithDecryption=True)

        for param in param_details.get('Parameters', []):
            param_path_array = param.get('Name').split("/")
            section_position = len(param_path_array) - 1
            section_name = param_path_array[section_position]
            configuration[section_name] = param.get('Value')

    except BaseException:
        logging.error("Encountered an error loading config from SSM.")
        traceback.print_exc()
    finally:
        return configuration


class DataIndexer:
    """Indexes transformed metadata records into Elasticsearch."""

    def __init__(self):
        """Initialize connections, configs, and clients"""

        self.config = get_config(FULL_CONFIG_PATH)

        # Elasticsearch connection
        hosts = self.config["ELASTICSEARCH_HOSTS"]
        connection_args = {"hosts": hosts, "timeout": 60}  # TODO: is this timeout still appropriate?
        if self.config.get("ELASTICSEARCH_API_KEY"):
            connection_args["api_key"] = self.config["ELASTICSEARCH_API_KEY"]
        self.connection = connections.create_connection(**connection_args)

        # Ensure the index exists
        index_name = self.config.get("ELASTICSEARCH_INDEX")
        if not Index(index_name).exists():
            raise RuntimeError(
                f"Elasticsearch index '{index_name}' does not exist. "
            )

        # SNS Setup
        self.sns_topic = self.config.get("AWS_SNS_TOPIC")
        self.sns_client = boto3.client("sns", region_name=getenv("AWS_DEFAULT_REGION", "us-east-1"))

    def run(self, event):
        """Main method that calls all other methods. Parses SQS messages,
        performs indexing actions, and sends notifications.
        """

        logger.info("Message batch received")
        grouped_actions = self.parse_batch(event)

        for object_type, actions in grouped_actions.items():

            indexed_ids = []
            deleted_ids = []

            # Index each object individually by type to send failure per object
            for obj in actions["add"]:
                try:
                    result = self.add(object_type, [obj])
                    indexed_ids += result
                except Exception as e:
                    self.deliver_failure_notification(obj["uri"], object_type, e)

            # Delete documents individually by type to send failure per object
            for obj in actions["delete"]:
                try:
                    result = self.delete([obj["es_id"]])
                    deleted_ids += result
                except Exception as e:
                    self.deliver_failure_notification(obj["uri"], object_type, e)

            # Notify success grouped by object_type
            self.deliver_success_notification(object_type, indexed_ids, deleted_ids)

    def parse_batch(self, event):
        """Parse SQS message data and group by object type and action.

        Batches contain multiple SQS records, each containing a single object.
        Records can contain different object types and actions. Delete actions come from
        data_fetch messages, while index actions come from data_transform messages.
        """

        grouped = {}

        for record in event.get("Records", []):
            try:
                body = json.loads(record["body"])
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON body")

            attributes = record.get("messageAttributes", {})
            requested_action = attributes.get("requested_action", {}).get("stringValue")

            grouped.setdefault(object_type, {"add": [], "delete": []})

            if requested_action == "index":
                obj = body["objects"][0]
                data = obj["data"]
                es_id = obj["es_id"]
                uri = data["uri"]
                object_type = data["object_type"]
                grouped[object_type]["add"].append({"es_id": es_id, "uri": uri, "data": data})

            elif requested_action == "delete":
                es_id = body["es_id"]
                uri = body["uri"]
                object_type = attributes.get("object_type", {}).get("stringValue")
                grouped[object_type]["delete"].append({"es_id": es_id, "uri": uri})

        return grouped

    def prepare_updates(self, doc_cls, objects):
        """Prepare documents for bulk indexing."""

        for obj in objects:
            doc = doc_cls(**obj["data"])
            yield doc.prepare_streaming_dict(obj["es_id"])

    def prepare_deletes(self, id_list):
        """Prepare document IDs for bulk deletion via BaseDescriptionComponent.

        Ignores documents which cannot be found in the index.
        """

        for obj_id in id_list:
            try:
                doc = BaseDescriptionComponent.get(id=obj_id)
                yield doc.prepare_streaming_dict(obj_id, "delete")
            except NotFoundError:
                pass

    def add(self, object_type, index_objects):
        """Add documents to the Elasticsearch index for a given object_type.

        Returns a list of successfully indexed ids.
        """

        doc_cls = OBJECT_TYPES.get(object_type)
        return doc_cls.bulk_action(
            self.connection,
            self.prepare_updates(doc_cls, index_objects)
        )

    def delete(self, delete_ids):
        """Bulk delete documents from Elasticsearch.

        Returns a list of successfully deleted ids.
        """

        return BaseDescriptionComponent.bulk_action(
            self.connection,
            self.prepare_deletes(delete_ids)
        )

    def deliver_success_notification(self, object_type, indexed_ids, deleted_ids):
        """Send a message to an SNS topic when indexing completes successfully for a batch.
        Includes a count of indexed and deleted documents, and groups messages by object type.
        """

        self.sns_client.publish(
            TopicArn=self.sns_topic,
            MessageGroupId=f'{SERVICE_NAME}-{object_type}',
            MessageDeduplicationId=f'{SERVICE_NAME}-{object_type}-success',
            Message=f"Successfully indexed {len(indexed_ids)} documents and deleted {len(deleted_ids)} documents for object type {object_type}",
            MessageAttributes={
                'service': {
                    'DataType': 'String',
                    'StringValue': SERVICE_NAME,
                },
                'object_type': {
                    'DataType': 'String',
                    'StringValue': object_type,
                },
                'outcome': {
                    'DataType': 'String',
                    'StringValue': 'SUCCESS',
                },
                'indexed_count': {
                    'DataType': 'Number',
                    'StringValue': str(len(indexed_ids)),
                },
                'deleted_count': {
                    'DataType': 'Number',
                    'StringValue': str(len(deleted_ids)),
                }
            })

    def deliver_failure_notification(self, uri, object_type, exception):
        """Send message to an SNS topic when indexing fails for an object.
        """

        tb = ''.join(traceback.format_exception(exception)[:-1])
        self.sns_client.publish(
            TopicArn=self.sns_topic,
            MessageGroupId=f'{SERVICE_NAME}-{uri}',
            MessageDeduplicationId=f'{SERVICE_NAME}-{uri}-failure',
            Message=tb,
            MessageAttributes={
                'service': {
                    'DataType': 'String',
                    'StringValue': SERVICE_NAME,
                },
                'object_type': {
                    'DataType': 'String',
                    'StringValue': object_type,
                },
                'outcome': {
                    'DataType': 'String',
                    'StringValue': 'FAILURE',
                },
                'message': {
                    'DataType': 'String',
                    'StringValue': str(exception),
                }
            })


def lambda_handler(event, context):
    """AWS Lambda entry point that initializes and runs the DataIndexer."""
    DataIndexer().run(event)
