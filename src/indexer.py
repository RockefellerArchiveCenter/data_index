# #!/usr/bin/env python3

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
        ssm_client = boto3.client(
            'ssm',
            region_name=getenv('AWS_DEFAULT_REGION', 'us-east-1'))

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
        # Doing my best to convert from Djano settings/configs, but could use a
        # second look
        hosts = self.config["ELASTICSEARCH_HOSTS"]
        # TODO: is this timeout still appropriate?
        connection_args = {"hosts": hosts, "timeout": 60}
        if self.config.get("ELASTICSEARCH_API_KEY"):
            connection_args["api_key"] = self.config["ELASTICSEARCH_API_KEY"]
        self.connection = connections.create_connection(**connection_args)

        # Ensure the index exists
        if not Index(self.config.get("ELASTICSEARCH_INDEX")).exists():
            BaseDescriptionComponent.init()

        # SNS Setup
        self.sns_topic = self.config.get("AWS_SNS_TOPIC")
        self.sns_client = boto3.client(
            "sns",
            region_name=getenv("AWS_DEFAULT_REGION", "us-east-1")
        )

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
            for obj in actions["merge"]:
                try:
                    result = self.add(object_type, [obj])
                    indexed_ids += result
                except Exception as e:
                    self.deliver_failure_notification(
                        obj["data"], object_type, e)

            # Delete documents individually by type to send failure per object
            for obj_id in actions["delete"]:
                try:
                    result = self.delete([obj_id])
                    deleted_ids += result
                except Exception as e:
                    self.deliver_failure_notification(
                        obj["data"], object_type, e)

            # Notify success grouped by object_type
            self.deliver_success_notification(
                object_type, indexed_ids, deleted_ids)

    def parse_batch(self, event):
        """Parse SQS message data and group by object type and action.

        Supports batches with mixed object types.
        Merge lists contain full object dicts; delete lists contain es_ids.
        """

        grouped = {}

        for record in event.get("Records", []):
            try:
                body = json.loads(record["body"])
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON body")

            attributes = record.get("messageAttributes", {})
            requested_action = attributes.get(
                "requested_action", {}).get("stringValue")

            # data_transform is not sending "objects" right now, though?
            objects = body.get("objects", [])

            # Get object_type from the object data, since message data can
            # contain multiple object types.
            for obj in objects:
                data = obj.get("data")
                # Assuming es_id is in the message data
                es_id = obj.get("es_id")
                object_type = data.get("object_type")

                # Group by object type and action
                grouped.setdefault(object_type, {"merge": [], "delete": []})

                if requested_action == "merge":
                    grouped[object_type]["merge"].append(obj)
                elif requested_action == "delete":
                    grouped[object_type]["delete"].append(es_id)

        return grouped

    def prepare_updates(self, doc_cls, objects):
        """Prepare documents for bulk indexing."""

        for obj in objects:
            doc = doc_cls(**obj["data"])
            try:
                yield doc.prepare_streaming_dict(obj["es_id"])
            except Exception as e:
                raise Exception(
                    # Use logger?
                    "Error preparing streaming dict: {}".format(e))

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
            except Exception as e:
                print(e)

    def add(self, object_type, index_objects):
        """Add documents to the Elasticsearch index for a given object_type.

        Returns a list of successfully indexed ids.
        """

        doc_cls = OBJECT_TYPES.get(object_type)
        # Not sure I actually need to create this list here and return it,
        # since the bulk_action method is returning the list of indexed ids.
        indexed_ids = []

        try:
            indexed_ids += doc_cls.bulk_action(
                self.connection,
                self.prepare_updates(doc_cls, index_objects)
            )
        except Exception as e:
            raise Exception("Error adding documents: {}".format(e))

        return indexed_ids

    def delete(self, delete_ids):
        """Bulk delete documents from Elasticsearch.

        Returns a list of successfully deleted ids.
        """

        deleted_ids = []  # Like the add method, not sure this is necessary.

        try:
            deleted_ids += BaseDescriptionComponent.bulk_action(
                self.connection,
                self.prepare_deletes(delete_ids))
        except Exception as e:
            raise Exception("Error deleting documents: {}".format(e))

        return deleted_ids

    def deliver_success_notification(
            self, object_type, indexed_ids, deleted_ids):
        """Send a message to an SNS topic when indexing completes successfully for a batch.
        Includes a count of indexed and deleted documents, and groups messages by object type.
        """

        client = boto3.client('sns', region_name=getenv('AWS_DEFAULT_REGION', 'us-east-1'))
        client.publish(
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

    def deliver_failure_notification(self, data, object_type, exception):
        """Send message to an SNS topic when indexing fails for an object.
        """

        client = boto3.client('sns', region_name=getenv('AWS_DEFAULT_REGION', 'us-east-1'))
        tb = ''.join(traceback.format_exception(exception)[:-1])
        client.publish(
            TopicArn=self.sns_topic,
            MessageGroupId=f'{SERVICE_NAME}-{data["uri"]}',
            MessageDeduplicationId=f'{SERVICE_NAME}-{data["uri"]}-failure',
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
