# New data_index service logic
# #!/usr/bin/env python3

# TODO: Only use logger instead of print statements? Generally review and align exception handling across methods.

import json
import logging
import traceback
import boto3

from os import getenv
from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl import Index, connections
from rac_es.documents import (
    Agent, Collection, Object, Term, BaseDescriptionComponent
)

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
full_config_path = f"/{getenv('ENV')}/{getenv('APP_CONFIG_PATH')}"  # Is this correct?

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
        
        config = get_config(full_config_path)

        # Elasticsearch connection
        # Doing my best to convert from Djano settings/configs, but could use a second look
        hosts = config["ELASTICSEARCH_HOSTS"]
        connection_args = {"hosts": hosts, "timeout": 60} # TODO: is this timeout still appropriate?
        if config.get("ELASTICSEARCH_API_KEY"):
            connection_args["api_key"] = config["ELASTICSEARCH_API_KEY"]
        self.connection = connections.create_connection(**connection_args)

        # Ensure the index exists
        if not Index(config.get("ELASTICSEARCH_INDEX")).exists():
            BaseDescriptionComponent.init()

        # SNS Setup
        self.sns_topic = config.get("AWS_SNS_TOPIC")
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
            try:
                indexed_ids = []
                deleted_ids = []

                # Merge/index all objects for this type
                if actions["merge"]:
                    indexed_ids = self.add(object_type, actions["merge"])

                # Delete all documents for this type
                if actions["delete"]:
                    deleted_ids = self.delete(actions["delete"])

                # Notify success by object_type
                self.deliver_success_notification(object_type, indexed_ids, deleted_ids) # Do we want success messages by object type, or just one message for the whole batch?

            except Exception as e:
                self.deliver_failure_notification(e)
    
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
            requested_action = attributes.get("requested_action", {}).get("stringValue")

            objects = body.get("objects", [])
            
            # Get object_type from the object data, since message data can contain multiple object types.
            for obj in objects:
                data = obj.get("data")
                es_id = obj.get("es_id")
                object_type = data.get("object_type")

                # Group by object type and action
                grouped.setdefault(object_type, {"merge": [], "delete": []})

                if requested_action == "merge":
                    grouped[object_type]["merge"].append(obj)
                elif requested_action == "delete":
                    grouped[object_type]["delete"].append(es_id) # Delete doesn't need object type, just the ids. Useful for logging?

        return grouped
    
    def prepare_updates(self, doc_cls, objects):
        """Prepare documents for bulk indexing."""

        for obj in objects:
            doc = doc_cls(**obj["data"])
            try:
                yield doc.prepare_streaming_dict(obj["es_id"])
            except Exception as e:
                raise Exception("Error preparing streaming dict: {}".format(e)) # Use logger?

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
                print(e) # TODO: use logger instead of print statements?

    def add(self, object_type, merge_objects):
        """Add (merge) documents to the Elasticsearch index for a given object_type.

        Returns a list of successfully indexed ids.
        """
        
        doc_cls = OBJECT_TYPES.get(object_type)
        indexed_ids = [] # Not sure I actually need to create this list here and return it, since the bulk_action method is returning the list of indexed ids.

        try:
            indexed_ids += doc_cls.bulk_action(
                self.connection,
                self.prepare_updates(doc_cls, merge_objects)
            )
        except Exception as e:
            raise Exception("Error adding documents: {}".format(e))
        
        return indexed_ids
    
    def delete(self, delete_ids):
        """Bulk delete documents from Elasticsearch.
        
        Returns a list of successfully deleted ids.
        """

        deleted_ids = [] # Like the add method, not sure this is necessary.

        try:
            deleted_ids += BaseDescriptionComponent.bulk_action(
                self.connection,
                self.prepare_deletes(delete_ids))
        except Exception as e:
            raise Exception("Error deleting documents: {}".format(e))
        
        return deleted_ids

    def deliver_success_notification(self, object_type, indexed_ids, deleted_ids):
        """Send a message to an SNS topic when indexing completes successfully."""
        # Not sure what level of data to include as part of success. Include ids for objects?
        pass

    def deliver_failure_notification(self, exception):
        """Send a message to an SNS topic when indexing fails."""
        # Not sure what level of data to include as part of failure.
        pass

def lambda_handler(event, context):
    """AWS Lambda entry point that initializes and runs the DataIndexer."""
    DataIndexer().run(event)