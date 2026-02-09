# New data_index service logic
# #!/usr/bin/env python3

# TODO: Only use logger instead of print statements?

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
    """Fetch config values from Parameter Store.

    Args:
        ssm_parameter_path (str): Path to parameters

    Returns:
        configuration (dict): all parameters found at the supplied path.
    """
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
        """Main method that calls all other methods.
        
        Args:
            event (dict): SQS event containing messages.
        """

        logger.info("Message batch received")
        pass

    
    def process_message(self, record):
        """Parse SQS message data.
        
        Args:
            record (dict): A single SQS message record.
        """
        pass

    def add(self, doc_cls, objects):
        """Bulk index documents into Elasticsearch.

        Args:
            doc_cls (class): Elasticsearch document class.
            objects (list): List of objects to be indexed.
        """
        pass

    def delete(self, doc_cls, objects):
        """Bulk delete documents from Elasticsearch.

        Args:
            doc_cls (class): Elasticsearch document class.
            objects (list): List of objects to delete.
        """
        pass

    def deliver_success_notification(self):
        """Send a message to an SNS topic when processing completes successfully."""
        # Not sure what level of data to include as part of success. Include ids for objects?
        pass

    def deliver_failure_notification(self, exception):
        """Send a message to an SNS topic when processing fails."""
        # Not sure what level of data to include as part of failure.
        pass


def lambda_handler(event, context):
    """AWS Lambda entry point that initializes and runs the DataIndexer."""
    DataIndexer().run(event)