# New data_index service logic
# #!/usr/bin/env python3

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
full_config_path = f"/{getenv('ENV')}/{getenv('APP_CONFIG_PATH')}"

class DataIndexer:
    """Indexes transformed metadata records into Elasticsearch."""

    def __init__(self):
        """Initialize connections, configs, and clients"""
        config = get_config(full_config_path)

        # Elasticsearch connection

        # Ensure the index exists

        # SNS Setup
        pass

    def run(self, event):
        """Main method that calls all other methods.
        
        Args:
            event (dict): SQS event containing messages.
        """

        logger.info("Message batch received")
        pass

    def get_config(ssm_parameter_path):
        """Fetch config values from Parameter Store.

        Args:
            ssm_parameter_path (str): Path to parameters

        Returns:
            configuration (dict): all parameters found at the supplied path.
        """
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
        """Send a message to an SNS topic when processesing completes successfully."""
        # Not sure what level of data to include as part of success. Include ids for objects?
        pass

    def deliver_failure_notification(self, exception):
        """Send a message to an SNS topic when processing fails."""
        # Not sure what level of data to include as part of failure.
        pass


def lambda_handler(event, context):
    """AWS Lambda entry point that initializes and runs the DataIndexer."""
    DataIndexer().run(event)