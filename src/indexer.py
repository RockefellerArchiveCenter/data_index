# New data_index service logic
# #!/usr/bin/env python3

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class DataIndexer:
    """Base class to index metadata."""

    def __init__(self):
        pass

    def run(self, event):
        """Process SQS message batch. Iterate over each message to parse data"""
        logger.info("Message batch received.")
        pass

    def process_message(self, record):
        """Parse message data."""
        pass

    def index_record(self, data):
        """Run indexing logic using parsed message data."""
        pass

    def deliver_success_notification(self, data):
        """Send SNS message notification of success."""
        pass
    

    def deliver_failure_notification(self, data, exception):
        """Send SNS message notification of failure.

        Args:
            exception (Exception): the exception that was thrown.
        """
        pass

if __name__ == "__main__":
    DataIndexer(
    ).run()

