# Copied from scorpio: base/indexer/indexers.py

import requests # Delete, probably
from django.utils import timezone # Replace with lambda-friendly datetime
from elasticsearch.exceptions import NotFoundError # Probably keep? Need elasticsearch
from elasticsearch_dsl import Index, connections # Probably keep? Need elasticsearch
from electronbonder.client import ElectronBond # Remove, not fetching data
from rac_es.documents import (Agent, BaseDescriptionComponent, Collection,
                              Object, Term) # Keep

from scorpio import settings # Remove and replace with secrets in AWS SSM

from .models import IndexRun, IndexRunError # Remove and replace with logging and SNS notifications

OBJECT_TYPES = {
    "agent": Agent,
    "collection": Collection,
    "object": Object,
    "term": Term
}

# Refactor: Delete. Not needed in data_index because it will use SNS notifications
# instead of post requests to pisces
def update_pisces(identifiers, action):
    try:
        resp = requests.post("/".join([
            settings.PISCES["baseurl"].rstrip("/"),
            settings.PISCES["post_index_path"].lstrip("/")]),
            json={"identifiers": identifiers, "action": action})
        resp.raise_for_status()
    except requests.HTTPError as e:
        print("Error sending request to Pisces: {}".format(e.response.json()["detail"]))

# Refactor: Delete? This isn't being used in the old script either.
class ScorpioIndexError(Exception):
    pass

# Refactor: Keep some of this logic in new code as part of DataIndexer
class Indexer:
    """
    Main indexer class, which adds merged documents to the index or removes
    documents from the index.
    """

    # Refactor: Still need to connect to ES and create index, but have to load elasticsearch 
    # configs from somewhere else (not Django Settings). Don't need to connect to pisces.
    def __init__(self):
        connection_args = {'hosts': settings.ELASTICSEARCH['default']['hosts'], 'timeout': 60}
        if settings.ELASTICSEARCH['default'].get('api_key'):
            connection_args['api_key'] = settings.ELASTICSEARCH['default']['api_key']
        self.connection = connections.create_connection(**connection_args)
        if not Index(settings.ELASTICSEARCH['default']['index']).exists():
            BaseDescriptionComponent.init()
        self.pisces_client = ElectronBond(baseurl=settings.PISCES['baseurl'])

    # Refactor: Data now comes pre-bundled in SQS message, so update data source.
    def prepare_updates(self, obj_type, doc_cls, clean):
        """Prepares objects to be indexed"""
        for obj in self.fetch_objects(obj_type, clean):
            doc = doc_cls(**obj["data"])
            try:
                yield doc.prepare_streaming_dict(obj["es_id"])
            except Exception as e:
                raise Exception("Error preparing streaming dict: {}".format(e))

    # Refactor: I think keep this as is? Not sure about printing exception v. logging, though.
    def prepare_deletes(self, id_list):
        """Prepares objects to be deleted.

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

    # Refactor: Delete. Not needed since data is coming from SQS messages
    def fetch_objects(self, object_type, clean):
        """Returns data to be indexed."""
        try:
            url = "objects/{}s/".format(object_type)
            return self.pisces_client.get_paged(url, params={"clean": clean})
        except Exception as e:
            raise Exception("Error fetching objects: {}".format(e))

    # Refactor: Still need to add and delete documents to/from index using ES bulk indexing,
    # but not involving Django or Pisces. SQS is batching them. Use SNS success/failure notification
    def add(self, object_type=None, clean=False, **kwargs):
        """Adds documents to index using ES bulk indexing."""
        object_types = [object_type] if object_type else OBJECT_TYPES
        indexed_ids = []
        for obj_type in object_types:
            current_run = IndexRun.objects.create(
                status=IndexRun.STARTED,
                object_type=obj_type,
                object_status="indexed")
            doc_cls = OBJECT_TYPES[obj_type]
            try:
                indexed_ids += doc_cls.bulk_action(
                    self.connection,
                    self.prepare_updates(obj_type, doc_cls, clean),
                    None if clean else settings.MAX_OBJECTS)
                current_run.status = IndexRun.FINISHED
                current_run.end_time = timezone.now()
                current_run.save()
            except Exception as e:
                print(e)
                IndexRunError.objects.create(
                    message=e,
                    run=current_run)
        update_pisces(indexed_ids, "indexed")
        return indexed_ids

    def delete(self, object_type=None, identifiers=[], **kwargs):
        """Deletes documents from the index using ES bulk indexing."""
        deleted_ids = []
        current_run = IndexRun.objects.create(
            status=IndexRun.STARTED,
            object_type=object_type,
            object_status="deleted")
        try:
            deleted_ids += BaseDescriptionComponent.bulk_action(
                self.connection,
                self.prepare_deletes(identifiers))
            current_run.status = IndexRun.FINISHED
            current_run.end_time = timezone.now()
            current_run.save()
        except Exception as e:
            print(e)
            IndexRunError.objects.create(
                message=e,
                run=current_run)
        update_pisces(deleted_ids, "deleted")
        return deleted_ids

    # Refactor: Delete this, I think?
    def reset(self, **kwargs):
        try:
            BaseDescriptionComponent._index.delete()
            return "Index deleted.", BaseDescriptionComponent._index._name
        except NotFoundError:
            return "Index does not exist."
