from peewee import *
from playhouse.shortcuts import ThreadSafeDatabaseMetadata


class BaseModel(Model):
    class Meta:
        database = DatabaseProxy()
        model_metadata_class = ThreadSafeDatabaseMetadata
        legacy_table_names=False
