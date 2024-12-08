from cassandra.cqlengine.columns import Text, DateTime, Integer, Double, List, Set, Map
from cassandra.cqlengine.models import Model

from django_cassandra_fake.connection import DMLQuery, ModelQuerySet


class User(Model):
    __abstract__ = True
    username = Text(primary_key=True, partition_key=True)
    company = Integer(primary_key=True, clustering_order='DESC')
    height = Double(db_field='user_height')
    created_at = DateTime()
    skills = List(value_type=Text)
    skill_matrix = Map(key_type=Text, value_type=Integer)
    assignments = Set(value_type=Text, required=True)


class UserReal(User):
    __abstract__ = False
    __keyspace__ = 'default'
    __connection__ = 'default'


class UserFake(User):
    __abstract__ = False
    __keyspace__ = 'fake'
    __dmlquery__ = DMLQuery
    __queryset__ = ModelQuerySet
    __connection__ = 'fake'
