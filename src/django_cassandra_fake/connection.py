import copy
import datetime
import logging
from collections import defaultdict
from dataclasses import dataclass
from threading import Lock
from typing import FrozenSet, Optional, OrderedDict, Tuple, Dict, List as TList, Set as TSet, Any, List

from cassandra.cqlengine.columns import Column, Set, List, DateTime, Date, Map, BaseContainerColumn
from cassandra.cqlengine.query import ModelQuerySet as ModelQuerySetBase, DMLQuery as DMLQueryBase, LWTException
from cassandra.cqlengine.statements import InsertStatement, DeleteStatement, UpdateStatement, SelectStatement, \
    BaseCQLStatement, ListUpdateClause, AssignmentClause
from cassandra.cqltypes import SimpleDateType

from .compat import (
    CQLEngineException,
    PlainTextAuthProvider,
    Session,
    connection,
)

log = logging.getLogger(__name__)


class Cursor(object):
    def __init__(self, connection):
        self.connection = connection

    def execute(self, *args, **kwargs):
        return self.connection.execute(*args, **kwargs)

    def close(self):
        pass

    def fetchmany(self, _):
        return []

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()


class FakeConnection(object):
    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self):
        return Cursor(None)

    def close(self):
        pass


@dataclass
class Keyspace:
    pass


@dataclass
class Cluster:
    connection: str

    def __getattr__(self, item):
        log.debug(f"{self.__class__.__name__}.{item} is called")

    def refresh_schema_metadata(self):
        pass


_connections = {}
_keyspaces = {}


class Table:
    def __init__(self, name, columns: OrderedDict[str, Column], db_map: Dict[str, str]):
        self._name = name
        self._columns = columns
        self._column_names = {
            column.db_field_name
            for column
            in columns.values()
        }
        self._db_map = db_map
        self._primary_key_names = {
            column_name
            for column_name, column
            in columns.items()
            if column.primary_key
        }
        self._entries = []
        self._lock = Lock()

    def _get_item_by_idx(self, idx: int, select: TList[str]) -> Dict[str, Any]:
        """
        :type idx: int

        :rtype:  Dict[str, Any]
        """
        return {
            self._db_map.get(field_name, field_name): self._entries[idx][field_name]
            for field_name
            in select
        }

    def _find_by_pk(self, *pks) -> Optional[int]:
        for idx, entry in enumerate(self._entries):
            match = True
            for pk_name, pk_value in zip(self._primary_key_names, pks):
                if entry[pk_name] != pk_value:
                    match = False

            if match:
                return idx

        return None

    def _get_indices_by_kwargs(self, limit: int = None, **kwargs) -> TList[int]:
        """
        :type limit: int

        :return: List[int]
        """
        if not kwargs:
            return range(len(self._entries))

        indices = []
        for idx, entry in enumerate(self._entries):
            match = True
            for kwarg_name, kwarg_value in kwargs.items():
                if kwarg_name not in entry:
                    match = False
                    break

                if entry[kwarg_name] != kwarg_value:
                    match = False
                    break

            if match:
                indices.append(idx)

            # limit number of returned items
            if limit is not None and len(indices) == limit:
                return indices

        return indices

    def find(self, filters: Dict[str, Any],
             limit: int,
             select: TList[str]) -> TList[Dict[str, Any]]:
        """
        :type filters: Dict[str, Any]
        :type limit: int
        :type select: TList[str]

        :rtype: TList[Dict[str, Any]]
        """
        with self._lock:
            return [
                copy.deepcopy(self._get_item_by_idx(idx, select))
                for idx
                in self._get_indices_by_kwargs(limit, **filters)
            ]

    def _default_value(self, column_name: str) -> Any:
        default_values = {
            List: [],
            Set: set(),
            Map: {}
        }

        column_name = self._db_map.get(column_name, column_name)
        column = self._columns.get(column_name)
        return default_values.get(type(column))


    def insert(self, values: Dict[str, Any], if_not_exists: Optional[bool] = False):
        """
        :type values: dict
        :type if_not_exists: Optional[bool]

        :raises:
            LWTException
        """
        entry = {
            column_name: values.get(column_name, self._default_value(column_name))
            for column_name
            in self._column_names
        }

        # Do not let insertions if primary key is not fully presented
        for pk_field in self._primary_key_names:
            if entry.get(pk_field) is None:
                raise CQLEngineException("Primary key was not provided")

        with self._lock:
            pks = [
                entry[pk_name]
                for pk_name
                in self._primary_key_names
            ]
            entry_idx = self._find_by_pk(*pks)
            if entry_idx is not None:
                if if_not_exists:
                    raise LWTException("LWT Query was not applied")

                self._entries[entry_idx] = entry
            else:
                self._entries.append(entry)

    def _assign_value(self, item_idx: int, column: str, operator: str, value: Any):
        if operator == DEFAULT_OPERATOR:
            self._entries[item_idx][column] = value
        elif isinstance(self._columns[column], List) and operator == 'append':
            self._entries[item_idx][column] += value
        elif isinstance(self._columns[column], List) and operator == 'prepend':
            self._entries[item_idx][column] = value + self._entries[item_idx][column]
        elif isinstance(self._columns[column], Set) and operator == 'add':
            self._entries[item_idx][column].update(value)
        elif isinstance(self._columns[column], Set) and operator == 'remove':
            self._entries[item_idx][column] -= value
        elif isinstance(self._columns[column], Map) and operator == 'update':
            self._entries[item_idx][column].update(**value)
        elif isinstance(self._columns[column], Map) and operator == 'remove':
            self._entries[item_idx][column] = {
                key: val
                for key, val
                in self._entries[item_idx][column].items()
                if key not in value
            }

    def update(self, filters: Dict[str, Any],
               values: TList[Tuple[str, str, Any]],
               if_exists: Optional[bool] = False):
        """
        :type filters: Dict[str, Any]
        :type values: Dict[str, Any]
        :type if_exists: Optional[bool] = False

        :raises:
            LWTException
        """
        with self._lock:
            indices = self._get_indices_by_kwargs(**filters)

            if not indices and if_exists:
                raise LWTException("LWT Query was not applied")

            for idx in indices:
                for attribute_to_update, operator, value in values:
                    if attribute_to_update in self._column_names:
                        self._assign_value(idx, attribute_to_update, operator, value)

    def delete(self, filters: Dict[str, Any], if_exists: Optional[bool] = False):
        """
        :type filters: Dict[str, Any]
        :type if_exists: Optional[bool] = False

        :raises:
            LWTException
        """
        with self._lock:
            if not filters:
                self._entries = []
            else:
                indices = self._get_indices_by_kwargs(**filters)
                if not indices and if_exists:
                    raise LWTException("LWT Query was not applied")

                for index in sorted(indices, reverse=True):
                    del self._entries[index]


class Keyspace:
    def __init__(self, name):
        self._name = name
        self._tables: Dict[str, Table] = {}

    @property
    def tables(self) -> Dict[str, Table]:
        return self._tables

    def add_table(self, name: str, fields: OrderedDict[str, Column], db_map: Dict[str, str]):
        """
        :type name: str
        :type fields: OrderedDict[str, Column]
        :type db_map: Dict[str, str]
        """
        self._tables[name] = Table(name, fields, db_map)

    def get_table(self, table_name) -> Table:
        return self._tables[table_name]


def create_keyspace(name: str, connections):
    global _keyspaces
    _keyspaces[name] = Keyspace(name)


def get_keyspace(ks_name: str) -> Keyspace:
    return _keyspaces[ks_name]


_entries = defaultdict(list)


def flush():
    global _entries
    _entries = defaultdict(list)


class CassandraConnection(object):
    def __init__(self, alias, **options):
        self.alias = alias
        self.hosts = options.get('HOST').split(',')
        self.keyspace = options.get('NAME')
        self.user = options.get('USER')
        self.password = options.get('PASSWORD')
        self.options = options.get('OPTIONS', {})
        self.cluster_options = self.options.get('connection', {})
        self.session_options = self.options.get('session', {})
        self.connection_options = {
            'lazy_connect': self.cluster_options.pop('lazy_connect', False),
            'retry_connect': self.cluster_options.pop('retry_connect', False),
            'consistency': self.cluster_options.pop('consistency', None),
        }
        if (
                self.user
                and self.password
                and 'auth_provider' not in self.cluster_options
        ):
            self.cluster_options['auth_provider'] = PlainTextAuthProvider(
                username=self.user, password=self.password
            )

        # self.default = (
        #    alias == 'default'
        #    or len(list(get_cassandra_connections())) == 1
        #    or self.cluster_options.pop('default', False)
        # )
        self.default = True

        self.register()

    def register(self):
        try:
            connection.get_connection(name=self.alias)
        except CQLEngineException:
            if self.default:
                from cassandra.cqlengine import models

                models.DEFAULT_KEYSPACE = self.keyspace

            for option, value in self.session_options.items():
                setattr(Session, option, value)

            connection.register_connection(
                self.alias,
                hosts=self.hosts,
                default=self.default,
                cluster_options=self.cluster_options,
                **self.connection_options
            )

    @property
    def cluster(self):
        return connection.get_cluster(connection=self.alias)

    @property
    def session(self):
        return connection.get_session(connection=self.alias)

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self):
        return Cursor(self)

    def execute(self, qs, *args, **kwargs):
        self.session.set_keyspace(self.keyspace)
        return self.session.execute(qs, *args, **kwargs)

    def close(self):
        """ We would like to keep connection always open by default """

    def unregister(self):
        """
        Unregister this connection
        """
        connection.unregister_connection(self.alias)


class InMemoryCassandraConnection(CassandraConnection):
    @property
    def cluster(self):
        return Cluster(self.alias)

    def register(self):
        try:
            connection.get_connection(name=self.alias)
        except CQLEngineException:
            if self.default:
                from cassandra.cqlengine import models

                models.DEFAULT_KEYSPACE = self.keyspace

            for option, value in self.session_options.items():
                setattr(Session, option, value)

            # connection.register_connection(
            #    self.alias,
            #    hosts=self.hosts,
            #    default=self.default,
            #    cluster_options=self.cluster_options,
            #    **self.connection_options
            # )

    def execute(self, qs, *args, **kwargs):
        self.session.set_keyspace(self.keyspace)
        return self.session.execute(qs, *args, **kwargs)

    def execute_sql_flush(sql_list):
        pass


def clauses_to_dict(assignments) -> dict:
    payload = {}
    for a in assignments:
        payload[a.field] = a.value

    return payload


DEFAULT_OPERATOR = 'assign'
SUPPORTED_OPERATIONS = {
    'append',
    'prepend',
    'update',
    'remove',
    'add',
    DEFAULT_OPERATOR
}


def _assigment_to_operator(assignment: AssignmentClause) -> str:
    """
    :type assignment: AssignmentClause

    :rtype: str
    """
    operation = getattr(assignment, '_operation', DEFAULT_OPERATOR)

    return (
        operation
        if operation and operation in SUPPORTED_OPERATIONS
        else DEFAULT_OPERATOR
    )


def _assignment_to_tuple(assignment: AssignmentClause) -> Tuple[str, str, Any]:
    return assignment.field, _assigment_to_operator(assignment), assignment.value


def assignments_to_list(assignments: TList[AssignmentClause]) -> TList[Tuple[str, str, Any]]:
    assignments_list = []

    for assignment in assignments:
        assignments_list.append(_assignment_to_tuple(assignment))

    return assignments_list


def attributes_matched(source_dict: dict, lookup_dict: dict) -> bool:
    if not lookup_dict:
        return False

    for lookup_key, lookup_value in lookup_dict.items():
        try:
            if source_dict[lookup_key] != lookup_value:
                return False
        except KeyError:
            return False

    return True


def lookup_items(tbl_name: str, assignments):
    assignments_dict = clauses_to_dict(assignments)

    indexes = []
    for idx, item in enumerate(_entries.get(tbl_name, [])):
        if attributes_matched(item, assignments_dict) or not assignments:
            indexes.append(idx)

    return indexes


def process_map(value, c: Map):
    if not value:
        return value

    key_type, value_type = c.types
    return {
        normalize_value(k, key_type): normalize_value(v, value_type)
        for k, v
        in value.items()
    }


def process_set(value, c: Set):
    if not value:
        return value

    value_type = c.types[0]
    return {
        normalize_value(v, value_type)
        for v
        in value
    }


def process_list(value, c: List):
    if not value:
        return value

    value_type = c.types[0]
    return [
        normalize_value(v, value_type)
        for v
        in value
    ]


def normalize_datetime(dt, c):
    return dt / 1e3


format_normalizers = {
    DateTime: normalize_datetime,
    Date: lambda dt, c: (
                datetime.datetime(1970, 1, 1, 0, 0) + datetime.timedelta(dt - SimpleDateType.EPOCH_OFFSET_DAYS)).date(),
    Map: process_map,
    Set: process_set,
    List: process_list,
}


def normalize_value(value, column: Column):
    col_type = type(column)

    formatter = get_formatter(col_type)
    if formatter:
        return formatter(value, col_type)

    return value


def get_formatter(column):
    for cls in column.mro():
        if cls is BaseContainerColumn:
            return None

        if cls in format_normalizers:
            return format_normalizers[cls]

    return None


def get_formatter_from_model(model, col_name):
    col_name = model._db_map.get(col_name) or col_name
    column = model._columns.get(col_name)
    return get_formatter(type(column))


def normalize(model, d):
    r = {}
    for k, v in d.items():
        r[k] = v
        normalizer = get_formatter_from_model(model, k)
        if normalizer:
            r[k] = normalizer(v, model._columns.get(k))

    return r


def _get_ks_table_name(model: 'Model', stmt: BaseCQLStatement) -> Tuple[str, str]:
    ks_name, table_name = model._get_keyspace(), stmt.table
    if '.' in stmt.table:
        ks_name, table_name = stmt.table.split('.')

    return ks_name, table_name


def _log_stmt(logger: logging.Logger, stmt: BaseCQLStatement):
    if stmt.get_context():
        log.debug(stmt, stmt.get_context())
    else:
        log.debug(stmt)


class ModelQuerySet(ModelQuerySetBase):
    def _execute(self, statement: BaseCQLStatement):
        _log_stmt(log, statement)

        ks_name, table_name = _get_ks_table_name(self.model, statement)
        keyspace = get_keyspace(ks_name)
        table = keyspace.get_table(table_name)

        if self._batch:
            return self._batch.add_query(statement)
        else:
            if isinstance(statement, SelectStatement):
                entries = table.find(clauses_to_dict(statement.where_clauses),
                                     statement.limit,
                                     statement.fields)
                if not entries:
                    self._result_cache = []
                    return []

                return [
                    normalize(self.model, entry)
                    for entry
                    in entries
                ]
            elif isinstance(statement, UpdateStatement):
                table.update(clauses_to_dict(statement.where_clauses),
                             assignments_to_list(statement.assignments),
                             statement.if_exists)
            elif isinstance(statement, DeleteStatement):
                table.delete(clauses_to_dict(statement.where_clauses),
                             statement.if_exists)

    def count(self):
        """
        Returns the number of rows matched by this query.

        *Note: This function executes a SELECT COUNT() and has a performance cost on large datasets*
        """
        if self._batch:
            raise CQLEngineException("Only inserts, updates, and deletes are available in batch mode")

        if self._count is None:
            query = self._select_query()
            query.count = True
            result = self._execute(query)
            self._count = len(result)
        return self._count


class DMLQuery(DMLQueryBase):
    def _execute(self, statement: BaseCQLStatement):

        ks_name, table_name = _get_ks_table_name(self.model, statement)
        keyspace = get_keyspace(ks_name)
        table = keyspace.get_table(table_name)

        if self._batch:
            # if self._batch._connection:
            #    if not self._batch._connection_explicit and connection and \
            #            connection != self._batch._connection:
            #                raise CQLEngineException('BatchQuery queries must be executed on the same connection')
            # else:
            #    # set the BatchQuery connection from the model
            #    self._batch._connection = connection
            _log_stmt(log, statement)
            return self._batch.add_query(statement)
        else:
            _log_stmt(log, statement)
            if isinstance(statement, InsertStatement):
                table.insert(clauses_to_dict(statement.assignments), statement.if_not_exists)
            elif isinstance(statement, DeleteStatement):
                indices = lookup_items(statement.table, statement.where_clauses)
                if statement.fields:
                    for idx in indices:
                        for stmt_field in statement.fields:
                            if len(_entries.get(statement.table, [])) > idx:
                                _entries.get(statement.table, [])[idx][stmt_field.field] = stmt_field.value
                else:
                    table.delete(clauses_to_dict(statement.where_clauses))
            elif isinstance(statement, UpdateStatement):
                table.update(clauses_to_dict(statement.where_clauses),
                             assignments_to_list(statement.assignments))

            return []
