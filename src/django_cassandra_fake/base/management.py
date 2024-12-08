from cassandra.cqlengine.connection import format_log_context
from cassandra.cqlengine.management import _get_create_table
from cassandra.cqlengine.models import Model

from django_cassandra_fake.compat import CQLEngineException


def sync_table(model, keyspaces=None, connections=None):
    """
    Inspects the model and creates / updates the corresponding table and columns.

    If `keyspaces` is specified, the table will be synched for all specified keyspaces.
    Note that the `Model.__keyspace__` is ignored in that case.

    If `connections` is specified, the table will be synched for all specified connections. Note that the `Model.__connection__` is ignored in that case.
    If not specified, it will try to get the connection from the Model.

    Any User Defined Types used in the table are implicitly synchronized.

    This function can only add fields that are not part of the primary key.

    Note that the attributes removed from the model are not deleted on the database.
    They become effectively ignored by (will not show up on) the model.

    **This function should be used with caution, especially in production environments.
    Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an environment-driven conditional.*
    """
    _sync_table(model, connection=connections)




def _sync_type(ks_name, type_model, omit_subtypes=None, connection=None):
    from django_cassandra_fake.compat import columns, get_create_type

    syncd_sub_types = omit_subtypes or set()
    for field in type_model._fields.values():
        udts = []
        columns.resolve_udts(field, udts)
        for udt in [u for u in udts if u not in syncd_sub_types]:
            _sync_type(ks_name, udt, syncd_sub_types, connection=connection)
            syncd_sub_types.add(udt)

    type_name = type_model.type_name()
    type_name_qualified = "%s.%s" % (ks_name, type_name)


    keyspace = get_keyspace(ks_name)
    defined_types = keyspace.user_types

    #log.debug(format_log_context("sync_type creating new type %s", keyspace=ks_name, connection=connection), type_name_qualified)
    cql = get_create_type(type_model, ks_name)
    #execute(cql, connection=connection)
    #cluster.refresh_user_type_metadata(ks_name, type_name)
    type_model.register_for_keyspace(ks_name, connection=connection)




def _sync_table(model, connection=None):
    from django_cassandra_fake.compat import columns
    from django_cassandra_fake.connection import get_keyspace, Keyspace

    if not issubclass(model, Model):
        raise CQLEngineException("Models must be derived from base Model.")

    if model.__abstract__:
        raise CQLEngineException("cannot create table from abstract model")

    cf_name = model.column_family_name()
    raw_cf_name = model._raw_column_family_name()

    ks_name = model._get_keyspace()
    connection = connection or model._get_connection()

    try:
        keyspace: Keyspace = get_keyspace(ks_name)
    except KeyError:
        msg = format_log_context("Keyspace '{0}' for model {1} does not exist.", connection=connection)
        raise CQLEngineException(msg.format(ks_name, model))

    tables = keyspace.tables

    syncd_types = set()
    for col in model._columns.values():
        udts = []
        columns.resolve_udts(col, udts)
        for udt in [u for u in udts if u not in syncd_types]:
            _sync_type(ks_name, udt, syncd_types, connection=connection)

    if raw_cf_name not in tables:
        #log.debug(format_log_context("sync_table creating new table %s", keyspace=ks_name, connection=connection), cf_name)
        keyspace.add_table(model.column_family_name(False), model._columns, model._db_map)

    #else:
    #    #log.debug(format_log_context("sync_table checking existing table %s", keyspace=ks_name, connection=connection), cf_name)
    #    table_meta = tables[raw_cf_name]
#
    #    _validate_pk(model, table_meta)
#
    #    table_columns = table_meta.columns
    #    model_fields = set()
#
    #    for model_name, col in model._columns.items():
    #        db_name = col.db_field_name
    #        model_fields.add(db_name)
    #        if db_name in table_columns:
    #            col_meta = table_columns[db_name]
    #            if col_meta.cql_type != col.db_type:
    #                msg = format_log_context('Existing table {0} has column "{1}" with a type ({2}) differing from the model type ({3}).'
    #                              ' Model should be updated.', keyspace=ks_name, connection=connection)
    #                msg = msg.format(cf_name, db_name, col_meta.cql_type, col.db_type)
    #                warnings.warn(msg)
    #                log.warning(msg)
#
    #            continue
#
    #        if col.primary_key or col.primary_key:
    #            msg = format_log_context("Cannot add primary key '{0}' (with db_field '{1}') to existing table {2}", keyspace=ks_name, connection=connection)
    #            raise CQLEngineException(msg.format(model_name, db_name, cf_name))
#
    #        query = "ALTER TABLE {0} add {1}".format(cf_name, col.get_column_def())
    #        execute(query, connection=connection)
#
    #    db_fields_not_in_model = model_fields.symmetric_difference(table_columns)
    #    if db_fields_not_in_model:
    #        msg = format_log_context("Table {0} has fields not referenced by model: {1}", keyspace=ks_name, connection=connection)
    #        log.info(msg.format(cf_name, db_fields_not_in_model))
#
    #    _update_options(model, connection=connection)
#
    #table = cluster.metadata.keyspaces[ks_name].tables[raw_cf_name]
#
    #indexes = [c for n, c in model._columns.items() if c.index]


def _validate_pk(model, table_meta):
    model_partition = [c.db_field_name for c in model._partition_keys.values()]
    meta_partition = [c.name for c in table_meta.partition_key]
    model_clustering = [c.db_field_name for c in model._clustering_keys.values()]
    meta_clustering = [c.name for c in table_meta.clustering_key]

    if model_partition != meta_partition or model_clustering != meta_clustering:
        def _pk_string(partition, clustering):
            return "PRIMARY KEY (({0}){1})".format(', '.join(partition), ', ' + ', '.join(clustering) if clustering else '')
        raise CQLEngineException("Model {0} PRIMARY KEY composition does not match existing table {1}. "
                                 "Model: {2}; Table: {3}. "
                                 "Update model or drop the table.".format(model, model.column_family_name(),
                                                                          _pk_string(model_partition, model_clustering),
                                                                          _pk_string(meta_partition, meta_clustering)))
