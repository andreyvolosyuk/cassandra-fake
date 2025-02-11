from collections import defaultdict

from django.db.backends.base.creation import BaseDatabaseCreation

from django_cassandra_fake.utils import get_default_cassandra_connection

_keyspaces = defaultdict(set)

_def_conn = None
_connections = {}


def set_default_connection(alias):
    global _def_conn
    _def_conn = alias


def drop_keyspace(name, aliases):
    for alias in aliases:
        _keyspaces[name].remove(alias)


class CassandraDatabaseCreation(BaseDatabaseCreation):
    def create_test_db(self, verbosity=1, autoclobber=False, **kwargs):
        """
        Creates a test database, prompting the user for confirmation if the
        database already exists. Returns the name of the test database created.
        """
        # Don't import django.core.management if it isn't needed.
        from django.conf import settings

        self.connection.connect()
        default_alias = get_default_cassandra_connection()[0]

        # If using django-nose, its runner has already set the db name
        # to test_*, so restore it here so that all the models for the
        # live keyspace can be found.
        self.connection.connection.keyspace = self.connection.settings_dict[
            'NAME'
        ]
        test_database_name = self._get_test_db_name()
        # Set all models keyspace to the test keyspace
        self.set_models_keyspace(test_database_name)

        if verbosity >= 1:
            test_db_repr = ''
            if verbosity >= 2:
                test_db_repr = " ('%s')" % test_database_name
            print(
                "Creating test database for alias '%s'%s..."
                % (self.connection.alias, test_db_repr)
            )

        options = self.connection.settings_dict.get('OPTIONS', {})

        # temporarily enable schema metadata for sync_cassandra
        connection_options_copy = options.get('connection', {}).copy()
        if not connection_options_copy.get('schema_metadata_enabled', True):
            options['connection']['schema_metadata_enabled'] = True
            self.connection.reconnect()
            set_default_connection(default_alias)

        from django_cassandra_fake.connection import create_keyspace
        create_keyspace(
            test_database_name,
            connections=[self.connection.alias],
        )

        settings.DATABASES[self.connection.alias]["NAME"] = test_database_name
        self.connection.settings_dict["NAME"] = test_database_name

        self.connection.reconnect()
        set_default_connection(default_alias)

        # Report syncdb messages at one level lower than that requested.
        # This ensures we don't get flooded with messages during testing
        # (unless you really ask to be flooded)
        #call_command(
        #    'sync_cassandra_fake',
        #    verbosity=max(verbosity - 1, 0),
        #    database=self.connection.alias,
        #)

        # restore the original connection options
        if not connection_options_copy.get('schema_metadata_enabled', True):
            print(
                'Disabling metadata on %s'
                % self.connection.settings_dict['NAME']
            )
            options['connection'][
                'schema_metadata_enabled'
            ] = connection_options_copy['schema_metadata_enabled']
            self.connection.reconnect()
            set_default_connection(default_alias)

        return test_database_name

    def _destroy_test_db(self, test_database_name, verbosity=1, **kwargs):
        from django_cassandra_fake.connection import flush
        flush()

    def set_models_keyspace(self, keyspace):
        """Set keyspace for all connection models"""

        for models in self.connection.introspection.cql_models.values():
            for model in models:
                model.__keyspace__ = keyspace
