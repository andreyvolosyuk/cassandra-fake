from django.apps import apps
from django.core.management.base import BaseCommand, CommandError
from django.db import connections

from django_cassandra_fake.compat import management
from django_cassandra_fake.utils import get_engine_from_db_alias


class Command(BaseCommand):
    help = 'Sync Cassandra database(s)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--database',
            action='store',
            dest='database',
            default=None,
            help='Nominates a database to synchronize.',
        )

    @staticmethod
    def _import_management():
        """
        Import the 'management' module within each installed app, to register
        dispatcher events.
        """

        from importlib import import_module

        for app_config in apps.get_app_configs():
            try:
                import_module('.management', app_config.name)
            except SystemError:
                # We get SystemError if INSTALLED_APPS contains the
                # name of a class rather than a module
                pass
            except ImportError as exc:
                # This is slightly hackish. We want to ignore ImportErrors
                # if the "management" module itself is missing -- but we don't
                # want to ignore the exception if the management module exists
                # but raises an ImportError for some reason. The only way we
                # can do this is to check the text of the exception. Note that
                # we're a bit broad in how we check the text, because different
                # Python implementations may not use the same text.
                # CPython uses the text "No module named management"
                # PyPy uses "No module named myproject.myapp.management"
                msg = exc.args[0]
                if not msg.startswith('No module named') \
                        or 'management' not in msg:
                    raise

    def sync(self, alias):
        engine = get_engine_from_db_alias(alias)

        if engine != 'django_cassandra_fake':
            raise CommandError('Database {} is not cassandra!'.format(alias))

        connection = connections[alias]
        connection.connect()
        keyspace = connection.settings_dict['NAME']

        self.stdout.write('Creating keyspace {} [CONNECTION {}] ..'.format(
            keyspace, alias))

        from django_cassandra_fake.connection import create_keyspace
        create_keyspace(keyspace, connections=[alias])

        connection.connection.cluster.refresh_schema_metadata()
        connection.connection.cluster.schema_metadata_enabled = True

        for app_name, app_models \
                in connection.introspection.cql_models.items():
            for model in app_models:
                self.stdout.write('Syncing %s.%s' % (app_name, model.__name__))
                management.sync_table(model, keyspaces=[keyspace],
                                      connections=[alias])

    def handle(self, **options):

        self._import_management()

        database = options.get('database')
        if database is not None:
            return self.sync(database)

        cassandra_alias = None
        for alias in connections:
            engine = get_engine_from_db_alias(alias)
            if engine == 'django_cassandra_fake':
                self.sync(alias)
                cassandra_alias = alias

        if cassandra_alias is None:
            raise CommandError(
                'Please add django_cassandra_fake backend to DATABASES!')
