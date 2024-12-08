from django.core.management.commands.flush import Command as FlushCommand

from django_cassandra_fake.connection import flush
from django_cassandra_fake.utils import get_engine_from_db_alias


class Command(FlushCommand):

    def handle_noargs(self, **options):
        flush()

    def handle(self, **options):
        flush()

    @staticmethod
    def emit_post_syncdb(verbosity, interactive, database):
        flush()

    @staticmethod
    def emit_post_migrate(verbosity, interactive, database):
        flush()
