from django_cassandra_fake.management.commands.sync_cassandra_fake import (
    Command as SyncCommand,
)

from django_cassandra_fake.management.commands import sync_cassandra_fake
from django_cassandra_fake.utils import get_engine_from_db_alias


class Command(SyncCommand):
    def handle_noargs(self, **options):
        engine = get_engine_from_db_alias(options['database'])

        # Call regular syncdb if engine is different from ours
        if engine != 'django_cassandra_fake':
            return super(Command, self).handle_noargs(**options)
        else:
            return sync_cassandra.Command().execute(**options)

    def handle(self, **options):
        engine = get_engine_from_db_alias(options['database'])

        # Call regular syncdb if engine is different from ours
        if engine != 'django_cassandra_fake':
            return super(Command, self).handle(**options)
        else:
            return sync_cassandra.Command().execute(**options)
