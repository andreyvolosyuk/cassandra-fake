import os

DEBUG = True
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "{levelname} {asctime} {module} {message}",
            "style": "{",
        },
        "simple": {
            "format": "{levelname} {message}",
            "style": "{",
        },
    },
    "filters": {
        "require_debug_true": {
            "()": "django.utils.log.RequireDebugTrue",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
    },
    "root": {
        "handlers": ["console"],
        "level": "WARNING",
    },
    "loggers": {
        "django_cassandra_fake.connection": {
            "handlers": ["console"],
            "level": "DEBUG",
            "filters": ["require_debug_true"],
        },
    },
}


INSTALLED_APPS = [
    'django_cassandra_engine',
    'django_cassandra_fake',
    'app'
]

DATABASES = {
    'default': {
        'ENGINE': 'django_cassandra_engine',
        'NAME': os.environ.get('ACCEPTANCE_KEYSPACE_NAME'),
        'USER': os.environ.get('ACCEPTANCE_USER'),
        'PASSWORD': os.environ.get('ACCEPTANCE_PASSWORD'),
        'HOST': os.environ.get('ACCEPTANCE_HOST'),
        'OPTIONS': {
            'replication': {
                'strategy_class': 'SimpleStrategy',
                'replication_factor': 1
            }
        }
    },
    'fake': {
        'ENGINE': 'django_cassandra_fake',
        'NAME': 'fake'
    }
}