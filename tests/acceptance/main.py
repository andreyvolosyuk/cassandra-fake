import datetime

import django

django.setup()

from cassandra.cqlengine.models import Model
from cassandra.cqlengine.query import LWTException

from app.models import UserReal, UserFake
from django.core.management import call_command

USERNAME = 'user1'
COMPANY = 390
PK = {
    'username': USERNAME,
    'company': COMPANY
}

user_data = {
    'username': USERNAME,
    'company': COMPANY,
    'height': 1.79,
    'created_at': datetime.datetime.utcnow(),
    'skills': ['python', 'docker'],
    'skill_matrix': {'python': 3, 'docker': 4}
}
user2_data = {
    'username': 'user2',
    'company': 800,
    'height': 2.00,
    'created_at': datetime.datetime.utcnow(),
    'skills': ['python', 'golang']
}
user3_data = {
    'username': USERNAME,
    'company': 900,
    'height': 1.65,
    'created_at': datetime.datetime.utcnow(),
    'skills': ['golang', 'docker']
}


def assert_objects_are_equal(obj1: Model, obj2: Model):
    assert obj1.values() == obj2.values(), (f"Objects are not equal "
                                            f"{obj1.values()} != {obj2.values()}")

def get_and_assert_equal(**kwargs):
    assert_objects_are_equal(UserReal.objects.get(**kwargs),
                             UserFake.objects.get(**kwargs))


if __name__ == '__main__':
    call_command('sync_cassandra')
    call_command('flush')
    call_command('sync_cassandra_fake')

    UserReal.objects.create(**user_data)
    UserReal.objects.create(**user_data)
    UserReal.objects.create(**user2_data)
    UserFake.objects.create(**user_data)
    UserFake.objects.create(**user_data)
    UserFake.objects.create(**user2_data)
    assert UserReal.objects.filter(**PK).count() == UserFake.objects.filter(**PK).count()
    user_real = UserReal.objects.all()
    user_fake = UserFake.objects.all()
    assert len(UserReal.objects.all()) == len(UserFake.objects.all())

    get_and_assert_equal(**PK)

    try:
        exception_thrown = False
        user_fake: UserFake = UserFake.objects.get(username='missing_user', company=10)
    except UserFake.DoesNotExist:
        exception_thrown = True
    assert exception_thrown

    user_real: UserReal = UserReal.objects.get(**PK)
    user_fake: UserFake = UserFake.objects.get(**PK)
    user_real.height = user_fake.height = 1.80
    user_real.save()
    user_fake.save()
    get_and_assert_equal(**PK)

    user_real.delete()
    user_fake.delete()
    assert UserReal.objects.filter(**PK).count() == UserFake.objects.filter(**PK).count()

    UserReal.objects.create(**user_data)
    UserReal.objects.create(**user3_data)
    UserFake.objects.create(**user_data)
    UserFake.objects.create(**user3_data)
    assert len(UserReal.objects.filter(username=USERNAME).limit(1)) == len(UserFake.objects.filter(username=USERNAME).limit(1))

    try:
        exception_thrown = False
        user_fake: UserFake = UserFake.objects.get(username=USERNAME)
    except UserFake.MultipleObjectsReturned:
        exception_thrown = True
    assert exception_thrown

    UserReal.objects.filter(**PK).update(height=1.50)
    UserFake.objects.filter(**PK).update(height=1.50)
    user_real: UserReal = UserReal.objects.get(**PK)
    user_fake: UserFake = UserFake.objects.get(**PK)
    assert user_real.height == 1.50
    assert user_fake.height == 1.50

    user_real: UserReal = UserReal.objects.only(['height']).get(**PK)
    user_fake: UserFake = UserFake.objects.only(['height']).get(**PK)
    assert_objects_are_equal(user_real, user_fake)

    user_fake: UserFake = UserFake.objects.defer(['height']).get(**PK)
    user_real: UserReal = UserReal.objects.defer(['height']).get(**PK)
    assert_objects_are_equal(user_real, user_fake)

    ### Check If Not Exists on insertions
    try:
        exception_thrown = False
        UserFake.objects.if_not_exists().create(**user_data)
    except LWTException:
        exception_thrown = True
    assert exception_thrown, "Fake engine does not throw an exception on insert if an entry exists"

    ### Check If Exists on update
    try:
        exception_thrown = False
        UserFake.objects.\
            filter(username='user111', company=999).\
            if_exists().\
            update(height=1.50)
    except LWTException:
        exception_thrown = True
    assert exception_thrown, "Fake engine does not throw an exception on update if an entry does not exist"

    ### Check If Exists on delete
    try:
        exception_thrown = False
        UserFake.objects.filter(**PK).if_exists().delete()
        UserFake.objects.filter(**PK).if_exists().delete()
    except LWTException:
        exception_thrown = True
    assert exception_thrown, "Fake engine does not throw an exception on delete if an entry does not exist"

    UserReal.objects.create(**user_data)
    UserFake.objects.create(**user_data)

    ### Check List append / prepend
    UserReal.objects.filter(**PK).update(skills__append=['php', 'java'])
    UserFake.objects.filter(**PK).update(skills__append=['php', 'java'])
    get_and_assert_equal(**PK)

    UserReal.objects.filter(**PK).update(skills__prepend=['spring'])
    UserFake.objects.filter(**PK).update(skills__prepend=['spring'])
    get_and_assert_equal(**PK)

    ### Check Set add / remove
    UserReal.objects.filter(**PK).update(assignments__add={'Design', 'Refactor'})
    UserFake.objects.filter(**PK).update(assignments__add={'Design', 'Refactor'})
    get_and_assert_equal(**PK)

    UserReal.objects.filter(**PK).update(assignments__remove={'Design', 'Test'})
    UserFake.objects.filter(**PK).update(assignments__remove={'Design', 'Test'})
    get_and_assert_equal(**PK)

    ### Check Map update / remove
    UserReal.objects.filter(**PK).update(skill_matrix__update={'python': 5, 'php': 4})
    UserFake.objects.filter(**PK).update(skill_matrix__update={'python': 5, 'php': 4})
    get_and_assert_equal(**PK)

    UserReal.objects.filter(**PK).update(skill_matrix__remove={'php', 'python'})
    UserFake.objects.filter(**PK).update(skill_matrix__remove={'php', 'python'})
    get_and_assert_equal(**PK)

    # + all
    # - batch
    # ? consistency
    # + count
    # + len
    # - distinct
    # + filter
    # + get
    # + limit
    # ? fetch_size
    # + if_not_exists
    # + if_exists
    # - order_by
    # - allow_filtering
    # + only
    # + defer
    # ? ttl
    # ? using
    # + update
    # + update Set
    # + update List
    # + update Map
    #
    # - batch.add_query
    # - batch.execute
    # - batch.add_callback
    # ["skills" = "skills" + %(2)s]
