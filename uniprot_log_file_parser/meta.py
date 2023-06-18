from collections import defaultdict
from peewee import CharField, DoesNotExist, IntegerField, Model, SqliteDatabase

database = SqliteDatabase(None)


class Parsed(Model):
    namespace = CharField()
    log_path = CharField(unique=True)
    total_bytes = IntegerField()
    lines_imported = IntegerField()
    lines_skipped = IntegerField()
    status_1xx = IntegerField()
    status_2xx = IntegerField()
    status_3xx = IntegerField()
    status_4xx = IntegerField()
    status_5xx = IntegerField()

    class Meta:
        database = database


def init_meta(meta_path: str):
    database.init(meta_path)
    with database:
        database.create_tables([Parsed])


def in_meta(log_path: str):
    database.connect()
    try:
        Parsed.get(Parsed.log_path == log_path)
    except DoesNotExist:
        return False
    else:
        return True
    finally:
        database.close()


def save_meta(
    namespace: str,
    log_path: str,
    total_bytes: int,
    lines_imported: int,
    lines_skipped: int,
    status_counts: defaultdict,
):
    print(status_counts)
    database.connect()
    parsed = Parsed(
        namespace=namespace,
        log_path=log_path,
        total_bytes=total_bytes,
        lines_imported=lines_imported,
        lines_skipped=lines_skipped,
        status_1xx=status_counts["1xx"],
        status_2xx=status_counts["2xx"],
        status_3xx=status_counts["3xx"],
        status_4xx=status_counts["4xx"],
        status_5xx=status_counts["5xx"],
    )
    parsed.save()
    database.close()
