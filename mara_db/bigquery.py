"""Easy access to BigQuery databases via google.cloud.bigquery"""

import contextlib
import typing

import mara_db.dbs


def bigquery_client(db: typing.Union[str, mara_db.dbs.BigQueryDB]) -> 'google.cloud.bigquery.client.Client':
    """Get an bigquery client for a bq database alias"""
    import google.oauth2.service_account
    import google.cloud.bigquery.client

    if isinstance(db, str):
        db = mara_db.dbs.db(db)

    assert (isinstance(db, mara_db.dbs.BigQueryDB))

    if db.service_account_file:
        credentials = google.oauth2.service_account.Credentials.from_service_account_file(db.service_account_file)
    elif db.service_account_info:
        credentials=  google.oauth2.service_account.Credentials.from_service_account_info(db.service_account_info)
    else:
        raise AttributeError('Either service_account_file or service_account_info needs to be set')

    return google.cloud.bigquery.client.Client(project=credentials.project_id, credentials=credentials)


@contextlib.contextmanager
def bigquery_cursor_context(db: typing.Union[str, mara_db.dbs.BigQueryDB]) \
        -> 'google.cloud.bigquery.dbapi.cursor.Cursor':
    """Creates a context with a bigquery cursor for a database alias"""
    client = bigquery_client(db)

    from google.cloud import bigquery
    connection = bigquery.dbapi.Connection(client)
    cursor = connection.cursor()  # type: google.cloud.bigquery.dbapi.cursor.Cursor
    try:
        yield cursor
        connection.commit()
    except Exception as e:
        connection.close()
        raise e
    finally:
        cursor.close()
        connection.close()
