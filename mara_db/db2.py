"""Easy access to DB2 databases via pyodbc-client"""

import contextlib
import typing

import mara_db.dbs


@contextlib.contextmanager
def db2_cursor_context(db: typing.Union[str, mara_db.dbs.Db2DB]) -> 'pyodbc.Cursor':
    """Creates a context with a pyodbc-client cursor for a database alias or database"""
    import pyodbc # requires https://github.com/mkleehammer/pyodbc/wiki/Install

    if isinstance(db, str):
        db = mara_db.dbs.db(db)

    assert (isinstance(db, mara_db.dbs.Db2DB))

    cursor = None
    # https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_74/rzaik/connectkeywords.htm
    # set ALLOWUNSCHAR=1 to avoid SQL_SUCCESS_WITH_INFO odbc return code, which causes isql to append "..." on fields
    connection = pyodbc.connect(f"DRIVER={{{db.odbc_driver}}};SYSTEM={db.host};DATABASE={db.database};UID={db.user};PWD={db.password};CONNTYPE=2;ALLOWUNSCHAR=1;")
    try:
        cursor = connection.cursor()
        yield cursor
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()
    finally:
        cursor.close()
        connection.close()
