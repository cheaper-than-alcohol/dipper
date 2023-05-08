import os

from retryz import retry
from contextlib import contextmanager
import uuid
import collections
import psycopg2
from psycopg2 import ProgrammingError
import logging

logger = logging.getLogger("db")

@contextmanager
def db_connection():
    try:
        conn = psycopg2.connect(host=os.environ["DB_ADDRESS"],
                                user=os.environ["DB_USER"],
                                password=os.environ["DB_PASS"])
        if conn is None:
            raise IOError("Cannot open db connection")
        yield conn
    finally:
        if conn is not None:
            conn.commit()
            conn.close()


@contextmanager
def cursor_manager(db_conn):
    try:
        cursor = db_conn.cursor()
        yield cursor
        if cursor:
            cursor.close()
    except Exception as e:
        logger.error("Problem observed managing a cursor:", repr(e))
        if cursor:
            cursor.close()
        raise e
    finally:
        db_conn.commit()


Observation = collections.namedtuple("Observation", ("observe_id", "observation_time", "temp", "pressure",
                                                     "rel_humidity", "wind_speed", "wind_dir", "dew_point"))


def create_db():
    ddl = """
    create table if not exists observations (
        id uuid primary key,
        observe_id text unique,
        observation_time timestamp unique,
        temp integer,
        rel_humidity real,
        pressure integer,
        wind_speed real,
        wind_dir integer,
        dew_point integer
        );
    
    create table if not exists event (
        id uuid primary key,
        last_alert_sent date,
        trigger_name text,
        measured_value real,
        trigger_value real,
        message text,
        last_modified date,
        archive int
    );
        """
    with db_connection(None) as conn:
        with cursor_manager(conn) as cursor:
            cursor.execute(ddl)


@retry(on_error=ProgrammingError, on_retry=create_db, limit=2)
def add_observation(connection, observation):
    check_sql = f"select 1 from observations where observation_time = '{observation.observation_time}'"
    insert_sql = """insert into observations 
    (id, observe_id, observation_time, temp, rel_humidity, pressure, wind_speed, wind_dir, dew_point)
     values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    with cursor_manager(connection) as cursor:
        cursor.execute(check_sql)
        if not cursor.fetchone():
            cursor.executemany(insert_sql, [(str(uuid.uuid4()), observation.observe_id, observation.observation_time,
                                             observation.temp, observation.rel_humidity, observation.pressure,
                                             observation.wind_speed, observation.wind_dir, observation.dew_point)])


def last_reading(connection):
    query = """select observe_id, observation_time, temp, pressure, rel_humidity, wind_speed, wind_dir, dew_point from observations
            where observation_time = (
            select max(observation_time) from observations
        )"""
    with cursor_manager(connection) as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
        return Observation(*result)


def observations(connection, hours=None):
    with cursor_manager(connection) as cursor:
        if hours is None:
            select = "select observe_id, observation_time, temp, pressure, rel_humidity, wind_speed, wind_dir, dew_point from observations order by observation_time desc"
            cursor.execute(select)
        else:
            select = "select observe_id, observation_time, temp, pressure, rel_humidity, wind_speed, wind_dir, dew_point from observations " \
                     "where observation_time >= current_timestamp - make_interval(hours:=%s) order by observation_time desc"
            cursor.execute(select, (hours,))
        return (Observation(*row) for row in cursor.fetchall())
