from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy import create_engine, insert, update, select, delete
import pandas as pd


def create_stations_table(meta, engine):
    stations_table = Table(
        "stations_table",
        meta,
        Column("id", Integer, primary_key=True),
        Column("station", String),
        Column("latitude", String),
        Column("longitude", String),
        Column("elevation", String),
        Column("name", String),
        Column("country", String),
        Column("state", String),
    )
    meta.create_all(engine)
    return stations_table


def create_measure_table(meta, engine):
    measure_table = Table(
        "measure_table",
        meta,
        Column("id", Integer, primary_key=True),
        Column("station", String),
        Column("date", String),
        Column("precip", String),
        Column("tobs", Integer),
    )
    meta.create_all(engine)
    return measure_table


def add_station(engine, station, latitude, longitude, elevation, name, country, state):
    connection = engine.connect()
    station_data = station_table.insert().values(
        station=station,
        latitude=latitude,
        longitude=longitude,
        elevation=elevation,
        name=name,
        country=country,
        state=state,
    )
    connection.execute(station_data)
    connection.close()


def add_measure(engine, station, date, precip, tobs):
    connection = engine.connect()
    measure_data = measure_table.insert().values(
        station=station, date=date, precip=precip, tobs=tobs
    )
    connection.execute(measure_data)
    connection.close()


def select_data(engine, table, field, value):
    connection = engine.connect()
    select_data = select([table]).where(getattr(table.c, field) == value)
    result = connection.execute(select_data).fetchall()
    connection.close()
    return result


def update_data(engine, table, id, field, value):
    connection = engine.connect()
    update_data = update(table).where(table.c.id == id).values(**{field: value})
    connection.execute(update_data)
    connection.close()


def delete_data(engine, table, id):
    connection = engine.connect()
    delete_data = delete(table).where(table.c.id == id)
    connection.execute(delete_data)
    connection.close()


if __name__ == "__main__":
    meta = MetaData()
    engine = create_engine("sqlite:///database.db")

    # Tworzenie tabel
    station_table = create_stations_table(meta, engine)
    measure_table = create_measure_table(meta, engine)

    # Dodawanie danych z DataFrame
    stations_data = pd.read_csv("clean_stations.csv")
    measure_data = pd.read_csv("clean_measure.csv")
    conn = engine.connect()
    station = station_table.insert()
    conn.execute(station, stations_data.to_dict("records"))
    measure = measure_table.insert()
    conn.execute(measure, measure_data.to_dict("records"))

    # Dodawanie danych
    add_station(
        engine,
        "USC00519397",
        "23.2743",
        "-155.8168",
        "3.0",
        "WAIKIKI 717.2",
        "US",
        "HI",
    )

    # Wyświetlanie danych:
    print(select_data(engine, station_table, "id", "10"))

    # Aktualizowanie:
    update_data(engine, station_table, 10, "name", "test")

    # Wyświetlanie zaktualiowanych danych:
    print(select_data(engine, station_table, "id", "10"))

    # Usuwanie danych:
    delete_data(engine, station_table, 10)
