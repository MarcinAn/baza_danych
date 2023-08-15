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


def add_station(conn, station, latitude, longitude, elevation, name, country, state):
    station_data = station_table.insert().values(
        station=station,
        latitude=latitude,
        longitude=longitude,
        elevation=elevation,
        name=name,
        country=country,
        state=state,
    )
    conn.execute(station_data)


def add_measure(conn, station, date, precip, tobs):
    measure_data = measure_table.insert().values(
        station=station, date=date, precip=precip, tobs=tobs
    )
    conn.execute(measure_data)


def select_data(conn, table, field, value):
    select_data = select([table]).where(getattr(table.c, field) == value)
    result = conn.execute(select_data).fetchall()
    return result


def update_data(conn, table, id, field, value):
    update_data = update(table).where(table.c.id == id).values(**{field: value})
    conn.execute(update_data)


def delete_data(conn, table, id):
    delete_data = delete(table).where(table.c.id == id)
    conn.execute(delete_data)


def get_data_from_csv(conn, name, table_name):
    data = pd.read_csv(name)
    station = table_name.insert()
    conn.execute(station, data.to_dict("records"))


if __name__ == "__main__":
    meta = MetaData()
    engine = create_engine("sqlite:///database.db")
    conn = engine.connect()
    if conn is not None:
        # Tworzenie tabel
        station_table = create_stations_table(meta, engine)
        measure_table = create_measure_table(meta, engine)

        # Dodawanie danych z DataFrame

        stations_data = get_data_from_csv(conn, "clean_stations.csv", station_table)
        measure_data = get_data_from_csv(conn, "clean_measure.csv", measure_table)

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
        print(select_data(conn, station_table, "id", "10"))

        # Aktualizowanie:
        update_data(conn, station_table, 10, "name", "test")

        # Wyświetlanie zaktualiowanych danych:
        print(select_data(conn, station_table, "id", "10"))

        # Usuwanie danych:
        delete_data(conn, station_table, 10)
        conn.close()
