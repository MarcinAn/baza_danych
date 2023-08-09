from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy import create_engine
import pandas as pd

stations_data = pd.read_csv("clean_stations.csv")
measure_data = pd.read_csv("clean_measure.csv")
engine = create_engine("sqlite:///database.db")
meta = MetaData()
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

measure_table = Table(
    "measure_table",
    meta,
    Column("id", Integer, primary_key=True),
    Column("station", String),
    Column("date", String),
    Column("precip", String),
    Column("tobs", String),
)

meta.create_all(engine)
conn = engine.connect()
station = stations_table.insert()
conn.execute(station, stations_data.to_dict("records"))
measure = measure_table.insert()
conn.execute(measure, measure_data.to_dict("records"))

result = engine.execute("SELECT * FROM stations_table LIMIT 5").fetchall()
print(result)
