from sqlalchemy import Table, Column, Integer, String, MetaData, Date, Float
from sqlalchemy import create_engine
import csv

meta = MetaData()
measure = Table(
    "measure",
    meta,
    Column("id", Integer, primary_key=True),
    Column("station", String),
    Column("date", String),
    Column("precip", Float),
    Column("tobs", Integer),
)

stations = Table(
    "stations",
    meta,
    Column("id", Integer, primary_key=True),
    Column("station", String),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("elevation", Integer),
    Column("name", String),
    Column("country", String),
    Column("state", String),
)


def insert_data(conn, tablename, filecsv):
    with open(filecsv, "r") as file:
        data = list(csv.DictReader(file))
    conn.execute(tablename.insert(), data)


if __name__ == "__main__":
    engine = create_engine("sqlite:///database.db")
    conn = engine.connect()
    meta.create_all(engine)

    insert_data(conn, stations, "clean_stations.csv")
    insert_data(conn, measure, "clean_measure.csv")

    selected_data = conn.execute(stations.select()).fetchmany(5)
    print(*selected_data, sep="\n")

    # delete
    conn.execute(stations.delete().where(stations.c.station == "USC00519397"))
    selected_data = conn.execute(stations.select())
    print("After delete", *selected_data, sep="\n")

    # insert
    deleted_row = (1, "USC00519397", 21.2716, -157.8168, 3, "WAIKIKI 717.2", "US", "HI")
    conn.execute(stations.insert().values(deleted_row))

    # update
    conn.execute(
        stations.update()
        .where(stations.c.station == "USC00519397")
        .values(name="*** WAIKIKI *** 717.2")
    )

    # select
    selected_data = conn.execute(
        stations.select().where(stations.c.station == "USC00519397")
    )
    print("selected:", *selected_data, sep="\n")
