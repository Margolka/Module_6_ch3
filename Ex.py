from sqlalchemy import Table, Column, Integer, String, MetaData, Date, Float, ForeignKey
from sqlalchemy import create_engine

meta = MetaData()
measure = Table(
    "measure",
    meta,
    Column("station", String),
    Column("date", String),
    Column("precip", Float),
    Column("tobs", Integer),
)

stations = Table(
    "stations",
    meta,
    Column("station", String),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("elevation", Integer),
    Column("name", String),
    Column("country", String),
    Column("state", String),
)


def import_data(filecsv):
    data = []
    with open(filecsv, "r") as file:
        next(file)
        for line in file:
            line = line.replace("\n", "")
            line = line.replace("\r", "")
            data.append(tuple(line.split(",")))
    return tuple(data)


if __name__ == "__main__":
    engine = create_engine("sqlite:///database.db")
    conn = engine.connect()
    meta.create_all(engine)
    stations_data = import_data("clean_stations.csv")
    measure_data = import_data("clean_measure.csv")
    conn.execute(stations.insert().values(stations_data))
    conn.execute(measure.insert().values(measure_data))

    list = conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
    print(*list, sep="\n")

    # delete
    conn.execute(stations.delete().where(stations.c.station == "USC00519397"))
    list = conn.execute("SELECT * FROM stations").fetchall()
    print("After delete", *list, sep="\n")

    # insert
    deleted_row = ("USC00519397", 21.2716, -157.8168, 3, "WAIKIKI 717.2", "US", "HI")
    conn.execute(stations.insert().values(deleted_row))

    # update
    conn.execute(
        stations.update()
        .where(stations.c.station == "USC00519397")
        .values(name="*** WAIKIKI *** 717.2")
    )

    # select
    selected = conn.execute(
        stations.select().where(stations.c.station == "USC00519397")
    )
    print("selected:\n", *selected)
