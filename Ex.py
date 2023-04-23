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
    Column("station", String, primary_key=True),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("elevation", Integer),
    Column("name", String),
    Column("country", String),
    Column("state", String),
)


def import_data(conn, filecsv, destynation):
    with open(filecsv, "r") as file:
        next(file)
        for line in file:
            line = line.replace("\n", "")
            line = line.replace("\r", "")
            data = tuple(line.split(","))
            conn.execute(destynation.insert().values(data))


if __name__ == "__main__":
    engine = create_engine("sqlite:///database.db")
    conn = engine.connect()
    meta.create_all(engine)
    import_data(conn, "clean_stations.csv", stations)
    import_data(conn, "clean_measure.csv", measure)

    list = conn.execute(stations.select()).fetchmany(5)
    print(*list, sep="\n")

    # delete
    conn.execute(stations.delete().where(stations.c.station == "USC00519397"))
    list = conn.execute(stations.select())
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
