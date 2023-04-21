from sqlalchemy import Table, Column, Integer, String, MetaData, Date, Float
from sqlalchemy import create_engine

meta = MetaData()
measure = Table(
    "measure",
    meta,
    Column("id", Integer, primary_key=True),
    Column("station", String),
    Column("date", Date),
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
    conn.execute(
        "INSERT INTO stations (station,latitude,longitude,elevation,name,country,state) VALUES(?,?,?,?,?,?,?)",
        stations_data,
    )

    measure_data = import_data("clean_measure.csv")
    conn.execute(
        "INSERT INTO measure (station,date,precip,tobs) VALUES(?,?,?,?)", measure_data
    )

    list = conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
    print(*list, sep="\n")
