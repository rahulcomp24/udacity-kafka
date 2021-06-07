"""Defines trends calculations for stations"""
import logging
import faust
from dataclasses import asdict, dataclass


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record, serializer="json"):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record, serializer="json"):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic(
    "org.chicago.stations",
    value_type=Station,
)

out_topic = app.topic(
    "org.chicago.cta.stations.table.v1",
    partitions=1,
    key_type=str,
    value_type=TransformedStation,
)

table = app.Table(
    name="my_table",
    default=str,
    partitions=1,
    changelog_topic=out_topic,
)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(topic)
async def transform_table(stations):
    async for station in stations:
        print(f"Processing {station}")
        table[station.station_id] = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line="red"
            if station.red
            else "blue"
            if station.blue
            else "green"
            if station.green
            else "other",
        )


if __name__ == "__main__":
    app.main()
