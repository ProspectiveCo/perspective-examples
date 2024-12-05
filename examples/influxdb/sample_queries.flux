// selecting without pivot. showing multiple rows
from(bucket: "trades")
    |> range(start: -10y)
    |> filter(fn: (r) => r._field == "bid_price" or r._field == "ask_price")
    |> yield(name: "table1")

// selecting all fields without pivot; result in many many rows
from(bucket: "trades")
    |> range(start: -10y)
    |> yield(name: "table1")

// correct selection with pivota and keep
from(bucket: "trades")
    |> range(start: -10y)
    |> filter(fn: (r) => r._field == "bid_price" or r._field == "ask_price" or r._field == "trade_price")
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> keep(columns: ["_time", "broker", "ticker", "bid_price", "ask_price", "trade_price"])
    |> yield(name: "table1")

// aggregated query
// from(bucket: "trades")
//   |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
//   |> filter(fn: (r) => r["_measurement"] == "trades")
//   |> filter(fn: (r) => r["_field"] == "ask_price" or r["_field"] == "bid_price" or r["_field"] == "trade_price")
//   |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
//   |> yield(name: "mean")

// more fields
from(bucket: "trades")
    |> range(start: -1y)
    |> filter(fn: (r) => 
        r._field == "bid_price" or 
        r._field == "ask_price" or 
        r._field == "trade_price" or
        r._field == "trade_value"
        )
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> keep(columns: [
        "_time", "broker", "ticker", "bid_price", "ask_price", "trade_price","trade_value"
        ])
    |> yield(name: "table1")


from(bucket: "smart_grid")
  |> range(start: -1s, stop: 1s)
  |> filter(fn: (r) => r["_measurement"] == "new_york_smart_grid")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> yield(name: "mean")

from(bucket: "smart_grid")
  |> range(start: -1s)
  |> filter(fn: (r) => r["_measurement"] == "new_york_smart_grid")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> yield(name: "newyork")


{
  "version": "3.1.5",
  "plugin": "Map Scatter",
  "plugin_config": {
    "center": [
      -8233916.460974932,
      4975833.61844814
    ],
    "zoom": 13.917077534993478
  },
  "columns_config": {},
  "settings": true,
  "theme": "Pro Dark",
  "title": null,
  "group_by": [
    "station_name"
  ],
  "split_by": [],
  "columns": [
    "longitude",
    "latitude",
    null,
    "energy_consumption",
    null
  ],
  "filter": [],
  "sort": [],
  "expressions": {
    "ts1": "bucket(\"_time\", '1m')"
  },
  "aggregates": {
    "longitude": "last",
    "latitude": "last",
    "energy_consumption": "mean"
  }
}