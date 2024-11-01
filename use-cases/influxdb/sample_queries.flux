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
