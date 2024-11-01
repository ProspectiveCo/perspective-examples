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
    |> filter(fn: (r) => r._field == "bid_price" or r._field == "ask_price")
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> keep(columns: ["_time", "broker", "ticker", "bid_price", "ask_price"])
    |> yield(name: "table1")
