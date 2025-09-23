use context dcic2024

include table 
include image
include csv

flights = load-table:
  rownames :: Number,
  dep_time :: Number,
  sched_dep_time :: Number,
  dep_delay :: Number,
  arr_time :: Number,
  sched_arr_time :: Number,
  arr_delay :: Number,
  carrier :: String,
  flight :: Number,
  tailnum :: String,
  origin :: String,
  dest :: String,
  air_time :: Number,
  distance :: Number,
  hour :: Number,
  minute :: Number,
  time_hour :: String
  source: csv-table-url(
    "https://raw.githubusercontent.com/vahabsamandi/Lab-4/refs/heads/main/.vscode/flights.csv",
    default-options)
end


flights

row1 = flights.row-n(1)
row1