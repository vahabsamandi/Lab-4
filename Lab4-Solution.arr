use context dcic2024

include table 
include image
include csv
include data-source

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
  sanitize rownames using num-sanitizer
  sanitize dep_time using num-sanitizer
  sanitize sched_dep_time using num-sanitizer
  sanitize dep_delay using num-sanitizer
  sanitize arr_time using num-sanitizer
  sanitize sched_arr_time using num-sanitizer
  sanitize arr_delay using num-sanitizer
  sanitize flight using num-sanitizer
  sanitize air_time using num-sanitizer
  sanitize distance using num-sanitizer
  sanitize hour using num-sanitizer
  sanitize minute using num-sanitizer
end

# =========================================
# Exercise 1 (Easy) — Long Flights
# =========================================

# 1) flights is assumed loaded.

# 2) Predicate: long-distance flights (>= 1500 miles)
fun is_long_flight(r :: Row) -> Boolean:
  r["distance"] >= 1500
end

# 3) Filter long flights
long-flights = filter-with(flights, is_long_flight)

# 4) Order by air_time descending (false => descending)
long-flights-by-time = order-by(long-flights, "air_time", false)

# 5) Extract carrier, origin, dest of the largest air_time flight
lf-top-row = long-flights-by-time.row-n(0)
lf-top-carrier = lf-top-row["carrier"]
lf-top-origin  = lf-top-row["origin"]
lf-top-dest    = lf-top-row["dest"]

# (Optionally display or use them)
# lf-top-carrier, lf-top-origin, lf-top-dest
