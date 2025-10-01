use context dcic2024

include image
include csv
include data-source

# Exercise 1 — Long Flights
# 1) Load the table

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
  source: csv-table-file("flights.csv",default-options)
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

# 2) Predicate: long-distance flights (>= 1500 miles)
fun is_long_flight(r :: Row) -> Boolean:
  r["distance"] >= 1500
end

# 3) Filter long flights
long-flights = filter-with(flights, is_long_flight)
# long-flights

# 4) Order by air_time descending (false => descending)
long-flights-by-time = order-by(long-flights, "air_time", false)

# 5) Extract carrier, origin, dest of the largest air_time flight
lf-top-row = long-flights-by-time.row-n(0)
lf-top-carrier = lf-top-row["carrier"]
lf-top-origin  = lf-top-row["origin"]
lf-top-dest    = lf-top-row["dest"]

#-------------------------------------------------------

# Exercise 2 — Delayed Morning Flights

# 1) Predicate: departure delay >= 30
fun is_delayed_departure(r :: Row) -> Boolean:
  r["dep_delay"] >= 30
end

# 2) Predicate: scheduled departure before noon (time encoded as HHMM integers)
fun is_morning_sched_dep(r :: Row) -> Boolean:
  r["sched_dep_time"] < 1200
end

# 3) Use lambdas to filter: delayed first, then morning
delayed = filter-with(flights, lam(r :: Row): r["dep_delay"] >= 30 end)
delayed-morning = filter-with(delayed, lam(r :: Row): r["sched_dep_time"] < 1200 end)

# 4) Further filter to only flights with distance > 500
delayed-morning-500 = filter-with(delayed-morning, lam(r :: Row): r["distance"] > 500 end)

# 5) Order by dep_delay descending and extract flight number, origin, dep_delay of worst case
dm500-by-delay = order-by(delayed-morning-500, "dep_delay", false)

# 6) The single worst delayed flight in that subset
dm500-top      = dm500-by-delay.row-n(0)
dm500-flight   = dm500-top["flight"]
dm500-origin   = dm500-top["origin"]
dm500-depdel   = dm500-top["dep_delay"]


# ---------------------------------------

# Exercise 3 — Clean Delays + Compute Effective Speed (transform + build)


# 1) Cap negative dep_delay and arr_delay at 0
no-neg-dep =
  transform-column(
    flights, "dep_delay",
    lam(d :: Number):
      if d < 0:
        0
      else:
        d
      end
    end)


clean-delays =
  transform-column(
    no-neg-dep, "arr_delay",
    lam(a :: Number):
      if a < 0:
        0
      else:
        a
      end
    end)


# 2) Add effective_speed = distance / (air_time / 60), only if air_time > 0
with-speed = build-column(
  clean-delays, "effective_speed",
  lam(r :: Row):
    if r["air_time"] > 0:
      r["distance"] / (r["air_time"] / 60)
    else:
      0
    end
  end)


# 3) Order by effective_speed descending
with-speed-desc = order-by(with-speed, "effective_speed", false)

# 4) Extract carrier, origin, dest of the fastest flight
spd-top      = with-speed-desc.row-n(0)
spd-carrier  = spd-top["carrier"]
spd-origin   = spd-top["origin"]
spd-dest     = spd-top["dest"]


# -------------------------------------------

# Exercise 4 — Discount Late Arrivals + On-Time Score (transform + build + test)


# 1) Table function: discount arr_delay by 20% only when 0 <= arr_delay <= 45
fun apply-arrival-discount(t :: Table) -> Table:
  doc: "Reduce arr_delay by 20% when 0 <= arr_delay <= 45; leave others unchanged"
  transform-column(
    t, "arr_delay",
    lam(a :: Number):
      if (a >= 0) and (a <= 45): a * 0.8 else: a end
    end)
  
# 2) Testing Function
where:
  # minimal unit-style examples for the function behavior
  test-delays =
    table: arr_delay
      row: -10
      row:   0
      row:  30
      row:  60
    end

  apply-arrival-discount(test-delays) is
    table: arr_delay
      row: -10              # early arrival => unchanged
      row:  0 * 0.8         # remains 0
      row: 30 * 0.8         # discounted
      row: 60               # unchanged (too large to discount)
    end
end

# apply discount to the flights table
discounted = apply-arrival-discount(flights)
# discounted
# 3) Build on_time_score:
#    score = 100 - max(0, dep_delay) - max(0, arr_delay) - (air_time / 30), clamped to >= 0
scored = build-column(
  discounted, "on_time_score",
  lam(r :: Row):
    block:
      ddep = if r["dep_delay"] < 0: 0 else: r["dep_delay"] end
      darr = if r["arr_delay"] < 0: 0 else: r["arr_delay"] end
      score  = 100 - ddep - darr - (r["air_time"] / 30)
      if score < 0: 0 else: score end
    end
  end)

# 4) Order by on_time_score desc, then by distance asc (tie-breaker)
# Pyret's order-by handles a single column; for a tie-breaker,
# do a stable two-pass sort: first ascending distance, then descending score.
by-distance-asc = order-by(scored, "distance", true)
final-ranked    = order-by(by-distance-asc, "on_time_score", false)

# 5) Extract top three rows' carrier, flight, origin, dest (if at least 3 exist)
top1 = final-ranked.row-n(0)
top2 = final-ranked.row-n(1)

top1-carrier = top1["carrier"]
top1-flight = top1["flight"]
top1-origin = top1["origin"]
top1-dest = top1["dest"]
top2-carrier = top2["carrier"]
top2-flight = top2["flight"]
top2-origin = top2["origin"]
top2-dest = top2["dest"]


# 6) The following is just one example solution. You can suggest your own alternative formula.

# If the objective is pure punctuality fairness, use the following (no duration penalty):
# score_fair = 100 
#              - max(0, dep_delay) 
#              - max(0, arr_delay)

# If the objective is to consider delay severity relative to trip length use the following:
# delay_rate = (max(0, dep_delay) + max(0, arr_delay)) / max(1, air_time/60)
# score_rate = 100 - delay_rate


