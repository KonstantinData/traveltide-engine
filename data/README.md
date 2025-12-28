# 📂 Data Engineering: Extraction & Architecture

This section documents the end-to-end data pipeline, from the initial SQL extraction strategies to the multi-hop architecture used to ensure data quality and reproducibility.

## 🎯 Cohort Definition & Extraction

To ensure a fair behavioral analysis, we limited the scope to a specific user cohort. Excluding long-term users and brand-new sign-ups avoids bias caused by varying tenure on the platform.

* **Cohort Period:** January 1, 2022 – December 31, 2022
* **Filter Logic:** Users are selected based on their `sign_up_date`. All related transactional data (sessions, flights, hotels) is filtered to match this specific user set.

### 🛠 SQL Extraction Queries

The following SQL queries were executed against the source database to generate the raw CSV files for the Bronze Layer.

#### 1. Users (`users.csv`)

This is the anchor table for the cohort definition. We filter directly on the sign-up date.

**SQL**

```
SELECT *
FROM users
WHERE users.sign_up_date >= '2022-01-01'
  AND users.sign_up_date <= '2022-12-31';
```

#### 2. Flights (`flights.csv`)

Flight data is linked to users via the `sessions` table. We perform a multi-table join to ensure we only extract flights belonging to the defined 2022 cohort.

**SQL**

```
SELECT
  fli.base_fare_usd,
  fli.checked_bags,
  fli.departure_time,
  fli.destination,
  fli.destination_airport,
  fli.destination_airport_lat,
  fli.destination_airport_lon,
  fli.origin_airport,
  fli.return_flight_booked,
  fli.return_time,
  fli.seats,
  fli.trip_airline,
  fli.trip_id
FROM flights as fli
LEFT JOIN sessions AS ses ON ses.trip_id = fli.trip_id
LEFT JOIN users AS use ON use.user_id = ses.user_id
WHERE use.sign_up_date >= '2022-01-01'
  AND use.sign_up_date <= '2022-12-31';
```

#### 3. Hotels (`hotels.csv`)

Similar to flights, hotel bookings are filtered by joining them through sessions to the user table.

**SQL**

```
SELECT
  hot.check_in_time,
  hot.check_out_time,
  hot.hotel_name,
  hot.hotel_per_room_usd,
  hot.nights,
  hot.rooms,
  hot.trip_id
FROM hotels as hot
LEFT JOIN sessions AS ses ON ses.trip_id = hot.trip_id
LEFT JOIN users AS use ON use.user_id = ses.user_id
WHERE use.sign_up_date >= '2022-01-01'
  AND use.sign_up_date <= '2022-12-31';
```

#### 4. Sessions (`sessions.csv`)

Session data captures the user's browsing behavior. We filter this by joining directly with the `users` table.

**SQL**

```
SELECT
  ses.cancellation,
  ses.flight_booked,
  ses.flight_discount,
  ses.flight_discount_amount,
  ses.hotel_booked,
  ses.hotel_discount,
  ses.hotel_discount_amount,
  ses.page_clicks,
  ses.session_end,
  ses.session_id,
  ses.session_start,
  ses.trip_id,
  ses.user_id
FROM sessions as ses
LEFT JOIN users AS use ON use.user_id = ses.user_id
WHERE use.sign_up_date >= '2022-01-01'
  AND use.sign_up_date <= '2022-12-31';
```

---

## 🏗 Architecture: The Medallion Pattern

To ensure data quality, lineage, and reproducibility, this project implements a **Medallion Architecture** (Multi-hop architecture). This transforms raw data into business-ready insights through three rigorous stages.

### 🥉 Bronze Layer: Raw Ingestion

**Status:** *Raw, Immutable, Historic*

* **Source:** Direct SQL exports from the source database (see queries above).
* **Format:** CSV (Comma Separated Values).
* **Content:** The raw `users`, `sessions`, `flights`, and `hotels` tables filtered for the 2022 cohort.
* **Role:** Acts as the landing zone. No transformations are applied here to ensure we always have a pristine copy of the original source data.

### 🥈 Silver Layer: Cleansed & Conformed

**Status:** *Cleaned, Typed, Validated*

* **Transformation:**
  * **Type Casting:** Converting string dates (e.g., `'2022-01-01'`) to native Python `datetime` objects.
  * **Data Correction (Hotels):** A logic error was identified in the source `hotels` table where the `nights` column contained negative values or mismatches. We implemented a recalculation logic: `nights = check_out - check_in` (normalized to calendar days). Invalid records (`nights <= 0`) are filtered out to protect downstream models.
  * **Schema Enforcement:** Ensuring no missing IDs or corrupt records enter the modeling phase.
* **Format:** **Parquet** (Columnar storage).
* **Why Parquet?** We chose Parquet over CSV for the internal data layers for three engineering reasons:
  1. **Schema Preservation:** Unlike CSVs, Parquet stores metadata about data types (e.g., distinguishing between a string and a datetime object). This prevents parsing errors and ensures type safety across the pipeline.
  2. **Performance (Columnar Storage):** Parquet is optimized for read-heavy analytical workloads. Operations that only require specific columns (e.g., calculating the average price) can skip reading irrelevant data, leading to significantly faster processing speeds.
  3. **Compression & Efficiency:** Parquet uses efficient compression algorithms (like Snappy) which can reduce file sizes by up to 80% compared to raw CSVs, optimizing both storage costs and I/O performance.

### 🥇 Gold Layer: Business-Level Aggregates

**Status:** *Enriched, Aggregated, Model-Ready*

* **Transformation:**
  * **Feature Engineering:** Aggregating transactional logs (sessions) into user-level behavioral metrics (e.g., `avg_clicks_per_session`, `cancellation_rate`).
  * **Segmentation:** Applying the K-Means clustering algorithm to assign strategic personas.
* **Content:** A final `user_segments` table where each row corresponds to a unique user with their assigned segment and recommended perk.
* **Role:** The "Source of Truth" for the marketing stakeholders and the final dashboard.
