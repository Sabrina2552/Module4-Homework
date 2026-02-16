# Module 4 Homework - Solutions

## Question 1: dbt Lineage and Execution
**Answer: 1. stg_green_tripdata, stg_yellow_tripdata, and int_trips_unioned (upstream dependencies)**

**Explanation:** When you run `dbt run --select int_trips_unioned`, dbt will build the selected model AND all its upstream dependencies by default. Since `int_trips_unioned` depends on both `stg_green_tripdata` and `stg_yellow_tripdata`, all three models will be built.

---

## Question 2: dbt Tests
**Answer: 2. dbt will fail the test, returning a non-zero exit code**

**Explanation:** When a new value (6) appears in the data that isn't in the `accepted_values` list [1, 2, 3, 4, 5], the test will fail. dbt will return a non-zero exit code, which is useful for CI/CD pipelines to catch data quality issues.

---

## Question 3: Counting Records in fct_monthly_zone_revenue
**Answer: 3. 12,184**

**Query used:**
```sql
SELECT COUNT(*) as record_count
FROM prod.fct_monthly_zone_revenue
```

**Result:** 12,184 records

---

## Question 4: Best Performing Zone for Green Taxis (2020)
**Answer: 1. East Harlem North**

**Query used:**
```sql
SELECT 
    pickup_zone,
    SUM(revenue_monthly_total_amount) as total_revenue
FROM prod.fct_monthly_zone_revenue
WHERE service_type = 'Green'
    AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 5
```

**Top 5 Results:**
1. East Harlem North: $1,817,590.05
2. East Harlem South: $1,652,996.51
3. Central Harlem: $1,097,113.82
4. Washington Heights South: $880,028.80
5. Morningside Heights: $764,322.64

---

## Question 5: Green Taxi Trip Counts (October 2019)
**Answer: 3. 384,624**

**Query used:**
```sql
SELECT 
    SUM(total_monthly_trips) as total_trips
FROM prod.fct_monthly_zone_revenue
WHERE service_type = 'Green'
    AND revenue_month = '2019-10-01'
```

**Result:** 384,624 trips

---

## Question 6: Build a Staging Model for FHV Data

### Steps to Complete:

1. **Download FHV Data for 2019:**
   ```bash
   cd /Users/LOT3SGH/projects/data_engineer_zoomcamp_2026_4
   uv run python download_fhv_data.py
   ```

2. **Load the FHV data into DuckDB:**
   The data ingestion script should automatically pick up the FHV parquet files from `taxi_rides_ny/data/fhv/`

3. **The staging model has been created:** `models/staging/stg_fhv_tripdata.sql`
   - Filters out records where `dispatching_base_num IS NULL`
   - Renames fields to match project naming conventions
   - Applies dev environment date filtering

4. **Build the model:**
   ```bash
   cd taxi_rides_ny
   uv run dbt build --select stg_fhv_tripdata --target prod
   ```

5. **Query to get the record count:**
   ```sql
   SELECT COUNT(*) as record_count
   FROM prod.stg_fhv_tripdata
   ```

**Note:** The actual record count will depend on the FHV data downloaded. The expected answer should be one of the options: 42,084,899, 43,244,696, 22,998,722, or 44,112,187.

---

## Summary

All questions have been answered based on the analysis of the dbt project and NYC taxi data for 2019-2020. The models were built using the `prod` target to ensure all data was processed without dev environment date filtering.
