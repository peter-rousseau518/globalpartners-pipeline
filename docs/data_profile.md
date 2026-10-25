# Source Data Profile — Step 2: Initial Data Analysis

Profiled with [`scripts/profile_sources.py`](../scripts/profile_sources.py) on the three source CSVs. All three files are complete: their row counts match the requirements document.

## Files

| File | Rows | Columns | Notes |
| --- | --- | --- | --- |
| order_items.csv | 203,519 | 13 | LF line endings; 3,760 quoted fields (commas in names); 12 item names contain a line break inside quotes |
| order_item_options.csv | 193,017 | 6 | LF line endings |
| date_dim.csv | 365 | 8 | CRLF line endings; `date_key` is `dd-mm-yyyy`; `month` is a number (1–12), not a name |

## Shape of the data

| Measure | Value |
| --- | --- |
| Order date range (UTC) | 2020-04-21 to 2024-02-21 |
| Orders | 131,328 |
| Restaurants | 28 (6 to 20,727 orders each) |
| Identified customers (user_id) | 20,174 (50% ordered only once) |
| Guest orders (no user_id) | 12,669 orders / 17,808 lines |
| Loyalty lines | 46,084 of 203,519 (22.6%) |
| Channels (app_name) | Alltown Fresh 201,423 · Alltown Neighborhood Perks 1,270 · Alltown Fresh - DEVELOPMENT 826 |
| Currency | USD only |

## Join keys verified

- `order_items (order_id, lineitem_id)` is unique; `lineitem_id` alone is also unique.
- Every order has exactly one user, restaurant, timestamp and loyalty flag, so order-level attributes can be taken from any of its lines.
- `is_loyalty = TRUE` exactly when `printed_card_number` is present.
- `order_item_options → order_items` on `(order_id, lineitem_id)`: 28 options have no parent line.
- `order_items → date_dim` on order date: **122,855 lines (60%) fall outside date_dim**, which covers 2023 only.

## Data quality issues and handling

| # | Issue | Count | Handling in the pipeline |
| --- | --- | --- | --- |
| 1 | date_dim covers 2023 only; orders run from 2020-04 to 2024-02 | 122,855 lines | Generate `dim_date` in PySpark for the full order range; keep source holiday names for 2023 and derive US federal holidays for other years |
| 2 | No negative `option_price` values (min is 0.00) | 0 discount rows | Discounts are detected on item price instead: a line sold below its usual menu price (see Discount definition below) |
| 3 | Exact duplicate option rows | 2,299 | Treated as legitimate repeats (option_quantity is always 1, so a repeated row likely means "added twice"); flagged in DQ audit, not removed |
| 4 | Orphan options | 28 | Quarantined |
| 5 | Malformed line: blank lineitem_id, category and name, quantity 0 | 1 | Quarantined |
| 6 | Test / development channel | 826 lines | Excluded from all metrics (`app_name = 'Alltown Fresh - DEVELOPMENT'`); `Test Items` category also excluded |
| 7 | Category values with typos or pasted admin URLs (e.g. `Sandwiches\`1`, `Bowls0`, `Sqalads`, `BBQ Plateshttps://…`) | 188 lines across 6 bad values | Mapped to clean categories in a Silver lookup; `Kid's`/`Kids` merged |
| 8 | Extreme prices (≥ $500, max $5,000) | 11 lines | Flagged as outliers; excluded from CLV and revenue until confirmed |
| 9 | Extreme quantities (≥ 100, max 500) | 9 lines | Flagged as outliers alongside #8 |
| 10 | $0.00 item price | 156 lines | Kept (likely comps or free items); counted as zero revenue |
| 11 | Timestamps without milliseconds | 187 | Parsed with ISO 8601 handling; no data loss |
| 12 | Line breaks inside item names | 12 | Replaced with a space on load |
| 13 | Guest orders (no user_id) | 12,669 orders | Included in sales, location and discount metrics; excluded from customer-level CLV, RFM and churn |
| 14 | Customers whose loyalty flag changes over time | 2,783 users | Handled by SCD Type 2 on `dim_customer` |

## Discount definition

Option prices are only add-on charges (0.05 to 8.00), so a discount is defined as **an item sold below its usual menu price**.

- **List price** = the most common `item_price` for the same restaurant, item and calendar month. If that group has fewer than 5 lines, use the most common price for the restaurant and item across all months. Ties take the higher price.
- **Discounted line** = `list_price − item_price ≥ $0.05`. **Item discount** = `(list_price − item_price) × item_quantity`.
- Grouping by restaurant and month keeps menu price increases (e.g. $9.89 → $11.89 over 2021–2023) and location pricing from being counted as discounts.

| Result on source data (excl. development channel and price outliers) | Value |
| --- | --- |
| Discounted lines | 13,743 (6.8%) |
| Discounted orders | 12,525 (9.6%) |
| Total item discount | ≈ $30,200 |
| Median discount per line | $1.25 (17.5% off) |

Typical cases: Buffalo Chicken Wrap $9.99 vs $11.99 list, Bacon Egg & Cheese $5.99 vs $7.99, smoothies $7.00 vs $9.00.
