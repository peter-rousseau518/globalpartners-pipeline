from datetime import datetime
from decimal import Decimal

from pyspark.sql import functions as F

COLS = ("row_id app_name restaurant_id creation_time_utc order_id user_id printed_card_number is_loyalty "
        "currency lineitem_id item_category item_name item_price item_quantity _ingest_ts_utc ingest_date").split()
SCHEMA = ("row_id long, app_name string, restaurant_id string, creation_time_utc timestamp, order_id string, "
          "user_id string, printed_card_number string, is_loyalty boolean, currency string, lineitem_id string, "
          "item_category string, item_name string, item_price decimal(10,2), item_quantity int, "
          "_ingest_ts_utc string, ingest_date string")
TS = datetime(2023, 3, 8, 15, 0, 0)


def row(rid, **kw):
    base = dict(row_id=rid, app_name="Alltown Fresh", restaurant_id="r1", creation_time_utc=TS, order_id=f"o{rid}",
                user_id="u1", printed_card_number=None, is_loyalty=False, currency="USD", lineitem_id=f"l{rid}",
                item_category="Breakfast", item_name="Bagel", item_price=Decimal("5.00"), item_quantity=1,
                _ingest_ts_utc="2026-10-24T00:00:00", ingest_date="2026-10-24")
    base.update(kw)
    return tuple(base[c] for c in COLS)


def test_clean_category_fixes_urls_and_typos(spark, silver):
    df = spark.createDataFrame([("Drip Chttps://x.com/admin/menuitems#offee",), ("BBQ Plateshttps://a/b#",),
                                ("Bowls0",), ("Kid's",), ("Salads",)], "c string")
    got = [r[0] for r in df.select(silver.clean_category(F.col("c"))).collect()]
    assert got == ["Drip Coffee", "BBQ Plates", "Bowls", "Kids", "Salads"]


def test_unit_price_is_line_total_divided_by_quantity(spark, silver):
    bronze = spark.createDataFrame([row(1, item_price=Decimal("29.97"), item_quantity=3)], SCHEMA)
    clean, _ = silver.build_order_items(bronze, "salt")
    r = clean.collect()[0]
    assert r.item_line_amount == Decimal("29.97")
    assert r.unit_price == Decimal("9.99")
    assert r.is_outlier is False


def test_rejects_test_channel_and_malformed_rows(spark, silver):
    bronze = spark.createDataFrame([
        row(1),
        row(2, app_name="Alltown Fresh - DEVELOPMENT"),
        row(3, lineitem_id=None),
        row(4, item_quantity=0),
    ], SCHEMA)
    clean, rejected = silver.build_order_items(bronze, "salt")
    assert clean.count() == 1
    reasons = sorted(r.reject_reason for r in rejected.collect())
    assert reasons == ["missing_lineitem_id", "non_positive_quantity", "test_channel"]


def test_bulk_quantity_flagged_as_outlier(spark, silver):
    bronze = spark.createDataFrame([row(1, item_price=Decimal("5000.00"), item_quantity=500)], SCHEMA)
    clean, _ = silver.build_order_items(bronze, "salt")
    r = clean.collect()[0]
    assert r.unit_price == Decimal("10.00") and r.is_outlier is True


def test_card_number_is_hashed_not_stored(spark, silver):
    bronze = spark.createDataFrame([row(1, printed_card_number="678050219321", is_loyalty=True), row(2)], SCHEMA)
    clean, _ = silver.build_order_items(bronze, "salt")
    rows = {r.lineitem_id: r for r in clean.collect()}
    assert "printed_card_number" not in clean.columns
    assert len(rows["l1"].card_hash) == 64 and rows["l2"].card_hash is None


def test_orphan_options_are_rejected(spark, silver):
    bronze = spark.createDataFrame([row(1)], SCHEMA)
    clean, rejected = silver.build_order_items(bronze, "salt")
    opts = spark.createDataFrame(
        [("o1", "l1", "Cheese", "American", Decimal("0.50"), 1, "t", "d"),
         ("o9", "l9", "Cheese", "Swiss", Decimal("0.50"), 1, "t", "d")],
        "order_id string, lineitem_id string, option_group_name string, option_name string, "
        "option_price decimal(10,2), option_quantity int, _ingest_ts_utc string, ingest_date string")
    good, bad = silver.build_options(opts, clean, rejected)
    assert good.count() == 1
    assert [r.reject_reason for r in bad.collect()] == ["orphan_option"]
