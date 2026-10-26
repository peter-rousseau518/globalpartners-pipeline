from datetime import date, datetime
from decimal import Decimal


def test_us_federal_holidays_2023(gold):
    h = gold.us_federal_holidays(2023)
    assert h[date(2023, 11, 23)] == "Thanksgiving Day"
    assert h[date(2023, 5, 29)] == "Memorial Day"
    assert h[date(2023, 1, 2)] == "New Year's Day Observed"
    assert h[date(2023, 1, 16)] == "Martin Luther King Jr."


def test_juneteenth_only_from_2021(gold):
    assert date(2020, 6, 19) not in gold.us_federal_holidays(2020)
    assert date(2022, 6, 19) in gold.us_federal_holidays(2022)


ITEM_SCHEMA = ("order_id string, lineitem_id string, order_ts_utc timestamp, order_date date, "
               "order_hour_local int, restaurant_id string, channel string, user_id string, is_guest boolean, "
               "is_loyalty boolean, card_hash string, item_category string, item_name string, "
               "item_line_amount decimal(10,2), item_quantity int, unit_price decimal(10,2), is_outlier boolean")


def item(i, price, qty=1):
    ts = datetime(2023, 3, 1 + i, 12)
    return (f"o{i}", f"l{i}", ts, ts.date(), 8, "r1", "Alltown Fresh", "u1", False, False, None,
            "Breakfast", "Bagel", Decimal(price) * qty, qty, Decimal(price), False)


def test_list_price_and_discount(spark, gold):
    # Five sales at the usual $10.00, then two units at $8.00 -> a $4.00 discount
    rows = [item(i, "10.00") for i in range(5)] + [item(6, "8.00", qty=2)]
    items = spark.createDataFrame(rows, ITEM_SCHEMA)
    opts = spark.createDataFrame([], "order_id string, lineitem_id string, option_amount decimal(12,2)")
    dc = gold.build_dim_customer(items)
    lines = gold.build_fact_order_line(items, opts, dc, gold.build_dim_restaurant(items),
                                       gold.build_dim_item(items), gold.build_dim_channel(items))
    d = {r.lineitem_id: r for r in lines.collect()}
    assert d["l6"].list_price == Decimal("10.00")
    assert d["l6"].is_discounted is True
    assert d["l6"].item_discount_amt == Decimal("4.00")
    assert d["l6"].line_net == Decimal("16.00")
    assert d["l0"].is_discounted is False and d["l0"].item_discount_amt == Decimal("0.00")
    assert all(r.line_gross - r.item_discount_amt == r.line_net for r in d.values())


def test_scd2_new_version_when_loyalty_changes(spark, gold):
    rows = [item(0, "5.00"), item(1, "5.00")]
    rows[1] = rows[1][:9] + (True,) + rows[1][10:]          # second order uses a loyalty card
    dc = gold.build_dim_customer(spark.createDataFrame(rows, ITEM_SCHEMA)).collect()
    assert len(dc) == 2
    assert sum(r.is_current for r in dc) == 1
