from datetime import date, timedelta
from decimal import Decimal


def dim_date(spark, start, days):
    return spark.createDataFrame([(start + timedelta(d),) for d in range(days)], "date_key date")


def test_clv_accumulates_and_churn_status(spark, metrics):
    start = date(2023, 1, 1)
    daily = spark.createDataFrame(
        [("a", start, Decimal("10.00"), 1), ("a", start + timedelta(5), Decimal("15.00"), 1),
         ("b", start, Decimal("4.00"), 1)],
        "user_id string, order_date date, revenue_on_day decimal(12,2), orders_on_day long")
    as_of = start + timedelta(99)
    clv = metrics.build_clv_daily(daily, dim_date(spark, start, 100), as_of)
    last = {r.user_id: r for r in clv.where(clv.snapshot_date == as_of).collect()}
    assert last["a"].clv_to_date == Decimal("25.00")
    assert last["a"].orders_to_date == 2
    assert last["a"].days_since_last_order == 94
    assert last["a"].churn_status == "Lapsed"
    assert last["b"].clv_to_date == Decimal("4.00")
    day6 = clv.where((clv.user_id == "a") & (clv.snapshot_date == start + timedelta(5))).collect()[0]
    assert day6.churn_status == "Active" and day6.clv_to_date == Decimal("25.00")


def test_inactive_customers_score_one_and_are_never_vip(spark, metrics):
    start = date(2023, 1, 1)
    daily = spark.createDataFrame([("a", start, Decimal("100.00"), 5)],
                                  "user_id string, order_date date, revenue_on_day decimal(12,2), orders_on_day long")
    as_of = start + timedelta(299)                     # beyond the 180-day RFM window
    clv = metrics.build_clv_daily(daily, dim_date(spark, start, 300), as_of)
    r = clv.where(clv.snapshot_date == as_of).collect()[0]
    assert r.rfm_active is False
    assert (r.r_score, r.f_score, r.m_score) == (1, 1, 1)
    assert r.rfm_segment != "VIP"
