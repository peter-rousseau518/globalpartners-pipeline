"""Profile the GlobalPartners source CSVs (Step 2: initial data analysis).

Usage:
    python scripts/profile_sources.py --items order_items.csv \
        --options order_item_options.csv --dates date_dim.csv
"""
import argparse

import pandas as pd


def load(path):
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def column_profile(name, df):
    print(f"\n=== {name}: {len(df):,} rows")
    for c in df.columns:
        s = df[c]
        print(f"  {c:22} blank={int((s == '').sum()):>7,}  distinct={s.nunique():>7,}  max_len={s.str.len().max()}")


def main(a):
    oi, op, dd = load(a.items), load(a.options), load(a.dates)
    for n, d in [("order_items", oi), ("order_item_options", op), ("date_dim", dd)]:
        column_profile(n, d)

    print("\n=== Keys and relationships")
    print("  duplicate (order_id, lineitem_id) in order_items:", oi.duplicated(["ORDER_ID", "LINEITEM_ID"]).sum())
    print("  rows with blank lineitem_id:", (oi.LINEITEM_ID == "").sum())
    print("  exact duplicate rows in order_item_options:", op.duplicated().sum())
    keys = set(zip(oi.ORDER_ID, oi.LINEITEM_ID))
    print("  orphan options (no parent line):", sum(k not in keys for k in zip(op.ORDER_ID, op.LINEITEM_ID)))
    g = oi.groupby("ORDER_ID").agg(u=("USER_ID", "nunique"), r=("RESTAURANT_ID", "nunique"),
                                    t=("CREATION_TIME_UTC", "nunique"), l=("IS_LOYALTY", "nunique"))
    print("  orders with >1 user / restaurant / timestamp / loyalty flag:",
          (g.u > 1).sum(), (g.r > 1).sum(), (g.t > 1).sum(), (g.l > 1).sum())

    print("\n=== Values")
    for c, d in [("ITEM_PRICE", oi), ("ITEM_QUANTITY", oi), ("OPTION_PRICE", op), ("OPTION_QUANTITY", op)]:
        x = pd.to_numeric(d[c], errors="coerce")
        print(f"  {c:16} non-numeric={x.isna().sum()} min={x.min()} max={x.max()} "
              f"negative={(x < 0).sum()} zero={(x == 0).sum()}")
    print("  item_price >= 500:", (oi.ITEM_PRICE.astype(float) >= 500).sum())
    print("  item_quantity >= 100:", (oi.ITEM_QUANTITY.astype(int) >= 100).sum())
    ts = pd.to_datetime(oi.CREATION_TIME_UTC, utc=True, format="ISO8601", errors="coerce")
    print("  unparseable timestamps:", ts.isna().sum(), "| range:", ts.min(), "to", ts.max())
    print("  timestamps without milliseconds:", (~oi.CREATION_TIME_UTC.str.contains(r"\.")).sum())
    print("  orders by year:", ts.dt.year.value_counts().sort_index().to_dict())
    print("  app_name:", oi.APP_NAME.value_counts().to_dict())
    print("  currency:", oi.CURRENCY.value_counts().to_dict())
    print("  is_loyalty:", oi.IS_LOYALTY.value_counts().to_dict())
    print("  loyalty flag == card number present:", ((oi.IS_LOYALTY == "TRUE") == (oi.PRINTED_CARD_NUMBER != "")).all())
    print("  item names containing a line break:", oi.ITEM_NAME.str.contains("\n").sum())
    print("  categories:", oi.ITEM_CATEGORY.value_counts().to_dict())

    print("\n=== Customers")
    named = oi[oi.USER_ID != ""]
    print("  distinct users:", named.USER_ID.nunique())
    print("  guest lines / guest orders:", (oi.USER_ID == "").sum(), oi[oi.USER_ID == ""].ORDER_ID.nunique())
    print("  users with mixed loyalty flags:", (named.groupby("USER_ID").IS_LOYALTY.nunique() > 1).sum())
    print("  share of users with one order:", round((named.groupby("USER_ID").ORDER_ID.nunique() == 1).mean(), 3))

    print("\n=== date_dim coverage")
    dk = pd.to_datetime(dd.date_key, format="%d-%m-%Y")
    print("  date_dim range:", dk.min().date(), "to", dk.max().date())
    od = ts.dt.tz_convert(None).dt.normalize()
    print("  order lines dated outside date_dim:", (~od.isin(set(dk))).sum())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--items", required=True)
    p.add_argument("--options", required=True)
    p.add_argument("--dates", required=True)
    main(p.parse_args())
