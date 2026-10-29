"""Turn a snapshot of ``fct_c360_profile`` rows into the dashboard payload.

Pure function, no I/O: the provider fetches rows (mock fixture or live Flink
snapshot), this folds them into a ``DataAnalytics``. Kept separate from the
providers so the aggregation is unit-testable without any Confluent wiring.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone

from analytics.models import (
    DataAnalytics,
    CategoryCount,
    CategoryValue,
    DataQuality,
)

# Top-N cap for the geography breakdown so a long tail of countries doesn't
# overrun the chart; everything past the cap folds into "Other".
_COUNTRY_TOP_N = 8


def _num(value: object) -> float:
    """Coerce a possibly-None / string-decimal cell to float; blanks → 0.0."""
    if value is None or value == "":
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _counts(rows: list[dict], field: str, *, default: str = "Unknown") -> list[CategoryCount]:
    """Count rows by a categorical column, largest-first (ties broken by label)."""
    counter: Counter[str] = Counter(str(r.get(field) or default) for r in rows)
    ordered = sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))
    return [CategoryCount(label=label, count=count) for label, count in ordered]


def aggregate(rows: list[dict], source_mode: str, query_ms: float = 0.0) -> DataAnalytics:
    """Fold profile rows into the full dashboard payload.

    ``query_ms`` is the backend-measured time the snapshot query took (0 when not
    measured). Safe on an empty snapshot: totals are 0, averages are ``None``,
    and every distribution is an empty list, so the UI renders an empty state
    instead of failing.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    row_count = len(rows)

    total_accounts = sum(int(_num(r.get("account_count"))) for r in rows)
    total_balance = round(sum(_num(r.get("total_balance")) for r in rows), 2)
    total_transactions = sum(int(_num(r.get("txn_count"))) for r in rows)
    total_txn_amount = round(sum(_num(r.get("txn_total")) for r in rows), 2)

    ages = [_num(r.get("age_years")) for r in rows if r.get("age_years") not in (None, "")]
    avg_age = round(sum(ages) / len(ages), 1) if ages else None
    avg_balance = round(total_balance / row_count, 2) if row_count else None

    missing_email = sum(1 for r in rows if int(_num(r.get("missing_email_flag"))))
    missing_phone = sum(1 for r in rows if int(_num(r.get("missing_phone_flag"))))
    complete = sum(
        1
        for r in rows
        if not int(_num(r.get("missing_email_flag"))) and not int(_num(r.get("missing_phone_flag")))
    )

    balance_by_segment_map: dict[str, float] = defaultdict(float)
    for r in rows:
        balance_by_segment_map[str(r.get("customer_segment") or "Unknown")] += _num(r.get("total_balance"))
    balance_by_segment = [
        CategoryValue(label=label, value=round(value, 2))
        for label, value in sorted(balance_by_segment_map.items(), key=lambda kv: (-kv[1], kv[0]))
    ]

    by_country = _counts(rows, "country")
    if len(by_country) > _COUNTRY_TOP_N:
        head = by_country[:_COUNTRY_TOP_N]
        other = sum(c.count for c in by_country[_COUNTRY_TOP_N:])
        head.append(CategoryCount(label="Other", count=other))
        by_country = head

    return DataAnalytics(
        source_mode=source_mode,
        generated_at=now,
        query_ms=query_ms,
        row_count=row_count,
        total_customers=row_count,
        total_accounts=total_accounts,
        total_balance=total_balance,
        total_transactions=total_transactions,
        total_txn_amount=total_txn_amount,
        avg_balance_per_customer=avg_balance,
        avg_age=avg_age,
        data_quality=DataQuality(
            complete=complete,
            missing_email=missing_email,
            missing_phone=missing_phone,
        ),
        by_segment=_counts(rows, "customer_segment"),
        by_generation=_counts(rows, "generation_segment"),
        by_gender=_counts(rows, "gender"),
        by_country=by_country,
        balance_by_segment=balance_by_segment,
    )
