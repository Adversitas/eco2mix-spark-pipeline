"""Batch transformations on 384 real eco2mix rows (2 regions x 4 days x 48 half-hours)."""

from datetime import date

import pytest

from eco2mix.batch import (
    annual_region_summary,
    daily_region_mix,
    duplicate_report,
    peak_hours,
    quality_report,
    to_silver,
)


def test_silver_is_typed_and_unique(silver):
    assert silver.count() == 384
    assert dict(silver.dtypes)["ts"] == "timestamp"
    assert dict(silver.dtypes)["consommation"] == "double"
    assert silver.select("region_code", "ts").distinct().count() == 384


def test_duplicates_are_removed(spark, raw):
    doubled = raw.unionByName(raw.limit(10))
    assert to_silver(doubled).count() == 384
    rep = duplicate_report(doubled)
    assert rep == {"duplicate_rows_identical": 10, "duplicate_rows_conflicting": 0}


def test_quality_report(raw, silver):
    rep = quality_report(raw, silver)
    assert rep["raw_rows"] == rep["silver_rows"] == 384
    assert rep["regions"] == 2
    assert rep["first_ts"] == "2023-01-09T23:00Z"  # 2023-01-10 00:00 in Paris
    # the sample has two separate 2-day blocks, so the span between them is "missing"
    assert set(rep["missing_half_hours"]) == {"84", "93"}
    assert 0 <= rep["balance_residual_share"]["p50"] <= rep["balance_residual_share"]["max"]


def test_daily_mix_energy_is_half_hour_power_sum(silver):
    daily = daily_region_mix(silver)
    one = daily.where("region_code = '84' and day = date'2023-06-21'").first()
    mw_sum = (
        silver.where("region_code = '84'")
        .selectExpr("to_date(from_utc_timestamp(ts, 'Europe/Paris')) as d", "consommation")
        .where("d = date'2023-06-21'")
        .groupBy()
        .sum("consommation")
        .first()[0]
    )
    assert one.half_hours == 48
    assert one.consommation_gwh == pytest.approx(mw_sum * 0.5 / 1000)
    assert 0 <= one.renewable_share <= 1


def test_summer_has_more_solar_than_winter(silver):
    daily = daily_region_mix(silver).where("region_code = '93' and half_hours = 48")
    by_day = {r.day: r.solaire_gwh for r in daily.collect()}
    assert by_day[date(2023, 6, 21)] > 2 * by_day[date(2023, 1, 11)]


def test_peak_hours_window(silver):
    peaks = peak_hours(silver, n=2).collect()
    assert len(peaks) == 4
    for region in ("84", "93"):
        top = [p.consommation for p in peaks if p.region_code == region]
        assert top == sorted(top, reverse=True)
        assert (
            top[0]
            == silver.where(f"region_code = '{region}'").agg({"consommation": "max"}).first()[0]
        )


def test_annual_summary_columns(silver):
    rows = annual_region_summary(daily_region_mix(silver)).collect()
    assert {r.region_code for r in rows} == {"84", "93"}
    for r in rows:
        assert r.consumption_twh > 0 and 0 <= r.renewable_share <= 1


def test_negative_production_is_reported(raw):
    cols = [c if c != "solaire" else "'-5' as solaire" for c in raw.columns]
    bad = raw.limit(1).selectExpr(*cols)
    assert quality_report(bad, to_silver(bad))["negative_production_values"] == {"solaire": 1}
