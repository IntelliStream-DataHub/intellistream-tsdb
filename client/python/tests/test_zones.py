# SPDX-License-Identifier: Apache-2.0
"""Grouping by local periods (protocol version 4): the client turns a time zone
into bucket bounds."""

import pathlib
from zoneinfo import ZoneInfo

import pytest

import tsdb_client
from tsdb_client import Client, TsdbError, calendar_bounds

HOUR, DAY = 3_600_000, 86_400_000
FIXTURES = pathlib.Path(__file__).resolve().parents[2] / "rust" / "tests" / "fixtures"
# every 30 minutes around the 2024 DST changes, as fixtures/group_by_zone.c inserts them
SPANS = [(1_711_670_400_000, 1_712_016_000_000), (1_729_814_400_000, 1_730_160_000_000)]
ROWS = [t for a, b in SPANS for t in range(a, b, 1_800_000)]
UNITS = {"3": "hour", "4": "day", "5": "week"}


def load(client):
    s = client.series("oslo")
    client.insert_series(s, ROWS, [1.0] * len(ROWS))
    return s


def test_groups_are_those_of_the_c_client(client):
    s = load(client)
    for line in (FIXTURES / "group_by_zone.txt").read_text().splitlines():
        head, expected = line.split(":")
        zone, unit, count = head.split()
        got = client.query_group(s, unit=UNITS[unit], count=int(count), timezone=zone)
        assert " ".join(f"({b.start},{b.count})" for b in got) == expected.strip(), head


def test_oslo_days(client):
    s = load(client)
    days = client.query_group(s, unit="day", timezone="Europe/Oslo")
    summer = range(1_711_846_800_000, 1_729_990_800_000)
    assert [d.start % DAY for d in days] == [22 * HOUR if d.start in summer else 23 * HOUR for d in days]
    assert days[3].start - days[2].start == 23 * HOUR and days[2].count == 46
    assert days[8].start - days[7].start == 25 * HOUR and days[7].count == 50
    assert client.query_group(s, unit="day", timezone=ZoneInfo("Europe/Oslo")) == days
    bounds = calendar_bounds("Europe/Oslo", "day", ROWS[0], ROWS[-1] + 1)
    assert client.query_bounds(s, bounds) == days
    assert client.query_bounds(s, bounds, order="desc", limit=2) == [days[9], days[8]]

    empty = client.series("empty")
    assert client.query_group(empty, unit="day", timezone="Europe/Oslo") == []
    for series in (s, empty):
        with pytest.raises(TsdbError) as e:
            client.query_group(series, unit="day", timezone="Mars/Olympus_Mons")
        assert e.value.code == tsdb_client.ERR_INVALID
    with pytest.raises(TypeError):
        client.query_group(s, unit="day", timezone=3)


def test_calendar_bounds():
    assert calendar_bounds("UTC", "day", 1, DAY + 1) == [0, DAY]
    assert calendar_bounds("+02:00", "hour", 0, 1, count=6) == [-2 * HOUR]
    assert calendar_bounds("-0530", "day", 0, DAY) == [-66_600_000, 19_800_000]
    assert calendar_bounds("Europe/Oslo", "day", 5, 5) == []
    # 2024-01-01 00:00 UTC is still December in St. John's (UTC-3:30)
    jan = calendar_bounds(ZoneInfo("America/St_Johns"), "month", 1_704_067_200_000, 1_706_745_600_000)
    assert jan == [1_701_401_400_000, 1_704_079_800_000]
    for bad in [("Mars/Olympus_Mons", "day"), ("UTC", "second")]:
        with pytest.raises(TsdbError) as e:
            calendar_bounds(*bad, -(2**62), 2**62)
        assert e.value.code == tsdb_client.ERR_INVALID
    with pytest.raises(ValueError):
        calendar_bounds("UTC", "fortnight", 0, 1)


def test_query_bounds(client):
    s = client.series("b")
    client.insert_series(s, [i * 1000 for i in range(10)], [float(i) for i in range(10)])
    got = client.query_bounds(s, [2000, 3000, 7000])
    assert [(b.start, b.count, b.sum) for b in got] == [(2000, 1, 2.0), (3000, 4, 18.0), (7000, 3, 24.0)]
    newest = client.query_bounds(s, [2000, 3000, 7000], None, 8000, order="desc", limit=2)
    assert [(b.start, b.count) for b in newest] == [(7000, 1), (3000, 4)]
    for bad in ([], [3000, 3000], [3000, 2000]):
        with pytest.raises(TsdbError) as e:
            client.query_bounds(s, bad)
        assert e.value.code == tsdb_client.ERR_INVALID
