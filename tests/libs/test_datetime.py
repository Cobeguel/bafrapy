from datetime import datetime, timezone

import pytest

from bafrapy.libs.datetime import normalize_mixed_timestamp, TimestampFormatter

class TestNormalizeMixedTimestamp:
    def test_timestamp_in_seconds(self):
        ts = 1748131200  # 2025-05-25 00:00:00 UTC
        dt = normalize_mixed_timestamp(ts)
        assert dt == datetime(2025, 5, 25, 0, 0, 0, tzinfo=timezone.utc)

    def test_timestamp_in_milliseconds_to_seconds(self):
        ts = 1748131200000
        dt = normalize_mixed_timestamp(ts)
        assert dt == datetime(2025, 5, 25, 0, 0, 0, tzinfo=timezone.utc)

    def test_timestamp_in_microseconds_to_seconds(self):
        ts = 1748131200000000
        dt = normalize_mixed_timestamp(ts)
        assert dt == datetime(2025, 5, 25, 0, 0, 0, tzinfo=timezone.utc)

    def test_timestamp_in_nanoseconds_to_seconds(self):
        ts = 1748131200000000000
        dt = normalize_mixed_timestamp(ts)
        assert dt == datetime(2025, 5, 25, 0, 0, 0, tzinfo=timezone.utc)

    def test_timestamp_digits_between_seconds_and_milliseconds(self):
        ts = 7258118400  # 2200-01-01 00:00:00 UTC
        dt = normalize_mixed_timestamp(ts)
        assert dt == datetime(2200, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


class TestTimestampFormatter:

    def test_timestamp_in_seconds(self):
        ts = 1748131200  # 2025-05-25 00:00:00 UTC
        dt = TimestampFormatter(ts).normalize_seconds()
        assert dt == datetime(2025, 5, 25, 0, 0, 0, tzinfo=timezone.utc)

    def test_timestamp_in_milliseconds_to_seconds(self):
        ts = 1748131200000
        dt = TimestampFormatter(ts).normalize_seconds()
        assert dt == datetime(2025, 5, 25, 0, 0, 0, tzinfo=timezone.utc)

    def test_timestamp_in_microseconds_to_seconds(self):
        ts = 1748131200000000
        dt = TimestampFormatter(ts).normalize_seconds()
        assert dt == datetime(2025, 5, 25, 0, 0, 0, tzinfo=timezone.utc)

    def test_timestamp_in_nanoseconds_to_seconds(self):
        ts = 1748131200000000000
        dt = TimestampFormatter(ts).normalize_seconds()
        assert dt == datetime(2025, 5, 25, 0, 0, 0, tzinfo=timezone.utc)

    def test_timestamp_digits_between_seconds_and_milliseconds(self):
        ts = 7258118400  # 2200-01-01 00:00:00 UTC
        dt = TimestampFormatter(ts).normalize_seconds()
        assert dt == datetime(2200, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
