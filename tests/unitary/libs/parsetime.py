from datetime import UTC, datetime

import pytest

from bafrapy.libs.parsetime import parse_timestamp


@pytest.mark.parametrize(
    ("timestamp", "expected"),
    [
        (1_735_689_600, datetime(2025, 1, 1, tzinfo=UTC)),
        (1_735_689_600_000, datetime(2025, 1, 1, tzinfo=UTC)),
        (1_735_689_600_000_000, datetime(2025, 1, 1, tzinfo=UTC)),
        (1_735_689_600_000_000_000, datetime(2025, 1, 1, tzinfo=UTC)),
    ],
)
def test_parse_timestamp_detects_epoch_unit(timestamp: int, expected: datetime) -> None:
    assert parse_timestamp(timestamp) == expected


@pytest.mark.parametrize(
    ("timestamp", "expected"),
    [
        (1_735_689_600_123, datetime(2025, 1, 1, 0, 0, 0, 123_000, tzinfo=UTC)),
        (1_735_689_600_123_456, datetime(2025, 1, 1, 0, 0, 0, 123_456, tzinfo=UTC)),
        (1_735_689_600_123_456_789, datetime(2025, 1, 1, 0, 0, 0, 123_456, tzinfo=UTC)),
    ],
)
def test_parse_timestamp_preserves_supported_subsecond_precision(timestamp: int, expected: datetime) -> None:
    assert parse_timestamp(timestamp) == expected


def test_parse_timestamp_accepts_numeric_strings() -> None:
    assert parse_timestamp("1735689600000000") == datetime(2025, 1, 1, tzinfo=UTC)
