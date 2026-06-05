from datetime import UTC, datetime

_SECONDS_RANGE = 10_000_000_000
_MILLISECONDS_RANGE = 10_000_000_000_000
_MICROSECONDS_RANGE = 10_000_000_000_000_000


def parse_timestamp(timestamp: int | str) -> datetime:
    timestamp = int(timestamp)
    magnitude = abs(timestamp)

    if magnitude < _SECONDS_RANGE:
        return datetime.fromtimestamp(timestamp, UTC)

    if magnitude < _MILLISECONDS_RANGE:
        seconds, milliseconds = divmod(timestamp, 1_000)
        return datetime.fromtimestamp(seconds, UTC).replace(microsecond=milliseconds * 1_000)

    if magnitude < _MICROSECONDS_RANGE:
        seconds, micros = divmod(timestamp, 1_000_000)
        return datetime.fromtimestamp(seconds, UTC).replace(microsecond=micros)

    seconds, nanoseconds = divmod(timestamp, 1_000_000_000)
    return datetime.fromtimestamp(seconds, UTC).replace(microsecond=nanoseconds // 1_000)
