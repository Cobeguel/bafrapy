from datetime import date, datetime, timezone


def normalize_mixed_timestamp(ts: int) -> datetime:
    if isinstance(ts, datetime):
        return ts.astimezone(timezone.utc)
    
    if isinstance(ts, date):
        return datetime(ts.year, ts.month, ts.day, tzinfo=timezone.utc)

    if isinstance(ts, str):
        ts = int(ts)

    ts_len = len(str(ts))
    if ts_len <= 10 or (10 < ts_len < 13):
        ts_normalized = ts
    else:
        ts_normalized = int(str(ts)[:10])
    return datetime.fromtimestamp(ts_normalized, tz=timezone.utc)


class TimestampFormatter:
    _reference_seconds_positions: int = 10           #1000000000  2021-09-09 01:46:40 UTC
    _reference_milliseconds_positions: int = 13

    def __init__(self, ts: int): 
        self.ts = ts

    def normalize_seconds(self) -> datetime:
        self.ts_len = len(str(self.ts))
        ts_normalized = 0
        if (
            self.ts_len <= self._reference_seconds_positions or
            (self._reference_seconds_positions < self.ts_len < self._reference_milliseconds_positions)
        ):
            ts_normalized = self.ts
        else:
            ts_normalized = int(str(self.ts)[:self._reference_seconds_positions])
        return datetime.fromtimestamp(ts_normalized, tz=timezone.utc)


