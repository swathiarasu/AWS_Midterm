from datetime import datetime, timezone, timedelta


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(dt: datetime) -> str:
    return dt.isoformat(timespec="microseconds")


def utc_now_iso() -> str:
    return iso_utc(utc_now())


def cutoff_iso(seconds: int) -> str:
    return iso_utc(utc_now() - timedelta(seconds=seconds))


def compact_timestamp(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%S%fZ")