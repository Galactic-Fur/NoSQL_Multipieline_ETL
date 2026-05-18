"""
parser.py – Shared NASA HTTP log parser.
All pipelines must use this parser to ensure equivalent semantics.

NASA log format:
  host - - [timestamp] "method path protocol" status bytes
"""
import re
from dataclasses import dataclass, field
from typing import Optional, Tuple

# Regex for the overall log line
LOG_REGEX = re.compile(
    r'^(\S+)'            # host
    r'\s+\S+\s+\S+'      # ident, authuser (ignored)
    r'\s+\[([^\]]+)\]'   # timestamp in brackets
    r'\s+"([^"]*)"'       # quoted request string
    r'\s+(\d{3})'        # status code
    r'\s+(\S+)$'         # bytes (number or -)
)

# Timestamp format: 01/Jul/1995:00:00:01 -0400
TS_REGEX = re.compile(
    r'^(\d{2})/(\w{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2})\s+[+-]\d{4}$'
)

MONTH_MAP = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}


@dataclass
class LogRecord:
    host: str
    timestamp: str          # raw timestamp string
    log_date: str           # YYYY-MM-DD
    log_hour: int           # 0-23
    http_method: str
    resource_path: str
    protocol_version: str
    status_code: int
    bytes_transferred: int


def parse_timestamp(ts_str: str) -> Tuple[Optional[str], Optional[int]]:
    """Return (log_date YYYY-MM-DD, log_hour) or (None, None)."""
    m = TS_REGEX.match(ts_str.strip())
    if not m:
        return None, None
    day, mon, year, hour = m.group(1), m.group(2), m.group(3), m.group(4)
    month_num = MONTH_MAP.get(mon)
    if not month_num:
        return None, None
    return f"{year}-{month_num}-{day}", int(hour)


def parse_request(req_str: str) -> Tuple[str, str, str]:
    """Parse 'METHOD /path PROTOCOL' into (method, path, protocol)."""
    parts = req_str.strip().split()
    if len(parts) == 3:
        return parts[0].upper(), parts[1], parts[2]
    elif len(parts) == 2:
        return parts[0].upper(), parts[1], ""
    elif len(parts) == 1:
        return parts[0].upper(), "", ""
    else:
        return "", req_str, ""


def parse_line(line: str) -> Tuple[Optional[LogRecord], bool]:
    """
    Parse one log line.
    Returns (LogRecord, True) on success, (None, False) on failure.
    """
    line = line.strip()
    if not line:
        return None, False

    m = LOG_REGEX.match(line)
    if not m:
        return None, False

    host = m.group(1)
    ts_raw = m.group(2)
    request_str = m.group(3)
    status_str = m.group(4)
    bytes_str = m.group(5)

    # Parse timestamp
    log_date, log_hour = parse_timestamp(ts_raw)
    if log_date is None:
        return None, False

    # Parse request
    http_method, resource_path, protocol_version = parse_request(request_str)

    # Parse status code
    try:
        status_code = int(status_str)
    except ValueError:
        return None, False

    # Parse bytes – treat '-' or missing as 0
    if bytes_str == "-":
        bytes_transferred = 0
    else:
        try:
            bytes_transferred = int(bytes_str)
        except ValueError:
            bytes_transferred = 0

    return LogRecord(
        host=host,
        timestamp=ts_raw,
        log_date=log_date,
        log_hour=log_hour,
        http_method=http_method,
        resource_path=resource_path,
        protocol_version=protocol_version,
        status_code=status_code,
        bytes_transferred=bytes_transferred,
    ), True


def parse_file_in_batches(filepath: str, batch_size: int = 10000):
    """
    Generator that yields (batch_id, records_list, malformed_in_batch).
    Each batch contains up to batch_size parsed LogRecord objects.
    """
    batch_id = 0
    records = []
    malformed_count = 0

    with open(filepath, "r", errors="replace") as f:
        for raw_line in f:
            record, ok = parse_line(raw_line)
            if ok:
                records.append(record)
            else:
                if raw_line.strip():   # don't count blank lines
                    malformed_count += 1

            if len(records) >= batch_size:
                batch_id += 1
                yield batch_id, records, malformed_count
                records = []
                malformed_count = 0

    # Final partial batch
    if records:
        batch_id += 1
        yield batch_id, records, malformed_count
