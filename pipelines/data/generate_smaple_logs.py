"""
Generates a realistic sample of NASA HTTP log format records for demo/testing.
In production, replace with the real NASA_access_log_Jul95.gz / Aug95.gz files.
Format: host timestamp "method path protocol" status bytes
"""
import random
import gzip
import os

HOSTS = [
    "199.72.81.55", "unicomp6.unicomp.net", "199.120.110.21",
    "burger.letters.com", "199.120.110.21", "205.212.115.106",
    "d104.aa.net", "129.94.144.152", "www-b4.proxy.aol.com",
    "remote.mathworks.com", "ix-esc-ca2-07.ix.netcom.com",
    "gw1.att.com", "piweba3y.prodigy.com", "146.129.66.109",
    "pc0.cc.uow.edu.au", "130.110.74.81", "203.11.71.83",
    "163.206.89.4", "erm.com", "ts8-1.westwood.ts.ucla.edu",
]

METHODS = ["GET", "POST", "HEAD", "GET", "GET", "GET"]

RESOURCES = [
    "/images/NASA-logosmall.gif", "/shuttle/countdown/",
    "/shuttle/missions/sts-73/mission-sts-73.html",
    "/images/KSC-logosmall.gif", "/images/launch-logo.gif",
    "/history/apollo/apollo-13.html", "/shuttle/missions/missions.html",
    "/images/ksclogo-medium.gif", "/facts/about_ksc.html",
    "/htbin/cdt_clock.pl", "/shuttle/countdown/count.gif",
    "/images/NASA-logosmall.gif", "/images/ksclogosmall.gif",
    "/cgi-bin/imagemap/countdown", "/shuttle/countdown/countdown.html",
    "/images/launchmedium.gif", "/elv/elvpage.htm",
    "/history/history.html", "/images/KSC-logosmall.gif",
    "/shuttle/resources/orbiters/discovery.html",
    "/software/winvn/winvn.html", "/news/", "/facts/facts.html",
    "/statistics/statistics.html", "/shuttle/missions/sts-69/",
    "/invalid/path/that/does/not/exist", "/badrequest",
    "/shuttle/countdown/", "/history/apollo/", "/images/",
]

PROTOCOLS = ["HTTP/1.0", "HTTP/1.0", "HTTP/1.0", "HTTP/1.1"]

STATUS_CODES = [
    200, 200, 200, 200, 200, 200, 200,
    304, 304, 302, 301, 404, 404, 403,
    500, 400, 401, 500, 503,
]

BYTES_BY_STATUS = {
    200: (500, 50000),
    304: (0, 0),
    302: (0, 500),
    301: (0, 500),
    404: (200, 1000),
    403: (200, 800),
    400: (100, 500),
    401: (200, 600),
    500: (100, 300),
    503: (100, 300),
}

MONTHS = ["Jul", "Aug"]
DAYS_PER_MONTH = {"Jul": 31, "Aug": 31}


def random_timestamp(month="Jul"):
    day = random.randint(1, DAYS_PER_MONTH[month])
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return f"[{day:02d}/{month}/1995:{hour:02d}:{minute:02d}:{second:02d} -0400]"


def random_bytes(status):
    lo, hi = BYTES_BY_STATUS.get(status, (100, 10000))
    if lo == hi == 0:
        return "-"
    return str(random.randint(lo, hi))


def generate_logs(n=50000, output_path="NASA_sample.log"):
    lines = []
    malformed_count = int(n * 0.005)  # 0.5% malformed
    for i in range(n):
        host = random.choice(HOSTS)
        ts = random_timestamp(random.choice(MONTHS))
        method = random.choice(METHODS)
        resource = random.choice(RESOURCES)
        protocol = random.choice(PROTOCOLS)
        status = random.choice(STATUS_CODES)
        byt = random_bytes(status)
        line = f'{host} - - {ts} "{method} {resource} {protocol}" {status} {byt}'
        lines.append(line)

    # inject malformed lines
    for _ in range(malformed_count):
        idx = random.randint(0, len(lines) - 1)
        lines[idx] = "MALFORMED LINE --- garbage data !!!"

    with open(output_path, "w") as f:
        f.write("\n".join(lines) + "\n")

    print(f"Generated {n} log lines ({malformed_count} malformed) -> {output_path}")
    return output_path


if __name__ == "__main__":
    out = os.path.join(os.path.dirname(__file__), "NASA_sample.log")
    generate_logs(50000, out)
