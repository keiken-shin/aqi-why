"""Fetch open data for Delhi air around Diwali. Write data/*.js for index.html.

Sources (all free, no key):
  fires     NASA FIRMS, VIIRS S-NPP 375 m yearly country archive
  meters    2025: OpenAQ public S3 archive (CPCB / DPCC / IMD ground stations)
            2024: OpenCity copy of the CPCB repository (OpenAQ has no Delhi CPCB data for 2019-2024)
            2022, 2023: US Embassy meter only, from the OpenAQ archive
  wind      Open-Meteo archive API (ERA5 reanalysis)
  pm model  Open-Meteo air quality API (CAMS global model), for the heatmap

Python standard library only. Downloads are cached in raw/.
Run:  python fetch_data.py
"""
import csv, gzip, io, json, math, os, statistics, struct, sys, time, urllib.error, urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.abspath(__file__))
RAW, OUT = os.path.join(ROOT, "raw"), os.path.join(ROOT, "data")
IST = timezone(timedelta(hours=5, minutes=30))
DIWALI = {2022: "2022-10-24", 2023: "2023-11-12", 2024: "2024-10-31", 2025: "2025-10-20", 2026: "2026-11-08"}
# Meter source per year. openaq = many CPCB/DPCC/IMD meters. opencity = CPCB repository copy, no values of 1000+.
# embassy = one meter (US Embassy, via OpenAQ): the only open hourly PM2.5 we found for these years.
# The OpenAQ archive gets each day about 3 days late.
SOURCE = {2022: "embassy", 2023: "embassy", 2024: "opencity", 2025: "openaq", 2026: "openaq"}
# Fire satellite per year (default S-NPP, from the FIRMS yearly archive). S-NPP sent no data after 2 Nov 2026,
# so 2026 uses NOAA-20 (same VIIRS instrument, pass about 50 minutes apart) from NASA GIBS vector tiles.
FIRE_SAT = {2026: "NOAA-20"}
DAYS = 61  # 1 Oct .. 30 Nov
N = DAYS * 24
PART = ""  # cache name suffix for a season that is not over


def set_window(year):
    """For a season in progress, use only the days that are over. Sets DAYS, N, PART."""
    global DAYS, N, PART
    DAYS = max(1, min(61, (datetime.now(IST).date() - t0(year).date()).days))
    N, PART = DAYS * 24, "" if DAYS == 61 else f"_d{DAYS}"
DELHI = (28.63, 77.22)
FIRE_BOX = (26.0, 33.5, 71.5, 80.5)  # lat0, lat1, lon0, lon1
UPWIND_BOX = (29.0, 32.6, 73.0, 77.2)  # Punjab (both sides of the border) + Haryana
WIND = dict(lat0=26.0, lon0=72.0, d=1.0, ny=8, nx=9)  # 100 m wind grid
PMG = dict(lat0=26.5, lon0=72.5, d=0.5, ny=13, nx=15)  # CAMS PM2.5 grid
CELL = 0.25  # fire lookup cell, degrees


def t0(year):
    return datetime(year, 10, 1, tzinfo=IST)


def hour_index(year, dt):
    return int((dt - t0(year)).total_seconds() // 3600)


def aqi_pm25(c):
    """Indian national AQI sub-index for a 24 h mean PM2.5 (CPCB breakpoints)."""
    bp = [(0, 30, 0, 50), (30, 60, 50, 100), (60, 90, 100, 200), (90, 120, 200, 300), (120, 250, 300, 400)]
    for lo, hi, a, b in bp:
        if c <= hi:
            return round(a + (c - lo) * (b - a) / (hi - lo))
    return min(500, round(400 + (c - 250) * 100 / 130))


def uv(speed, direction):
    """Wind 'from' direction in degrees to east (u) and north (v) parts."""
    r = math.radians(direction)
    return -speed * math.sin(r), -speed * math.cos(r)


def selfcheck():
    assert aqi_pm25(30) == 50 and aqi_pm25(60) == 100 and aqi_pm25(250) == 400 and aqi_pm25(900) == 500
    u, v = uv(10, 270)  # wind from the west blows to the east
    assert round(u) == 10 and round(v) == 0
    u, v = uv(10, 0)  # wind from the north blows to the south
    assert round(u) == 0 and round(v) == -10
    assert hour_index(2024, datetime(2024, 10, 2, 1, tzinfo=IST)) == 25
    assert list(pb(b"\x08\x96\x01\x12\x02hi")) == [(1, 0, 150), (2, 2, b"hi")]  # protobuf: varint 150, then a string


# ---------- download helpers ----------

def http(url, tries=10):
    err = None
    for k in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "aqi-why/1.0"})
            with urllib.request.urlopen(req, timeout=180) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code in (403, 404):
                return None
            err = e
            if e.code == 429:
                print("  rate limit, wait", 65 * (k + 1), "s", flush=True)
                time.sleep(65 * (k + 1))
                continue
        except Exception as e:
            err = e
        time.sleep(2 + 3 * k)
    raise RuntimeError(f"{url[:160]}: {err}")


def cached(name, make):
    path = os.path.join(RAW, name)
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        data = make()
        with open(path + ".tmp", "wb") as f:
            f.write(data)
        os.replace(path + ".tmp", path)
    return path


# ---------- fires (NASA FIRMS) ----------

def fires(year, product="viirs-snpp", veg_only=True):
    """Fire detections from the FIRMS yearly country archive. product viirs-jpss1 = NOAA-20."""
    out = []
    la0, la1, lo0, lo1 = FIRE_BOX
    for country in ("India", "Pakistan"):
        url = f"https://firms.modaps.eosdis.nasa.gov/data/country/{product}/{year}/{product}_{year}_{country}.csv"
        path = cached(f"firms_{year}_{country}.csv" if product == "viirs-snpp" else f"firms_{product}_{year}_{country}.csv", lambda: http(url))
        with open(path, newline="") as f:
            rd = csv.DictReader(f)
            assert {"latitude", "longitude", "acq_date", "acq_time", "confidence", "frp", "type"} <= set(rd.fieldnames), rd.fieldnames
            for r in rd:
                lat, lon = float(r["latitude"]), float(r["longitude"])
                if not (la0 <= lat <= la1 and lo0 <= lon <= lo1):
                    continue
                if (veg_only and r["type"] != "0") or r["confidence"] == "l":  # keep vegetation fires, drop low confidence
                    continue
                dt = datetime.strptime(r["acq_date"] + r["acq_time"].zfill(4), "%Y-%m-%d%H%M").replace(tzinfo=timezone.utc)
                t = hour_index(year, dt)
                if 0 <= t < N:
                    out.append((t, round(lat, 2), round(lon, 2), round(float(r["frp"]), 1)))
    out.sort()
    return out


def varint(b, i):
    v = s = 0
    while True:
        c = b[i]
        i += 1
        v |= (c & 127) << s
        s += 7
        if c < 128:
            return v, i


def pb(b):
    """Fields of one protobuf message: (field number, wire type, value)."""
    i = 0
    while i < len(b):
        k, i = varint(b, i)
        w = k & 7
        if w == 0:
            v, i = varint(b, i)
        elif w == 2:
            n, i = varint(b, i)
            v, i = b[i:i + n], i + n
        else:
            n = 4 if w == 5 else 8
            v, i = b[i:i + n], i + n
        yield k >> 3, w, v


def mvt_value(b):
    for f, _, v in pb(b):
        return v.decode() if f == 1 else struct.unpack("<f", v)[0] if f == 2 else struct.unpack("<d", v)[0] if f == 3 else v


def mvt_features(tile):
    """Properties of each feature in a Mapbox vector tile. We do not need the shapes: lat and lon are properties."""
    if tile[:2] == b"\x1f\x8b":
        tile = gzip.decompress(tile)
    for f, _, layer in pb(tile):
        if f != 3:
            continue
        keys, vals, feats = [], [], []
        for f2, _, v in pb(layer):
            (feats if f2 == 2 else keys if f2 == 3 else vals if f2 == 4 else []).append(v)
        keys, vals = [k.decode() for k in keys], [mvt_value(v) for v in vals]
        for ft in feats:
            for f3, _, v in pb(ft):
                if f3 == 2:
                    tags, i = [], 0
                    while i < len(v):
                        t, i = varint(v, i)
                        tags.append(t)
                    yield {keys[tags[j]]: vals[tags[j + 1]] for j in range(0, len(tags), 2)}


def gibs_fires(year):
    """NOAA-20 VIIRS fire detections from NASA GIBS vector tiles (near real time, no key).

    Zoom 5 tiles are 9 degrees wide. Rows 6-7 and columns 27-28 cover FIRE_BOX. A tile holds every detection
    of that UTC day (checked against zoom 7). There is no fire type here, so factories are not filtered out.
    """
    la0, la1, lo0, lo1 = FIRE_BOX

    def day(date):
        def make():
            rows = {}
            for r in (6, 7):
                for c in (27, 28):
                    tile = http(f"https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/VIIRS_NOAA20_Thermal_Anomalies_375m_All/default/{date}/500m/5/{r}/{c}.mvt")
                    for p in mvt_features(tile or b""):
                        rows[p["UID"]] = [p["LATITUDE"], p["LONGITUDE"], p["ACQ_DATE"], p["ACQ_TIME"], p["CONFIDENCE"], p["FRP"]]
            return json.dumps(list(rows.values())).encode()

        if (datetime.now(timezone.utc).date() - date).days < 3:  # fresh tiles still get more detections: no cache
            return json.loads(make())
        return json.load(open(cached(f"gibs/N20_{date}.json", make)))

    dates = [(t0(year) + timedelta(days=d)).date() for d in range(-1, DAYS + 1)]
    out = []
    with ThreadPoolExecutor(4) as ex:
        for rows in ex.map(day, dates):
            for lat, lon, d, hm, conf, frp in rows:
                t = hour_index(year, datetime.strptime(d + hm, "%Y-%m-%d%H:%M").replace(tzinfo=timezone.utc))
                if 0 <= t < N and la0 <= lat <= la1 and lo0 <= lon <= lo1 and conf != "low":
                    out.append((t, round(lat, 2), round(lon, 2), round(frp, 1)))
    out.sort()
    return out


# ---------- ground meters (OpenAQ archive) ----------

def hourly(buckets):
    return [round(sum(b) / len(b)) if b else None for b in buckets]


def openaq_series(sid, year):
    """Hourly PM2.5 list (None = no data) for one OpenAQ location.

    Time stamps: files with 15 minute rows (CPCB feed, 2025 on) use the CPCB label, checked against
    the CPCB repository copy: no shift. Files with one row per hour stamp the END of the hour: shift 1 h.
    """
    days = [t0(year) + timedelta(days=d) for d in range(-1, DAYS + 1)]

    def make():
        buckets = [[] for _ in range(N)]

        def one(day):
            u = (f"https://openaq-data-archive.s3.amazonaws.com/records/csv.gz/locationid={sid}/"
                 f"year={day.year}/month={day.month:02d}/location-{sid}-{day:%Y%m%d}.csv.gz")
            raw = http(u)
            return gzip.decompress(raw).decode("utf-8", "replace") if raw else ""

        with ThreadPoolExecutor(5) as ex:  # more threads than this: the connection gets reset
            for text in ex.map(one, days):
                rows = [r for r in csv.DictReader(io.StringIO(text)) if r["parameter"] == "pm25"]
                shift = 0 if len(rows) > 30 else 1
                for r in rows:
                    v = float(r["value"])
                    t = hour_index(year, datetime.fromisoformat(r["datetime"])) - shift
                    if 0 <= t < N and 0 < v < 10000:
                        buckets[t].append(v)
        return json.dumps(hourly(buckets)).encode()

    return json.load(open(cached(f"openaq/{sid}_{year}{PART}.json", make)))


def opencity_series(st, year):
    """Hourly PM2.5 from the OpenCity copy of the CPCB repository (15 minute rows, IST clock).

    This copy has NO readings of 1000 or more: they were cut out. A short gap with 700+ on both
    sides is one of these cuts, so we fill it with 1000. Read such hours as "1000 or more".
    """
    slug = st["opencity"].rsplit("/", 1)[1][:-4]

    def make():
        text = http(st["opencity"]).decode("utf-8", "replace")
        out = {}
        for y in (2024, 2025):
            buckets = [[] for _ in range(N)]
            for r in csv.reader(io.StringIO(text)):
                if len(r) > 5 and r[5] and r[4][:4] == str(y) and r[4][5:7] in ("10", "11"):
                    t = hour_index(y, datetime.fromisoformat(r[4][:13] + ":00").replace(tzinfo=IST))
                    v = float(r[5])
                    if 0 <= t < N and v > 0:
                        buckets[t].append(v)
            vals = hourly(buckets)
            t = 1
            while t < N:
                if vals[t] is None and vals[t - 1] is not None and vals[t - 1] >= 700:
                    e = t
                    while e < N and vals[e] is None:
                        e += 1
                    if e < N and e - t <= 6 and vals[e] >= 700:
                        vals[t:e] = [1000] * (e - t)
                    t = e
                t += 1
            out[str(y)] = vals
        return json.dumps(out).encode()

    return json.load(open(cached(f"opencity/{slug}.json", make)))[str(year)]


def meters(year, stations):
    """Pick the meter source for the year. Returns (stations, series), only meters with 50%+ of hours."""
    src = SOURCE[year]
    if src == "opencity":
        sts = [s for s in stations if s.get("opencity")]
        with ThreadPoolExecutor(4) as ex:
            series = list(ex.map(lambda s: opencity_series(s, year), sts))
    else:
        sts = [s for s in stations if s.get("openaq") and (src == "openaq" or s["openaq"] == 8118)]
        with ThreadPoolExecutor(3) as ex:
            series = list(ex.map(lambda s: openaq_series(s["openaq"], year), sts))
    keep = [i for i, s in enumerate(series) if sum(v is not None for v in s) >= N * 0.5]
    return [{"name": sts[i]["name"], "lat": sts[i]["lat"], "lon": sts[i]["lon"]} for i in keep], [series[i] for i in keep]


# ---------- Open-Meteo (wind and CAMS model) ----------

def grid_points(g):
    return [(g["lat0"] + iy * g["d"], g["lon0"] + ix * g["d"]) for iy in range(g["ny"]) for ix in range(g["nx"])]


def open_meteo(base, pts, hourly, year, tag):
    """Hourly series for many points. Returns {var: [per point list of N values]}."""
    res = {v: [] for v in hourly}
    end = (t0(year) + timedelta(days=DAYS - 1)).date()
    for c in range(0, len(pts), 100):
        chunk = pts[c:c + 100]
        url = (f"{base}?latitude={','.join(f'{p[0]:.2f}' for p in chunk)}&longitude={','.join(f'{p[1]:.2f}' for p in chunk)}"
               f"&start_date={year}-10-01&end_date={end}&hourly={','.join(hourly)}&timezone=Asia%2FKolkata")

        def make():
            data = http(url)
            if len(chunk) > 20:
                time.sleep(61)  # free tier: 600 weighted calls per minute
            return data

        js = json.load(open(cached(f"om_{tag}_{year}{PART}_{c}.json", make), encoding="utf-8"))
        js = js if isinstance(js, list) else [js]
        assert len(js) == len(chunk), (len(js), len(chunk))
        for loc in js:
            assert len(loc["hourly"]["time"]) == N and loc["hourly"]["time"][0] == f"{year}-10-01T00:00"
            for v in hourly:
                res[v].append(loc["hourly"][v])
    return res


# ---------- analysis ----------

def bilinear(g, flat, t, lat, lon):
    fy, fx = (lat - g["lat0"]) / g["d"], (lon - g["lon0"]) / g["d"]
    if not (0 <= fy <= g["ny"] - 1 and 0 <= fx <= g["nx"] - 1):
        return None
    y, x = min(int(fy), g["ny"] - 2), min(int(fx), g["nx"] - 2)
    ay, ax, b = fy - y, fx - x, t * g["ny"] * g["nx"]
    f = lambda j, i: flat[b + j * g["nx"] + i]
    return (f(y, x) * (1 - ax) + f(y, x + 1) * ax) * (1 - ay) + (f(y + 1, x) * (1 - ax) + f(y + 1, x + 1) * ax) * ay


def back_path(wu, wv, t, hours=48):
    """Where the air that is over Delhi at hour t was before. Uses 100 m wind. Returns [(hour, lat, lon)]."""
    lat, lon = DELHI
    pts = [(t, lat, lon)]
    for k in range(hours):
        if t - k < 0:
            break
        u, v = bilinear(WIND, wu, t - k, lat, lon), bilinear(WIND, wv, t - k, lat, lon)
        if u is None:
            break
        lat -= v / 111.0
        lon -= u / (111.0 * math.cos(math.radians(lat)))
        pts.append((t - k - 1, lat, lon))
    return pts


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def r1(x):
    return None if x is None else round(x, 1)


def spearman(a, b):
    pairs = [(x, y) for x, y in zip(a, b) if x is not None and y is not None]
    if len(pairs) < 10:
        return None
    return round(statistics.correlation([p[0] for p in pairs], [p[1] for p in pairs], method="ranked"), 2)


def build(year, stations):
    set_window(year)
    dd = (datetime.fromisoformat(DIWALI[year]).replace(tzinfo=IST) - t0(year)).days
    assert dd < DAYS, f"{year}: Diwali is not in the data yet"
    print(year, "fires", flush=True)
    fr = gibs_fires(year) if FIRE_SAT.get(year) == "NOAA-20" else fires(year)
    print(year, "meters", flush=True)
    sts, series = meters(year, stations)
    print(year, len(sts), "meters. wind + model (slow: rate limit pauses)", flush=True)
    A = "https://archive-api.open-meteo.com/v1/archive"
    w = open_meteo(A, grid_points(WIND), ["wind_speed_100m", "wind_direction_100m"], year, "wind")
    has_model = year >= 2022  # the CAMS archive at Open-Meteo starts in August 2022
    pmg = open_meteo("https://air-quality-api.open-meteo.com/v1/air-quality", grid_points(PMG), ["pm2_5"], year, "pm") if has_model else None
    dl = open_meteo(A, [DELHI], ["wind_speed_10m", "wind_direction_10m", "boundary_layer_height", "precipitation"], year, "delhi")

    np_ = WIND["ny"] * WIND["nx"]
    wu, wv = [0] * (N * np_), [0] * (N * np_)
    for p in range(np_):
        for t in range(N):
            s, d = w["wind_speed_100m"][p][t], w["wind_direction_100m"][p][t]
            u, v = uv(s or 0, d or 0)
            wu[t * np_ + p], wv[t * np_ + p] = round(u), round(v)
    npm = PMG["ny"] * PMG["nx"]
    pg = [0] * (N * npm) if has_model else None
    for p in range(npm if has_model else 0):
        for t in range(N):
            pg[t * npm + p] = round(pmg["pm2_5"][p][t] or 0)

    iso = lambda t: (t0(year) + timedelta(hours=t)).strftime("%Y-%m-%dT%H:%M")
    dayiso = lambda d: (t0(year) + timedelta(days=d)).strftime("%Y-%m-%d")
    # faulty readings: one meter at 8x the city median or more (and above 500). Drop them, but keep a list to show.
    glitches = []
    for t in range(N):
        vs = sorted(s[t] for s in series if s[t] is not None)
        if len(vs) < 10:
            continue
        med = vs[len(vs) // 2]
        for i, s in enumerate(series):
            if s[t] is not None and s[t] > 500 and s[t] > 8 * med:
                glitches.append({"time": iso(t), "name": sts[i]["name"], "value": s[t], "cityMedian": med})
                s[t] = None

    # city numbers per hour, from the ground meters
    need = min(3, len(sts))
    pm, pmax, pmax_i, pn = [], [], [], []
    for t in range(N):
        vs = [(s[t], i) for i, s in enumerate(series) if s[t] is not None]
        pn.append(len(vs))
        if len(vs) >= need:
            pm.append(round(sum(v for v, _ in vs) / len(vs)))
            mv, mi = max(vs)
            pmax.append(mv), pmax_i.append(mi)
        else:
            pm.append(None), pmax.append(None), pmax_i.append(None)
    pm24, aqi = [], []
    for t in range(N):
        win = [x for x in pm[max(0, t - 23):t + 1] if x is not None]
        m = sum(win) / len(win) if len(win) >= 16 else None
        pm24.append(None if m is None else round(m))
        aqi.append(None if m is None else aqi_pm25(m))

    # daily numbers
    day = lambda arr, d, need=18: (mean(arr[d * 24:d * 24 + 24]) if sum(x is not None for x in arr[d * 24:d * 24 + 24]) >= need else None)
    dpm = [day(pm, d) for d in range(DAYS)]
    dws = [day(dl["wind_speed_10m"][0], d) for d in range(DAYS)]
    dblh = [day(dl["boundary_layer_height"][0], d) for d in range(DAYS)]
    drain = [sum(x or 0 for x in dl["precipitation"][0][d * 24:d * 24 + 24]) for d in range(DAYS)]
    fday, fup, cells = [0] * DAYS, [0] * DAYS, {}
    for t, lat, lon, frp in fr:
        d = t // 24
        fday[d] += 1
        if UPWIND_BOX[0] <= lat <= UPWIND_BOX[1] and UPWIND_BOX[2] <= lon <= UPWIND_BOX[3]:
            fup[d] += 1
        k = (d, math.floor(lat / CELL), math.floor(lon / CELL))
        cells[k] = cells.get(k, 0) + 1
    # fires on the path of Delhi's air: for 4 arrival times per day, walk the air back 48 h,
    # and count that day's fire detections within about 40 km of each step.
    onpath = []
    for d in range(DAYS):
        tot = []
        for h in (0, 6, 12, 18):
            seen = set()
            for tt, lat, lon in back_path(wu, wv, d * 24 + h):
                if tt < 0:
                    continue
                cy, cx = math.floor(lat / CELL), math.floor(lon / CELL)
                seen |= {(tt // 24, cy + a, cx + b) for a in (-1, 0, 1) for b in (-1, 0, 1)}
            tot.append(sum(cells.get(k, 0) for k in seen))
        onpath.append(round(sum(tot) / len(tot)))

    # Diwali night. g() = safe read: a season in progress can stop before these hours and days.
    g = lambda a, i: a[i] if 0 <= i < len(a) else None
    T = dd * 24
    night = range(T + 20, T + 29)  # 20:00 .. 04:00
    prev = [t - 24 * k for k in (1, 2, 3) for t in night]
    win = [t for t in range(T + 18, min(N, T + 33)) if pm[t] is not None]
    tp = max(win, key=lambda t: pm[t]) if win else None
    before24 = mean(pm[T - 6:T + 18])  # the 24 h that end at 18:00 on Diwali
    back = None
    if tp is not None and before24 is not None:
        back = next((t - tp for t in range(tp, min(N, tp + 120)) if pm[t] is not None and pm[t] <= before24), None)
    ts = max((t for t in range(T + 18, min(N, T + 33)) if pmax[t] is not None), key=lambda t: pmax[t], default=None)
    tsea = max((t for t in range(N) if pmax[t] is not None), key=lambda t: pmax[t])
    ok = [d for d in range(DAYS) if dpm[d] is not None]
    worst = max(ok, key=lambda d: dpm[d])
    top = sorted(ok, key=lambda d: -dpm[d])[:10]
    severe = [d for d in ok if dpm[d] > 250]
    fpk = max(range(DAYS), key=lambda d: fday[d])
    lag1 = lambda a: [None] + a[:-1]
    notdiw = [None if d in (dd, dd + 1) else dpm[d] for d in range(DAYS)]  # drop the cracker days for the fire test
    stats = {
        "glitches": glitches, "seasonMean": r1(mean(dpm)), "seasonMeanNoDiwali": r1(mean(notdiw)),
        "nightGapHours": sum(1 for t in night if g(pm, t) is None),  # hours of Diwali night with no city value
        "diwali": DIWALI[year], "diwaliDay": dd, "stations": len(sts), "source": SOURCE[year],
        "fireSat": FIRE_SAT.get(year, "S-NPP"), "days": DAYS, "lastDay": dayiso(DAYS - 1), "lastMeterDay": dayiso(max(ok)),
        "nightMean": r1(mean(g(pm, t) for t in night)), "prevNightsMean": r1(mean(pm[t] for t in prev)),
        "peak": None if tp is None else pm[tp], "peakTime": None if tp is None else iso(tp),
        "peakSameHourPrev": None if tp is None else r1(mean(pm[tp - 24 * k] for k in (1, 2, 3))),
        "before24": r1(before24), "hoursBack": back,
        "stationPeak": None if ts is None else pmax[ts], "stationPeakTime": None if ts is None else iso(ts),
        "stationPeakName": None if ts is None else sts[pmax_i[ts]]["name"],
        "seasonStationPeak": pmax[tsea], "seasonStationPeakTime": iso(tsea), "seasonStationPeakName": sts[pmax_i[tsea]]["name"],
        "dayBefore": r1(dpm[dd - 1]), "diwaliDayMean": r1(dpm[dd]), "dayAfter": r1(g(dpm, dd + 1)), "day2After": r1(g(dpm, dd + 2)),
        "worstDay": dayiso(worst), "worstDayPm": r1(dpm[worst]), "worstDayFromDiwali": worst - dd,
        "severeDays": len(severe), "severeNearDiwali": sum(1 for d in severe if dd <= d <= dd + 2),
        "fireTotal": sum(fday), "firePeakDay": dayiso(fpk), "firePeakCount": fday[fpk], "firePeakFromDiwali": fpk - dd,
        "fireBeforeDiwaliShare": round(sum(fday[:dd]) / max(1, sum(fday)), 2),
        "fireDiwaliDay": fday[dd], "onPathDiwali": onpath[dd], "onPathDayAfter": g(onpath, dd + 1),
        "rainDiwaliWeek": r1(sum(drain[dd - 3:dd + 4])),
        "corr": {
            "pm_onPath": spearman(notdiw, onpath), "pm_upwindFiresLag1": spearman(notdiw, lag1(fup)),
            "pm_wind": spearman(notdiw, dws), "pm_blh": spearman(notdiw, dblh),
        },
        "topDays": [{"date": dayiso(d), "pm": round(dpm[d]), "fromDiwali": d - dd, "onPath": onpath[d], "upwindFiresPrev": fup[d - 1] if d else None,
                     "wind": r1(dws[d]), "blh": round(dblh[d]) if dblh[d] is not None else None} for d in top],
    }
    data = {
        "year": year, "diwali": DIWALI[year], "t0": f"{year}-10-01T00:00", "n": N,
        "stations": sts, "st": series, "pm": pm, "pmMax": pmax, "pmN": pn, "pm24": pm24, "aqi": aqi,
        "fires": [x for f in fr for x in (f[0], round(f[1] * 100), round(f[2] * 100), round(f[3]))],
        "fireDaily": fday, "fireUpwind": fup, "onPath": onpath, "dpm": [None if x is None else round(x) for x in dpm],
        "wind": dict(WIND, u=wu, v=wv), "pmGrid": dict(PMG, v=pg) if has_model else None,
        "delhi": {"ws": dl["wind_speed_10m"][0], "wd": dl["wind_direction_10m"][0],
                  "blh": dl["boundary_layer_height"][0], "rain": dl["precipitation"][0]},
        "stats": stats,
    }
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, f"y{year}.js"), "w", encoding="utf-8") as f:
        f.write(f"window.AQI_DATA=window.AQI_DATA||{{}};AQI_DATA[{year}]=" + json.dumps(data, separators=(",", ":")) + ";")
    return stats, {"dpm": dpm, "onpath": onpath, "dd": dd}


def main():
    selfcheck()
    stations = json.load(open(os.path.join(ROOT, "stations.json"), encoding="utf-8"))
    summary, pooled = {}, []
    for year in DIWALI:
        summary[year], extra = build(year, stations)
        if DAYS < 61:  # season in progress (other fire satellite too): keep it out of the all-years test
            continue
        pooled += [(extra["onpath"][d], extra["dpm"][d]) for d in range(DAYS)
                   if extra["dpm"][d] is not None and d not in (extra["dd"], extra["dd"] + 1)]
    # all years together, cracker days removed: PM2.5 on days with many / few fires on the air path
    pooled.sort()
    third = len(pooled) // 3
    summary["pooled"] = {
        "days": len(pooled),
        "pmLowFireThird": r1(mean(p[1] for p in pooled[:third])), "pmHighFireThird": r1(mean(p[1] for p in pooled[-third:])),
        "onPathLowMax": pooled[third - 1][0], "onPathHighMin": pooled[-third][0],
    }
    with open(os.path.join(OUT, "summary.js"), "w", encoding="utf-8") as f:
        f.write("window.AQI_SUMMARY=" + json.dumps(summary, indent=1) + ";")
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
