#!/usr/bin/env python3
# Plane Tracker
#
# first seen, last seen, flight #s, plane types
# CREATE INDEX icao_idx ON planes(icao);
# CREATE INDEX ptype_idx ON planes(ptype);
# CREATE INDEX icao_day_idx ON plane_days(icao);
# CREATE INDEX plane_day_idx ON plane_days(day);
#
from collections import defaultdict, OrderedDict
from operator import itemgetter
import time
import re
import sqlite3
import logging
import requests
import mpu
from sqlite3 import Error
from datetime import date, datetime, timedelta
import argparse
import configparser
import csv
import signal
import sys

config = dict()

save_cycle = 1
logger = None

alerted = dict()
ptype_alerted = dict()
pdict = dict()
cdict = defaultdict(int)
lookup = None
fa_lookup = dict()
sounds = False

# Create tables if they don't exist
sql_create_planes_table = """ CREATE TABLE IF NOT EXISTS planes (
                                    icao text PRIMARY KEY,
                                    ident text,
                                    ptype text,
                                    distance float,
                                    closest float,
                                    altitude float,
                                    lowest_altitude float,
                                    speed float,
                                    lowest_speed float,
                                    squawk text,
                                    heading float,
                                    firstseen timestamp,
                                    lastseen timestamp,
                                    registration text,
                                    country text,
                                    owner text,
                                    military text,
                                    day_count integer,
                                    category text
                                ); """

sql_create_plane_days_table = """ CREATE TABLE IF NOT EXISTS plane_days (
                                    icao text,
                                    day date,
                                    ident text,
                                    distance float,
                                    closest float,
                                    altitude float,
                                    lowest_altitude float,
                                    speed float,
                                    lowest_speed float,
                                    squawk text,
                                    heading float,
                                    firstseen timestamp,
                                    lastseen timestamp
                                ); """

sql_create_types_table = """ CREATE TABLE IF NOT EXISTS plane_types (
                                    ptype text PRIMARY KEY,
                                    last_icao text,
                                    firstseen timestamp,
                                    lastseen timestamp,
                                    count int,
                                    manufacturer text,
                                    model text
                                ); """


def setup_logger(logfile="ads-db.log", level=logging.INFO):

    logFormatter = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")
    logger = logging.getLogger()

    fileHandler = logging.FileHandler(logfile)
    fileHandler.setFormatter(logFormatter)
    logger.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    logger.addHandler(consoleHandler)
    logger.setLevel(logging.INFO)

    return logger


def create_connection(db_file):
    """create a database connection to a SQLite database"""
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
        return None
    cur = conn.cursor()
    commands = [
        "pragma journal_mode = WAL;",
        "pragma synchronous = normal;",
        "pragma temp_store = memory;",
        "pragma mmap_size = 30000000000;",
        "pragma optimize;",
    ]
    for cmd in commands:
        res = cur.execute(cmd)
    return conn


def create_table(conn, create_table_sql):
    """create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        print("Setup:", create_table_sql)
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def play_sound(filename):
    from playsound import playsound

    global sounds

    if sounds:
        playsound(filename)


def update_plane(
    icao,
    ident,
    squawk,
    ptype,
    distance,
    altitude,
    heading,
    speed,
    reg,
    country,
    owner,
    military,
    category,
    site,
):

    now = datetime.now()
    global sounds

    alert_types = ["B788", "B789", "B78X", "B744", "A388", "GLF6"]

    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM planes WHERE icao=?", (icao,))
        rows = cur.fetchall()
    except sqlite3.OperationalError as e:
        logger.warning(f"New Database: Trying to create DB: {e}")

        create_table(conn, sql_create_planes_table)
        create_table(conn, sql_create_types_table)
        create_table(conn, sql_create_plane_days_table)
        # Setup indexes and initial DB
        commands = [
            "pragma journal_mode = WAL;",
            "pragma synchronous = normal;",
            "pragma temp_store = memory;",
            "pragma mmap_size = 30000000000;",
            "CREATE INDEX icao_day_idx ON plane_days(icao);",
            "CREATE INDEX plane_day_idx ON plane_days(day);",
            "CREATE INDEX icao_idx ON planes(icao);",
            "CREATE INDEX ptype_idx ON planes(ptype);",
            "pragma optimize;",
        ]
        for cmd in commands:
            print("DB Setup:", cmd)
            res = cur.execute(cmd)

        return

    # print(f'ic:{icao}, ident:{ident}, sq:{squawk}, pt:{ptype}, dist:{distance}, alt:{altitude}, head:{heading}, spd:{speed}')
    if not rows:
        logger.info(
            f"New Plane: {icao} t:{ptype} id:{ident} alt:{altitude} site:{site}"
        )
        if ptype in alert_types and sounds:
            play_sound("/Users/yantisj/dev/ads-db/sounds/ding-low.mp3")
        sql = """INSERT INTO planes(icao,ident,ptype,speed,altitude,lowest_altitude,distance,closest,heading,firstseen,lastseen,registration,country,owner,military,day_count,category)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) """
        cur.execute(
            sql,
            (
                icao,
                ident,
                ptype,
                speed,
                altitude,
                altitude,
                distance,
                distance,
                heading,
                now,
                now,
                reg,
                country,
                owner,
                military,
                1,
                category,
            ),
        )
    else:
        try:
            day_count = get_day_count(icao)
            # print('updating icao', icao, day_count)
            row = rows[0]
            low_dist = row[4]
            if not low_dist and distance:
                low_dist = distance
            elif distance and distance < low_dist:
                # print('New Low Distance', icao, distance)
                low_dist = distance
            low_alt = row[6]
            if not low_alt and altitude:
                low_alt = altitude
            elif altitude and altitude < low_alt:
                # print('New Low Altitude', icao, altitude)
                low_alt = altitude
            nident = row[1]
            if ident:
                nident = ident
            nsquawk = row[9]
            if squawk:
                nsquawk = squawk
            sql = """UPDATE planes SET ident = ?, ptype = ?, squawk = ?, speed = ?, altitude = ?, lowest_altitude = ?, distance = ?, closest = ?, heading = ?, lastseen = ?, registration = ?, country = ?, owner = ?, military = ?, day_count = ?, category = ?
                WHERE icao = ? """
            cur.execute(
                sql,
                (
                    nident,
                    ptype,
                    nsquawk,
                    speed,
                    altitude,
                    low_alt,
                    distance,
                    low_dist,
                    heading,
                    now,
                    reg,
                    country,
                    owner,
                    military,
                    day_count,
                    category,
                    icao,
                ),
            )
        except TypeError as e:
            logger.critical(
                f"Error updating plane: {icao} t:{ptype} id:{ident} alt:{altitude}: {e}"
            )
            pass


def update_plane_day(
    icao, ident, squawk, ptype, distance, altitude, heading, speed, reg
):

    now = datetime.now()
    now_date = datetime.now().date()

    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM plane_days WHERE icao=? and day = ?",
            (
                icao,
                now_date,
            ),
        )
        rows = cur.fetchall()
    except sqlite3.OperationalError as e:
        logger.warning(f"DB Load Error: {e}")
        return

    # print(f'ic:{icao}, ident:{ident}, sq:{squawk}, pt:{ptype}, dist:{distance}, alt:{altitude}, head:{heading}, spd:{speed}')
    if not rows:
        logger.debug(
            f"new plane day: {now_date} {icao} t:{ptype} id:{ident} alt:{altitude}"
        )
        sql = """INSERT INTO plane_days(icao,day,ident,speed,altitude,lowest_altitude,distance,closest,heading,firstseen,lastseen)
              VALUES(?,?,?,?,?,?,?,?,?,?,?) """
        cur.execute(
            sql,
            (
                icao,
                now_date,
                ident,
                speed,
                altitude,
                altitude,
                distance,
                distance,
                heading,
                now,
                now,
            ),
        )
    else:
        try:
            row = rows[0]
            low_dist = row[4]
            if not low_dist and distance:
                low_dist = distance
            elif distance and distance < low_dist:
                # print('New Low Distance', icao, distance)
                low_dist = distance
            low_alt = row[6]
            if not low_alt and altitude:
                low_alt = altitude
            elif altitude and altitude < low_alt:
                # print('New Low Altitude', icao, altitude)
                low_alt = altitude
            nident = row[2]
            if ident:
                nident = ident
            nsquawk = row[9]
            if squawk:
                nsquawk = squawk
            sql = """UPDATE plane_days SET ident = ?, squawk =?, speed = ?, altitude = ?, lowest_altitude = ?, distance = ?, closest = ?, heading = ?, lastseen = ?
                WHERE icao = ? AND day = ?"""
            cur.execute(
                sql,
                (
                    nident,
                    nsquawk,
                    speed,
                    altitude,
                    low_alt,
                    distance,
                    low_dist,
                    heading,
                    now,
                    icao,
                    now_date,
                ),
            )
        except TypeError as e:
            logger.critical(
                f"Error updating plane: {icao} t:{ptype} id:{ident} alt:{altitude}: {e}"
            )
            pass


def update_ptype(ptype, icao, mfr, model):
    now = datetime.now()
    cur = conn.cursor()
    cur.execute("SELECT * FROM plane_types WHERE ptype=?", (ptype,))
    rows = cur.fetchall()
    new = False
    if not rows:
        new = True
        logger.info(f"!! New Type of Plane !!: {icao} t:{ptype}")
        sql = """INSERT INTO plane_types(ptype,last_icao,firstseen,lastseen,count,manufacturer,model)
              VALUES(?,?,?,?,?,?,?) """
        cur.execute(sql, (ptype, icao, now, now, 1, mfr, model))
    else:
        row = rows[0]
        logger.debug(f"Updating ptype: {ptype}")
        cur.execute("SELECT * FROM planes WHERE ptype = ?", (ptype,))
        rows = cur.fetchall()
        pcount = 0
        for p in rows:
            pcount += 1
        nmfr = row[5]
        if mfr:
            nmfr = mfr

        # Full data update
        if model and nmfr:
            sql = """UPDATE plane_types SET last_icao = ?, lastseen = ?, count = ?, manufacturer = ?, model = ?
                WHERE ptype = ? """
            cur.execute(sql, (icao, now, pcount, nmfr, model, ptype))
        # Partial data, don't update type
        else:
            sql = """UPDATE plane_types SET last_icao = ?, lastseen = ?, count = ?
                WHERE ptype = ? """
            cur.execute(sql, (icao, now, pcount, ptype))

    return new


def get_day_count(icao):

    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM plane_days WHERE icao = ?", (icao,))
    (count,) = cur.fetchone()
    return count


def lookup_ptype(
    ptype, count=0, cat_min=0, low_alt=0, no_a0=False, military=True, hours=0
):

    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM planes WHERE ptype LIKE ? ORDER BY lastseen DESC", (ptype,)
    )
    rows = cur.fetchall()
    new_rows = list()

    hours_ago = None
    if hours:
        hours_ago = datetime.now() - timedelta(hours=hours)

    for row in rows:
        add = True
        if count and not row[17]:
            continue
        if count and row[17] and row[17] < count:
            # print('too low', row, row[17])
            continue
        if hours_ago and row[12] < hours_ago:
            continue
        if cat_min:
            if row[18]:
                cat_re = re.search(r"A(\d)", row[18])
                if cat_re:
                    cat = int(cat_re.group(1))
                    if cat and cat < cat_min:
                        continue
                    elif not cat and no_a0:
                        continue
            else:
                continue
        if low_alt:
            if not row[6]:
                continue
            if row[6] > low_alt:
                continue
        if military:
            if row[16] != "M":
                continue
        if add:
            new_rows.append(row)
    total = print_planes(new_rows)

    print("\nTotal:", total)


def print_planes(rows):

    total = 0
    print("")
    print(
        "IACO   TYPE  REG         IDENT      CAT M CT  DST MIN  ALT     LOW     COUNTRY     OWNER                FIRST                 LAST"
    )
    print(
        "----   ----  ----------  ---------  --- - --  --- ---  -----   -----   ----------  -------------------- -------------------   -------------------"
    )
    for row in rows:
        total += 1
        altitude = 0
        alt_low = 0
        distance = 0
        closest = 0
        country = ""
        owner = ""
        reg = ""
        mlt = ""
        day_count = 1
        cat = ""
        first = str(row[11]).split(".")[0]
        last = str(row[12]).split(".")[0]

        if row[3]:
            distance = int(row[3])
        if row[4]:
            closest = int(row[4])
        if row[5]:
            altitude = int(row[5])
        if row[6]:
            alt_low = int(row[6])
        if row[13]:
            reg = row[13]
        if row[14]:
            country = row[14][:10]
        if row[15]:
            owner = row[15][:20]
        if row[17]:
            day_count = row[17]
        if row[18]:
            cat = row[18]
        if row[16]:
            mlt = row[16]
        print(
            f"{row[0]:<5} {row[2]:<4}  {reg:11} {row[1]:<10} {cat:<3} {mlt:<1} {day_count:<3} {distance:<3} {closest:<4} {altitude:<7} {alt_low:<7} {country:<10}  {owner:<20} {first:<10}   {last:<10}"
        )

    return total


def print_plane_days(rows):

    total = 0
    print("")
    print("IACO   IDENT        DST MIN  ALT     LOW     FIRST                 LAST")
    print(
        "----   ----         --- ---  -----   -----   -------------------   -------------------"
    )
    for row in rows:
        total += 1
        altitude = 0
        alt_low = 0
        distance = 0
        closest = 0
        country = ""
        owner = ""
        reg = ""
        mlt = ""
        day_count = 1
        cat = ""
        first = str(row[11]).split(".")[0]
        last = str(row[12]).split(".")[0]

        if row[3]:
            distance = int(row[3])
        if row[4]:
            closest = int(row[4])
        if row[5]:
            altitude = int(row[5])
        if row[6]:
            alt_low = int(row[6])
        print(
            f"{row[0]:<5} {row[2]:<12} {distance:<3} {closest:<4} {altitude:<7} {alt_low:<7} {first:<10}   {last:<10}"
        )

    return total


def lookup_ptypes(ptype):

    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM plane_types WHERE ptype LIKE ? ORDER BY count DESC", (ptype,)
    )
    rows = cur.fetchall()
    total = 0
    planes = 0
    print(
        "MFR      TYPE  CNT  LAST_IDENT      FIRST SEEN            LAST SEEN             MODEL"
    )
    for row in rows:
        cnt = 1
        if row[4]:
            cnt = row[4]
        mfr = ""
        if row[5]:
            mfr = row[5][:8]
        total += 1
        planes += cnt
        first = str(row[2]).split(".")[0]
        last = str(row[3]).split(".")[0]
        model = row[6]
        if model:
            model = model[:50]
        print(
            f"{mfr:<8} {row[0]:<5} {cnt:<4} {row[1]:<6}          {first:<10}   {last:<10}   {model}"
        )

    if total > 1:
        print(f"\nAircraft Types: {total} / Total Aircraft: {planes}")


def lookup_icao(icao):

    cur = conn.cursor()
    cur.execute("SELECT * FROM planes WHERE icao = ?", (icao,))
    rows = cur.fetchall()
    total = print_planes(rows)
    if total == 1:
        cur.execute("SELECT * FROM plane_days WHERE icao = ?", (icao,))
        rows = cur.fetchall()
        print_plane_days(rows)


def lookup_ident(ident):

    cur = conn.cursor()
    cur.execute("SELECT * FROM planes WHERE ident LIKE ?", (ident,))
    rows = cur.fetchall()
    total = print_planes(rows)
    if total == 1:
        row = rows[0]
        cur.execute("SELECT * FROM plane_days WHERE icao = ?", (row[0],))
        rows = cur.fetchall()
        print_plane_days(rows)
    else:
        print("\nTotal:", total)


def lookup_reg(reg):

    cur = conn.cursor()
    cur.execute("SELECT * FROM planes WHERE registration LIKE ?", (reg,))
    rows = cur.fetchall()
    total = print_planes(rows)
    if total == 1:
        row = rows[0]
        cur.execute("SELECT * FROM plane_days WHERE icao = ?", (row[0],))
        rows = cur.fetchall()
        print_plane_days(rows)
    else:
        print("\nTotal:", total)


def lookup_model_mfr(icao):

    cur = lookup.cursor()
    # cur.execute("SELECT * FROM Aircraft LEFT JOIN Model ON Aircraft.ModelID = Model.ModelID LEFT JOIN Operator ON Aircraft.OperatorID = Operator.OperatorID WHERE Aircraft.Icao = ?", (icao,))
    # cur.execute("select Icao,Engines,Model,Manufacturer from AircraftTypeView WHERE Icao = ? LIMIT 1;", (ptype,))
    cur.execute(
        "SELECT ModeS,OperatorFlagCode,CurrentRegDate,ModeSCountry,Country,AircraftClass,Engines,YearBuilt,Manufacturer,Type,RegisteredOwners,Registration FROM Aircraft WHERE ModeS = ?",
        (icao,),
    )
    rows = cur.fetchall()
    mfr = None
    pname = None
    country = None
    owner = None
    ptype = None
    military = None
    reg = None
    for r in rows:
        # print(r)
        mfr = r[8]
        if mfr:
            mfr = mfr.title()
        pname = r[9]
        reg = r[11]
        country = r[3]
        if r[10]:
            owner = r[10].rstrip()
            if re.search(r"United States Air Force", owner):
                military = "M"
            elif re.search(r"United States Marine", owner):
                military = "M"
            elif re.search(r"United States Navy", owner):
                military = "M"
            elif re.search(r"United States Army", owner):
                military = "M"

        ptype = r[1]
        if re.search(r"United\sStates", country):
            country = "USA"

    return (ptype, mfr, pname, country, owner, military, reg)


def alert_landing(
    icao,
    ident,
    squawk,
    ptype,
    distance,
    altitude,
    heading,
    speed,
    lat,
    lon,
    baro_rate,
    category,
):
    global sounds

    # print(f'ic:{icao}, ident:{ident}, sq:{squawk}, pt:{ptype}, dist:{distance}, alt:{altitude}, head:{heading}, spd:{speed}')
    if icao and distance and heading and speed and altitude:
        try:
            # and heading < 365 and heading > 240
            if (
                distance < 4.0
                and lat < 32.78
                and lon < -79.90
                and baro_rate < 1000
                and altitude > 1000
                and altitude < 4500
                and speed < 300
                and speed > 150
                and (heading > 310 or heading < 30)
            ):
                if icao not in alerted:
                    alerted[icao] = 1
                    logger.warning(
                        f"!! Landing Alert !!: {icao} {ident} {ptype} {category} d:{distance} h:{heading} s:{speed} a:{altitude} lat:{lat} lon:{lon}"
                    )
                    if sounds and check_quiet_time():
                        play_sound("/Users/yantisj/dev/ads-db/sounds/ding-high.mp3")
                        # time.sleep(0.5)
                        play_sound(
                            "/Users/yantisj/dev/ads-db/sounds/airplane-fly-over.mp3"
                        )
        except TypeError as e:
            logger.critical(
                f"Error updating plane: {icao} t:{ptype} id:{ident} alt:{altitude}: {e}"
            )
            pass


def alert_b787(icao, ident, squawk, ptype, distance, altitude, heading, speed):

    if ident and icao:
        try:
            if (
                altitude
                and altitude < 20000
                and (
                    re.search(r"^B78", ptype)
                    or (re.search("(BOE\d+|000000)", ident) and not ptype)
                )
            ):
                if icao not in ptype_alerted:
                    ptype_alerted[icao] = 1
                    logger.warning(
                        f"Boeing 787 Airborne!!!  ic:{icao}, ident:{ident}, sq:{squawk}, pt:{ptype}, dist:{distance}, alt:{altitude}, head:{heading}, spd:{speed}"
                    )
                    # print(f'ic:{icao}, ident:{ident}, sq:{squawk}, pt:{ptype}, dist:{distance}, alt:{altitude}, head:{heading}, spd:{speed}')
                    if sounds and check_quiet_time():
                        play_sound("/Users/yantisj/dev/ads-db/sounds/warnone.mp3")
        except TypeError as e:
            logger.critical(
                f"Error updating plane: {icao} t:{ptype} id:{ident} alt:{altitude}: {e}"
            )
            pass


def alert_ident(ident, sites=["127.0.0.1"], min_distance=0):
    "Play alert on ident when called from CLI"

    global sounds

    logger.info(f"Searching for {ident}")

    tracking = ident.split(",")

    while tracking:
        for site in sites:
            for ident in tracking:
                r = requests.get(
                    f"http://{site}/dump1090-fa/data/aircraft.json", timeout=5
                )
                planes = r.json()

                for p in planes["aircraft"]:
                    flight = ""
                    icao = ""
                    distance = 100.0
                    if "flight" in p:
                        flight = p["flight"].rstrip()
                    if "hex" in p:
                        icao = p["hex"].upper().replace("~", "")
                    if "lat" in p:
                        distance = mpu.haversine_distance(
                            (p["lat"], p["lon"]),
                            (
                                float(config["global"]["lat"]),
                                float(config["global"]["lon"]),
                            ),
                        )
                        distance = round(distance * 0.621371, 1)
                    if flight == ident or icao == ident:
                        if not min_distance or distance < min_distance:
                            logger.info(f"Located Plane: {ident} from {site}")
                            lookup_icao(icao)
                            play_sound("/Users/yantisj/dev/ads-db/sounds/ding.mp3")
                            time.sleep(5)
                            play_sound("/Users/yantisj/dev/ads-db/sounds/ding.mp3")
                            tracking.remove(ident)

        time.sleep(3)


def get_db_stats():

    cur = conn.cursor()
    cur.execute(
        'SELECT icao,ptype,lastseen "[timestamp]", firstseen "[timestamp]" FROM planes ORDER BY lastseen ASC'
    )

    day_ago = datetime.now() - timedelta(days=1)

    rows = cur.fetchall()
    types = defaultdict(int)
    ttypes = dict()
    ttypes_day = dict()
    ttypes_new = dict()
    ttypes_old = dict()
    total = 0
    total_day = 0
    total_new = 0
    type_count = 0
    type_count_day = 0
    type_count_new = 0
    last_seen = ""

    for row in rows:
        total += 1
        last_seen = str(row[2]).split(".")[0]

        icao = row[0]
        ptype = row[1]
        types["TOTL"] += 1
        types[ptype] += 1
        if ptype not in ttypes:
            ttypes[ptype] = 1
            type_count += 1
            types["TYPE"] = type_count
        if row[3] < day_ago:
            if ptype not in ttypes_old:
                ttypes_old[ptype] = 1
        if row[2] > day_ago:
            total_day += 1
            if ptype not in ttypes_day:
                ttypes_day[ptype] = 1
                type_count_day += 1
        if row[3] > day_ago:
            total_new += 1
            if ptype not in ttypes_new and ptype not in ttypes_old and ptype:
                ttypes_new[ptype] = 1
                type_count_new += 1

    sort_types = OrderedDict(sorted(types.items(), key=itemgetter(1), reverse=True))
    tcount = 0
    # for en in sort_types:
    #     tcount += 1
    #     if tcount < 10:
    #         print(en, sort_types[en])
    print(f"\nADS-DB Database Stats")
    print("--------------------------")
    print(
        f" Types: {type_count:<5}  24hr: {type_count_day:<3}    New: {type_count_new:<3}"
    )
    print(f" Total: {total:<5}  24hr: {total_day:<4}   New: {total_new:<4}")
    print(f"\nLast Seen: {last_seen}\n")
    return types


conn = None


def check_quiet_time():
    now = datetime.now()
    # print('time now', now.hour)
    if now.hour > 21 or now.hour < 9:
        return False

    return True


def dict_gen(curs):
    """From Python Essential Reference by David Beazley"""
    import itertools

    field_names = [d[0].lower() for d in curs.description]
    while True:
        rows = curs.fetchmany()
        if not rows:
            return
        for row in rows:
            yield dict(zip(field_names, row))


def update_missing_data():
    "Refresh all data from databases on missing info"

    cur = conn.cursor()
    cur2 = conn.cursor()
    now = datetime.now()
    rows = dict_gen(cur.execute("SELECT * FROM planes"))
    # rows = cur.fetchall()
    count = 0
    models = dict()

    for row in rows:
        update = False
        # print(row)
        icao = row["icao"]
        ptype = ""
        reg = ""
        if icao in fa_lookup:
            # print('Plane Type', fa_lookup[icao])
            ptype = fa_lookup[icao][0]
            reg = fa_lookup[icao][1]
        # print(dets)

        dets = lookup_model_mfr(icao)
        # print(dets)
        country = ""
        model = ""
        mfr = ""
        owner = ""
        military = "."
        if dets[0]:
            if not ptype:
                logger.debug(f"Ptype secondary lookup: {icao} {dets[0]}")
                ptype = dets[0]
                reg = dets[6]
            mfr = dets[1]
            model = dets[2]
            country = dets[3]
            owner = dets[4]
            military = dets[5]
            model = model[:50]
            if model:
                models[ptype] = model
            elif len(model) > len(models[ptype]):
                models[ptype] = model
        if owner and owner != row["owner"]:
            print("Owner needs updating", row)
            update = True
        elif ptype and ptype != row["ptype"]:
            logger.info(f'Ptype needs updating: {ptype} vs {row["ptype"]}')
            update = True
        elif reg and reg != "None" and reg != row["registration"]:
            logger.info(f"Registration needs updating: {reg} vs {row['registration']}")
            update = True
        if update:
            if ptype:
                cur2.execute("SELECT * FROM plane_types WHERE ptype=?", (ptype,))
                ptypes = cur2.fetchall()
                new = False
                if not ptypes:
                    new = True
                    logger.info(f"!! New Plane Type !!: {icao} t:{ptype}")
                    sql = """INSERT INTO plane_types(ptype,last_icao,firstseen,lastseen,count,manufacturer,model)
                        VALUES(?,?,?,?,?,?,?) """
                    cur2.execute(sql, (ptype, icao, now, now, 1, mfr, model))
                else:
                    prow = ptypes[0]
                    cur2.execute("SELECT * FROM planes WHERE ptype = ?", (ptype,))
                    prows = cur2.fetchall()
                    pcount = 0
                    for pc in prows:
                        pcount += 1
                    nmfr = prow[5]
                    if mfr:
                        nmfr = mfr

                    sql = """UPDATE plane_types SET last_icao = ?, count = ?, manufacturer = ?, model = ?
                        WHERE ptype = ? """
                    cur2.execute(sql, (icao, pcount, nmfr, model, ptype))

            count += 1
            sql = """UPDATE planes SET ptype = ?, registration = ?, country = ?, owner = ?, military = ?
                WHERE icao = ? """
            cur2.execute(sql, (ptype, reg, country, owner, military, icao))

    cur3 = conn.cursor()
    rows = dict_gen(cur.execute("SELECT * FROM plane_types"))
    for row in rows:
        if row["ptype"] in models and row["model"] != models[row["ptype"]]:
            logger.info(
                f"Updating {row['ptype']} model: {row['model']} -> {models[row['ptype']]}"
            )
            sql = """UPDATE plane_types SET model = ?
                WHERE ptype = ? """
            cur3.execute(sql, (models[row["ptype"]], row["ptype"]))
            count += 1

    if count:
        logger.warning(f"Total Updates: {count}")
        conn.commit()
    else:
        logger.info("No updates required")


def cleanup_db(days=365):
    "Make entries weekly after so many days: (use indexes)"

    cur = conn.cursor()

    now = datetime.now()
    time_ago = datetime.now() - timedelta(days=days)
    rows = dict_gen(cur.execute("SELECT * FROM planes"))
    total = 0
    total_lastsave = 0

    for row in rows:
        count = 0
        update = False
        cur2 = conn.cursor()
        cur_week = 0
        first_day = None
        extra_days = list()
        lastseen = None
        lowest_altitude = 0

        cur2.execute("SELECT COUNT(*) from plane_days WHERE icao = ?", (row["icao"],))
        day_count = cur2.fetchone()[0]

        if int(day_count) > 100:
            print("Squash", row["icao"], day_count)
            days = dict_gen(
                cur2.execute(
                    "SELECT * FROM plane_days WHERE icao = ? ORDER BY day ASC",
                    (row["icao"],),
                )
            )
            for d in days:
                if d["day"] < time_ago.date():
                    if cur_week != d["day"].isocalendar()[1]:
                        if extra_days:
                            squash_plane_days(
                                first_day, lastseen, lowest_altitude, extra_days
                            )
                            total += 1
                        first_day = d
                        lowest_altitude = d["lowest_altitude"]
                        extra_days = list()
                        lastseen = d["lastseen"]
                        cur_week = d["day"].isocalendar()[1]
                    else:
                        extra_days.append(d)
                        lastseen = d["lastseen"]
                        if (
                            d["lowest_altitude"]
                            and d["lowest_altitude"] < lowest_altitude
                            and d["lowest_altitude"] > 0
                        ):
                            lowest_altitude = d["lowest_altitude"]

                        # if d['day'].isoweekday() != 5 and d['day'].isoweekday() != 6:
                        #     print('DELETE', d, d['day'].isoweekday())
                        #     total += 1
        if total - total_lastsave > 10000:
            print("Saving DB:", total)
            total_lastsave = total
            conn.commit()

    conn.commit()
    print("TOTAL DELETE", total)


def squash_plane_days(entry, lastseen, lowest_altitude, delete):
    "Squash plane day entries into single entry with lastseen (day to week)"

    cur = conn.cursor()
    # print('Squash ', len(delete), ' plane_day entries:', entry['icao'], lowest_altitude, entry['firstseen'], ' - ', lastseen)

    sql = "UPDATE plane_days SET lastseen = ?, lowest_altitude = ? WHERE icao = ? AND day = ?"
    cur.execute(sql, (lastseen, lowest_altitude, entry["icao"], entry["day"]))

    for dentry in delete:
        sql = "DELETE FROM plane_days WHERE icao = ? AND day = ?"
        cur.execute(sql, (dentry["icao"], dentry["day"]))


def run_daemon(refresh=10, sites=["127.0.0.1"]):
    # option = webdriver.ChromeOptions()
    # option.add_argument(" — incognito")
    # browser = webdriver.Chrome(executable_path='/Users/yantisj/dev/arbitragerx/venv/bin/chromedriver', chrome_options=option)
    timeout = 20
    cdict_counter = 10
    new = False
    global sounds
    while True:
        cdict_counter += 1
        for site in sites:
            try:
                logger.debug(f"Updating aircraft.json: {site}")
                r = requests.get(
                    f"http://{site}/dump1090-fa/data/aircraft.json", timeout=5
                )
                planes = r.json()

                for p in planes["aircraft"]:
                    if "hex" in p and p["hex"] and "lat" in p and p["lat"]:
                        # for en in p:
                        #     print(en, p[en])

                        category = ""
                        if "category" in p:
                            category = p["category"]
                        baro_rate = 0
                        if "baro_rate" in p:
                            baro_rate = p["baro_rate"]

                        icao = p["hex"].upper().replace("~", "")
                        flight = ""
                        if "flight" in p:
                            flight = p["flight"].rstrip()

                        squawk = ""
                        if "squawk" in p:
                            squawk = p["squawk"].rstrip()

                        lat = p["lat"]
                        lon = p["lon"]

                        # KM -> NM
                        distance = mpu.haversine_distance(
                            (lat, lon),
                            (
                                float(config["global"]["lat"]),
                                float(config["global"]["lon"]),
                            ),
                        )
                        distance = round(distance * 0.621371, 1)
                        # print('distance', distance)

                        heading = 0
                        if "track" in p:
                            heading = int(p["track"])

                        altitude = 0
                        if "alt_baro" in p:
                            altitude = p["alt_baro"]
                        if altitude == "ground":
                            altitude = 0

                        speed = 0
                        if "gs" in p:
                            speed = int(p["gs"])

                        # lookup_type_reg(icao)
                        ptype = ""
                        reg = ""
                        if icao in fa_lookup:
                            # print('Plane Type', fa_lookup[icao])
                            ptype = fa_lookup[icao][0]
                            reg = fa_lookup[icao][1]

                        dets = lookup_model_mfr(icao)
                        # print(dets)
                        country = ""
                        model = ""
                        mfr = ""
                        owner = ""
                        military = "."
                        if dets[0]:
                            if not ptype:
                                logger.debug(
                                    f"Ptype secondary lookup: {icao} {dets[0]}"
                                )
                                ptype = dets[0]
                                reg = dets[6]
                            mfr = dets[1]
                            model = dets[2]
                            model = model[:50]
                            country = dets[3]
                            owner = dets[4]
                            military = dets[5]

                        # print(f'{icao} {reg} {ptype} {flight} {category} {squawk} {lat} {lon} {altitude} {heading} {distance} {speed}')

                        update_plane_day(
                            icao,
                            flight,
                            squawk,
                            ptype,
                            distance,
                            altitude,
                            heading,
                            speed,
                            reg,
                        )
                        update_plane(
                            icao,
                            flight,
                            squawk,
                            ptype,
                            distance,
                            altitude,
                            heading,
                            speed,
                            reg,
                            country,
                            owner,
                            military,
                            category,
                            site,
                        )
                        if ptype:
                            new = update_ptype(ptype, icao, mfr, model)
                            if new:
                                logger.warning(
                                    f"New Plane Type!!: {icao} {reg} {ptype} {flight} {country} {owner} d:{distance}"
                                )
                                if sounds and check_quiet_time():
                                    play_sound(
                                        "/Users/yantisj/dev/ads-db/sounds/ding-low.mp3"
                                    )
                        else:
                            logger.debug(
                                f"No Plane Type: {icao} {reg} {ptype} {flight} {squawk} {lat} {lon} {altitude} {heading} {distance} {speed}"
                            )
                        if config["alerts"]["landing"] in ["true", "True", "1"]:
                            alert_landing(
                                icao,
                                flight,
                                squawk,
                                ptype,
                                distance,
                                altitude,
                                heading,
                                speed,
                                lat,
                                lon,
                                baro_rate,
                                category,
                            )
                        if config["alerts"]["boeing"] in ["true", "True", "1"]:
                            alert_b787(
                                icao,
                                flight,
                                squawk,
                                ptype,
                                distance,
                                altitude,
                                heading,
                                speed,
                            )

            except requests.exceptions.ConnectionError as e:
                logger.warning(f"ConnectionError: {e}")
                time.sleep(30)
                pass
            except Exception as e:
                logger.critical(f"General Update Exception: {e}")
                # raise e
                time.sleep(60)
                pass

        if cdict_counter > save_cycle:
            cdict_counter = 0
            if save_cycle >= 10:
                logger.info("Committing Data to DB")
            conn.commit()
        time.sleep(refresh)


def load_fadb():
    global lookup
    logger.info("FA Database Loading")
    with open(config["db"]["flight_aware"], "r") as f:
        dr = csv.DictReader(f)
        for en in dr:
            fa_lookup[en["icao24"]] = (en["t"], en["r"])
        logger.debug("FA Database Loaded")

    # https://data.flightairmap.com/
    lookup = create_connection(config["db"]["base_station"])


def read_config(config_file):
    "Read in config and setup config dict"
    global sounds

    logger.debug("Reading config file")

    config = configparser.ConfigParser()
    config.read(config_file)
    if config["alerts"]["sounds"] in ["true", "True", "1"]:
        logger.info("Enabling Sounds")
        sounds = True
    return config


def sigterm_handler(_signo, _stack_frame):
    logger.warning("Caught Kill Signal, closing DB")
    conn.commit()
    sys.exit(0)


signal.signal(signal.SIGTERM, sigterm_handler)


check_quiet_time()

parser = argparse.ArgumentParser(description="Save ADSB Data to SQLite")
parser.add_argument("-D", action="store_true", help="Run Daemon")
parser.add_argument("-st", action="store_true", help="Database Stats")
parser.add_argument(
    "-rs", type=str, help="Receiver IP List separated by commas (default 127.0.0.1)"
)
parser.add_argument("-rf", type=int, help="Refresh Interval (default 10sec)")
parser.add_argument("-db", type=str, help="Different Database File")
parser.add_argument("-lt", type=str, help="Lookup Planes by Type")
parser.add_argument(
    "-lts", type=str, help="Lookup Type Totals by type (use percent-sign for all type)"
)
parser.add_argument("-li", type=str, help="Lookup Planes by IACO")
parser.add_argument("-ld", type=str, help="Lookup Planes by IDENT")
parser.add_argument("-lr", type=str, help="Lookup Planes by Registration")
parser.add_argument("-af", type=str, help="Alert on Flight Name / ICAO")
parser.add_argument(
    "-ad", type=int, help="Alert Distance from Receiver (default unlimited)"
)
parser.add_argument("-fa", type=int, help="Filter Low Altitude")
parser.add_argument("-fc", type=int, help="Filter Category Above int (A[3])")
parser.add_argument("-fc0", action="store_true", help="Filter A0 no categories")
parser.add_argument("-fm", action="store_true", help="Filter Military Planes")
parser.add_argument("-fh", type=float, help="Filter by hours since seen")
parser.add_argument("-fd", type=int, help="Filter by Days Seen Above Count")
parser.add_argument(
    "-sc", type=int, help="Save Cycle (increase > 10 to reduce disk writes)"
)
parser.add_argument("-S", action="store_true", help="Play Sounds")
parser.add_argument("-v", action="store_true", help="Debug Mode")
parser.add_argument(
    "--update_db", action="store_true", help="Update all planes with latest DB info"
)
parser.add_argument(
    "--cleanup_db", action="store_true", help="Cleanup excess plane days"
)
args = parser.parse_args()

# http://www.virtualradarserver.co.uk/Files/StandingData.sqb.gz
# lookup = create_connection('StandingData.sqb')

if args.v:
    logger = setup_logger(logfile="debug.log", level=logging.DEBUG)
else:
    logger = setup_logger()

# Load config from file
config = read_config("ads-db.conf")

if args.db:
    conn = sqlite3.connect(
        "./sqb/" + args.db,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )
else:
    conn = sqlite3.connect(
        "./sqb/ads-db-planes.sqb",
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )
if args.S:
    logger.info("Playing Sounds")
    sounds = True

if args.sc:
    save_cycle = args.sc
    logger.info(f"Increasing Save Cycle to {args.sc}")

if args.update_db:
    load_fadb()
    logger.info("Updating All Plane Data")
    update_missing_data()
elif args.cleanup_db:
    cleanup_db()

elif args.D:
    load_fadb()

    sites = ["127.0.0.1"]
    refresh = 10
    if args.rs:
        sites = args.rs.split(",")
    if args.rf:
        refresh = args.rf
    # Run Daemon
    try:
        logger.info("Daemon Started")
        run_daemon(refresh=refresh, sites=sites)
    except KeyboardInterrupt:
        logger.info("Closing Database")
        conn.commit()
elif args.st:
    get_db_stats()
elif args.lt:
    count = 0
    hours = 0
    if args.fd:
        count = args.fd

    if args.fh:
        hours = args.fh
    cat_min = 0
    if args.fc:
        cat_min = args.fc
    no_a0 = False
    low_alt = 0
    if args.fa:
        low_alt = args.fa

    no_a0 = False
    if args.fc0:
        no_a0 = True
    military = False
    if args.fm:
        military = True

    lookup_ptype(
        args.lt,
        count=count,
        cat_min=cat_min,
        low_alt=low_alt,
        no_a0=no_a0,
        military=military,
        hours=hours,
    )
elif args.lts:
    lookup_ptypes(args.lts)
elif args.li:
    lookup_icao(args.li)
elif args.ld:
    lookup_ident(args.ld)
elif args.lr:
    lookup_reg(args.lr)
elif args.af:
    sites = ["127.0.0.1"]
    refresh = 10
    if args.rs:
        sites = args.rs.split(",")
    alert_ident(args.af, sites=sites, min_distance=args.ad)
else:
    parser.print_help()

print()
