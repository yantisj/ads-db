#!/usr/bin/env python3
# Plane Tracker Database
#  - Reads from http://{site}/skyaware/data/aircraft.json
#  - Registration required better Basestation:
#       https://radarspotting.com/forum/index.php?action=tportal;sa=download;dl=item521
#  - Relies on BaseStation.sqb from https://data.flightairmap.com/ (no registration)
#  - Uses flightaware csv for IACO -> PTYPE quick lookup (included but needs updating)
#
from collections import defaultdict, OrderedDict
from operator import itemgetter
import time
import re
import sqlite3
import logging
from unittest import signals
import requests
import mpu
from sqlite3 import Error
from datetime import date, datetime, timedelta
import argparse
import configparser
import requests
import signal
import sys

config = dict()

save_cycle = 1
logger = None

alerted = dict()
reactivated = dict()
ptype_alerted = dict()
pdict = dict()
cdict = defaultdict(int)
local_flights = dict()
local_fixed = 10
local_correct = 2
lookup = None
flight_conn = None
sounds = False
saving_db = False
api_count = 0

# Holddown empty data for a few cycles
holddown = defaultdict(int)

# Stop API Call's after threshold reached (reset when process reloads)
MAX_API_COUNT = 1000
AEROAPI_BASE_URL = "https://aeroapi.flightaware.com/aeroapi"

STATIC_CALL_SIGNS = [
    "DAL",
    "SWA",
    "RPA",
    "UAL",
    "AAL",
    "JBU",
    "FFT",
    "MXY",
    "NKS",
    "JIA",
    "FDX",
    "UPS",
    "DLH",
    "ACA",
    "ENY",
    "ROU",
    "VOC",
    "ICE",
    "AAY",
    "EDV",
    "AJT",
    "WJA",
    "ASH",
    "VXP",
    "FLE",
    "SIL",
    "TSC",
    "SKW",
    "WSW",
    "SWG",
    "AVA",
    "AWI",
    "EUK",
    "LAN",
    "QTR",
    "GTI",
    "BAW",
    "THY",
    "TNO",
    "ATN",
    "MNU",
    "VIR",
    "TAI",
    "SAS",
    "EIN",
    "TOM",
    "SCX",
    "AUA",
    "BCS",
    "GJS",
    "ASA",
    "BAF",
    "FIN",
    "KLM",
    "SUB",
    "FRG",
    "LPE",
    "JAF",
    "AMX",
]   

STATIC_CATEGORIES = {
    'E3TF': "A3",
    'E8': "A3",
    'B52': "A5",
    'C17': "A5",
}

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
                                    category text,
                                    opcode varchar(20),
                                    status varchar(1),
                                    model varchar(40),
                                    serial varchar(30)
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

sql_create_flights_table = """ CREATE TABLE IF NOT EXISTS flights (
                                    flight text PRIMARY KEY,
                                    icao text,
                                    ptype text,
                                    distance float,
                                    closest float,
                                    altitude float,
                                    lowest_altitude float,
                                    speed float,
                                    lowest_speed float,
                                    squawk text,
                                    heading float,
                                    registration text,
                                    day_count integer,
                                    from_airport text,
                                    to_airport text,
                                    firstseen timestamp,
                                    lastseen timestamp,
                                    route_distance integer
                                ); """

sql_create_flight_cache_table = """ CREATE TABLE IF NOT EXISTS flight_cache (
                                    flight text PRIMARY KEY,
                                    from_airport text,
                                    to_airport text,
                                    distance int,
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
                                    model text,
                                    category varchar(2),
                                    active integer
                                ); """


def setup_logger(logfile="ads-db.log", level=logging.INFO):

    logging.getLogger("urllib3").setLevel(logging.WARNING)

    logFormatter = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")
    logger = logging.getLogger()

    fileHandler = logging.FileHandler(logfile)
    fileHandler.setFormatter(logFormatter)
    logger.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    logger.addHandler(consoleHandler)
    logger.setLevel(level)

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


def connect_ads_db(db_file):

    conn_db = sqlite3.connect(
        db_file,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )


    create_table(conn_db, sql_create_flight_cache_table)

    # Try to populated reactived database
    try:
        rcount = 0
        cur = conn_db.cursor()
        cur.execute("SELECT icao FROM planes WHERE status=?", ("R",))
        rows = cur.fetchall()
        for row in rows:
            reactivated[row[0]] = 1
            rcount +=1
        if rcount:
            logger.debug(f"Connected to DB: {rcount} Reactivated Planes")
    except sqlite3.OperationalError as e:
        logger.warning(f"New Database: Trying to create DB: {e}")

        create_table(conn_db, sql_create_planes_table)
        create_table(conn_db, sql_create_types_table)
        create_table(conn_db, sql_create_plane_days_table)
        create_table(conn_db, sql_create_flights_table)
        create_table(conn_db, sql_create_flight_cache_table)

        # Setup indexes and initial DB
        commands = [
            "pragma journal_mode = WAL;",
            "pragma synchronous = normal;",
            "pragma temp_store = memory;",
            "pragma mmap_size = 30000000000;",
            "CREATE INDEX icao_day_idx ON plane_days(icao);",
            "CREATE INDEX plane_day_idx ON plane_days(day);",
            "CREATE INDEX plane_ident_idx ON plane_days(ident);",
            "CREATE INDEX icao_idx ON planes(icao);",
            "CREATE INDEX ptype_idx ON planes(ptype);",
            "CREATE INDEX flights_flight_idx ON flights(flight);",
            "CREATE INDEX flights_icao_idx ON flights(icao);",
        ]
        logger.warning(f"Initializing Database: {db_file}")
        for cmd in commands:
            print("DB Setup:", cmd)
            res = cur.execute(cmd)
        conn_db.commit()

    return conn_db


def create_table(conn, create_table_sql):
    """create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        # print("Setup:", create_table_sql)
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def play_sound(filename):
    
    global sounds

    if sounds and check_quiet_time():
        from playsound import playsound
        playsound(filename)


def update_plane(
    icao,
    ident,
    squawk,
    ptype,
    model,
    distance,
    altitude,
    flight_level,
    heading,
    speed,
    reg,
    country,
    owner,
    military,
    category,
    site,
    mfr,
    status,
    opcode,
    serial
):

    now = datetime.now()
    today = date.today()
    global sounds

    # Holddown up to five cycles while waiting for better data
    if not ident and holddown[icao] < 5:
        holddown[icao] += 1
        return

    # Play sounds on new versions of these aircraft
    alert_types = dict()
    if "new_planes" in config["alerts"]:
        alert_types = config["alerts"]["new_planes"].split(",")
    if "local_planes" in config["alerts"]:
        local_types = config["alerts"]["local_planes"].split(",")
    else:
        local_types = dict()
    if "local_altitude" in config["alerts"]:
        local_altitude = int(config["alerts"]["local_altitude"])
    else:
        local_altitude = 12000
    if "local_distance" in config["alerts"]:
        local_distance = int(config["alerts"]["local_distance"])
    else:
        local_distance = 40

    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM planes WHERE icao=?", (icao,))
        rows = cur.fetchall()
    except sqlite3.OperationalError as e:
        logger.warning(f"Database Error: {e}")
        return

    # print(f'ic:{icao}, ident:{ident}, sq:{squawk}, pt:{ptype}, dist:{distance}, alt:{altitude}, head:{heading}, spd:{speed}')
    if not rows:
        model_str = model[:11]
        dist_int = int(distance)
        category = get_category(ptype)
        size = 0
        if re.search(r'A\d', category):
            size = int(category[1])
        if ptype in alert_types or size == 5:
            logger.warning(
                f"NEW PLANE {model_str:>11} ({ptype:>4}) {category:<2} [{dist_int:>3}nm {flight_level:<5}] {ident:>7} {reg} {country} {owner} {mfr} {icao} site:{site}"
            )
            play_sound("/Users/yantisj/dev/ads-db/sounds/ding.mp3")
            play_sound("/Users/yantisj/dev/ads-db/sounds/ding.mp3")
        else:
            logger.info(
                f"New Plane {model_str:>11} ({ptype:>4}) {category:<2} [{dist_int:>3}nm {flight_level:<5}] {ident:>7} {reg} {country} {owner} {mfr} {icao} site:{site}"
            )
        sql = """INSERT INTO planes(icao,ident,ptype,speed,altitude,lowest_altitude,distance,closest,heading,firstseen,lastseen,registration,country,owner,military,day_count,category,status,opcode,model,serial)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) """
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
                status,
                opcode,
                model,
                serial
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
            # Reset low/distance stats once per day
            if row[12].date() < today:
                # print('Lastseen date reset plane', row)
                # print(distance, altitude)
                low_dist = distance
                low_alt = altitude
            sql = """UPDATE planes SET ident = ?, ptype = ?, squawk = ?, speed = ?, altitude = ?, lowest_altitude = ?, distance = ?, closest = ?, heading = ?, lastseen = ?, registration = ?, country = ?, owner = ?, military = ?, day_count = ?, category = ?, status = ?, opcode = ?, model = ?, serial = ?
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
                    status,
                    opcode,
                    model,
                    serial,
                    icao,
                ),
            )
            if (
                (ptype in local_types or reg in local_types or ident in local_types)
                and (icao, today, 'local') not in alerted
                and altitude < local_altitude
                and distance < local_distance
            ):
                alerted[(icao, today, 'local')] = 1
                dist_int = int(distance)
                category = get_category(ptype)
                logger.warning(
                    f"Local Alert! {ident:>8} ({ptype:<4}) {category:<2} [{dist_int:>3}nm {flight_level:<5}] {ident:<7} {reg} {country} {owner} {mfr} {icao} site:{site}"
                )
                play_sound("/Users/yantisj/dev/ads-db/sounds/ding.mp3")
                play_sound("/Users/yantisj/dev/ads-db/sounds/ding-high.mp3")

        except TypeError as e:
            logger.critical(
                f"Error updating plane: {icao} t:{ptype} id:{ident} alt:{altitude}: {e}"
            )
            pass


def update_plane_day(
    icao,
    ident,
    squawk,
    ptype,
    distance,
    altitude,
    flight_level,
    heading,
    speed,
    reg,
    category,
    site,
    owner,
    baro_rate
):
    "Store planes per day. If callsign is True, store each ident per plane per day"

    now = datetime.now()
    now_date = datetime.now().date()

    if not ident:
        logger.debug(f"no ident, returning: {icao}")
        return

    signs = get_call_signs()


    # Track all flights instead of just plane days
    if (
        "flights" in config
        and "all_flights" in config["flights"]
        and config["flights"]["all_flights"] in ["True", "true", "1"]
    ):
        call_sign = True
    else:
        call_sign = False

    for sign in signs:
        if re.search(f"^{sign}", ident):
            call_sign = True

    (from_airport, to_airport, route_distance) = get_flight_data(ident, distance=distance, altitude=altitude, vs=baro_rate, force=call_sign)

    try:
        cur = conn.cursor()
        if call_sign:

            cur.execute(
                "SELECT * FROM plane_days WHERE icao = ? and day = ? and ident = ?",
                (
                    icao,
                    now_date,
                    ident,
                ),
            )
            rows = cur.fetchall()
            # print(icao, ident, now_date, rows)
        else:
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
        cur = conn.cursor()
        dist_int = int(distance)
        category = get_category(ptype)
        if call_sign:
            route_type = get_route_type(route_distance)
            if route_type:
                flight_text = 'FLT ' + route_type
            else:
                flight_text = 'Flight'
            fstr = f"Todays {flight_text:<6} {ident:>7} ({ptype}) {category:<2} [{dist_int:>3}nm {flight_level:<5}] {ident:>7} {from_airport:>4}->{to_airport:<4}{route_distance:>5}nm {reg:<6} {icao} site:{site}"

            logger.debug(fstr)
        else:
            logger.debug(
                f"Todays Plane  {ident:>7} ({ptype}) {category:<2} [{dist_int:>3}nm {flight_level:<5}] {ident:>7} {reg} {owner} {icao} site:{site}"
            )

        sql = """INSERT INTO plane_days(icao,day,ident,speed,altitude,lowest_altitude,distance,closest,heading,firstseen,lastseen)
              VALUES(?,?,?,?,?,?,?,?,?,?,?) """
        # print(
        #         icao,
        #         now_date,
        #         ident,
        #         speed,
        #         altitude,
        #         altitude,
        #         distance,
        #         distance,
        #         heading,
        #         now,
        #         now,
        #     )
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
            cur = conn.cursor()
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

            # Use Cache ident
            nident = row[2]
            if ident and nident != ident:
                if ident not in alerted:
                    alerted[ident] = 1
                    logger.info(f"Day Ident mismatch (Flight?): {ident}  <-> {nident}")
            if ident:
                nident = ident
            elif not nident:
                print("No ident!", nident, icao)
            nsquawk = row[9]
            if squawk:
                nsquawk = squawk
            if call_sign:
                sql = """UPDATE plane_days SET squawk =?, speed = ?, altitude = ?, lowest_altitude = ?, distance = ?, closest = ?, heading = ?, lastseen = ?
                    WHERE icao = ? AND ident = ? AND day = ?"""
                cur.execute(
                    sql,
                    (
                        nsquawk,
                        speed,
                        altitude,
                        low_alt,
                        distance,
                        low_dist,
                        heading,
                        now,
                        icao,
                        nident,
                        now_date,
                    ),
                )
            else:
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


def update_flight(
    flight,
    icao,
    ptype,
    distance,
    altitude,
    flight_level,
    speed,
    squawk,
    heading,
    reg,
    owner,
    category,
    baro_rate,
):

    if not flight:
        print("no flight, returning", icao)
        return

    alert_flights = []

    signs = get_call_signs()

    # Track all flights instead of just plane days
    if (
        "flights" in config
        and "all_flights" in config["flights"]
        and config["flights"]["all_flights"] in ["True", "true", "1"]
    ):
        call_sign = True
    else:
        call_sign = False

    for sign in signs:
        if re.search(f"^{sign}", flight):
            call_sign = True
    if not call_sign:
        if (flight, "tracking") not in alerted:
            alerted[(flight, "tracking")] = 1
            # logger.debug(f"Flight Tracking not enabled for this flight: {flight}")
        return

    (from_airport, to_airport, route_distance) = get_flight_data(flight, distance=distance, altitude=altitude, vs=baro_rate, force=call_sign)


    now = datetime.now()
    today = date.today()

    try:
        cur = conn.cursor()

        # Check plane types first because of holddown
        cur.execute("SELECT * FROM plane_types WHERE ptype=?", (ptype,))
        count = cur.fetchall()

        cur.execute("SELECT count(1) FROM flights WHERE flight=?", (flight,))
        count = cur.fetchall()[0][0]
        if count:
            rows = dict_gen(
                cur.execute("SELECT * FROM flights WHERE flight=?", (flight,))
            )
        else:
            rows = None
    except sqlite3.OperationalError as e:
        logger.warning(f"New Database: Trying to create DB: {e}")

        create_table(conn, sql_create_flights_table)
        conn.commit()

        # Setup indexes and initial DB
        commands = [
            "CREATE INDEX flights_flight_idx ON flights(flight);",
            "CREATE INDEX flights_icao_idx ON flights(icao);",
        ]
        for cmd in commands:
            print("DB Setup:", cmd)
            res = cur.execute(cmd)
        

        return

    # print(f'ic:{icao}, ident:{ident}, sq:{squawk}, pt:{ptype}, dist:{distance}, alt:{altitude}, head:{heading}, spd:{speed}')
    if not rows:
        dist_int = int(distance)
        route_type = get_route_type(route_distance)
        if route_type:
            flight_text = 'Flight ' + route_type
        else:
            flight_text = 'Flight'

        if flight in alert_flights:
            logger.warning(
                f"ALERT FLT    {flight:>8} ({ptype}) {category:<2} [{dist_int:>3}nm {flight_level:<5}] {reg:<6} {icao} {owner}"
            )
            play_sound("/Users/yantisj/dev/ads-db/sounds/ding.mp3")
            time.sleep(0.5)
            play_sound("/Users/yantisj/dev/ads-db/sounds/ding.mp3")
        else:
            logger.info(
                f"New {flight_text:<9} {flight:>7} ({ptype}) {category:<2} [{dist_int:>3}nm {flight_level:<5}] {flight:>7} {from_airport:>4}->{to_airport:<4}{route_distance:>5}nm {reg:<6} {icao} {owner}"
            )
        sql = """INSERT INTO flights(flight,icao,ptype,distance,closest,altitude,lowest_altitude,speed,lowest_speed,squawk,heading,registration,from_airport,to_airport,firstseen,lastseen,route_distance)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) """
        cur.execute(
            sql,
            (
                flight,
                icao,
                ptype,
                distance,
                distance,
                altitude,
                altitude,
                speed,
                speed,
                squawk,
                heading,
                reg,
                from_airport,
                to_airport,
                now,
                now,
                route_distance
            ),
        )
    else:
        for row in rows:
            try:
                # print('updating icao', icao, flight)

                low_dist = row["closest"]
                if not low_dist and distance:
                    low_dist = distance
                elif distance and distance < low_dist:
                    # print('New Low Distance', icao, distance)
                    low_dist = distance
                low_alt = row["lowest_altitude"]
                if not low_alt and altitude:
                    low_alt = altitude
                elif altitude and altitude < low_alt:
                    # print('New Low Altitude', icao, altitude)
                    low_alt = altitude
                low_speed = row["lowest_speed"]
                if not low_speed and speed:
                    low_speed = speed
                elif speed and speed < low_speed:
                    # print('New Low speed', icao, altitude)
                    low_speed = speed
                nsquawk = row["squawk"]
                if squawk:
                    nsquawk = squawk

                if row["lastseen"].date() < today:
                    # print('Lastseen date reset', row)
                    low_dist = distance
                    low_alt = altitude

                sql = """UPDATE flights SET icao=?, ptype=?, distance=?, closest=?, altitude=?, lowest_altitude=?, speed=?, lowest_speed=?, squawk=?, heading=?, registration=?, from_airport=?, to_airport=?, lastseen=?, route_distance=?
                    WHERE flight = ? """
                cur.execute(
                    sql,
                    (
                        icao,
                        ptype,
                        distance,
                        low_dist,
                        altitude,
                        low_alt,
                        speed,
                        low_speed,
                        nsquawk,
                        heading,
                        reg,
                        from_airport,
                        to_airport,
                        now,
                        route_distance,
                        flight,
                    ),
                )
                # if ptype in local_flights and (icao, today) not in alerted and altitude < local_altitude and distance < local_distance:
                #     alerted[(icao, today)] = 1
                #     logger.info(f"Local Alert: {icao} t:{ptype} d:{distance} alt:{altitude} id:{ident} site:{site}")
                #     play_sound("/Users/yantisj/dev/ads-db/sounds/ding.mp3")
                break

            except TypeError as e:
                logger.critical(
                    f"Error updating plane: {icao} t:{ptype} id:{flight} alt:{altitude}: {e}"
                )
                pass


def update_ptype(ptype, icao, mfr, model, lastseen=None):

    if not lastseen:
        lastseen = datetime.now()
    cur = conn.cursor()
    cur.execute("SELECT * FROM plane_types WHERE ptype=?", (ptype,))
    rows = cur.fetchall()
    new = False
    if not rows:
        new = True
        # logger.debug(f"!!!  New Type of Plane   !!!: {icao} t:{ptype}")
        sql = """INSERT INTO plane_types(ptype,last_icao,firstseen,lastseen,count,manufacturer,model)
              VALUES(?,?,?,?,?,?,?) """
        cur.execute(sql, (ptype, icao, lastseen, lastseen, 1, mfr, model))
    else:
        row = rows[0]
        cur.execute("SELECT category,status,model FROM planes WHERE ptype = ? AND NOT status = 'D'", (ptype,))
        rows = cur.fetchall()
        pcount = 0
        categories = defaultdict(int)
        models = defaultdict(int)
        active = 0
        inactive = 0
        for p in rows:
            pcount += 1
            if p[0]:
                categories[p[0]] += 1
            if p[1] == 'A' or p[1] == 'R' or not p[1]:
                active += 1
            else:
                inactive += 1
            if p[2]:
                models[p[2]] += 1

        perc_active = 100
        if active and inactive:
            perc_active = round(active/(active + inactive)*100)
            # Round down to 99% if any inactive
            if perc_active == 100:
                perc_active = 99

        # Get the top category if multiple categories for type level
        top_cat = "A0"
        if categories:
            top_cat = list(
                dict(
                    sorted(categories.items(), reverse=True, key=lambda item: item[1])
                ).items()
            )[0][0]
        if ptype in STATIC_CATEGORIES:
            top_cat = STATIC_CATEGORIES[ptype]

        top_model = model
        if models:
            top_model = list(
                dict(
                    sorted(models.items(), reverse=True, key=lambda item: item[1])
                ).items()
            )[0][0]
        # if top_model != model:
        #     print('Model Update', model, top_model)

        nmfr = row[5]
        if mfr:
            nmfr = mfr

        # Full data update
        if model and nmfr:
            sql = """UPDATE plane_types SET last_icao = ?, lastseen = ?, count = ?, manufacturer = ?, model = ?, category = ?, active = ?
                WHERE ptype = ? """
            cur.execute(sql, (icao, lastseen, pcount, nmfr, top_model, top_cat, perc_active, ptype))
        # Partial data, don't update type
        else:
            sql = """UPDATE plane_types SET last_icao = ?, lastseen = ?, count = ?
                WHERE ptype = ? """
            cur.execute(sql, (icao, lastseen, pcount, ptype))

    return new


def get_day_count(icao):

    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM plane_days WHERE icao = ?", (icao,))
    (count,) = cur.fetchone()
    return count


def get_flight_level(altitude):

    if altitude >= 18000:
        fl = round(altitude / 100)
        return f"FL{fl}"
    return altitude


def lookup_ptype(
    ptype, count=0, cat_min=0, low_alt=0, no_a0=False, military=True, hours=0, owner=None
):

    cur = conn.cursor()
    if owner:
        cur.execute(
            "SELECT * FROM planes WHERE ptype LIKE ? AND owner LIKE ? ORDER BY lastseen DESC", (ptype,owner,)
        )
    else:
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
                    elif cat and cat > 5:
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
        "IACO   TYPE  REG         IDENT      MODEL       CAT M CT  S DST MIN  ALT     LOW     COUNTRY     OWNER                FIRST                 LAST"
    )
    print(
        "----   ----  ----------  ---------  ----------- --- - --- - --- ---  -----   -----   ----------  -------------------- -------------------   -------------------"
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
        status = " "
        opcode = ""
        model = ""
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
        if row[19]:
            opcode = row[19][:9]
        if row[20]:
            status = row[20]
            if status == 'A':
                status = '.'
        if row[21]:
            model = row[21][:11]
        print(
            f"{row[0]:<5} {row[2]:<4}  {reg:11} {row[1]:<10} {model:<11} {cat:<3} {mlt:<1} {day_count:<3} {status:1} {distance:<3} {closest:<4} {altitude:<7} {alt_low:<7} {country:<10}  {owner:<20} {first:<10}   {last:<10}"
        )

    return total


def print_plane_days(rows, hours=0):

    total = 0
    print("")
    print(
        "IACO   TYPE  REG      FLIGHT       DST MIN  ALT     LOW     FIRST                 LAST"
    )
    print(
        "----   ----  -------  -------      --- ---  -----   -----   -------------------   -------------------"
    )

    hours_ago = None
    if hours:
        hours_ago = datetime.now() - timedelta(hours=hours)

    cur = conn.cursor()

    ptypes = dict()

    for row in rows:
        total += 1
        if hours_ago and row[12] < hours_ago:
            continue
        if row[0] not in ptypes:
            cur.execute(
                "SELECT ptype, registration from planes where icao = ?", (row[0],)
            )
            (ptype, reg) = cur.fetchone()
            ptypes[row[0]] = (ptype, reg)
        else:
            (ptype, reg) = ptypes[row[0]]

        altitude = 0
        alt_low = 0
        distance = 0
        closest = 0
        country = ""
        owner = ""
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
            f"{row[0]:<6} {ptype:<5} {reg:<8} {row[2]:<12} {distance:<3} {closest:<4} {altitude:<7} {alt_low:<7} {first:<10}   {last:<10}"
        )

    return total


def print_flights(rows, hours=0, low_alt=0, route_distance=0, airport=None):

    print(
        "\nFLIGHT#   FROM-->TO   DIST   TYPE  REGISTR    ICAO     CT DST  MIN   ALT     LOW     FIRST                 LAST"
    )
    print(
        "-------   ---- ----  ------  ----  --------   ------   --- --- --- -----   -----   --------------------  -------------------"
    )

    hours_ago = None
    if hours:
        hours_ago = datetime.now() - timedelta(hours=hours)

    day_count = 0
    count = 0
    for r in rows:
        if hours_ago and r["lastseen"] < hours_ago:
            continue
        if low_alt and r["lowest_altitude"] > low_alt:
            continue

        r_distance = 0
        try:
            r_distance = int(r["route_distance"])
        except TypeError:
            pass
        if route_distance and r_distance < route_distance:
            continue

        
        distance = int(r["distance"])
        closest = int(r["closest"])
        altitude = int(r["altitude"])
        lowest_altitude = int(r["lowest_altitude"])
        first = str(r["firstseen"]).split(".")[0]
        last = str(r["lastseen"]).split(".")[0]
        from_airport = str(r["from_airport"])
        to_airport = str(r["to_airport"])

        if airport and from_airport != airport and to_airport != airport:
            continue
    
        count += 1

        print(
            f"{r['flight']:<9} {from_airport:<4} {to_airport:<4}  {r_distance:>4}nm  {r['ptype']:<5} {r['registration']:<9}  {r['icao']:<6}   {day_count:<3} {distance:<3} {closest:<3} {altitude:<7} {lowest_altitude:<7} {first:<10}   {last:<10}"
        )

    return count


def lookup_ptypes(ptype, hours=0, mfr=None):

    cur = conn.cursor()
    if mfr:
        rows = dict_gen(
            cur.execute(
                "SELECT * FROM plane_types WHERE manufacturer LIKE ? ORDER BY count DESC",
                (mfr,),
            )
        )
    else:
        rows = dict_gen(
            cur.execute(
                "SELECT * FROM plane_types WHERE ptype LIKE ? ORDER BY count DESC", (ptype,)
            )
        )
    total = 0
    planes = 0
    print(
        "\nMFR          TYPE CL  CNT  L_ICAO  MODEL           LIVE FIRST SEEN            LAST SEEN"
    )
    print(
        "--------     ---- --  ---  ------  --------------  ---- --------------------  -------------------"
    )
    hours_ago = None
    if hours:
        hours_ago = datetime.now() - timedelta(hours=hours)

    for row in rows:
        if hours_ago and row[3] < hours_ago:
            continue
        cnt = 1
        if row['count']:
            cnt = row['count']
        mfr = ""
        if row['manufacturer']:
            mfr = row['manufacturer'][:12]
        total += 1
        planes += cnt
        first = str(row['firstseen']).split(".")[0]
        last = str(row['lastseen']).split(".")[0]
        model = row['model']
        active = ""
        if row['active']:
            active = str(row['active'])
            if row['active'] >= 100:
                active = ' - '
            else:
                active = active + '%'
        if model:
            model = model[:16]
        category = ""
        if row['category']:
            category = row['category']
        print(
            f"{mfr:<12} {row['ptype']:<4} {category:<2}  {cnt:<4} {row['last_icao']:<6}  {model:<16} {active:>3} {first:<10}   {last:<10}"
        )

    if total > 1:
        print(f"\nAircraft Types: {total} / Total Aircraft: {planes}")


def lookup_flight(flight, hours=0, low_alt=0, route_distance=0, airport=None):

    cur = conn.cursor()
    rows = dict_gen(
        cur.execute(
            "SELECT * FROM flights WHERE flight LIKE ? ORDER BY lastseen DESC",
            (flight,),
        )
    )
    total = print_flights(rows, hours=hours, low_alt=low_alt, route_distance=route_distance, airport=airport)
    if total == 1:
        cur.execute(
            "SELECT * FROM plane_days WHERE ident = ? ORDER BY lastseen DESC", (flight,)
        )
        rows = cur.fetchall()
        print_plane_days(rows, hours=hours)


def lookup_icao(icao):

    cur = conn.cursor()
    cur.execute("SELECT * FROM planes WHERE icao = ?", (icao,))
    rows = cur.fetchall()
    total = print_planes(rows)
    if total == 1:
        cur.execute(
            "SELECT * FROM plane_days WHERE icao = ? ORDER BY lastseen DESC", (icao,)
        )
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
    cur.execute("SELECT * FROM planes WHERE registration LIKE ? ORDER BY lastseen DESC", (reg,))
    rows = cur.fetchall()
    total = print_planes(rows)
    if total == 1:
        row = rows[0]
        cur.execute(
            "SELECT * FROM plane_days WHERE icao = ? ORDER BY lastseen DESC", (row[0],)
        )
        rows = cur.fetchall()
        print_plane_days(rows)
    else:
        print("\nTotal:", total)


def lookup_model_mfr(icao):

    cur = lookup.cursor()
    # cur.execute("SELECT * FROM Aircraft LEFT JOIN Model ON Aircraft.ModelID = Model.ModelID LEFT JOIN Operator ON Aircraft.OperatorID = Operator.OperatorID WHERE Aircraft.Icao = ?", (icao,))
    # cur.execute("select Icao,Engines,Model,Manufacturer from AircraftTypeView WHERE Icao = ? LIMIT 1;", (ptype,))
    cur.execute(
        "SELECT ModeS,OperatorFlagCode,CurrentRegDate,ModeSCountry,Country,AircraftClass,Engines,PopularName,Manufacturer,Type,RegisteredOwners,Registration,ICAOTypeCode,Status,OperatorFlagCode,SerialNo FROM Aircraft WHERE ModeS = ?",
        (icao,),
    )
    rows = cur.fetchall()
    # mfr = None
    # pname = None
    # country = None
    # owner = None
    # ptype = None
    # military = None
    # reg = None

    ptype = ""
    reg = ""
    country = ""
    model = ""
    mfr = ""
    owner = ""
    military = "."
    status = ""
    opcode = ""
    serial = ""

    for r in rows:
        # print(r)
        mfr = r[8]
        if mfr:
            mfr = mfr.title()
        model = r[9]
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

        ptype = r[12]
        status = r[13]
        opcode = r[14]
        serial = r[15]

        model = model[:50]

        if re.search(r"United\sStates", country):
            country = "USA"

    return (ptype, mfr, model, country, owner, military, reg, status, opcode, serial)


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
    reg,
):
    global sounds
    today = date.today()
    local_types = list()

    # Only alert on A3+ flights or flights that don't report
    alert_size = 3
    if "landing_size" in config["alerts"]:
        alert_size = int(config["alerts"]["landing_size"])
    size = alert_size

    parent_cat = get_category(ptype)
    if category and parent_cat and parent_cat != category and (icao, 'catmiss') not in alerted:
        alerted[(icao, 'catmiss')] = 1
        logger.debug(f"Category Mistmatch    ({ptype}) {parent_cat} (vs {category}) {icao}")
    if parent_cat:
        category = parent_cat

    if category and re.search("^A\d", category):
        size = int(category[1])
    if "local_planes" in config["alerts"]:
        local_types = config["alerts"]["local_planes"].split(",")
        if ptype in local_types and size < alert_size:
            size = alert_size

    # print(f'ic:{icao}, ident:{ident}, sq:{squawk}, pt:{ptype}, dist:{distance}, alt:{altitude}, head:{heading}, spd:{speed}')
    if icao and distance and heading and speed and altitude:
        try:
            # and heading < 365 and heading > 240
            if (
                distance < 3.7
                and lat < 32.777 # 3.8
                and lon < -79.895  # -79.9
                and baro_rate < 1000
                and altitude > 1000
                and altitude < 4500
                and speed < 300
                and speed > 130
                and (heading > 280 or heading < 30)
            ):
                if (icao, ident, today) not in alerted:
                    alerted[(icao, ident, today)] = 1
                    flight_level = get_flight_level(altitude)
                    dist_int = int(distance)
                    (from_airport, to_airport, route_distance) = get_flight_data(ident, distance=distance, altitude=altitude, vs=baro_rate)

                    # Only alert on larger sizes or tracked types, otherwise log landing
                    if size >= alert_size or re.search(r'^BOE', ident):
                        logger.warning(
                            f"Landing Alert {reg:>7} ({ptype:<4}) {category:<2} [{dist_int:>3}nm {flight_level:<5}] {ident:>7} {from_airport:>4}->{to_airport:<4}{route_distance:>5}nm lat:{lat} lon:{lon}"
                        )
                        if sounds and check_quiet_time():
                            play_sound("/Users/yantisj/dev/ads-db/sounds/ding-low.mp3")
                            if size == 5 or ptype in local_types or re.search(r'^BOE', ident):
                                play_sound(
                                    "/Users/yantisj/dev/ads-db/sounds/ding-low-fast.mp3"
                                )
                    # Log details on all landing aircraft
                    logger.info(
                        f"Plane Landing {ident:>7} ({ptype:<4}) {category:<2} [{dist_int:>3}nm {flight_level:<5}] {ident:>7} {from_airport:>4}->{to_airport:<4}{route_distance:>5}nm {reg:<6} {icao} s:{speed} vs:{baro_rate} h:{heading} lat:{lat} lon:{lon}"
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
                today = date.today()
                if (icao, ident, today) not in ptype_alerted:
                    ptype_alerted[(icao, ident, today)] = 1
                    logger.warning(
                        f"!!!Boeing 787 Airborne!!!  ic:{icao}, ident:{ident}, sq:{squawk}, pt:{ptype}, dist:{distance}, alt:{altitude}, head:{heading}, spd:{speed}"
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
                    f"http://{site}/skyaware/data/aircraft.json", timeout=5
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
                            logger.warning(f"Located Plane!!: {ident} from {site}")
                            lookup_icao(icao)
                            play_sound("/Users/yantisj/dev/ads-db/sounds/ding.mp3")
                            time.sleep(3)
                            play_sound("/Users/yantisj/dev/ads-db/sounds/ding.mp3")
                            tracking.remove(ident)

        time.sleep(3)


def mark_dups():
    "Check for duplicate serials and registration counts"

    cur = conn.cursor()
    rows = dict_gen(
        cur.execute(
            'SELECT * FROM planes ORDER BY lastseen DESC'
        )
    )

    planes = dict()
    count = 0
    reg_count = 0
    noserial = 0
    year_ago = datetime.now() - timedelta(days=365)

    for r in rows:
        if not r['serial']:
            noserial += 1
            continue
        elif (r['ptype'], r['serial']) not in planes:
            planes[(r['ptype'], r['serial'])] = r
        else:
            count += 1
            n = planes[(r['ptype'], r['serial'])]

            if r['registration'] != n['registration']:
                reg_count += 1

            # print('OLD REG!', r)
            n = planes[(r['ptype'], r['serial'])]
            
            # print('LATEST!', planes[(r['ptype'], r['serial'])])
            if r['lastseen'] < n['firstseen'] or r['lastseen'] < year_ago:
                if r['status'] != 'D':
                    logger.info(f"Deactivate Registration ({r['ptype']}): nr:{n['registration']} ns:{n['serial']} nl:{n['lastseen']} VS or:{r['registration']} os:{r['serial']} ol:{r['lastseen']}")
                    degregister_plane(r, n)
                else:
                    print(f"Registration Dup Deactive ({r['ptype']}): nr:{n['registration']} ni:{n['icao']} ns:{n['serial']} nl:{n['lastseen']} VS or:{r['registration']} ni:{n['icao']} os:{r['serial']} ol:{r['lastseen']}")
            else:
                print(f"ICAO Toggling ({r['ptype']}): nr:{n['registration']} ni:{n['icao']} ns:{n['serial']} nl:{n['lastseen']} VS or:{r['registration']} ni:{n['icao']} os:{r['serial']} ol:{r['lastseen']}")

    print('Total Duplicate Serials:', count, 'Reg Mismatch:', reg_count, 'No serial:', noserial)
    conn.commit()


def degregister_plane(old, new):
    "Deregister plane/icao"

    cur = conn.cursor()
    cur.execute(
            'UPDATE planes SET status = "D" WHERE icao = ?', (old['icao'],)
        )
    

def get_db_stats():

    cur = conn.cursor()
    cur.execute(
        'SELECT icao,ptype,lastseen "[timestamp]", firstseen "[timestamp]" FROM planes ORDER BY lastseen ASC'
    )

    day_ago = datetime.now() - timedelta(days=1)
    month_ago = datetime.now() - timedelta(days=30)

    rows = cur.fetchall()

    cur.execute(
        'SELECT flight,ptype,lastseen "[timestamp]", firstseen "[timestamp]" FROM flights ORDER BY lastseen ASC'
    )

    all_flights = cur.fetchall()

    types = defaultdict(int)
    ttypes = dict()
    ttypes_day = dict()
    ttypes_mon = dict()
    ttypes_new = dict()
    ttypes_old = dict()
    total = 0
    total_day = 0
    total_mon = 0
    total_new = 0
    type_count = 0
    type_count_day = 0
    type_count_mon = 0
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
        if row[2] > month_ago:
            total_mon += 1
            if ptype not in ttypes_mon:
                ttypes_mon[ptype] = 1
                type_count_mon += 1
        if row[3] > day_ago:
            total_new += 1
            if ptype not in ttypes_new and ptype not in ttypes_old and ptype:
                ttypes_new[ptype] = 1
                type_count_new += 1

    flights = defaultdict(int)
    fflights = dict()
    fflights_day = dict()
    fflights_mon = dict()
    fflights_new = dict()
    fflights_old = dict()
    ftotal = 0
    ftotal_day = 0
    ftotal_mon = 0
    ftotal_new = 0
    flight_count = 0
    flight_count_day = 0
    flight_count_mon = 0
    flight_count_new = 0

    for row in all_flights:
        ftotal += 1
        last_seen = str(row[2]).split(".")[0]

        flight = row[0]
        flights["TOTL"] += 1
        flights[flight] += 1
        if flight not in fflights:
            fflights[flight] = 1
            flight_count += 1
            flights[icao] = type_count
        if row[3] < day_ago:
            if flight not in fflights_old:
                fflights_old[flight] = 1
        if row[2] > day_ago:
            ftotal_day += 1
            if flight not in fflights_day:
                fflights_day[flight] = 1
                flight_count_day += 1
        if row[2] > month_ago:
            ftotal_mon += 1
            if flight not in fflights_mon:
                fflights_mon[flight] = 1
                flight_count_mon += 1
        if row[3] > day_ago:
            ftotal_new += 1
            if flight not in fflights_new and flight not in fflights_old and flight:
                fflights_new[flight] = 1
                flight_count_new += 1

    print(f"\n   ADS-DB Stats:                          [{last_seen}]")
    print("=================================================================")
    print(
        f" Flight Numbers: {flight_count:<6,}  30days: {flight_count_mon:<6,}  24hrs: {flight_count_day:<6,} New: {flight_count_new:<3,}"
    )

    print(
        f"   Total Planes: {total:<6,}  30days: {total_mon:<6,}  24hrs: {total_day:<6,} New: {total_new:<4,}"
    )
    print(
        f"     Hull Types: {type_count:<6,}  30days: {type_count_mon:<6,}  24hrs: {type_count_day:<6} New: {type_count_new:<3}"
    )
    print()
    return types


conn = None


def check_quiet_time():
    now = datetime.now()
    # print('time now', now.hour)
    if now.hour > 21 or now.hour < 8:
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


def get_call_signs():
    "merge all call signs"

    if "flights" in config and "all_call_signs" in config["flights"]:
        signs = config["flights"]["call_signs"].split(",")
    else:
        signs = STATIC_CALL_SIGNS

    if "flights" in config and "extra_call_signs" in config["flights"]:
        for sign in config["flights"]["extra_call_signs"].split(","):
            signs.append(sign)
    return signs


def get_route_type(route_distance):
    "Route type by distance (requires flight data)"

    route_type = ""
    if route_distance > 7500:
        route_type = "UR"
    if route_distance > 5000:
        route_type = "XR"
    elif route_distance > 3000:
        route_type = "LR"
    elif route_distance > 2000:
        route_type = "ER"
    # elif route_distance < 600:
    #     route_type = "SH"

    return route_type


def get_flight_data(flight, distance=0, altitude=0, vs=0, force=False):

    global flight_conn
    global local_fixed
    global local_correct

    route_distance = 0

    # In Memory Cache
    if flight in local_flights:
        return local_flights[flight]

    # Check local flight DB cache populated via API
    (from_airport, to_airport, route_distance) = flight_cache_check(flight)
    if from_airport and to_airport:
        local_flights[flight] = (from_airport, to_airport, route_distance)
        return (from_airport, to_airport, route_distance)

    # Local Airport DST/SRC detection via altitude and distance
    if "local_airport" in config["flights"]:
        LOCAL_AIRPORT = config["flights"]["local_airport"]

    # force = known call sign
    if not flight_conn and not force:
        return (from_airport, to_airport, route_distance)

    if not flight_conn and not LOCAL_AIRPORT and 'flightaware_api' not in config['db'] :
        return (from_airport, to_airport, route_distance)

    if flight_conn:
        cur = flight_conn.cursor()
        rows = dict_gen(
            cur.execute("SELECT * FROM RouteView WHERE Callsign = ?", (flight,))
        )
        for row in rows:
            if row:
                from_airport = str(row["fromairporticao"])
                to_airport = str(row["toairporticao"])
                break

    # Local Flight, fix flight numbers if incorrect
    update = False
    if LOCAL_AIRPORT and (from_airport or force) and distance and altitude:
        if distance < 20 and altitude < 4500:
            vs_range = False
            if vs > 500:
                vs_range = True
                if from_airport != 'KCHS':
                    from_airport = 'KCHS'
                    update = True
            elif vs < -500:
                vs_range = True
                if to_airport != 'KCHS':
                    to_airport = 'KCHS'
                    update = True
            # else:
            #     print('watching vs', flight, vs)
            if update:
                local_flights[flight] = (from_airport, to_airport, route_distance)
                local_fixed += 1
                broken = round(local_fixed / (local_correct + local_fixed)*100)
                logger.debug(f"Local Flight Fix {flight} ({broken}%): {from_airport} <-> {to_airport} {vs}")

            elif vs_range:
                local_flights[flight] = (from_airport, to_airport, route_distance)
                local_correct += 1
                broken = round(local_fixed / (local_correct + local_fixed)*100)
                logger.debug(f"Local Flight OK {flight} ({broken}%): {from_airport} <-> {to_airport}")

    if 'flightaware_api' in config['db'] and force:
        (from_airport, to_airport, route_distance) = flight_api_lookup(flight, from_airport, to_airport)
        local_flights[flight] = (from_airport, to_airport, route_distance)

    return (from_airport, to_airport, route_distance)


def flight_api_lookup(flight, from_airport="", to_airport=""):
    'Attempt API Lookup on Flight'
    global api_count

    AEROAPI = requests.Session()
    AEROAPI.headers.update({"x-apikey": config['db']['flightaware_api']})

    flightd = None
    now = datetime.now()
    cur = conn.cursor()

    # Cache Check
    (from_airport, to_airport, route_distance) = flight_cache_check(flight)
    if from_airport and to_airport:
        return (from_airport, to_airport, route_distance)

    max_api_count = MAX_API_COUNT
    if 'max_api_count' in config["db"]:
        max_api_count = int(config["db"]["max_api_count"])

    # Attempt API Lookup
    if api_count > max_api_count:
        logger.warning(f'API Count Limit!!! {api_count}')
        return (from_airport, to_airport, route_distance)

    try:
        api_count += 1
        result = AEROAPI.get(f"{AEROAPI_BASE_URL}/flights/{flight}")
        if result.status_code != 200:
            if result.status_code == 429:
                logger.info(f"FlightAware API Throttled ({api_count}): 429")
            else:
                logger.warning(f"API Error {result.status_code}: {flight}")
        else:
            flightdict = result.json()
            flightd = None
            if "flights" in flightdict:
                # print(json.dumps(flightdict, sort_keys=True, indent=4, separators=(',', ': ')))
                for flight_entry in flightdict['flights']:
                    if flight_entry['actual_off']:
                        flightd = flight_entry
                        # print('ACTUAL!!!!')
                        # print(json.dumps(flightd, sort_keys=True, indent=4, separators=(',', ': ')))
                        break
            else:
                logger.debug(f'No lookup: {flight}')
                # print(json.dumps(flightdict, sort_keys=True, indent=4, separators=(',', ': ')))

            if flightd:
                from_airport = flightd['origin']['code']
                to_airport = flightd['destination']['code']
                route_distance = flightd['route_distance']
                logger.debug(f'FA API Lookup {flight:>7} ({api_count}): {from_airport} -> {to_airport} d:{route_distance}')
                # print(from_airport, to_airport, now, flight,)
                cur.execute("INSERT INTO flight_cache (flight,from_airport,to_airport,distance,firstseen,lastseen) VALUES(?,?,?,?,?,?)", (flight, from_airport, to_airport, route_distance, now, now,))  
                # cur.execute("UPDATE flight_cache SET from_airport = ?, to_airport = ?, firstseen = ? WHERE flight = ?", (from_airport, to_airport, now, flight,))

    except Exception as e:
        print(flightd)
        logger.warning(f"FA API Lookup Failed: {e}")

    # Throttle API Lookups
    time.sleep(5)
    return (from_airport, to_airport, route_distance)


def flight_cache_check(flight):
    "Get any entry from flight cache"

    cur = conn.cursor()
    rows = dict_gen(
        cur.execute("SELECT * FROM flight_cache WHERE flight = ?", (flight,))
    )
    if rows:
        for row in rows:
            from_airport = row['from_airport']
            to_airport = row['to_airport']
            route_distance = row['distance']
            # logger.debug(f'Flight API Cache Hit {flight}: {from_airport} -> {to_airport} {route_distance}nm')
            return (from_airport, to_airport, route_distance)
    
    return ("", "", 0)


def get_category(ptype):

    cur = conn.cursor()
    rows = dict_gen(cur.execute("SELECT category from plane_types WHERE ptype = ?", (ptype,)))
    category = 'A0'
    for row in rows:
        category = row['category']

    if not category:
        category = ""
    return category  


def update_missing_data():
    "Refresh all data from databases on missing info"

    cur = conn.cursor()
    cur2 = conn.cursor()
    now = datetime.now()
    rows = dict_gen(cur.execute("SELECT * FROM planes ORDER BY lastseen ASC"))
    # rows = cur.fetchall()
    count = 0
    models = dict()
    ptyped = dict()
    ptype_count = defaultdict(int)

    for row in rows:
        update = False
        # print(row)
        icao = row["icao"]
        (
            ptype,
            mfr,
            model,
            country,
            owner,
            military,
            reg,
            status,
            opcode,
            serial
        ) = lookup_model_mfr(icao)
        if model:
            models[ptype] = model
        ptype_count[ptype] += 1
        if ptype and ptype not in ptyped:
            ptyped[ptype] = (mfr, model)
        if owner and owner != row["owner"]:
            logger.info(f"Owner needs updating: {icao} {owner}")
            update = True
        elif ptype and ptype != row["ptype"]:
            logger.info(f'Ptype needs updating: {ptype} vs {row["ptype"]}')
            update = True
        elif reg and reg != "None" and reg != row["registration"]:
            logger.info(f"Registration needs updating: {reg} vs {row['registration']}")
            update = True
        elif status and status != row["status"]:
            update = True
            if status != 'A':
                if row["status"] == 'R':
                    update = False
                    logger.warning(f"DB Has active plane as out of service: {ptype} {icao}")
                    status = 'R'
                elif row["status"] == 'D':
                    update = False
                    logger.warning(f"DB Has Deactive Registration as {status}: {ptype} {icao}")
                    status = 'D'
                else:
                    logger.warning(f"Plane Retired / Out of Service ({status}): {ptype} {model} {reg} {owner} {row['lastseen']}")
            elif row["status"] == 'D':
                update = False
                logger.warning(f"DB Has Deactive Registration as {status}: {ptype} {icao}")
                status = 'D'
        elif opcode and opcode != row['opcode']:
            logger.info(f'Opcode needs updating ({ptype}): {opcode}')
            update = True
        elif model and model != row['model']:
            logger.info(f"Model Update {model} ({ptype})")
            update = True
        elif serial and serial != row['serial']:
            logger.info(f"Serial Update {icao} {serial}")
            update = True
        if update:
            if ptype:
                cur2.execute("SELECT * FROM plane_types WHERE ptype=?", (ptype,))
                ptypes = cur2.fetchall()
                new = False
                if not ptypes:
                    new = True
                    logger.warning(
                        f"!!    New Hull Type    !!: t:{ptype} m:{model} {icao} "
                    )
                    sql = """INSERT INTO plane_types(ptype,last_icao,firstseen,lastseen,count,manufacturer,model)
                        VALUES(?,?,?,?,?,?,?) """
                    cur2.execute(sql, (ptype, icao, now, now, 1, mfr, model))

            count += 1
            sql = """UPDATE planes SET ptype = ?, registration = ?, country = ?, owner = ?, military = ?, status = ?, opcode = ?, model = ?, serial = ?
                WHERE icao = ? """
            cur2.execute(sql, (ptype, reg, country, owner, military, status, opcode, model, serial, icao))

    cur4 = conn.cursor()
    # Update missing fields and remove bad plane type objects
    rows = dict_gen(cur.execute("SELECT * from plane_types"))
    for row in rows:
        if row["ptype"] not in ptyped:
            print("Missing ptype", row["ptype"])
            # cur4.execute("DELETE FROM plane_types WHERE ptype = ?", (row["ptype"],))
        # elif not row["active"]:
        #     count += 1
        #     update_ptype(row["ptype"], row["last_icao"], row["manufacturer"], row["model"], lastseen=row["lastseen"])
        # elif row["model"] != models[ptype]:
        #     count += 1
        else:
            update_ptype(row["ptype"], row["last_icao"], row["manufacturer"], row["model"], lastseen=row["lastseen"])
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
    plane_count = 0
    new = False
    global sounds
    first_run = False
    fail_count = defaultdict(int)
    while True:
        cdict_counter += 1
        plane_count = 0
        for site in sites:
            try:
                r = requests.get(
                    f"http://{site}/skyaware/data/aircraft.json", timeout=5
                )
                planes = r.json()
                fail_count[site] = 0

                for p in planes["aircraft"]:

                    if "hex" in p and p["hex"] and "lat" in p and p["lat"]:
                        plane_count += 1

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

                        (
                            ptype,
                            mfr,
                            model,
                            country,
                            owner,
                            military,
                            reg,
                            status,
                            opcode,
                            serial
                        ) = lookup_model_mfr(icao)
                        flight_level = get_flight_level(altitude)
                        dist_int = int(distance)

                        # Reactivate airframes that were marked parked/retired and cache
                        if not status:
                            status = 'A'
                        elif status != 'A':
                            if (icao) not in reactivated:
                                if not flight and holddown[(icao, 'reactivate')] < 5:
                                    holddown[(icao, 'reactivate')] += 1
                                else:
                                    reactivated[icao] = 1
                                    model_str = model[:6]
                                    logger.warning(f"Reactivate ({status}) {model_str:>6} ({ptype:>4}) {category:<2} [{dist_int:>3}nm {flight_level:<5}] {flight:>7} {reg} {country} {owner} {mfr} {icao} site:{site}")
                                    play_sound(
                                        "/Users/yantisj/dev/ads-db/sounds/ding.mp3"
                                    )
                            status = 'R'

                        # print(f'{icao} {reg} {ptype} {flight} {category} {squawk} {lat} {lon} {altitude} {heading} {distance} {speed}')

                        # Play sounds on emergency bit set
                        if "emergency" in p:
                            if p["emergency"] and p["emergency"] != "none":
                                if (icao, 'emerg') not in alerted:
                                    alerted[(icao, 'emerg')] = 1
                                    logger.critical(
                                        f'Emergency Bit Set! {p["emergency"]}: i:{icao} r:{reg} t:{ptype} f:{flight} c:{category} a:{altitude} h:{heading} d:{distance} s:{speed}'
                                    )
                                    play_sound(
                                        "/Users/yantisj/dev/ads-db/sounds/warnone.mp3"
                                    )
                                    play_sound(
                                        "/Users/yantisj/dev/ads-db/sounds/warntwo.mp3"
                                    )

                        # Plane days and flight tracking require ident set
                        if flight:
                            update_flight(
                                flight,
                                icao,
                                ptype,
                                distance,
                                altitude,
                                flight_level,
                                speed,
                                squawk,
                                heading,
                                reg,
                                owner,
                                category,
                                baro_rate,
                            )
                            # Only update plane_days if flight info
                            update_plane_day(
                                icao,
                                flight,
                                squawk,
                                ptype,
                                distance,
                                altitude,
                                flight_level,
                                heading,
                                speed,
                                reg,
                                category,
                                site,
                                owner,
                                baro_rate
                            )
                        update_plane(
                            icao,
                            flight,
                            squawk,
                            ptype,
                            model,
                            distance,
                            altitude,
                            flight_level,
                            heading,
                            speed,
                            reg,
                            country,
                            owner,
                            military,
                            category,
                            site,
                            mfr,
                            status,
                            opcode,
                            serial
                        )
                        if ptype:
                            new = update_ptype(ptype, icao, mfr, model)
                            model_str = model[:11]
                            if new:
                                logger.warning(
                                    f"!! NEW HULL TYPE !!   ({ptype:<4}) {category:<2}: {mfr} {model_str:<8} r:{reg} fl:{flight} c:{country} o:{owner} d:{distance} {icao}"
                                )
                                if sounds and check_quiet_time():
                                    play_sound(
                                        "/Users/yantisj/dev/ads-db/sounds/ding-high.mp3"
                                    )
                                    play_sound(
                                        "/Users/yantisj/dev/ads-db/sounds/ding-high.mp3"
                                    )
                        else:
                            if (icao, "notype") not in alerted:
                                alerted[(icao, "notype")] = 1
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
                                reg,
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
                fail_count[site] += 1
                if fail_count[site] <= 5:
                    logger.warning(f"ConnectionError {site}: {e}")
                pass
            except Exception as e:
                fail_count[str(e)] += 1
                if fail_count[str(e)] <= 5:
                    logger.critical(f"General Update Exception {site}: {e}")
                time.sleep(1)
                pass
        if not first_run:
            first_run = True
            logger.info(f"Daemon Started: Received {plane_count} planes")
        if cdict_counter > save_cycle:
            cdict_counter = 0
            if save_cycle > 50:
                logger.info("Committing Data to DB")
            conn.commit()

        time.sleep(refresh)


def load_fadb():
    global lookup

    # https://data.flightairmap.com/
    lookup = create_connection(config["db"]["base_station"])


def read_config(config_file):
    "Read in config and setup config dict"
    global sounds

    logger.debug("Reading config file")

    config = configparser.ConfigParser()
    config.read(config_file)
    return config


def sigterm_handler(_signo, _stack_frame):
    "Catch Kill Signal and Close Database"
    global saving_db

    # Throttle kill calls and only save once on exit
    if not saving_db:
        saving_db = True
        logger.warning("Caught SIGTERM, closing DB")
        conn.commit()
        logger.debug("DB Saved, exiting")
        sys.exit(0)
    else:
        logger.info("Caught SIGTERM, Still Saving Data to Disk")


def sighup_handler(_signo, _stack_frame):
    "Save DB to Disk on sighup"

    logger.info("Saving DB to Disk")
    conn.commit()


signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGHUP, sighup_handler)


check_quiet_time()

parser = argparse.ArgumentParser(description="Save ADSB Data to SQLite")
parser.add_argument("-D", action="store_true", help="Run Daemon")
parser.add_argument(
    "-rs", type=str, help="Receiver IP List separated by commas (default 127.0.0.1)"
)
parser.add_argument("-st", action="store_true", help="Database Stats")
parser.add_argument("-lf", type=str, help="Lookup Flights by Name (DALpercent)")
parser.add_argument("-lt", type=str, help="Lookup Planes by Type (B77percent)")
parser.add_argument(
    "-lts", type=str, help="Lookup Type Totals by type (use percent-sign for all type)"
)
parser.add_argument("-li", type=str, help="Lookup Plane by IACO")
parser.add_argument("-ld", type=str, help="Lookup Planes by IDENT/Flight #s")
parser.add_argument("-lr", type=str, help="Lookup Planes by Registration")
parser.add_argument("-lm", type=str, help="Lookup Planes by Manufacturer")
parser.add_argument("-lo", type=str, help="Lookup Planes by Owner")
parser.add_argument("-af", type=str, help="Alert on Flight Name / ICAO")
parser.add_argument(
    "-ad", type=int, help="Alert Distance from Receiver (default unlimited)"
)
parser.add_argument("-fa", type=int, help="Filter Low Altitude")
parser.add_argument("-fac", type=str, help="Airport Code")
parser.add_argument("-fc", type=int, help="Filter Category Above int (A[3])")
parser.add_argument("-fc0", action="store_true", help="Filter A0 no categories")
parser.add_argument("-fm", action="store_true", help="Filter Military Planes")
parser.add_argument("-fh", type=float, help="Filter by hours since seen")
parser.add_argument("-fd", type=int, help="Filter by Days Seen Above Count")
parser.add_argument("-frd", type=int, help="Filter by Route Distance")
parser.add_argument("-S", action="store_true", help="Play Sounds")
parser.add_argument("-rf", type=int, help="Refresh Interval (default 10sec)")
parser.add_argument("-db", type=str, help="Different Database File")
parser.add_argument(
    "-sc", type=int, help="Save Cycle (increase > 10 to reduce disk writes)"
)
parser.add_argument("-v", action="store_true", help="Debug Mode")
parser.add_argument(
    "--update_db", action="store_true", help="Update all planes with latest DB info"
)
parser.add_argument(
    "--mark_dups", action="store_true", help="Check for duplicate serial numbers"
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

database_file = "./sqb/ads-db-planes.sqb"
if args.db:
    database_file = "./sqb/" + args.db

# Connect to database
conn = connect_ads_db(database_file)
if "standing_data" in config["db"]:
    flight_conn = sqlite3.connect(
        config["db"]["standing_data"],
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )
if args.S:
    if not sounds:
        logger.debug("Enabling Sounds")
        sounds = True

if args.sc:
    save_cycle = args.sc
    logger.debug(f"Increasing Save Cycle to {args.sc}")

if args.lo and not args.lt:
    args.lt = '%'

if args.update_db:
    load_fadb()
    logger.info("Updating All Plane Data")
    update_missing_data()
elif args.cleanup_db:
    cleanup_db()
elif args.mark_dups:
    mark_dups()

elif args.D:
    load_fadb()

    if config["alerts"]["sounds"] in ["true", "True", "1"] and not sounds:
        logger.debug("Enabling Sounds")
        sounds = True

    sites = ["127.0.0.1"]
    refresh = 10
    if args.rs:
        sites = args.rs.split(",")
    if args.rf:
        refresh = args.rf
    # Run Daemon
    try:
        logger.debug("Daemon Starting")
        run_daemon(refresh=refresh, sites=sites)
    except KeyboardInterrupt:
        logger.info("Closing Database")
        conn.commit()
elif args.st:
    get_db_stats()
    exit()

elif args.lt:
    count = 0
    hours = 0
    owner = args.lo
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
        owner=owner
    )
elif args.lts:
    hours = 0
    if args.fh:
        hours = args.fh
    lookup_ptypes(args.lts, hours=hours)
elif args.lf:
    hours = 0
    low_alt = 0
    if args.fh:
        hours = args.fh
    if args.fa:
        low_alt = args.fa
    lookup_flight(args.lf, hours=hours, low_alt=low_alt, route_distance=args.frd, airport=args.fac)
elif args.lm:
    hours = 0
    if args.fh:
        hours = args.fh
    lookup_ptypes(None, mfr=args.lm)
elif args.li:
    lookup_icao(args.li)
elif args.ld:
    lookup_ident(args.ld)
elif args.lr:
    lookup_reg(args.lr)
elif args.lm:
    hours = 0
    if args.fh:
        hours = args.fh
    lookup_ptypes(None, mfr=args.lm)
elif args.af:
    sites = ["127.0.0.1"]
    refresh = 10
    if args.rs:
        sites = args.rs.split(",")
    alert_ident(args.af, sites=sites, min_distance=args.ad)
else:
    parser.print_help()

print()
