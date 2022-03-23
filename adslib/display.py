# Print/Display Routines
from collections import defaultdict
from datetime import date, datetime, timedelta
from .helpers import dict_gen
import logging

logger = logging.getLogger('ads-display')

conn = None


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

