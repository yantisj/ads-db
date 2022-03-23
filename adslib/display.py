# Print/Display Routines
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
