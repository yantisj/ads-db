# Landing routine for customization (TBC)
import re
from datetime import date, datetime, timedelta
import logging

logger = logging.getLogger(__name__)

config = dict()
alerted = dict()


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
    reg
):
    global sounds
    today = date.today()


    # Only alert on A3+ flights or flights that don't report
    size = 3
    if category and re.search('^A\d', category):
        size = int(category[1])
    if "local_planes" in config["alerts"]:
        local_types = config["alerts"]["local_planes"].split(",")
        if ptype in local_types and size < 3:
            size = 3

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
                if (icao, ident, today) not in alerted:
                    alerted[(icao, ident, today)] = 1
                    flight_level = get_flight_level(altitude)
                    dist_int = int(distance)
                    (from_airport, to_airport) = get_flight_data(ident)

                    # Only alert on larger sizes or tracked types, otherwise log landing
                    if size >= 3:
                        logger.warning(
                            f"Landing Alert {ident:>7} ({ptype:<4}) {category:<2} [{dist_int:>3}nm {flight_level:<5}] {ident:>7} {from_airport:>4}<>{to_airport:<4} {reg:<6} {icao} lat:{lat} lon:{lon}"
                        )
                        if sounds and check_quiet_time():
                            play_sound("/Users/yantisj/dev/ads-db/sounds/ding-low.mp3")
                            if size == 5:
                                play_sound("/Users/yantisj/dev/ads-db/sounds/ding-low-fast.mp3")
                    else:
                        logger.info(
                            f"Plane Landing {ident:>7} ({ptype:<4}) {category:<2} [{dist_int:>3}nm {flight_level:<5}] {ident:>7} {from_airport:>4}<>{to_airport:<4} {reg:<6} {icao} lat:{lat} lon:{lon}"
                        )

        except TypeError as e:
            logger.critical(
                f"Error updating plane: {icao} t:{ptype} id:{ident} alt:{altitude}: {e}"
            )
            pass