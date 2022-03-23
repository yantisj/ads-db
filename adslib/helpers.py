# Helper Routines

from datetime import date, datetime, timedelta
from .constants import STATIC_CALL_SIGNS
import logging

logger = logging.getLogger('ads-helper')

config = None


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
