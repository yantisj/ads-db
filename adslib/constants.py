# Constants for main program

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
    "CMP",
    "GLG",
    "SWQ",
    "BLX",
    "LRC",
    "PAC",
    "CKS",
    "AFR",
    "CLX",
    "LOT",
    "TFL",
    "ELY",
    "BWA",
    "IBE",
    "SIA",
    "OCN",
    "NCR",
    "CKS",
    "BAF",
    "MBK",
    "JUS",
    "VTM",
    "KAL",
    "UAE",
    "GEC",
    "ETH",
    "BOX",
    "CXB",
    "SWR",
    "OAE",
    "ABX",
    "LCO",
    "EAL",
    "CFG",
    "CJT",
    "NAC",
    "TPA",
    "NOS",
    "AEA",
    "ETD",
    "SVA",
    "KYE",
    "MPH",
    
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

