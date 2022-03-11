# ADS-DB Install and User Guide

## Benefits of ADS-DB
Keep track of every plane/flight you're receiever has ever seen, so you can check for past activity in your area. Get alerted to when a specific plane is overhead or even landing at a nearby airport with approach detection. Keeps a record of each plane's registration data, IDENT, and detailed plane information for each manufacturer for every plane your receiver detects for additional analysis and reporting.

ADS-DB connects to one or more Piaware/dump1090-fa ADSB nodes and saves all planes and flights to a SQLite database for later analysis. ADS-DB records the first and last time a plane is seen, each day, along with the last known altitude, heading and other information. This program will also record and flag all military planes seen.

The database has been in testing for 3 years and can scale to tens of thousands of planes. When new registration/plane data is released, you can backfill your database with all the latest registration data as it becomes available.


## Database Summary
```
 ADS-DB Stats           [2022-03-09 21:16:36]
==============================================
 Flight Numbers: 259    24hrs: 259    New: 259
 Hull Classes:   543    24hrs: 151    New: 0
 Total Planes:   31525  24hrs: 1650   New: 73
```

## Command Help
```
Save ADSB Data to SQLite

optional arguments:
  -h, --help    show this help message and exit
  -D            Run Daemon
  -st           Database Stats
  -rs RS        Receiver IP List separated by commas (default 127.0.0.1)
  -rf RF        Refresh Interval (default 10sec)
  -db DB        Different Database File
  -lt LT        Lookup Planes by Type
  -lts LTS      Lookup Type Totals by type (use percent-sign for all type)
  -li LI        Lookup Planes by IACO
  -ld LD        Lookup Planes by IDENT
  -lr LR        Lookup Planes by Registration
  -af AF        Alert on Flight Name / ICAO
  -ad AD        Alert Distance from Receiver (default unlimited)
  -fa FA        Filter Low Altitude
  -fc FC        Filter Category Above int (A[3])
  -fc0          Filter A0 no categories
  -fm           Filter Military Planes
  -fd FD        Filter by Days Seen Above Count
  -sc SC        Save Cycle (increase > 10 to reduce disk writes)
  -S            Play Sounds
  -v            Debug Mode
  --update_db   Update all planes with latest DB info
  --cleanup_db  Cleanup excess plane days
```

# Useful Examples

### Flight lookup
```
ads -lf SWA1231

FLIGHT#   PTYPE  REGISTR    ICAO    CT  DST MIN  ALT     LOW     FIRST                 LAST
-------   ----   --------   ------  --- --- ---  -----   -----   --------------------  -------------------
SWA1231   B38M   N8747Q     AC088E  0   48  48   38000   38000   2022-03-09 20:37:36   2022-03-09 20:40:00

IACO   TYPE  REG      FLIGHT       DST MIN  ALT     LOW     FIRST                 LAST
----   ----  -------  -------      --- ---  -----   -----   -------------------   -------------------
AC088E B38M  N8747Q   SWA1231      48  48   38000   38000   2022-03-09 20:37:36   2022-03-09 20:40:00
ABAAA8 B738  N8507C   SWA1231      118 108  40000   40000   2022-03-08 21:02:35   2022-03-08 21:10:30
AAA668 B737  N7857B   SWA1231      108 104  38000   38000   2022-03-07 12:51:51   2022-03-07 20:47:06
```

### Specific plane lookup by IACO (or registration/ident)
```
ads -li 06A104

IACO   TYPE  REG         IDENT      CAT M CT  DST MIN  ALT     LOW     COUNTRY     OWNER                FIRST                 LAST
----   ----  ----------  ---------  --- - --  --- ---  -----   -----   ----------  -------------------- -------------------   -------------------
06A104 A359  A7-ALP      QTR777     A5    5   109 20   40000   35000   Qatar       Qatar Airways        2019-10-18 15:39:31   2020-03-02 15:03:26

IACO   IDENT        DST MIN  ALT     LOW     FIRST                 LAST
----   ----         --- ---  -----   -----   -------------------   -------------------
06A104 QTR777       55  20   40000   39975   2019-10-18 15:39:31   2019-10-18 15:54:24
06A104 QTR778       44  22   37000   37000   2019-11-12 19:46:29   2019-11-12 19:57:42
06A104              104 104  35000   35000   2020-02-04 19:37:05   2020-02-04 19:55:52
06A104 QTR777       106 48   43000   43000   2020-02-14 15:25:13   2020-02-14 15:45:09
06A104 QTR777       109 109  40000   40000   2020-03-02 15:02:42   2020-03-02 15:03:25
```

### All Airbus 300 series plane types:
```
$ ads -lts A3%
MFR      TYPE  CNT  LAST_IDENT      FIRST SEEN            LAST SEEN             MODEL
Airbus   A320  669  A67C3A          2019-07-17 13:13:45   2022-03-08 15:11:28   A-320/A-320 Prestige/Prestige (A-320)
Airbus   A321  488  A742C2          2019-07-17 13:14:02   2022-03-08 15:11:28   A-321
Airbus   A319  410  ACE81D          2019-07-17 13:13:45   2022-03-08 15:11:27   A-319/VC-1 ACJ/A-319 ACJ/ACJ
Airbus   A332  154  C07A0C          2019-07-18 06:31:59   2022-03-08 11:35:19   A-330-200/A-330-200 Voyager/Voyager/KC-30/A-330-20
Airbus   A333  145  40655D          2019-07-17 17:04:43   2022-03-08 15:11:28   A-330-300
Airbus   A306  117  A1226B          2019-07-17 21:47:32   2022-03-08 09:55:08   A-300B4-600/A-300C4-600/A-300F4-600
Airbus   A359  42   4ACA63          2019-08-02 16:00:26   2022-03-08 13:08:17   A-350-900/A-350-900 XWB/A-350-900 XWB Prestige/Pre
Airbus   A346  30   400E09          2019-07-18 16:51:10   2020-06-12 00:47:10   A-340-600/A-340-600 Prestige/Prestige (A-340-600)
Airbus   A343  28   4B1900          2019-07-19 15:13:22   2022-03-08 02:20:54   A-340-300/A-340-300 Prestige/Prestige (A-340-300)
Airbus   A388  24   406A03          2019-07-18 16:42:46   2022-02-28 18:25:16   A-380-800/A-380-800 Prestige/Prestige (A-380-800)
Airbus   A310  8    C06C58          2019-07-20 17:40:48   2022-03-05 14:37:05   A-310/CC-150 Polaris/Polaris
         A35K  4    407699          2020-07-12 21:58:05   2022-03-05 15:00:28   None
Airbus   A318  3    406090          2019-10-27 19:20:44   2020-06-30 12:38:21   A-318/Elite/A-318 Elite
Airbus   A330  1    06A01C          2019-07-17 14:04:37   2020-09-22 16:00:09   Airbus A330
         A339  1    A4B827          2020-03-21 13:56:51   2020-03-21 13:56:51   None

Aircraft Types: 15 / Total Aircraft: 2124
```

### All planes of a type
```
$ ads -lt A359

IACO   TYPE  REG         IDENT      CAT M CT  DST MIN  ALT     LOW     COUNTRY     OWNER                FIRST                 LAST
----   ----  ----------  ---------  --- - --  --- ---  -----   -----   ----------  -------------------- -------------------   -------------------
4ACA63 A359  SE-RSC      SAS953     A5  . 3   118 108  40000   40000                                    2022-03-03 13:26:41   2022-03-08 13:08:17
A6745A A359  N515DN      DAL201     A5  . 5   110 4    39200   32075                                    2022-03-01 07:20:42   2022-03-08 07:58:42
4ACA61 A359  SE-RSA      SAS953         . 4   124 38   43000   40000                                    2022-02-28 12:25:28   2022-03-06 12:38:52
A670A3 A359  N514DN      DAL201     A5  . 2   102 5    36000   32200                                    2022-03-04 19:32:43   2022-03-06 07:48:38
461F4E A359  OH-LWG      FIN7       A5    1   81  56   43000   43000   Finland     Finnair              2022-03-05 19:19:32   2022-03-05 19:35:38
3C6707 A359  D-AIXG      DLH460     A5    1   108 108  40000   40000   Germany     Lufthansa            2022-03-05 15:28:57   2022-03-05 15:28:57
39CF04 A359  F-HTYE      AFR092     A5  . 1   108 108  40000   40000                                    2022-03-04 13:17:35   2022-03-04 13:17:35
...
```

# Install Instruction for Raspberry Pi with Flightaware installed

## Clone the git repo to home directory: /home/pi
```git clone https://github.com/yantisj/ads-db ads-db/```

## Install virtualenv and required packages
```
sudo apt install virtualenv
cd ads-db/
virtualenv venv/
source venv/bin/activate
pip install -r requirements.txt
```

## Copy over default config file and update lat/lon for distance calculations
```cp ads-db.conf.example ads-db.conf```

Edit file and set your local lat/lon
```
[global]
lat = 32.7800
lon = -79.9400
```

## Install the latest BaseStation.sqb from https://data.flightairmap.com/

### Note: More accurate BaseStation.sqb available from radarspotting (registration required)
- https://radarspotting.com/forum/index.php?action=tportal;sa=download;dl=item521


### Optionally install StandingData.sqb for flight data

Unzip StandingData.sqb to sqb directory and update ads-db.conf to use it.

```
wget https://data.flightairmap.com/data/basestation/BaseStation.sqb.gz
gunzip BaseStation.sqb.gz
mkdir sqb
mv BaseStation.sqb sqb/
```

## Test pointing ads-db to localhost's receiver (initializes database...)
```
./ads-db.py -D -v

2022-03-08 15:27:46,515 INFO     FA Database Loading
2022-03-08 15:28:19,007 INFO     Daemon Started
2022-03-08 15:28:19,031 WARNING  DB Load Error: no such table: plane_days
2022-03-08 15:28:19,032 WARNING  DB Load Error: Trying to create database: no such table: planes
2022-03-08 15:28:19,113 INFO     !! New Type of Plane !!: A40670 t:B350
2022-03-08 15:28:19,114 WARNING  New Plane Type!!: A40670 N359CB B350  USA Task Aviation LLC d:40.7
2022-03-08 15:28:19,117 INFO     New Plane: ACBFE9 t:C750 id: alt:43000 site:127.0.0.1
2022-03-08 15:28:19,118 INFO     !! New Type of Plane !!: ACBFE9 t:C750
2022-03-08 15:28:19,119 WARNING  New Plane Type!!: ACBFE9 N920TX C750  USA None d:108.5
2022-03-08 15:28:19,122 INFO     New Plane: AC575B t:CRJ2 id:EDV4714 alt:24150 site:127.0.0.1
...
```

## Press ctrl-c to exit the daemon and check your database stats
```
./ads-db.py -st

ADS-B Database Stats
--------------------------
 Types: 33     24hr: 33     New: 32
 Total: 56     24hr: 56     New: 56

Last Seen: 2022-03-08 15:28:59
```

You should see planes in your database if it successfull connects to http://127.0.0.1/dump1090-fa/



## Add the service to systemd and enable on startup
```
sudo cp extra/ads-db.service /etc/systemd/system/
sudo systemctl enable ads-db
sudo systemctl start ads-db
sudo systemctl status ads-db
```

## Daemon Results
```
● ads-db.service - ADS-DB Collector
     Loaded: loaded (/etc/systemd/system/ads-db.service; enabled; vendor preset: enabled)
     Active: active (running) since Tue 2022-03-08 15:43:56 EST; 58s ago
   Main PID: 21024 (ads-db.sh)
      Tasks: 2 (limit: 2059)
        CPU: 37.872s
     CGroup: /system.slice/ads-db.service
             ├─21024 /bin/bash /home/pi/ads-db/extra/ads-db.sh
             └─21027 python3 ./ads-db.py --quiet -D -sc 200 -rs 172.20.30.30

Mar 08 15:43:56 ads-db-1 systemd[1]: Started ADS-DB Collector.
Mar 08 15:43:58 ads-db-1 ads-db.sh[21027]: 2022-03-08 15:43:58,020 INFO     Silencing Sounds
Mar 08 15:43:58 ads-db-1 ads-db.sh[21027]: 2022-03-08 15:43:58,021 INFO     Increasing Save Cycle to 200
Mar 08 15:43:58 ads-db-1 ads-db.sh[21027]: 2022-03-08 15:43:58,021 INFO     FA Database Loading
Mar 08 15:44:30 ads-db-1 ads-db.sh[21027]: 2022-03-08 15:44:30,105 INFO     Daemon Started
Mar 08 15:44:31 ads-db-1 ads-db.sh[21027]: 2022-03-08 15:44:31,639 INFO     New Plane: A89BE8 t:GLF6 id:N654DG alt:36900 site:172.20.30.30
```

# FAQ

## How to read from more than one receiver at a time?

Use the -r [receivers] command: ```./ads-db.py -D -r 192.168.1.10,192.168.1.20```

## How do sounds work?

Sounds are only tested on Macs. Use the requirements-mac.txt file to install your pip dependencies. Then enable sounds in the config file, and run the daemon locally to point to a remote receiver.

## New database results are not showing up, what gives?

By default, results are only written to the database every 50 minutes when running as a daemon. This prevents flash card wear. This can be adjusted in the extra/ads-db.sh file or via a command line option to always write out results, or decrease the cycle between writing the results to disk.

## How do I watch for activity?

```tail -f ~/ads-db/ads-db.log```

## What is the landing alert functionality?

Landing alerts are not supported currently, but can be hacked by forking the code and changing the landing_alert method to suit your needs. The program can take a number of measurements to determine if a plane is on approach to your local runway, and play sounds to alert you.

## What is the boeing 787 functionality

This was added for the author's hometown of Charleston, SC to detect whenver a newly manufactured 787 takes flight. This could be modified for your needs to track special planes, or see the other flight alerting capabilies for more ideas

