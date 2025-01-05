#!/usr/bin/env python3
"""
mta_elevator_collector.py
-------------------------
Fetches Elevator & Escalator data from MTA JSON feeds:
  1) Current Outages
  2) Upcoming Outages
  3) Equipment Info

Inserts into Postgres:
  - elevator_outages (for current/upcoming)
  - elevator_equipment (for equipment details)

Environment variables (for DB config):
  PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE

Usage:
  python mta_elevator_collector.py
"""

import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# ------------------------------------------------------------------------------
# 1) Feeds for Elevator/Escalator (JSON endpoints)
# ------------------------------------------------------------------------------
ELEVATOR_FEEDS = {
    "ElevatorCurrent":  "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fnyct_ene.json",
    "ElevatorUpcoming": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fnyct_ene_upcoming.json",
    "ElevatorEquip":    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fnyct_ene_equipments.json",
}

# ------------------------------------------------------------------------------
# 2) Database Connection
# ------------------------------------------------------------------------------
def get_db_connection():
    """
    Reads environment variables for PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE.
    Returns a psycopg2 connection object.
    """
    return psycopg2.connect(
        host=os.getenv('PGHOST', 'localhost'),
        port=os.getenv('PGPORT', '5432'),
        user=os.getenv('PGUSER', 'postgres'),
        password=os.getenv('PGPASSWORD', 'password'),
        dbname=os.getenv('PGDATABASE', 'mta_db')
    )

# ------------------------------------------------------------------------------
# 3) Fetch JSON
# ------------------------------------------------------------------------------
def fetch_json_data(url: str) -> dict:
    """
    Fetches and returns JSON data from the MTA's Elevator/Escalator endpoints.
    Returns an empty dict if there's an error.
    """
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"Error fetching JSON from {url}: {e}")
        return {}

# ------------------------------------------------------------------------------
# 4) Outages (Current & Upcoming)
# ------------------------------------------------------------------------------
def process_outages(conn, feed_name: str, outages_json: dict):
    """
    Inserts elevator/escalator outages into `elevator_outages`.

    - feed_name: "ElevatorCurrent" or "ElevatorUpcoming" => sets feed_type.
    - outages_json: the JSON data from that feed.
    """
    if feed_name == "ElevatorCurrent":
        feed_type = "CURRENT"
    elif feed_name == "ElevatorUpcoming":
        feed_type = "UPCOMING"
    else:
        feed_type = "UNKNOWN"

    outages_list = outages_json.get("outages", [])
    if not outages_list:
        print(f"No outages found for feed: {feed_name}")
        return

    records = []
    for item in outages_list:
        station        = item.get("station", "").strip()
        borough        = item.get("borough", "").strip()
        trainno        = item.get("trainno", "").strip()
        equipment_id   = item.get("equipment", "").strip()
        equip_type     = item.get("equipmenttype", "").strip()   # "EL" or "ES"
        ada            = item.get("ADA", "N").strip()
        reason         = item.get("reason", "").strip()
        serving        = item.get("serving", "").strip()
        upcoming       = item.get("isupcomingoutage", "N").strip()
        maintenance    = item.get("ismaintenanceoutage", "N").strip()

        outage_date      = parse_datetime(item.get("outagedate"))
        estimated_return = parse_datetime(item.get("estimatedreturntoservice"))

        records.append((
            feed_type,
            station,
            borough,
            trainno,
            equipment_id,
            equip_type,
            ada,
            outage_date,
            estimated_return,
            reason,
            serving,
            upcoming,
            maintenance
        ))

    if records:
        with conn.cursor() as cur:
            sql = """
            INSERT INTO elevator_outages (
              feed_type,
              station,
              borough,
              trainno,
              equipment_id,
              equipment_type,
              ada,
              outage_date,
              estimated_return,
              reason,
              serving,
              is_upcoming_outage,
              is_maintenance_outage
            )
            VALUES %s
            """
            execute_values(cur, sql, records)
        conn.commit()

        print(f"Inserted {len(records)} outages from {feed_name} ({feed_type}).")

# ------------------------------------------------------------------------------
# 5) Equipment
# ------------------------------------------------------------------------------
def process_equipment(conn, equip_json: dict):
    """
    Inserts elevator/escalator equipment info into `elevator_equipment`.
    The JSON presumably has top-level "equipments" array.
    """
    equipment_list = equip_json.get("equipments", [])
    if not equipment_list:
        print("No equipment items found in equipment feed.")
        return

    records = []
    for item in equipment_list:
        equipment_id   = item.get("equipment", "").strip()
        equip_type     = item.get("equipmenttype", "").strip()   # "EL" / "ES"
        station        = item.get("station", "").strip()
        borough        = item.get("borough", "").strip()
        trainno        = item.get("trainno", "").strip()
        lat_str        = item.get("latitude", "")
        lon_str        = item.get("longitude", "")
        ada            = item.get("ADA", "N").strip()
        serving        = item.get("serving", "").strip()
        status         = item.get("status", "").strip()  # if provided
        notes          = ""  # optional field if you want to store extra info

        # Convert lat/lon to float if possible
        lat = float(lat_str) if lat_str else None
        lon = float(lon_str) if lon_str else None

        records.append((
            equipment_id,
            equip_type,
            station,
            borough,
            trainno,
            lat,
            lon,
            ada,
            serving,
            status,
            notes
        ))

    if records:
        with conn.cursor() as cur:
            sql = """
            INSERT INTO elevator_equipment (
              equipment_id,
              equipment_type,
              station,
              borough,
              trainno,
              latitude,
              longitude,
              ada,
              serving,
              status,
              notes
            )
            VALUES %s
            """
            execute_values(cur, sql, records)
        conn.commit()

        print(f"Inserted {len(records)} equipment entries.")

# ------------------------------------------------------------------------------
# 6) Date/Time Parsing
# ------------------------------------------------------------------------------
def parse_datetime(dt_str: str):
    """
    Tries to parse MTA datetime strings like 'MM/DD/YYYY HH:MM:SS AM/PM'.
    e.g. '10/13/2023 10:34:00 PM'.
    Returns a datetime or None if parsing fails.
    """
    if not dt_str:
        return None
    dt_str = dt_str.strip()

    # Common formats used by MTA feeds
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            pass
    return None

# ------------------------------------------------------------------------------
# 7) Main
# ------------------------------------------------------------------------------
def main():
    conn = get_db_connection()

    try:
        # (A) Fetch & process Current Outages
        current_json = fetch_json_data(ELEVATOR_FEEDS["ElevatorCurrent"])
        process_outages(conn, "ElevatorCurrent", current_json)

        # (B) Fetch & process Upcoming Outages
        upcoming_json = fetch_json_data(ELEVATOR_FEEDS["ElevatorUpcoming"])
        process_outages(conn, "ElevatorUpcoming", upcoming_json)

        # (C) Fetch & process Equipment
        equip_json = fetch_json_data(ELEVATOR_FEEDS["ElevatorEquip"])
        process_equipment(conn, equip_json)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
