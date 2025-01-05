#!/usr/bin/env python3
"""
mta_transit_collector.py
------------------------
Fetches MTA GTFS-RealTime data for subways (and possibly buses) from
the MTA's GTFS-RT endpoints. Parses Trip Updates, Vehicle Positions,
and Alerts, then stores them in PostgreSQL.

ENV variables for DB config (recommended):
  PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE

Usage:
  python mta_transit_collector.py
"""

import os
import time
import logging
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from google.transit import gtfs_realtime_pb2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ------------------------------------------------------------------------------
# 1) GTFS-RT Feed URLs
#    Replace or comment out whichever feeds you want to poll.
# ------------------------------------------------------------------------------

GTFS_RT_FEEDS = {
    # Subways:
    "Subway-ACE":       "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "Subway-BDFM":      "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "Subway-G":         "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    "Subway-JZ":        "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    "Subway-NQRW":      "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "Subway-L":         "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
    "Subway-1234567S":  "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "Subway-SIR":       "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si",

    # LIRR:
    "LIRR":             "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/lirr%2Fgtfs-lirr",

    # Metro-North:
    "MNR":              "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/mnr%2Fgtfs-mnr",

    # Service Alerts (also GTFS-RT):
    "All-Alerts":       "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fall-alerts",
    "Subway-Alerts":    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts",
    "Bus-Alerts":       "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fbus-alerts",
    "LIRR-Alerts":      "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Flirr-alerts",
    "MNR-Alerts":       "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fmnr-alerts"
}

# Polling interval in seconds (adjust if you want more or less frequent)
POLL_INTERVAL = 120

# ------------------------------------------------------------------------------
# 2) Database Connection
# ------------------------------------------------------------------------------
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('PGHOST', 'localhost'),
        port=os.getenv('PGPORT', '5432'),
        user=os.getenv('PGUSER', 'postgres'),
        password=os.getenv('PGPASSWORD', 'password'),
        dbname=os.getenv('PGDATABASE', 'mta_db')
    )

# ------------------------------------------------------------------------------
# 3) Fetch & Parse GTFS-RT (Protobuf)
# ------------------------------------------------------------------------------
def fetch_gtfs_rt_feed(feed_url: str) -> gtfs_realtime_pb2.FeedMessage:
    """
    Returns a FeedMessage on success, or None on failure.
    """
    try:
        resp = requests.get(feed_url, timeout=30)
        resp.raise_for_status()
        feed_message = gtfs_realtime_pb2.FeedMessage()
        feed_message.ParseFromString(resp.content)
        return feed_message
    except Exception as e:
        logging.error(f"Error fetching feed from {feed_url}: {e}")
        return None

# ------------------------------------------------------------------------------
# 4) Trip Updates
# ------------------------------------------------------------------------------
def process_trip_updates(conn, feed_log_id: int, feed_message: gtfs_realtime_pb2.FeedMessage):
    """
    Parses 'TripUpdate' entities and inserts into:
      - realtime_trip_updates
      - realtime_stop_time_updates
    (Adjust table names & columns to suit your schema.)
    """
    trip_updates_data = []
    stop_time_updates_data = []

    for entity in feed_message.entity:
        if entity.trip_update:
            tu = entity.trip_update
            trip_id = tu.trip.trip_id
            route_id = tu.trip.route_id
            start_date = tu.trip.start_date  # YYYYMMDD
            schedule_rel = str(tu.trip.schedule_relationship)

            # Collect top-level trip update data
            trip_updates_data.append((feed_log_id, trip_id, route_id, start_date, schedule_rel))

            # Collect per-stop updates
            for stu in tu.stop_time_update:
                stop_id = stu.stop_id
                stop_seq = stu.stop_sequence
                arrival_time = _ts_to_datetime(stu.arrival.time) if (stu.arrival and stu.arrival.time) else None
                departure_time = _ts_to_datetime(stu.departure.time) if (stu.departure and stu.departure.time) else None
                delay = stu.arrival.delay if (stu.arrival and stu.arrival.delay) else None

                stop_time_updates_data.append((feed_log_id, trip_id, stop_id, stop_seq, arrival_time, departure_time, delay))

    # Insert into realtime_trip_updates
    if trip_updates_data:
        with conn.cursor() as cur:
            sql = """
            INSERT INTO realtime_trip_updates (
                feed_log_id, trip_id, route_id, start_date, schedule_relationship
            )
            VALUES %s
            """
            execute_values(cur, sql, trip_updates_data)
        conn.commit()
        logging.info(f"Inserted {len(trip_updates_data)} trip_updates.")

    # Insert into realtime_stop_time_updates
    if stop_time_updates_data:
        with conn.cursor() as cur:
            sql = """
            INSERT INTO realtime_stop_time_updates (
                feed_log_id, trip_id, stop_id, stop_sequence, arrival_time, departure_time, delay_seconds
            )
            VALUES %s
            """
            execute_values(cur, sql, stop_time_updates_data)
        conn.commit()
        logging.info(f"Inserted {len(stop_time_updates_data)} stop_time_updates.")

# ------------------------------------------------------------------------------
# 5) Vehicle Positions
# ------------------------------------------------------------------------------
def process_vehicle_positions(conn, feed_log_id: int, feed_message: gtfs_realtime_pb2.FeedMessage):
    """
    Parses 'VehiclePosition' entities and inserts into realtime_vehicle_positions.
    """
    vehicle_positions_data = []

    for entity in feed_message.entity:
        if entity.vehicle:
            v = entity.vehicle
            trip_id = v.trip.trip_id
            route_id = v.trip.route_id
            vehicle_id = v.vehicle.id
            stop_id = v.stop_id
            current_status = str(v.current_status)
            lat = v.position.latitude if v.position else None
            lon = v.position.longitude if v.position else None
            bearing = v.position.bearing if v.position else None
            speed = v.position.speed if v.position else None
            pos_ts = _ts_to_datetime(v.timestamp) if v.timestamp else None

            vehicle_positions_data.append((
                feed_log_id, vehicle_id, trip_id, route_id, stop_id,
                current_status, lat, lon, bearing, speed, pos_ts
            ))

    if vehicle_positions_data:
        with conn.cursor() as cur:
            sql = """
            INSERT INTO realtime_vehicle_positions (
                feed_log_id,
                vehicle_id,
                trip_id,
                route_id,
                current_stop_id,
                current_status,
                lat,
                lon,
                bearing,
                speed,
                position_timestamp
            )
            VALUES %s
            """
            execute_values(cur, sql, vehicle_positions_data)
        conn.commit()
        logging.info(f"Inserted {len(vehicle_positions_data)} vehicle_positions.")

# ------------------------------------------------------------------------------
# 6) Service Alerts
# ------------------------------------------------------------------------------
def process_service_alerts(conn, feed_log_id: int, feed_message: gtfs_realtime_pb2.FeedMessage):
    """
    Parses 'Alert' entities from feed_message and inserts into realtime_service_alerts.
    """
    alerts_data = []
    for entity in feed_message.entity:
        if entity.alert:
            a = entity.alert
            header_text = _translate(a.header_text)
            description_text = _translate(a.description_text)
            cause = str(a.cause)
            effect = str(a.effect)

            alerts_data.append((feed_log_id, header_text, description_text, cause, effect))

    if alerts_data:
        with conn.cursor() as cur:
            sql = """
            INSERT INTO realtime_service_alerts (
                feed_log_id, header_text, description_text, cause, effect
            )
            VALUES %s
            """
            execute_values(cur, sql, alerts_data)
        conn.commit()
        logging.info(f"Inserted {len(alerts_data)} service_alerts.")

# ------------------------------------------------------------------------------
# 7) Feed Logging (Optional)
# ------------------------------------------------------------------------------
def log_feed_fetch(conn, feed_name, feed_url, feed_timestamp, success=True, error_message=None):
    """
    If you keep a 'realtime_feed_logs' table to track each fetch event:
    feed_url, feed_name, feed_timestamp, success/failure, etc.
    """
    with conn.cursor() as cur:
        sql = """
        INSERT INTO realtime_feed_logs (
            feed_url, feed_name, fetched_at, feed_timestamp, success, error_message
        )
        VALUES (%s, %s, NOW(), %s, %s, %s)
        RETURNING feed_log_id
        """
        cur.execute(sql, (feed_url, feed_name, feed_timestamp, success, error_message))
        feed_log_id = cur.fetchone()[0]
    conn.commit()
    return feed_log_id

# ------------------------------------------------------------------------------
# 8) Helper Functions
# ------------------------------------------------------------------------------
def _ts_to_datetime(ts: int):
    """
    Converts Unix timestamp (int) to Python datetime (UTC).
    """
    return datetime.utcfromtimestamp(ts) if ts else None

def _translate(translated_string_msg):
    """
    Extract text from GTFS-RT TranslatedString.
    Often: translated_string_msg.translation[0].text
    """
    if translated_string_msg and translated_string_msg.translation:
        return translated_string_msg.translation[0].text
    return ""

# ------------------------------------------------------------------------------
# 9) Main Loop
# ------------------------------------------------------------------------------
def main():
    while True:
        conn = None
        try:
            conn = get_db_connection()

            # Iterate over each feed in the dictionary
            for feed_name, feed_url in GTFS_RT_FEEDS.items():
                logging.info(f"Fetching feed: {feed_name} from {feed_url}")
                feed_message = fetch_gtfs_rt_feed(feed_url)
                if not feed_message:
                    # Could log a failure
                    log_feed_fetch(
                        conn,
                        feed_name=feed_name,
                        feed_url=feed_url,
                        feed_timestamp=None,
                        success=False,
                        error_message="Failed to fetch or parse protobuf"
                    )
                    continue

                # Log success
                feed_timestamp = _ts_to_datetime(feed_message.header.timestamp) if feed_message.header.timestamp else None
                feed_log_id = log_feed_fetch(
                    conn,
                    feed_name=feed_name,
                    feed_url=feed_url,
                    feed_timestamp=feed_timestamp,
                    success=True
                )

                # Check which entity types exist in this feed
                has_trip_updates = any(e.trip_update for e in feed_message.entity)
                has_vehicle_positions = any(e.vehicle for e in feed_message.entity)
                has_alerts = any(e.alert for e in feed_message.entity)

                # Process accordingly
                if has_trip_updates:
                    process_trip_updates(conn, feed_log_id, feed_message)
                if has_vehicle_positions:
                    process_vehicle_positions(conn, feed_log_id, feed_message)
                if has_alerts:
                    process_service_alerts(conn, feed_log_id, feed_message)

        except Exception as e:
            logging.error(f"Unexpected error: {e}", exc_info=True)
        finally:
            if conn:
                conn.close()

        logging.info(f"Sleeping {POLL_INTERVAL} seconds before next fetch...")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
