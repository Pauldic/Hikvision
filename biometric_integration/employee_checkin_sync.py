import frappe
from frappe.utils import get_datetime
import requests

# -------------------------------
# Geolocation helpers
# -------------------------------

# Fallback coordinates (your site location)
DEFAULT_LAT = 31.4812872
DEFAULT_LON = 74.2520218


def get_geolocation():
    """
    Try to get geolocation from ipinfo.io.
    If it fails, return DEFAULT_LAT/LON.

    This is called ONCE per sync and reused for all Employee Checkins.
    """
    try:
        r = requests.get("https://ipinfo.io/json", timeout=5)
        if r.status_code == 200:
            data = r.json()
            loc = data.get("loc")
            if loc:
                lat_str, lon_str = loc.split(",")
                return float(lat_str), float(lon_str)
    except Exception:
        # Silently fall back
        pass

    return DEFAULT_LAT, DEFAULT_LON


def sync_punches_to_employee_checkin():
    """
        Convert unsynced biometric punches into Employee Checkin records.

        Rules:
        - Group punches by (employee_no, event_date)
        - Map employee_no -> Employee via Employee.attendance_device_id
        - For each group (one employee, one date):
            * First punch of the day  -> Employee Checkin (IN)
            * Last punch of the day   -> Employee Checkin (OUT)
            * Middle punches stay only in the punch table, but are marked as synced
        - Avoid duplicate Employee Checkin rows (same employee + time)
        - Mark punches as synced via Biometric Attendance Punch Table.synced_to_employee_checkin
        - Copy device_id (device IP) from punches/logs into Employee Checkin.device_id
        - When HRMS geolocation is enabled, fill latitude/longitude fields so validation passes.

        SIMPLE RULE for missing/inactive employees:
        - If no Employee is found for attendance_device_id, or Employee is NOT Active:
            -> Do NOT create Employee Checkin
            -> Do NOT mark punches as synced
            -> Just skip silently (punches remain in biometric tables only)
    """

    # Check optional columns exist (DB-level)
    punch_has_employee_checkin = frappe.db.has_column(
        "Biometric Attendance Punch Table", "employee_checkin"
    )
    checkin_has_biometric_log = frappe.db.has_column("Employee Checkin", "biometric_log")
    checkin_has_biometric_punch = frappe.db.has_column(
        "Employee Checkin", "biometric_punch"
    )
    checkin_has_device_id = frappe.db.has_column("Employee Checkin", "device_id")

    # Geolocation-related fields
    checkin_has_latitude = frappe.db.has_column("Employee Checkin", "latitude")
    checkin_has_longitude = frappe.db.has_column("Employee Checkin", "longitude")
    checkin_has_geo_latitude = frappe.db.has_column("Employee Checkin", "geo_latitude")
    checkin_has_geo_longitude = frappe.db.has_column("Employee Checkin", "geo_longitude")

    # Fetch geolocation once for this sync
    latitude, longitude = get_geolocation()

    # Get all unsynced punches joined with their parent logs
    punches = frappe.db.sql(
        """
        SELECT
            p.name AS punch_name,
            p.punch_time,
            p.punch_type,
            COALESCE(p.synced_to_employee_checkin, 0) AS synced,
            p.device_id AS punch_device_id,
            l.name AS log_name,
            l.employee_no,
            l.event_date,
            l.device_id AS log_device_id
        FROM `tabBiometric Attendance Punch Table` p
        JOIN `tabBiometric Attendance Log` l ON l.name = p.parent
        WHERE COALESCE(p.synced_to_employee_checkin, 0) = 0
        ORDER BY l.employee_no, l.event_date, p.punch_time
        """,
        as_dict=True,
    )

    if not punches:
        return 0, 0

    created = 0
    already_synced = 0

    # Group punches by (employee_no, event_date)
    groups = {}
    for p in punches:
        key = (p["employee_no"], p["event_date"])
        groups.setdefault(key, []).append(p)

    for (emp_no, event_date), group_punches in groups.items():
        # If no device employee number, just skip this group
        if not emp_no:
            continue

        # Map device employee_no -> Employee via attendance_device_id
        emp_row = frappe.db.get_value(
            "Employee",
            {"attendance_device_id": emp_no},
            ["name", "status"],
            as_dict=True,
        )

        # If no Employee found OR Employee is not Active → skip silently
        if not emp_row or emp_row.status != "Active":
            # punches stay unsynced; once Employee is created/activated,
            # they can be processed in a future sync
            continue

        employee = emp_row.name

        # Ensure sorted by time
        group_punches.sort(key=lambda x: x["punch_time"] or "")

        # First and last of the day
        first = group_punches[0]
        last = group_punches[-1]

        def _create_checkin_for_punch(punch, log_type):
            nonlocal created, already_synced

            time_str = f"{event_date} {punch['punch_time']}"
            punch_dt = get_datetime(time_str)

            # Avoid duplicate Employee Checkin rows for the same employee+time
            exists = frappe.db.exists(
                "Employee Checkin",
                {"employee": employee, "time": punch_dt},
            )
            if exists:
                already_synced += 1
                frappe.db.set_value(
                    "Biometric Attendance Punch Table",
                    punch["punch_name"],
                    "synced_to_employee_checkin",
                    1,
                )
                return

            checkin = frappe.new_doc("Employee Checkin")
            checkin.employee = employee
            checkin.time = punch_dt
            checkin.log_type = log_type  # 'IN' or 'OUT'

            # Device ID (prefer punch device_id, fall back to log device_id)
            device_id = punch.get("punch_device_id") or punch.get("log_device_id")
            if checkin_has_device_id and device_id:
                checkin.device_id = device_id

            # Geolocation fields: required when "Allow Geolocation Tracking" is enabled
            if checkin_has_latitude:
                checkin.latitude = latitude
            if checkin_has_longitude:
                checkin.longitude = longitude
            if checkin_has_geo_latitude:
                checkin.geo_latitude = latitude
            if checkin_has_geo_longitude:
                checkin.geo_longitude = longitude

            # Optional back-links to biometric log/punch if those fields exist
            if checkin_has_biometric_log:
                checkin.biometric_log = punch["log_name"]
            if checkin_has_biometric_punch:
                checkin.biometric_punch = punch["punch_name"]

            checkin.insert(ignore_permissions=True)

            # Mark this punch as synced and link to Employee Checkin if possible
            update_values = {"synced_to_employee_checkin": 1}
            if punch_has_employee_checkin:
                update_values["employee_checkin"] = checkin.name

            frappe.db.set_value(
                "Biometric Attendance Punch Table",
                punch["punch_name"],
                update_values,
            )

            created += 1

        # Create IN checkin for first punch
        _create_checkin_for_punch(first, "IN")

        # If there is more than one punch, create OUT checkin for last punch
        if last["punch_name"] != first["punch_name"]:
            _create_checkin_for_punch(last, "OUT")

        # Mark middle punches as synced (but no Employee Checkin)
        middle = [
            p
            for p in group_punches
            if p["punch_name"] not in {first["punch_name"], last["punch_name"]}
        ]
        for p in middle:
            frappe.db.set_value(
                "Biometric Attendance Punch Table",
                p["punch_name"],
                "synced_to_employee_checkin",
                1,
            )

    frappe.db.commit()
    return created, already_synced
