import frappe
import requests
from frappe.model.document import Document
from requests.auth import HTTPDigestAuth
from datetime import datetime, timedelta

from biometric_integration.employee_checkin_sync import (
    sync_punches_to_employee_checkin,
)


class BiometricIntegrationSettings(Document):
    pass


def _get_device_configs(settings):
    """Return a list of (label, ip, username, password) for all active devices.

    Priority:
    - If child table has active rows -> use those
    - Else -> use main IP/username/password from settings
    """
    devices = []

    # Child table devices
    if getattr(settings, "devices", None):
        for d in settings.devices:
            # Expecting fields: device, ip_address, username, password, is_active
            if getattr(d, "is_active", 0):
                ip = d.ip_address
                username = d.username or settings.username
                pwd = d.get_password("password") if hasattr(d, "get_password") else d.password
                if ip:
                    devices.append((d.device or ip, ip, username, pwd))

    # Fallback to main IP if no active child devices
    if not devices and settings.ip:
        pwd = settings.get_password("password")
        devices.append(("Main Device", settings.ip, settings.username, pwd))

    print(devices)
    
    return devices


def _sync_for_single_device(settings, label, ip, username, password, start_time, end_time):
    """
    Sync attendance for a single device (one IP).
    Returns (count, skipped) for that device.
    Also sets device_id (IP) on logs and punches if those fields exist.
    """
    url = f"http://{ip}/ISAPI/AccessControl/AcsEvent?format=json"
    headers = {"Content-Type": "application/json"}

    log_has_device_id = frappe.db.has_column("Biometric Attendance Log", "device_id")
    punch_has_device_id = frappe.db.has_column(
        "Biometric Attendance Punch Table", "device_id"
    )

    # Initial fetch to determine total records
    payload = {
        "AcsEventCond": {
            "searchID": "123456789",
            "searchResultPosition": 0,
            "maxResults": 1,
            "major": 5,
            "minor": 75,
            "startTime": start_time,
            "endTime": end_time,
        }
    }

    response = requests.post(
        url,
        auth=HTTPDigestAuth(username, password),
        headers=headers,
        json=payload,
        verify=False,
        timeout=600,
    )

    if response.status_code != 200:
        frappe.throw(
            f"[{label}] Failed to fetch attendance logs. "
            f"Status: {response.status_code}, Response: {response.text}"
        )

    data = response.json()
    total_records = data.get("AcsEvent", {}).get("totalMatches", 0)

    if total_records == 0:
        return 0, 0

    if total_records > 1500:
        frappe.throw(f"[{label}] Too many records to process ({total_records}). Reduce date range.")

    count = 0
    skipped = 0
    position = 0
    batch_size = 30

    while True:
        payload["AcsEventCond"]["searchResultPosition"] = position
        payload["AcsEventCond"]["maxResults"] = batch_size

        response = requests.post(
            url,
            auth=HTTPDigestAuth(username, password),
            headers=headers,
            json=payload,
            verify=False,
            timeout=600,
        )

        if response.status_code != 200:
            frappe.throw(
                f"[{label}] Failed to fetch attendance logs. "
                f"Status: {response.status_code}, Response: {response.text}"
            )

        data = response.json()
        events = data.get("AcsEvent", {}).get("InfoList", [])

        if not events:
            break

        for log in events:
            emp_no = log.get("employeeNoString")
            event_timestamp = log.get("time", "")
            if not emp_no or not event_timestamp:
                continue

            # Convert device time format to Frappe format
            event_datetime = datetime.strptime(event_timestamp[:19], "%Y-%m-%dT%H:%M:%S")

            # Create or get Attendance Log doc for employee and date
            attendance_log = frappe.get_all(
                "Biometric Attendance Log",
                filters={"employee_no": emp_no, "event_date": event_datetime.date()},
                limit_page_length=1,
            )
            if attendance_log:
                doc = frappe.get_doc("Biometric Attendance Log", attendance_log[0].name)
            else:
                doc = frappe.new_doc("Biometric Attendance Log")
                doc.employee_no = emp_no
                doc.event_date = event_datetime.date()

            # Set device_id on log if field exists
            if log_has_device_id:
                doc.device_id = ip

            # Avoid exact duplicate punch time for that employee/date
            existing_punch = (
                frappe.db.sql(
                    """
                    SELECT COUNT(*)
                    FROM `tabBiometric Attendance Punch Table`
                    WHERE parent = %(parent)s
                      AND punch_time = %(punch_time)s
                """,
                    {
                        "parent": doc.name,
                        "punch_time": event_datetime.time(),
                    },
                )[0][0]
                > 0
            )

            if not existing_punch:
                punch_row = {
                    "punch_time": event_datetime.time(),
                    "punch_type": "Auto",  # device punch
                }
                if punch_has_device_id:
                    punch_row["device_id"] = ip

                doc.append("punch_table", punch_row)
                try:
                    doc.save(ignore_permissions=True)
                    count += 1
                except Exception:
                    frappe.log_error(
                        frappe.get_traceback(),
                        f"[{label}] Insert failed for employee {emp_no}",
                    )
                    continue
            else:
                skipped += 1

        position += len(events)

        if len(events) < batch_size:
            break

    return count, skipped


@frappe.whitelist()
def sync_attendance():
    """
    Manual sync from device(s) AND directly to Employee Checkin.
    This is used by the button "Sync Attendance (Device + Checkin)".
    """
    msg_parts = []

    # First: sync from devices into logs/punches
    device_msg = sync_attendance_device_only()
    msg_parts.append(device_msg)

    # Then: convert punches -> Employee Checkin
    created, already_synced = sync_punches_to_employee_checkin()
    msg_parts.append(
        f"{created} Employee Checkins created, {already_synced} punches were already synced."
    )

    full_msg = " ".join(msg_parts)
    frappe.msgprint(full_msg)
    return full_msg


@frappe.whitelist()
def sync_attendance_device_only():
    """
    Manual sync from device(s) ONLY:
    - For each active device (child table) OR main IP:
        -> fetch events
        -> fill Biometric Attendance Log + Punch Table
    Does NOT create Employee Checkins. Used internally and can be called separately.
    """
    settings = frappe.get_doc("Biometric Integration Settings", "Biometric Integration Settings")

    # Prepare time window used for ALL devices
    start_time = datetime.strptime(
        settings.start_date_and_time, "%Y-%m-%d %H:%M:%S"
    ).strftime("%Y-%m-%dT%H:%M:%S+08:00")
    end_time = datetime.strptime(
        settings.end_date_and_time, "%Y-%m-%d %H:%M:%S"
    ).strftime("%Y-%m-%dT%H:%M:%S+08:00")

    device_configs = _get_device_configs(settings)
    if not device_configs:
        frappe.throw("No device configured (no IP in settings and no active rows in Devices table).")

    total_count = 0
    total_skipped = 0

    frappe.publish_progress(
        0,
        title="Attendance Sync",
        description="Starting attendance sync from devices...",
    )

    for idx, (label, ip, username, password) in enumerate(device_configs, start=1):
        frappe.publish_progress(
            (idx - 1) * 100.0 / max(len(device_configs), 1),
            title="Attendance Sync",
            description=f"Syncing device {idx}/{len(device_configs)}: {label} ({ip})",
        )

        c, s = _sync_for_single_device(
            settings=settings,
            label=label,
            ip=ip,
            username=username,
            password=password,
            start_time=start_time,
            end_time=end_time,
        )
        total_count += c
        total_skipped += s

    # Save all logs/punches
    frappe.db.commit()

    msg = (
        f"{total_count} attendance records synced from devices; "
        f"{total_skipped} duplicate punches skipped."
    )

    frappe.publish_progress(100, title="Attendance Sync", description=msg)
    return msg


@frappe.whitelist()
def sync_to_employee_checkin_only():
    """
    Manual sync: ONLY convert Biometric Attendance Punch Table -> Employee Checkin,
    without calling any device.
    Used by 'Sync to Employee Checkin' button.
    """
    try:
        created, already_synced = sync_punches_to_employee_checkin()
        msg = (
            f"{created} Employee Checkins created from punches. "
            f"{already_synced} punches were already synced."
        )
        frappe.msgprint(msg)
        return msg
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Error in sync_to_employee_checkin_only")
        frappe.throw(f"Error syncing to Employee Checkin: {str(e)}")


def scheduled_attendance_sync():
    """
    AUTO sync (every 15 minutes via hooks.py scheduler):

    - Set Biometric Integration Settings date range to last N days (default: 3)
    - Enqueue:
        * sync_attendance_device_only()      -> get logs from device(s)
        * sync_to_employee_checkin_only()    -> convert punches -> Employee Checkin
    """
    try:
        settings = frappe.get_doc("Biometric Integration Settings", "Biometric Integration Settings")

        BACK_DAYS = 3  # change to 5 if you prefer last 5 days

        today = datetime.now().date()
        start_date = today - timedelta(days=BACK_DAYS - 1)

        start_time = datetime.combine(
            start_date, datetime.strptime("00:00:00", "%H:%M:%S").time()
        )
        end_time = datetime.combine(
            today, datetime.strptime("23:59:59", "%H:%M:%S").time()
        )

        settings.start_date_and_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
        settings.end_date_and_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
        settings.save()

        # 1) Fetch from devices (logs + punches)
        frappe.enqueue(
            "biometric_integration.biometric_integration.doctype.biometric_integration_settings.biometric_integration_settings.sync_attendance_device_only",
            queue="long",
            timeout=1500,
        )

        # 2) Convert punches -> Employee Checkin
        frappe.enqueue(
            "biometric_integration.biometric_integration.doctype.biometric_integration_settings.biometric_integration_settings.sync_to_employee_checkin_only",
            queue="long",
            timeout=1500,
        )

        frappe.logger().info("Scheduled attendance sync (device + checkin) started successfully")

    except Exception as e:
        frappe.logger().error(f"Scheduled attendance sync failed: {str(e)}")
        frappe.log_error(
            f"Scheduled attendance sync failed: {str(e)}",
            "Scheduled Attendance Sync Error",
        )
