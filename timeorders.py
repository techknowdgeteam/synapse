import os
import json
import calendar
from datetime import datetime
import os
import insiders_server

def update_calendar():
    """Update the calendar and write to JSON, using only type, saving to script directory."""

    # Get current date and time
    now = datetime.now()
    current_year = now.year
    current_month = now.month
    current_day = now.day
    current_time_12hour = now.strftime("%I:%M %p").lower()
    current_time_24hour = now.strftime("%H:%M")
    current_date = datetime.strptime(f"{current_day:02d}/{current_month:02d}/{current_year}", "%d/%m/%Y")
    
    print(f"Current date and time: {current_date.strftime('%d/%m/%Y')} {current_time_12hour} ({current_time_24hour})")
    
    # Read timeauthor.json (only need 'type')
    pageauthors_path = r"C:\xampp\htdocs\chronedge\synarex\timeauthor.json"
    print(f"Reading timeauthor.json from {pageauthors_path}")
    try:
        with open(pageauthors_path, 'r') as f:
            pageauthors = json.load(f)
    except FileNotFoundError:
        print(f"Error: timeauthor.json not found at {pageauthors_path}")
        return
    except json.decoder.JSONDecodeError:
        print(f"Error: timeauthor.json contains invalid JSON")
        return
    
    type_value = pageauthors.get('type')
    if not type_value:
        print("Error: 'type' field missing in timeauthor.json")
        return
    print(f"Type: {type_value}")
    
    # Read timeorders.json
    timeorders_path = r"C:\xampp\htdocs\chronedge\synarex\timeorders.json"
    print(f"Reading timeorders.json from {timeorders_path}")
    try:
        with open(timeorders_path, 'r') as f:
            timeorders_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: timeorders.json not found at {timeorders_path}")
        return
    except json.decoder.JSONDecodeError:
        print(f"Error: timeorders.json contains invalid JSON")
        return
    
    # Select time slots based on type
    if type_value not in timeorders_data:
        print(f"Error: Type '{type_value}' not found in timeorders.json")
        return
    timeorders = timeorders_data[type_value]
    print(f"Time slots loaded for type '{type_value}': {[t['12hours'] for t in timeorders]}")
    
    # Sort timeorders by 24-hour format
    sorted_timeorders = sorted(timeorders, key=lambda x: x["24hours"])
    
    # Find ALL time slots after current time for TODAY
    time_ahead_today = []
    current_time = datetime.strptime(current_time_24hour, "%H:%M")
    current_datetime = datetime.combine(current_date, current_time.time())
    
    print(f"Searching for time slots after {current_time_24hour}")
    for t in sorted_timeorders:
        slot_time = datetime.strptime(t["24hours"], "%H:%M")
        delta = slot_time - current_time
        minutes_distance = int(delta.total_seconds() / 60)
        
        if minutes_distance >= 0 and t["24hours"] != "00:00":
            slot = {
                "id": f"{current_day:02d}_{t['24hours'].replace(':', '')}",
                "12hours": t["12hours"],
                "24hours": t["24hours"],
                "minutes_distance": minutes_distance,
                "consideration": f"passed {t['12hours']}" if minutes_distance >= 50 else f"skip {t['12hours']}"
            }
            time_ahead_today.append(slot)
            print(f"Slot TODAY: {t['12hours']} ({t['24hours']}): id={slot['id']}, minutes_distance={minutes_distance}, consideration={slot['consideration']}")
    
    # Calculate next month and year
    next_month = current_month + 1 if current_month < 12 else 1
    next_year = current_year if current_month < 12 else current_year + 1
    
    # Create calendar data structure
    calendar_data = {
        "calendars": [
            {
                "year": current_year,
                "month": calendar.month_name[current_month],
                "days": [
                    {
                        "week": week_idx + 1,
                        "days": [
                            {
                                "day": {
                                    "date": f"{day:02d}/{current_month:02d}/{current_year}" if day != 0 else None,
                                    "time_12hour": current_time_12hour if day == current_day else "00:00 pm" if day != 0 else None,
                                    "time_24hour": current_time_24hour if day == current_day else "00:00" if day != 0 else None,
                                    "time_ahead": (
                                        time_ahead_today if day == current_day else
                                        [
                                            {
                                                "id": f"{day:02d}_{t['24hours'].replace(':', '')}",
                                                "12hours": t["12hours"],
                                                "24hours": t["24hours"],
                                                "minutes_distance": int((
                                                    datetime.strptime(
                                                        f"{day:02d}/{current_month:02d}/{current_year} {t['24hours']}",
                                                        "%d/%m/%Y %H:%M"
                                                    ) - current_datetime
                                                ).total_seconds() / 60),
                                                "consideration": f"passed {t['12hours']}"
                                            } for t in sorted_timeorders
                                        ] if day != 0 else []
                                    )
                                } if day != 0 and day >= current_day else {"day": None}
                            } for day in week
                        ]
                    } for week_idx, week in enumerate(calendar.monthcalendar(current_year, current_month))
                    if any(day >= current_day or day == 0 for day in week)
                ]
            },
            {
                "year": next_year,
                "month": calendar.month_name[next_month],
                "days": [
                    {
                        "week": week_idx + 1,
                        "days": [
                            {
                                "day": {
                                    "date": f"{day:02d}/{next_month:02d}/{next_year}" if day != 0 else None,
                                    "time_12hour": "00:00 pm" if day != 0 else None,
                                    "time_24hour": "00:00" if day != 0 else None,
                                    "time_ahead": [
                                        {
                                            "id": f"{day:02d}_{t['24hours'].replace(':', '')}",
                                            "12hours": t["12hours"],
                                            "24hours": t["24hours"],
                                            "minutes_distance": int((
                                                datetime.strptime(
                                                    f"{day:02d}/{next_month:02d}/{next_year} {t['24hours']}",
                                                    "%d/%m/%Y %H:%M"
                                                ) - current_datetime
                                            ).total_seconds() / 60),
                                            "consideration": f"passed {t['12hours']}"
                                        } for t in sorted_timeorders
                                    ] if day != 0 else []
                                } if day != 0 else {"day": None}
                            } for day in week
                        ]
                    } for week_idx, week in enumerate(calendar.monthcalendar(next_year, next_month))
                ]
            }
        ]
    }
    
    # Save to script's current directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, f"{type_value}calendar.json")
    print(f"Writing calendar data to {output_path}")
    
    # Write to JSON file
    with open(output_path, 'w') as f:
        json.dump(calendar_data, f, indent=4)
    print(f"Calendar data successfully written to {output_path}")
    
    # Call schedule_time
    update_timeschedule()

def deletejson():
    """
    Deletes the file: C:\\xampp\\htdocs\\chronedge\\fullordersschedules.json
    Returns True if deleted successfully, False otherwise.
    """
    file_path = r"C:\xampp\htdocs\chronedge\synarex\fullordersschedules.json"
    
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Deleted: {file_path}")
            return True
        else:
            print(f"File not found: {file_path}")
            return False
    except PermissionError:
        print(f"Permission denied: Cannot delete {file_path}")
        return False
    except OSError as e:
        print(f"OS error while deleting {file_path}: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False


def update_timeschedule():
    """Determine the next schedule time and write to schedules.json (no author, script dir only)."""
    # Get current date and time
    now = datetime.now()
    current_year = now.year
    current_month = now.month
    current_day = now.day
    current_time_24hour = now.strftime("%H:%M")
    current_time_12hour = now.strftime("%I:%M %p").lower()
    current_date = now.strftime("%d/%m/%Y")
    
    print(f"Current date and time: {current_date} {current_time_12hour} ({current_time_24hour})")
    
    # Read timeauthor.json to get type only
    pageauthors_path = r"C:\xampp\htdocs\chronedge\synarex\timeauthor.json"
    print(f"Reading timeauthor.json from {pageauthors_path}")
    try:
        with open(pageauthors_path, 'r') as f:
            pageauthors = json.load(f)
    except FileNotFoundError:
        print(f"Error: timeauthor.json not found at {pageauthors_path}")
        return
    except json.decoder.JSONDecodeError:
        print(f"Error: timeauthor.json contains invalid JSON")
        return
    
    type_value = pageauthors.get('type')
    if not type_value:
        print("Error: 'type' field missing in timeauthor.json")
        return
    print(f"Type: {type_value}")
    
    # Script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Read calendar.json from script directory
    calendar_path = os.path.join(script_dir, f"{type_value}calendar.json")
    print(f"Reading calendar.json from {calendar_path}")
    try:
        with open(calendar_path, 'r') as f:
            calendar_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: calendar.json not found at {calendar_path}")
        return
    except json.decoder.JSONDecodeError:
        print(f"Error: calendar.json contains invalid JSON")
        return
    
    # Read existing schedules.json from script directory
    schedules_path = os.path.join(script_dir, f"{type_value}schedules.json")
    last_schedule = None
    previous_next_schedule = None
    if os.path.exists(schedules_path):
        try:
            with open(schedules_path, 'r') as f:
                existing_data = json.load(f)
                if isinstance(existing_data, dict):
                    last_schedule = existing_data.get('last_schedule')
                    previous_next_schedule = existing_data.get('next_schedule')
                    if last_schedule:
                        print(f"Previous last_schedule: {last_schedule}")
                    if previous_next_schedule:
                        print(f"Previous next_schedule: {previous_next_schedule}")
                else:
                    print("schedules.json exists but is not a dict, ignoring")
        except json.decoder.JSONDecodeError:
            print("schedules.json is corrupted, ignoring")
    
    # Read timeorders.json
    timeorders_path = r"C:\xampp\htdocs\chronedge\synarex\timeorders.json"
    print(f"Reading timeorders.json from {timeorders_path}")
    try:
        with open(timeorders_path, 'r') as f:
            timeorders_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: timeorders.json not found at {timeorders_path}")
        return
    except json.decoder.JSONDecodeError:
        print(f"Error: timeorders.json contains invalid JSON")
        return
    
    if type_value not in timeorders_data:
        print(f"Error: Type '{type_value}' not found in timeorders.json")
        return
    timeorders = timeorders_data[type_value]
    sorted_timeorders = sorted(timeorders, key=lambda x: x["24hours"])
    
    # Current datetime
    current_time = datetime.strptime(current_time_24hour, "%H:%M")
    current_datetime = datetime.combine(datetime.strptime(current_date, "%d/%m/%Y"), current_time.time())
    
    next_slot = None
    found_last_slot = last_schedule is None
    found_previous_next_slot = previous_next_schedule is None
    
    last_schedule_date = None
    last_schedule_time = None
    previous_next_schedule_date = None
    previous_next_schedule_time = None
    
    if last_schedule:
        try:
            last_schedule_date = datetime.strptime(last_schedule["date"], "%d/%m/%Y")
            last_schedule_time = datetime.strptime(last_schedule["time_24hour"], "%H:%M")
        except (ValueError, KeyError):
            print("Invalid last_schedule format, ignoring")
            found_last_slot = True
    
    if previous_next_schedule:
        try:
            previous_next_schedule_date = datetime.strptime(previous_next_schedule["date"], "%d/%m/%Y")
            previous_next_schedule_time = datetime.strptime(previous_next_schedule["time_24hour"], "%H:%M")
        except (ValueError, KeyError):
            print("Invalid previous_next_schedule format, ignoring")
            found_previous_next_slot = True
    
    # Search calendar
    for cal in calendar_data["calendars"]:
        for week in cal["days"]:
            for day_entry in week["days"]:
                day = day_entry.get("day")
                if not day or not day.get("date"):
                    continue
                
                date = day["date"]
                try:
                    slot_date = datetime.strptime(date, "%d/%m/%Y")
                except ValueError:
                    continue
                
                if slot_date.date() < current_datetime.date():
                    continue
                
                for slot in day.get("time_ahead", []):
                    if slot["24hours"] not in [t["24hours"] for t in sorted_timeorders]:
                        continue
                    if "passed" not in slot["consideration"].lower():
                        continue
                    
                    # Today: must be >=50 min ahead
                    if slot_date.date() == current_datetime.date():
                        try:
                            slot_time = datetime.strptime(slot["24hours"], "%H:%M")
                            delta = slot_time - current_time
                            if delta.total_seconds() / 60 < 50:
                                continue
                        except ValueError:
                            continue
                    
                    # Skip until after last_schedule
                    if last_schedule and not found_last_slot:
                        if (last_schedule.get("date") == date and 
                            last_schedule.get("time_24hour") == slot["24hours"] and 
                            last_schedule.get("id") == slot["id"]):
                            found_last_slot = True
                            continue
                        slot_time = datetime.strptime(slot["24hours"], "%H:%M")
                        if (slot_date.date() > last_schedule_date.date() or 
                            (slot_date.date() == last_schedule_date.date() and slot_time > last_schedule_time)):
                            found_last_slot = True
                    
                    # Skip previous_next_schedule
                    if previous_next_schedule and not found_previous_next_slot:
                        if (previous_next_schedule.get("date") == date and 
                            previous_next_schedule.get("time_24hour") == slot["24hours"] and 
                            previous_next_schedule.get("id") == slot["id"]):
                            found_previous_next_slot = True
                            continue
                        slot_time = datetime.strptime(slot["24hours"], "%H:%M")
                        if (slot_date.date() > previous_next_schedule_date.date() or 
                            (slot_date.date() == previous_next_schedule_date.date() and slot_time > previous_next_schedule_time)):
                            found_previous_next_slot = True
                    
                    if found_last_slot and found_previous_next_slot:
                        next_slot = {
                            "id": slot["id"],
                            "date": date,
                            "time_12hour": slot["12hours"],
                            "time_24hour": slot["24hours"]
                        }
                        print(f"Next slot: {next_slot['time_12hour']} on {date}")
                        break
                if next_slot:
                    break
            if next_slot:
                break
        if next_slot:
            break
    
    if not next_slot:
        print("No valid next slot found.")
        return
    
    # Write to schedules.json in script dir
    output_data = {
        "last_schedule": previous_next_schedule or last_schedule,
        "next_schedule": next_slot
    }
    
    print(f"Writing schedules to {schedules_path}")
    with open(schedules_path, 'w') as f:
        json.dump(output_data, f, indent=4)
    print(f"Schedule updated: {schedules_path}")

def current_time():
    """
    Save current time in 12h and 24h format to current_time.json in script directory.
    If the next_schedule in {type}schedules.json is in the past (or missing), 
    automatically call update_calendar() to refresh the calendar and schedules.
    """
    # Get current time info
    now = datetime.now()
    time_12h = now.strftime("%I:%M %p").lstrip("0").replace(" 0", " ").lower()
    time_24h = now.strftime("%H:%M")
    
    current_time_data = {
        "time_12hour": time_12h,
        "time_24hour": time_24h
    }
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, "current_time.json")
    
    print(f"Saving current time: {time_12h} ({time_24h}) → {output_path}")
    with open(output_path, 'w') as f:
        json.dump(current_time_data, f, indent=4)
    print(f"Current time saved to {output_path}")
    
    # --- Check if we need to refresh the calendar because next_schedule is outdated ---
    
    # First get the current 'type' from timeauthor.json
    pageauthors_path = r"C:\xampp\htdocs\chronedge\synarex\timeauthor.json"
    type_value = None
    try:
        with open(pageauthors_path, 'r') as f:
            pageauthors = json.load(f)
            type_value = pageauthors.get('type')
    except Exception as e:
        print(f"Could not read type from timeauthor.json: {e}")
        return
    
    if not type_value:
        print("No 'type' found in timeauthor.json – skipping schedule check")
        return
    
    # Path to the schedules file for this type
    schedules_path = os.path.join(script_dir, f"{type_value}schedules.json")
    
    need_update = False
    
    if not os.path.exists(schedules_path):
        print(f"{type_value}schedules.json not found → forcing calendar update")
        need_update = True
    else:
        try:
            with open(schedules_path, 'r') as f:
                sched_data = json.load(f)
            
            next_sched = sched_data.get("next_schedule")
            if not next_sched:
                print("No next_schedule in schedules.json → forcing update")
                need_update = True
            else:
                # Combine date + time from next_schedule
                sched_date_str = next_sched.get("date")      # "dd/mm/yyyy"
                sched_time_str = next_sched.get("time_24hour")  # "HH:MM"
                
                if not sched_date_str or not sched_time_str:
                    print("Invalid next_schedule format → forcing update")
                    need_update = True
                else:
                    next_datetime_str = f"{sched_date_str} {sched_time_str}"
                    next_datetime = datetime.strptime(next_datetime_str, "%d/%m/%Y %H:%M")
                    
                    if now >= next_datetime:
                        print(f"Next schedule {next_sched['time_12hour']} on {sched_date_str} is in the past or now → updating calendar")
                        need_update = True
                    else:
                        print(f"Next schedule still ahead ({next_sched['time_12hour']} on {sched_date_str}) – no update needed")
        
        except Exception as e:
            print(f"Error reading/parsing {type_value}schedules.json: {e} → forcing update")
            need_update = True
    
    # If needed, trigger full calendar + schedule refresh
    if need_update:
        print("Calling update_calendar() to refresh everything...")
        update_calendar()
        updating_insiderservers()
    else:
        print("Schedule is up to date.")

def updating_insiderservers():
    """Run the updateorders script for M5 timeframe."""
    try:
        insiders_server.requirements()
        insiders_server.update_table_fromupdatedusers()
        print("updated table")
    except Exception as e:
        print(f"Error updating table{e}")
    


if __name__ == "__main__":
    current_time()
