import connectwithinfinitydb as db
import json
import os
import shutil
from datetime import datetime
from colorama import Fore, Style, init

# Initialize colorama for cross-platform terminal colors
init()

OUTPUT_FILE_PATH = r"C:\xampp\htdocs\chronedge\synarex\usersdictionary.json"
MT5_TEMPLATE_SOURCE_DIR = r"C:\xampp\htdocs\chronedge\synarex\mt5\MetaTrader 5"
BROKERS_OUTPUT_FILE_PATH = r"C:\xampp\htdocs\chronedge\synarex\developersdictionary.json"

# --- HELPER FUNCTIONS ---

def log_and_print(message, level="INFO"):
    """Helper function to print formatted messages with color coding and spacing."""
    indent = "    "
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    level_colors = {
        "INFO": Fore.CYAN,
        "SUCCESS": Fore.GREEN,
        "ERROR": Fore.RED,
        "TITLE": Fore.MAGENTA,
        "WARNING": Fore.CYAN,
    }
    color = level_colors.get(level, Fore.WHITE)
    formatted_message = f"[ {timestamp} ] │ {level:7} │ {indent}{message}"
    print(f"{color}{formatted_message}{Style.RESET_ALL}")

def safe_float(value):
    """
    Safely converts a value (which might be None or the string 'None') to a float, 
    defaulting to 0.00.
    """
    if value is None:
        return 0.00
    
    value_str = str(value).strip().lower()
    
    if value_str in ('none', ''):
        return 0.00
    
    try:
        return float(value)
    except ValueError:
        log_and_print(f"WARNING: Non-numeric value encountered for float conversion: '{value}'. Defaulting to 0.00.", "WARNING")
        return 0.00

def update_history_string(current_history, new_value):
    """Appends a new value (which can be a raw string like 'None' or a number) to a comma-separated history string."""
    new_value_str = str(new_value).strip()
    
    # Check if the new value is insignificant or already captured
    if not new_value_str or new_value_str.lower() in ('null', ''):
        return str(current_history).strip()

    # Treat 'None' (from DB) as an empty string for concatenation purposes if it's the only entry
    current_history_str = str(current_history).strip()
    if current_history_str.lower() in ('none', 'null', ''):
        current_history_str = ""
    
    if not current_history_str:
        return new_value_str
    
    # Check if the new value is already the last entry
    last_entry = current_history_str.split(',')[-1].strip()
    
    # Use string comparison for raw values (like 'None')
    if last_entry == new_value_str:
        return current_history_str
    
    # Try numeric comparison if both values are convertible (for 0.0 vs 0.00 consistency)
    try:
        if safe_float(last_entry) == safe_float(new_value_str):
             return current_history_str
    except ValueError:
        # Ignore if conversion fails (e.g., comparing 'None' to '1.0')
        pass
    
    return f"{current_history_str},{new_value_str}"


def recordsfrom_developersdictionary():
    """
    Copies/Updates ALL broker records from developersdictionary.json
    → into updatedusersdictionary.json

    - If the broker name (key) already exists in updatedusersdictionary.json → UPDATE it
    - If it doesn't exist → ADD it (append)
    - developersdictionary.json is treated as the SOURCE OF TRUTH (latest data)
    - Safe, idempotent, can be run anytime (e.g. every hour or after updatebrokerrecords())
    """

    BROKERS_JSON = r"C:\xampp\htdocs\chronedge\synarex\developersdictionary.json"
    USERS_JSON   = r"C:\xampp\htdocs\chronedge\synarex\updatedusersdictionary.json"

    # Load developersdictionary.json (source of truth)
    if not os.path.exists(BROKERS_JSON):
        print(f"CRITICAL: {BROKERS_JSON} not found! Cannot sync.", "CRITICAL")
        return

    try:
        with open(BROKERS_JSON, "r", encoding="utf-8") as f:
            brokers_dict = json.load(f)
        if not isinstance(brokers_dict, dict):
            print("developersdictionary.json is not a valid dictionary", "ERROR")
            return
    except Exception as e:
        print(f"Failed to read developersdictionary.json: {e}", "CRITICAL")
        return

    # Load updatedusersdictionary.json (target)
    if os.path.exists(USERS_JSON):
        try:
            with open(USERS_JSON, "r", encoding="utf-8") as f:
                users_dict = json.load(f)
            if not isinstance(users_dict, dict):
                users_dict = {}
        except Exception as e:
            print(f"updatedusersdictionary.json corrupted, starting fresh: {e}", "WARNING")
            users_dict = {}
    else:
        print("updatedusersdictionary.json not found → will be created", "INFO")
        users_dict = {}

    updated_count = 0
    added_count = 0

    for user_brokerid, broker_data in brokers_dict.items():
        # Clean copy: remove fields that shouldn't go into users dictionary
        clean_data = broker_data.copy()

        # Remove MT5-specific or temporary fields (optional but recommended)
        fields_to_exclude = {
            "TERMINAL_PATH",
            "BASE_FOLDER",
            "RESET_EXECUTION_DATE_AND_BROKER_BALANCE",
            # Add more if needed later
        }
        for field in fields_to_exclude:
            clean_data.pop(field, None)

        if user_brokerid in users_dict:
            # Update existing record
            old_balance = users_dict[user_brokerid].get("BROKER_BALANCE")
            new_balance = clean_data.get("BROKER_BALANCE")
            users_dict[user_brokerid] = clean_data
            print(f"{user_brokerid}: UPDATED in updatedusersdictionary.json "
                  f"(Balance: {old_balance} → {new_balance})", "INFO")
            updated_count += 1
        else:
            # Add new record
            users_dict[user_brokerid] = clean_data
            print(f"{user_brokerid}: ADDED to updatedusersdictionary.json "
                  f"(Balance: {clean_data.get('BROKER_BALANCE', 0.0)})", "SUCCESS")
            added_count += 1

    # Save updated updatedusersdictionary.json
    try:
        with open(USERS_JSON, "w", encoding="utf-8") as f:
            json.dump(users_dict, f, indent=4, ensure_ascii=False)
            f.write("\n")
        print(f"SYNC COMPLETE! {added_count} added, {updated_count} updated → updatedusersdictionary.json", "SUCCESS")
    except Exception as e:
        print(f"FAILED to save updatedusersdictionary.json: {e}", "CRITICAL")

def update_table_fromupdatedusers():
    """
    Reads updatedusersdictionary.json and pushes ALL relevant fields BACK to insiders table.
    Now 100% reliable key matching + proper escaping + full field sync.
    FIX: Ensures CONTRACT_DAYS_LEFT is updated in its own column and not concatenated to 'loyalties'.
    """
    USERS_JSON_PATH = r"C:\xampp\htdocs\chronedge\synarex\updatedusersdictionary.json"
    insiders_TABLE = "insiders"

    if not os.path.exists(USERS_JSON_PATH):
        log_and_print(f"{USERS_JSON_PATH} not found! Nothing to sync.", "CRITICAL")
        return

    try:
        with open(USERS_JSON_PATH, "r", encoding="utf-8") as f:
            users_dict = json.load(f)
        if not isinstance(users_dict, dict) or not users_dict:
            log_and_print("updatedusersdictionary.json is empty or invalid.", "ERROR")
            return
    except Exception as e:
        log_and_print(f"Failed to read JSON: {e}", "CRITICAL")
        return

    log_and_print("=== Syncing updatedusersdictionary.json → Database ===", "TITLE")

    # Fetch all valid broker + id rows
    query = f"""
        SELECT id, broker, login
        FROM {insiders_TABLE}
        WHERE broker IS NOT NULL
          AND TRIM(broker) != ''
          AND broker != 'None'
    """
    result = db.execute_query(query)
    if result.get('status') != 'success':
        log_and_print("Failed to fetch broker mapping.", "ERROR")
        return

    # Build reliable map: normalized_key → db_id
    broker_to_id = {}
    for row in result['results']:
        broker_raw = str(row['broker']).strip()
        if not broker_raw:
            continue
        broker_norm = broker_raw.lower().replace(" ", "")
        key = f"{broker_norm}{row['id']}"
        broker_to_id[key] = row['id']

    updated = skipped = errors = 0

    for json_key, data in users_dict.items():
        db_id = broker_to_id.get(json_key.lower())

        if not db_id:
            log_and_print(f"SKIP: No DB match for key '{json_key}'", "WARNING")
            skipped += 1
            continue

        # Loyalty string - ONLY the base loyalty is used for the database 'loyalties' column
        base_loyalty = str(data.get("LOYALTIES", "low")).strip()
        loyalty_str = base_loyalty # <--- FIX: Removed contract_days_left concatenation

        # Build update fields
        fields = []

        # Numeric
        if "BROKER_BALANCE" in data:
            fields.append(f"broker_balance = {safe_float(data['BROKER_BALANCE'])}")
        if "PROFITANDLOSS" in data:
            fields.append(f"profitandloss = {safe_float(data['PROFITANDLOSS'])}")
            
        # Contract Days Left (UPDATED IN ITS OWN COLUMN)
        contract_days_update = data.get("CONTRACT_DAYS_LEFT")
        if contract_days_update is not None and str(contract_days_update).strip() not in ("None", "null", ""):
            # Use safe_float for database insertion to handle potential floats and set NULL if invalid
            safe_days = safe_float(contract_days_update)
            fields.append(f"contract_days_left = {safe_days}")
        else:
             # Add this to explicitly set it to NULL if not present/valid in JSON
             fields.append("contract_days_left = NULL")

        # Date
        exec_date = data.get("EXECUTION_START_DATE")
        if exec_date and str(exec_date).strip() not in ("None", "null", ""):
            date_str = str(exec_date).split(" ")[0]
            fields.append(f"execution_start_date = '{date_str}'")
        else:
            fields.append("execution_start_date = NULL")

        # Text history fields
        for json_field, db_field in [
            ("BROKER_BALANCE_HISTORY", "broker_balance_history"),
            ("EXECUTION_DATES_HISTORY", "execution_dates_history"),
            ("PROFITANDLOSS_HISTORY", "profitandlosshistory"),
            ("TRADES", "trades"),
        ]:
            if json_field in data:
                val = str(data[json_field]).replace("'", "''")
                fields.append(f"{db_field} = '{val}'")

        # Application status
        verification = str(data.get("ACCOUNT_VERIFICATION", "")).lower().strip()
        if verification == "verified":
            fields.append("application_status = 'approved'")
        elif verification == "invalid":
            fields.append("application_status = 'declined'")
        else:
            fields.append("application_status = 'pending'")

        # Loyalty (Only the base string)
        safe_loyalty = loyalty_str.replace("'", "''")
        fields.append(f"loyalties = '{safe_loyalty}'") # <--- FIX: loyalty_str no longer contains contract_days_left

        if not fields:
            continue

        sql = f"UPDATE {insiders_TABLE} SET " + ", ".join(fields) + f" WHERE id = {db_id}"
        res = db.execute_query(sql)

        if res.get('status') == 'success':
            log_and_print(f"UPDATED → {json_key} (ID: {db_id}) | Loyalty: {loyalty_str}", "SUCCESS")
            updated += 1
        else:
            log_and_print(f"FAILED → {json_key} (ID: {db_id}): {res.get('message')}", "ERROR")
            errors += 1

    log_and_print("=== Sync Complete ===", "TITLE")
    log_and_print(f"Updated: {updated} | Skipped: {skipped} | Errors: {errors}", "INFO")

def fetch_insiders_rows():
    """
    Pure DB to JSON export.
    ACCOUNT_VERIFICATION is now treated as persistent/config field.
    It is NEVER derived from application_status.
    Only defaults to "waiting" for completely new users (first export).
    Existing values are fully preserved across exports.
    """
    insiders_TABLE = "insiders"
    
    # These are the ONLY fields that do NOT exist in the database
    CONFIG_ONLY_FIELDS = {
        "ACCOUNT": "real",
        "STRATEGY": "hightolow",
        "SCALE": "consistency",
        "RISKREWARD": 3,
        "SYMBOLS": "all",
        "MARTINGALE_MARKETS": "neth25, usdjpy",
        "RESET_EXECUTION_DATE_AND_BROKER_BALANCE": "none",
    }
    
    JSON_KEY_ORDER = [
        "LOGIN_ID", "PASSWORD", "SERVER", "EXECUTION_START_DATE",
        "BROKER_BALANCE", "PROFITANDLOSS", 
        "ACCOUNT", "STRATEGY", "SCALE", "RISKREWARD",
        "SYMBOLS", "MARTINGALE_MARKETS",
        "ACCOUNT_VERIFICATION", "DB_APPLICATION_STATUS", "LOYALTIES",
        "PROFITANDLOSS_HISTORY", "BROKER_BALANCE_HISTORY", "EXECUTION_DATES_HISTORY",
        "RESET_EXECUTION_DATE_AND_BROKER_BALANCE",
        "CONTRACT_DAYS_LEFT", "TRADES",
        "BASE_FOLDER", "TERMINAL_PATH"
    ]
    
    users_dictionary = {}
    skipped_count = 0
    
    # Load existing JSON first to preserve ACCOUNT_VERIFICATION (and any future manual fields)
    existing_data = {}
    if os.path.exists(OUTPUT_FILE_PATH):
        try:
            with open(OUTPUT_FILE_PATH, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
        except Exception as e:
            log_and_print(f"Could not load existing JSON for preservation: {e}", "WARNING")
    
    try:
        log_and_print(f"\n===== Exporting Fresh Data from DB to JSON =====", "TITLE")
        
        query = f"""
            SELECT 
                id, broker, login, password, server, execution_start_date,
                application_status, broker_balance, profitandloss,
                broker_balance_history, execution_dates_history, profitandlosshistory,
                trades, loyalties, contract_days_left
            FROM {insiders_TABLE}
        """
        result = db.execute_query(query)
        if result.get('status') != 'success' or not result.get('results'):
            log_and_print("No data returned from database.", "WARNING")
            return
        
        rows = result['results']
        log_and_print(f"Fetched {len(rows)} rows from {insiders_TABLE}.", "SUCCESS")
        
        for row in rows:
            broker_raw = row.get('broker')
            if not broker_raw or str(broker_raw).strip().lower() in ('', 'none', 'null'):
                skipped_count += 1
                continue
                
            broker_clean = str(broker_raw).strip()
            broker_key = broker_clean.lower().replace(" ", "")
            user_id = str(row.get('id', ''))
            json_key = f"{broker_key}{user_id}"
            
            # Paths
            base_folder = rf"C:\xampp\htdocs\chronedge\synarex\usersdata\{broker_clean} {user_id}"
            terminal_folder = rf"C:\xampp\htdocs\chronedge\synarex\mt5\MetaTrader 5 {broker_clean} {user_id}"
            terminal_path = os.path.join(terminal_folder, "terminal64.exe")
            os.makedirs(base_folder, exist_ok=True)
            if not os.path.isdir(terminal_folder):
                try:
                    shutil.copytree(MT5_TEMPLATE_SOURCE_DIR, terminal_folder)
                    log_and_print(f"Auto-created MT5 folder: {terminal_folder}", "INFO")
                except Exception as e:
                    log_and_print(f"Failed to create MT5 folder: {e}", "ERROR")
            
            # Loyalty logic
            exec_history = str(row.get('execution_dates_history') or '').strip()
            db_loyalty = str(row.get('loyalties') or '').strip()
            final_loyalty = "justjoined" if not exec_history or exec_history.lower() in ('none', 'null', '') else db_loyalty
            
            # Application status (only for display, no longer controls verification)
            app_status_raw = str(row.get('application_status') or '').lower().strip()
            app_status = app_status_raw if app_status_raw in ('approved', 'declined', 'pending') else 'pending'
            
            # Safe contract days
            db_contract_days = row.get('contract_days_left')
            contract_days_value = None
            if db_contract_days is not None:
                cds = str(db_contract_days).strip().lower()
                if cds not in ('none', 'null', ''):
                    try:
                        contract_days_value = int(float(db_contract_days))
                    except (ValueError, TypeError):
                        pass
            
            # Build fresh user data
            user_data = {
                "LOGIN_ID": str(row.get('login', '')),
                "PASSWORD": str(row.get('password', '')),
                "SERVER": str(row.get('server', '')),
                "EXECUTION_START_DATE": row.get('execution_start_date'),
                "BROKER_BALANCE": safe_float(row.get('broker_balance', 0.0)),
                "PROFITANDLOSS": safe_float(row.get('profitandloss', 0.0)),
                "TRADES": str(row.get('trades') or ''),
                "DB_APPLICATION_STATUS": app_status,
                "LOYALTIES": final_loyalty,
                "PROFITANDLOSS_HISTORY": str(row.get('profitandlosshistory') or ''),
                "BROKER_BALANCE_HISTORY": str(row.get('broker_balance_history') or ''),
                "EXECUTION_DATES_HISTORY": exec_history,
                "BASE_FOLDER": base_folder,
                "TERMINAL_PATH": terminal_path,
                "CONTRACT_DAYS_LEFT": contract_days_value
            }
            
            # Merge with existing record to preserve ACCOUNT_VERIFICATION
            existing_record = existing_data.get(json_key, {})
            if "ACCOUNT_VERIFICATION" in existing_record:
                user_data["ACCOUNT_VERIFICATION"] = existing_record["ACCOUNT_VERIFICATION"]
            else:
                user_data["ACCOUNT_VERIFICATION"] = "waiting"  # Only for brand-new users
            
            # Add config-only fields
            user_data.update(CONFIG_ONLY_FIELDS)
            
            # Enforce key order
            ordered_data = {key: user_data.get(key) for key in JSON_KEY_ORDER}
            users_dictionary[json_key] = ordered_data
        
        # Write final JSON
        os.makedirs(os.path.dirname(OUTPUT_FILE_PATH), exist_ok=True)
        with open(OUTPUT_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(users_dictionary, f, indent=4)
            
        log_and_print(f"Successfully exported {len(users_dictionary)} accounts to JSON.", "SUCCESS")
        if skipped_count:
            log_and_print(f"Skipped {skipped_count} rows (invalid broker).", "INFO")
            
    except Exception as e:
        log_and_print(f"Critical error in fetch_insiders_rows(): {e}", "ERROR")
    finally:
        db.shutdown()
        log_and_print("===== Export Complete =====", "TITLE")

def validdetails_verified():
    """
    Synchronizes 'ACCOUNT_VERIFICATION' and 'DB_APPLICATION_STATUS' fields
    from usersdictionary.json into both developersdictionary.json and updatedusersdictionary.json.
    
    The 'ACCOUNT_VERIFICATION' value in the source dictionary now determines 
    the 'DB_APPLICATION_STATUS' value in ALL dictionaries, including usersdictionary.json itself.
    - 'invalid' -> 'declined'
    - 'verified' -> 'approved'
    - 'waiting' -> 'pending'
    
    This function ONLY updates the status fields, preserving all other data in the target files.
    """
    UPDATED_USERS_OUTPUT_FILE_PATH = r"C:\xampp\htdocs\chronedge\synarex\updatedusersdictionary.json"
    log_and_print("--- Starting Status Sync: usersdictionary → brokers & updatedusers ---", "TITLE")

    # 1. Load Source Dictionary (usersdictionary.json)
    if not os.path.exists(OUTPUT_FILE_PATH):
        log_and_print(f"Source file not found: {OUTPUT_FILE_PATH}. Cannot sync.", "CRITICAL")
        return
    try:
        with open(OUTPUT_FILE_PATH, 'r', encoding='utf-8') as f:
            source_dict = json.load(f)
    except Exception as e:
        log_and_print(f"Failed to read source JSON ({OUTPUT_FILE_PATH}): {e}", "ERROR")
        return

    # 2. Load Target Dictionaries (developersdictionary.json & updatedusersdictionary.json)
    
    # Load Brokers Dictionary
    brokers_dict = {}
    if os.path.exists(BROKERS_OUTPUT_FILE_PATH):
        try:
            with open(BROKERS_OUTPUT_FILE_PATH, 'r', encoding='utf-8') as f:
                brokers_dict = json.load(f)
        except Exception as e:
            log_and_print(f"Brokers dictionary corrupted, starting fresh in memory: {e}", "WARNING")
            brokers_dict = {}

    # Load Updated Users Dictionary
    updated_users_dict = {}
    if os.path.exists(UPDATED_USERS_OUTPUT_FILE_PATH):
        try:
            with open(UPDATED_USERS_OUTPUT_FILE_PATH, 'r', encoding='utf-8') as f:
                updated_users_dict = json.load(f)
        except Exception as e:
            log_and_print(f"Updated Users dictionary corrupted, starting fresh in memory: {e}", "WARNING")
            updated_users_dict = {}

    # 3. Synchronize Fields
    brokers_updated_count = 0
    updated_users_updated_count = 0
    source_updated_count = 0 # Counter for usersdictionary updates
    
    for key, data in source_dict.items():
        # Get ACCOUNT_VERIFICATION status from the source
        verification = str(data.get("ACCOUNT_VERIFICATION", "waiting")).strip().lower()

        # Determine the derived DB_APPLICATION_STATUS
        if verification == "invalid":
            app_status = "declined"
        elif verification == "verified":
            app_status = "approved"
        elif verification == "waiting":
            app_status = "pending"
        else:
            app_status = "pending"

        # **NEW LOGIC: Update Source Dictionary (usersdictionary.json) in memory**
        current_app_status = str(data.get("DB_APPLICATION_STATUS")).strip().lower()
        if current_app_status != app_status:
             source_dict[key]["DB_APPLICATION_STATUS"] = app_status
             source_updated_count += 1
             log_and_print(f"Source: Updated DB_APPLICATION_STATUS for {key} to '{app_status}'", "INFO")


        # Update Brokers Dictionary
        if key in brokers_dict:
            brokers_dict[key]["ACCOUNT_VERIFICATION"] = verification
            brokers_dict[key]["DB_APPLICATION_STATUS"] = app_status
            brokers_updated_count += 1
            log_and_print(f"Brokers: Updated status for {key} to Verification='{verification}', Status='{app_status}'", "INFO")
        
        # Update UpdatedUsers Dictionary
        if key in updated_users_dict:
            updated_users_dict[key]["ACCOUNT_VERIFICATION"] = verification
            updated_users_dict[key]["DB_APPLICATION_STATUS"] = app_status
            updated_users_updated_count += 1
            log_and_print(f"Updated Users: Updated status for {key}", "INFO")


    # 4. Save Target Files (Including the updated Source Dictionary)
    
    # Save Source Dictionary (usersdictionary.json)
    if source_updated_count > 0:
        try:
            os.makedirs(os.path.dirname(OUTPUT_FILE_PATH), exist_ok=True)
            with open(OUTPUT_FILE_PATH, 'w', encoding='utf-8') as f:
                json.dump(source_dict, f, indent=4, ensure_ascii=False)
            log_and_print(f"Saved usersdictionary.json. Updated {source_updated_count} records.", "SUCCESS")
        except Exception as e:
            log_and_print(f"FAILED to save usersdictionary.json: {e}", "CRITICAL")
            
    # Save Brokers Dictionary
    try:
        os.makedirs(os.path.dirname(BROKERS_OUTPUT_FILE_PATH), exist_ok=True)
        with open(BROKERS_OUTPUT_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(brokers_dict, f, indent=4, ensure_ascii=False)
        log_and_print(f"Saved developersdictionary.json. Updated {brokers_updated_count} records.", "SUCCESS")
    except Exception as e:
        log_and_print(f"FAILED to save developersdictionary.json: {e}", "CRITICAL")

    # Save Updated Users Dictionary
    try:
        os.makedirs(os.path.dirname(UPDATED_USERS_OUTPUT_FILE_PATH), exist_ok=True)
        with open(UPDATED_USERS_OUTPUT_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(updated_users_dict, f, indent=4, ensure_ascii=False)
        log_and_print(f"Saved updatedusersdictionary.json. Updated {updated_users_updated_count} records.", "SUCCESS")
    except Exception as e:
        log_and_print(f"FAILED to save updatedusersdictionary.json: {e}", "CRITICAL")
    
    log_and_print("--- Status Sync Complete ---", "TITLE")

def copy_verified_users_to_developers_dictionary():
    """
    Reads the main user dictionary, filters users where:
    1. ACCOUNT_VERIFICATION is 'verified'
    2. LOYALTIES is 'justjoined' OR 'elligible'
    
    Copies these records to a NEW, empty brokers dictionary in memory, and then 
    overwrites the BROKERS_OUTPUT_FILE_PATH completely.
    """
    log_and_print("--- Starting Copy Verified Users to Brokers Dictionary (OVERWRITE MODE) ---", "TITLE")
    
    # --- 1. Load existing data (Source only) ---
    users_dictionary = {}
    developers_dictionary = {} # **Starts as an empty dictionary to ensure a complete overwrite**
    
    try:
        # Load the main users dictionary (source)
        if os.path.exists(OUTPUT_FILE_PATH):
            with open(OUTPUT_FILE_PATH, 'r') as f:
                users_dictionary = json.load(f)
        else:
            log_and_print("Main users dictionary not found. Nothing to copy.", "WARNING")
            return
            
    except Exception as e:
        log_and_print(f"ERROR: Failed to load users JSON file: {str(e)}", "ERROR")
        return

    # --- 2. Filter and copy users into the empty brokers dictionary ---
    copied_count = 0
    
    for key, user_data in users_dictionary.items():
        account_verified = str(user_data.get("ACCOUNT_VERIFICATION", "")).lower().strip() == "verified"
        loyalty_status = str(user_data.get("LOYALTIES", "")).lower().strip()
        loyalty_check = loyalty_status in ("justjoined", "re-enrolled")
        
        if account_verified and loyalty_check:
            # 🔔 Action: Copy user data to the brokers dictionary
            developers_dictionary[key] = user_data.copy()
            copied_count += 1
            log_and_print(f"Copying user {key} (Loyalty: {loyalty_status}) to brokers dictionary.", "INFO")
            
    # --- 3. Write output file (Completely overwrites the file with only the new data) ---
    
    if copied_count > 0 or os.path.exists(BROKERS_OUTPUT_FILE_PATH):
        try:
            os.makedirs(os.path.dirname(BROKERS_OUTPUT_FILE_PATH), exist_ok=True)
            with open(BROKERS_OUTPUT_FILE_PATH, 'w') as f:
                json.dump(developers_dictionary, f, indent=4) 
            log_and_print(f"Successfully **copied** {copied_count} user(s) and **completely OVERWROTE** the brokers JSON file. Total brokers: {len(developers_dictionary)}.", "SUCCESS")
        except IOError as file_error:
            log_and_print(f"ERROR: Failed to write brokers JSON file: {file_error}", "ERROR")
    else:
        log_and_print("No qualified users found to copy. Brokers JSON file remains unchanged.", "INFO")

    log_and_print("--- Finished Copy Verified Users to Brokers Dictionary ---", "TITLE")

def update_application_status_in_database():
    USERS_JSON_PATH = r"C:\xampp\htdocs\chronedge\synarex\usersdictionary.json"
    insiders_TABLE = "insiders"

    if not os.path.exists(USERS_JSON_PATH):
        log_and_print(f"{USERS_JSON_PATH} not found! Skipping application status sync.", "WARNING")
        return

    try:
        with open(USERS_JSON_PATH, "r", encoding="utf-8") as f:
            users_dict = json.load(f)
        if not isinstance(users_dict, dict) or not users_dict:
            log_and_print("usersdictionary.json is empty or invalid.", "ERROR")
            return
    except Exception as e:
        log_and_print(f"Failed to read usersdictionary.json: {e}", "CRITICAL")
        return

    log_and_print("=== Updating Application Status from usersdictionary.json ===", "TITLE")

    # Build broker + id → db_id map (same logic as original function)
    query = f"""
        SELECT id, broker
        FROM {insiders_TABLE}
        WHERE broker IS NOT NULL AND TRIM(broker) != '' AND broker != 'None'
    """
    result = db.execute_query(query)
    if result.get('status') != 'success':
        log_and_print("Failed to fetch broker mapping for application status sync.", "ERROR")
        return

    broker_to_id = {}
    for row in result['results']:
        broker_norm = str(row['broker']).strip().lower().replace(" ", "")
        key = f"{broker_norm}{row['id']}"
        broker_to_id[key] = row['id']

    updated = skipped = 0

    for json_key, data in users_dict.items():
        db_id = broker_to_id.get(json_key.lower())

        if not db_id:
            log_and_print(f"SKIP (no DB match): {json_key}", "WARNING")
            skipped += 1
            continue

        verification = str(data.get("ACCOUNT_VERIFICATION", "")).strip().lower()

        if verification == "verified":
            new_status = "approved"
        elif verification == "invalid":
            new_status = "declined"
        elif verification == "waiting":
            new_status = "pending"
        else:
            new_status = "pending"  # default/fallback

        sql = f"UPDATE {insiders_TABLE} SET application_status = '{new_status}' WHERE id = {db_id}"
        res = db.execute_query(sql)

        if res.get('status') == 'success':
            log_and_print(f"STATUS → {json_key} (ID: {db_id}) | {verification} → {new_status}", "SUCCESS")
            updated += 1
        else:
            log_and_print(f"FAILED → {json_key} (ID: {db_id}): {res.get('message')}", "ERROR")

    log_and_print("=== Application Status Sync Complete ===", "TITLE")
    log_and_print(f"Updated: {updated} | Skipped: {skipped}", "INFO")



def move_verifiedusers_to_developersdictionary():  
    validdetails_verified()
    copy_verified_users_to_developers_dictionary()
    update_application_status_in_database()

if __name__ == "__main__":
    fetch_insiders_rows()
    #login the users broker and set account verification to 'verified' if valid
    move_verifiedusers_to_developersdictionary()
