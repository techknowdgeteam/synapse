import os
import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import glob
import MetaTrader5 as mt5
import copy
import math
import shutil
import re

# --- GLOBALS ---
BROKER_DICT_PATH = r"C:\xampp\htdocs\synapse\synarex\brokers.json"
DEVELOPER_USERS = r"C:\xampp\htdocs\synapse\synarex\usersdata\developers\developers.json"
INVESTOR_USERS = r"C:\xampp\htdocs\synapse\synarex\usersdata\investors\investors.json"
SYMBOL_CATEGORY_PATH = r"C:\xampp\htdocs\synapse\synarex\symbolscategory.json"
DEV_PATH = r"C:\xampp\htdocs\synapse\synarex\usersdata\developers"
INV_PATH = r"C:\xampp\htdocs\synapse\synarex\usersdata\investors"
DEFAULT_ACCOUNTMANAGEMENT = r"C:\xampp\htdocs\synapse\synarex\default_accountmanagement.json"
NORMALIZE_SYMBOLS_PATH = r"C:\xampp\htdocs\synapse\synarex\symbols_normalization.json"



def get_normalized_symbol(record_symbol, norm_map):
    """
    1. Takes "US Oil" from record.
    2. Cleans it to "USOIL".
    3. Finds the list in Normalization JSON.
    4. Checks broker for any name in that list.
    5. Handles special suffixes like m, pro, +, \, . etc.
    """
    if not record_symbol:
        return None

    # Step 1: Clean the record name for searching (remove spaces, underscores, dots)
    # "US Oil" -> "USOIL" | "US_OIL" -> "USOIL"
    search_term = record_symbol.replace(" ", "").replace("_", "").replace(".", "").upper()
    
    # Also create a base version without any special suffixes for matching
    # Remove common suffixes like m, pro, +, \, etc.
    import re
    
    # Pattern to remove common suffixes (m, pro, +, \, etc.) at the end
    # This handles cases like CHFJPY+ -> CHFJPY
    base_search_term = re.sub(r'[+\\]|\.PRO|\.M|M$|PRO$', '', search_term)
    
    norm_data = norm_map.get("NORMALIZATION", {})
    target_synonyms = []
    base_target_synonyms = []  # Store base versions without suffixes

    # Step 2: Go to Normalization straight
    for standard_key, synonyms in norm_data.items():
        # Clean the standard key and all synonyms for a fair comparison
        clean_key = standard_key.replace("_", "").upper()
        clean_syns = [s.replace(" ", "").replace("_", "").replace("/", "").upper() for s in synonyms]
        
        # Also create base versions without suffixes
        base_clean_key = re.sub(r'[+\\]|\.PRO|\.M|M$|PRO$', '', clean_key)
        base_clean_syns = [re.sub(r'[+\\]|\.PRO|\.M|M$|PRO$', '', s) for s in clean_syns]
        
        # Check both the full term and base term
        if (search_term == clean_key or search_term in clean_syns or 
            base_search_term == base_clean_key or base_search_term in base_clean_syns):
            target_synonyms = synonyms
            # Also store base versions for suffix matching
            base_target_synonyms = [re.sub(r'[+\\]|\.PRO|\.M|M$|PRO$', '', s.replace(" ", "").replace("_", "").replace("/", "").upper()) 
                                   for s in synonyms]
            break

    # If the record symbol isn't in our map, at least try the cleaned versions
    if not target_synonyms:
        target_synonyms = [record_symbol, search_term, base_search_term]
        base_target_synonyms = [base_search_term]

    # Step 3: Check Broker for any of these possibilities
    # Fetch all available names once to save time
    available_symbols = [s.name for s in mt5.symbols_get()]
    
    # Create a map of base symbol names to their actual broker names
    # This helps with suffix matching
    base_to_actual = {}
    for broker_name in available_symbols:
        # Remove common suffixes for base matching
        base_name = re.sub(r'[+\\]|\.PRO|\.M|M$|PRO$', '', broker_name.upper())
        base_name = base_name.replace(".", "")
        if base_name not in base_to_actual:
            base_to_actual[base_name] = []
        base_to_actual[base_name].append(broker_name)
    
    # First try exact matches
    for option in target_synonyms:
        # Check for Exact Match (e.g., "USOUSD")
        if option in available_symbols:
            return option
            
        # Check with different case variations
        for broker_name in available_symbols:
            if broker_name.upper() == option.upper():
                return broker_name
    
    # Then try matching with suffixes - using base versions
    for option in base_target_synonyms:
        # Clean the option for matching
        clean_option = re.sub(r'[+\\]|\.PRO|\.M|M$|PRO$', '', option.upper())
        if clean_option in base_to_actual:
            # Return the first actual broker name (preferring standard format)
            actual_symbols = base_to_actual[clean_option]
            # Prefer symbols without special suffixes first
            for sym in actual_symbols:
                if not re.search(r'[+\\]|\.PRO|\.M|M$|PRO$', sym.upper()):
                    return sym
            # If all have suffixes, return the first one
            return actual_symbols[0]
    
    # Finally try partial matching with any remaining options
    for option in target_synonyms:
        # Clean the option for suffix matching
        clean_opt = re.sub(r'[+\\]|\.PRO|\.M|M$|PRO$', '', option.replace("/", "").upper())
        
        # Check for Suffix Match in our pre-built map
        if clean_opt in base_to_actual:
            actual_symbols = base_to_actual[clean_opt]
            # Prefer symbols without special suffixes first
            for sym in actual_symbols:
                if not re.search(r'[+\\]|\.PRO|\.M|M$|PRO$', sym.upper()):
                    return sym
            return actual_symbols[0]
        
        # Legacy check: try if any broker symbol starts with our cleaned option
        for broker_name in available_symbols:
            broker_upper = broker_name.upper()
            broker_base = re.sub(r'[+\\]|\.PRO|\.M|M$|PRO$', '', broker_upper)
            if broker_upper.startswith(clean_opt) or broker_base.startswith(clean_opt):
                return broker_name

    print(f"[!] No broker match found for {record_symbol} even after normalization check.")
    return None

def clean_risk_folders():
    """
    Scans the DEV_PATH and permanently deletes all folders ending in 'usd_risk'.
    This clears out all calculated risk buckets and their contained JSON files.
    """
    print(f"\n{'='*10} RISK FOLDER CLEANUP {'='*10}")
    
    if not os.path.exists(DEV_PATH):
        print(f" [!] Error: DEV_PATH {DEV_PATH} does not exist.")
        return False

    # Find all directories that end with 'usd_risk'
    # Recursive search ensures we hit sub-strategy folders
    risk_folders = glob.glob(os.path.join(DEV_PATH, "**", "*usd_risk"), recursive=True)

    if not risk_folders:
        print(f" 🛡️  System clean: No risk buckets found.")
        print(f"{'='*10} CLEANUP COMPLETE {'='*10}\n")
        return True

    deleted_count = 0
    print(f" 🧹 Starting deep purge of risk directories...")

    for folder_path in risk_folders:
        # Check if it exists and is a directory (shutil.rmtree requires a dir)
        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            try:
                shutil.rmtree(folder_path)
                deleted_count += 1
            except Exception as e:
                print(f"  └─ ❌ Error purging {os.path.basename(folder_path)}: {e}")

    if deleted_count > 0:
        print(f"  └─ ✅ Successfully purged {deleted_count} risk bucket folders")
    else:
        print(f"  └─ 🔘 No folders required deletion")

    print(f"{'='*10} CLEANUP COMPLETE {'='*10}\n")
    return True

def purge_unauthorized_symbols():
    """
    Iterates through users and their specific strategy folders (new_filename).
    Validates symbols in limit_orders.json against the local strategy config.
    Removes unauthorized orders from BOTH 'limit_orders.json' and 'limit_orders_backup.json'.
    If allowedsymbolsandvolumes.json is missing in strategy folder, copies from user root.
    """
    print(f"\n{'='*10} PURGING UNAUTHORIZED SYMBOLS BY STRATEGY {'='*10}")
    
    try:
        # 1. Load User IDs
        if not os.path.exists(DEVELOPER_USERS):
            print(f" [!] Error: Users file not found: {DEVELOPER_USERS}")
            return False

        with open(DEVELOPER_USERS, 'r') as f:
            users_data = json.load(f)

        total_purged_overall = 0

        for dev_broker_id in users_data.keys():
            print(f" [{dev_broker_id}] 🔍 Auditing strategy-specific permissions...")
            
            user_folder = os.path.join(DEV_PATH, dev_broker_id)
            acc_mgmt_path = os.path.join(user_folder, "accountmanagement.json")
            
            if not os.path.exists(acc_mgmt_path):
                print(f"  └─ ⚠️  accountmanagement.json missing: Skipping user")
                continue

            # 2. Identify Strategy Folders (new_filename) from accountmanagement.json
            strategy_folders = []
            strategy_names = []  # Track names for logging
            try:
                with open(acc_mgmt_path, 'r') as f:
                    acc_data = json.load(f)
                
                # Navigate the nested JSON structure to find 'new_filename'
                poi = acc_data.get("chart", {}).get("define_candles", {}).get("entries_poi_condition", {})
                for app_val in poi.values():
                    if isinstance(app_val, dict):
                        for ent_val in app_val.values():
                            if isinstance(ent_val, dict) and ent_val.get("new_filename"):
                                strategy_dir = os.path.join(user_folder, ent_val["new_filename"])
                                strategy_name = ent_val["new_filename"]
                                if strategy_dir not in strategy_folders:
                                    strategy_folders.append(strategy_dir)
                                    strategy_names.append(strategy_name)
                                    print(f"  └─ 📁 Found strategy folder: {strategy_name}")
            except Exception as e:
                print(f"  └─ ❌ Error parsing strategy folders: {e}")
                continue

            if not strategy_folders:
                print(f"  └─ ℹ️  No strategy folders found for user {dev_broker_id}")
                continue

            # 3. Process each Strategy Folder independently
            for idx, strategy_folder in enumerate(strategy_folders):
                folder_name = os.path.basename(strategy_folder)
                print(f"\n    📂 Processing strategy: {strategy_names[idx] if idx < len(strategy_names) else folder_name}")
                
                # Define source and destination paths for allowedsymbolsandvolumes.json
                user_root_symbols_path = os.path.join(user_folder, "allowedsymbolsandvolumes.json")
                strategy_symbols_path = os.path.join(strategy_folder, "allowedsymbolsandvolumes.json")
                
                # Check if strategy folder has allowedsymbolsandvolumes.json
                if not os.path.exists(strategy_symbols_path):
                    print(f"      └─ ⚠️  allowedsymbolsandvolumes.json missing in strategy folder")
                    
                    # Check if source file exists in user root
                    if os.path.exists(user_root_symbols_path):
                        try:
                            # Create strategy folder if it doesn't exist (though it should)
                            os.makedirs(strategy_folder, exist_ok=True)
                            
                            # Copy the file
                            import shutil
                            shutil.copy2(user_root_symbols_path, strategy_symbols_path)
                            print(f"      └─ ✅ Copied allowedsymbolsandvolumes.json from user root to strategy folder")
                        except Exception as e:
                            print(f"      └─ ❌ Failed to copy symbols file: {e}")
                            continue
                    else:
                        print(f"      └─ ❌ No source allowedsymbolsandvolumes.json found in user root - skipping strategy")
                        continue

                # Load allowed symbols for THIS specific strategy
                allowed_symbols = set()
                allowed_base_symbols = set()  # Store base versions without suffixes
                try:
                    with open(strategy_symbols_path, 'r') as f:
                        v_data = json.load(f)
                        for category in v_data.values():
                            if isinstance(category, list):
                                for item in category:
                                    if 'symbol' in item:
                                        symbol = item['symbol'].upper()
                                        allowed_symbols.add(symbol)
                                        # Also store base version without suffixes
                                        import re
                                        base_symbol = re.sub(r'[+\\]|\.PRO|\.M|M$|PRO$', '', symbol)
                                        allowed_base_symbols.add(base_symbol)
                    print(f"      └─ ✅ Loaded {len(allowed_symbols)} allowed symbols from {os.path.basename(strategy_symbols_path)}")
                except Exception as e:
                    print(f"      └─ ❌ Failed to load symbols: {e}")
                    continue

                # 4. Audit limit_orders and limit_orders_backup in this folder
                target_patterns = ["limit_orders.json", "limit_orders_backup.json"]
                strategy_purged_count = 0
                files_processed = 0

                for pattern in target_patterns:
                    # Look only within this specific strategy directory
                    files_to_audit = glob.glob(os.path.join(strategy_folder, "**", pattern), recursive=True)

                    for file_path in files_to_audit:
                        if "risk_reward_" in file_path: 
                            continue
                        
                        try:
                            if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
                                continue

                            with open(file_path, 'r') as f:
                                orders = json.load(f)
                            
                            if not isinstance(orders, list):
                                continue

                            original_count = len(orders)
                            filename = os.path.basename(file_path)
                            files_processed += 1
                            
                            # Filter logic: Keep only symbols authorized in THIS strategy's config
                            # Check both exact match and base symbol match (without suffixes)
                            purged_orders = []
                            for order in orders:
                                order_symbol = order.get('symbol', '').upper()
                                
                                # Check if symbol is directly allowed
                                if order_symbol in allowed_symbols:
                                    purged_orders.append(order)
                                    continue
                                
                                # Check if base symbol (without suffixes) is allowed
                                import re
                                base_order_symbol = re.sub(r'[+\\]|\.PRO|\.M|M$|PRO$', '', order_symbol)
                                if base_order_symbol in allowed_base_symbols:
                                    purged_orders.append(order)
                                    print(f"        └─ 🔍 Matched {order_symbol} to base symbol {base_order_symbol}")
                                    continue
                                
                                # If we get here, symbol is not authorized
                                #print(f"        └─ ❌ Unauthorized symbol: {order_symbol}")

                            diff = original_count - len(purged_orders)
                            if diff > 0:
                                with open(file_path, 'w') as f:
                                    json.dump(purged_orders, f, indent=4)
                                strategy_purged_count += diff
                                print(f"      └─ 🔄 {filename}: Purged {diff}/{original_count} orders")
                            else:
                                if original_count > 0:
                                    print(f"      └─ ✓ {filename}: All {original_count} orders authorized")
                                
                        except Exception as e:
                            print(f"      └─ ❌ Failed to process {filename}: {e}")

                if files_processed == 0:
                    print(f"      └─ ℹ️  No order files found to process")

                if strategy_purged_count > 0:
                    print(f"    ✅ [{folder_name}] Total purged: {strategy_purged_count} unauthorized entries")
                    total_purged_overall += strategy_purged_count
                else:
                    if files_processed > 0:
                        print(f"    ℹ️ [{folder_name}] No unauthorized entries found")
                    else:
                        print(f"    ℹ️ [{folder_name}] No order files to check")

        print(f"\n{'='*10} PURGE COMPLETE: {total_purged_overall} TOTAL REMOVED {'='*10}\n")
        return True

    except Exception as e:
        print(f" [!] Critical Error during symbol purge: {e}")
        return False      

def backup_limit_orders():
    """
    Identifies strategy folders via accountmanagement.json.
    Targets limit orders specifically within the 'pending_orders' subfolder.
    Prioritizes restoring from backup; otherwise, creates a new backup.
    """
    print(f"\n{'='*10} SYNCING STRATEGY-SPECIFIC LIMIT ORDERS {'='*10}")
    
    try:
        # 1. Load User IDs
        if not os.path.exists(DEVELOPER_USERS) or os.path.getsize(DEVELOPER_USERS) == 0:
            print(f" [!] Error: Users file missing or empty: {DEVELOPER_USERS}")
            return False
            
        with open(DEVELOPER_USERS, 'r') as f:
            users_data = json.load(f)
        
        total_restored = 0
        total_backed_up = 0
        
        for dev_broker_id in users_data.keys():
            print(f" [{dev_broker_id}] 🔍 Auditing strategy directories for sync...")
            
            user_folder = os.path.join(DEV_PATH, dev_broker_id)
            acc_mgmt_path = os.path.join(user_folder, "accountmanagement.json")
            
            if not os.path.exists(acc_mgmt_path):
                print(f"  └─ ⚠️  accountmanagement.json missing: Skipping user")
                continue

            # 2. Extract specific strategy folders (new_filename)
            strategy_folders = []
            try:
                with open(acc_mgmt_path, 'r') as f:
                    acc_data = json.load(f)
                
                # Navigate nested structure to find 'new_filename'
                poi = acc_data.get("chart", {}).get("define_candles", {}).get("entries_poi_condition", {})
                for app_val in poi.values():
                    if isinstance(app_val, dict):
                        for ent_val in app_val.values():
                            if isinstance(ent_val, dict) and ent_val.get("new_filename"):
                                strategy_dir = os.path.join(user_folder, ent_val["new_filename"])
                                if strategy_dir not in strategy_folders:
                                    strategy_folders.append(strategy_dir)
            except Exception as e:
                print(f"  └─ ❌ Error parsing strategy folders: {e}")
                continue

            # 3. Sync files within the 'pending_orders' subfolder
            for strategy_folder in strategy_folders:
                # Update path to target the 'pending_orders' subfolder
                pending_folder = os.path.join(strategy_folder, "pending_orders")
                
                if not os.path.exists(pending_folder):
                    # We skip if the subfolder doesn't exist yet
                    continue

                folder_name = os.path.basename(strategy_folder)
                limit_path = os.path.join(pending_folder, "limit_orders.json")
                backup_path = os.path.join(pending_folder, "limit_orders_backup.json")

                # --- SYNC LOGIC ---
                
                # Priority 1: Restore (Backup exists and has data)
                if os.path.exists(backup_path) and os.path.getsize(backup_path) > 0:
                    try:
                        shutil.copy2(backup_path, limit_path)
                        total_restored += 1
                        print(f"  └─ [{folder_name}/pending_orders] 🔄 Restored (Backup -> Original)")
                    except Exception as e:
                        print(f"  └─ [{folder_name}] ❌ Restore failed: {e}")

                # Priority 2: Create Backup (Original exists, but backup is missing/empty)
                elif os.path.exists(limit_path) and os.path.getsize(limit_path) > 0:
                    try:
                        shutil.copy2(limit_path, backup_path)
                        total_backed_up += 1
                        print(f"  └─ [{folder_name}/pending_orders] 💾 Backed up (Original -> Backup)")
                    except Exception as e:
                        print(f"  └─ [{folder_name}] ❌ Backup failed: {e}")
        return True

    except Exception as e:
        print(f" [!] Critical Error during sync: {e}")
        return False

def provide_orders_volume():
    """
    Synchronizes specific volumes from allowedsymbolsandvolumes.json to limit_orders.json.
    Includes professional logging with change detection and Developer/Broker grouping.
    """
    try:
        if not os.path.exists(DEVELOPER_USERS) or os.path.getsize(DEVELOPER_USERS) == 0:
            print(f" [!] Error: Users file not found: {DEVELOPER_USERS}")
            return False

        with open(DEVELOPER_USERS, 'r', encoding='utf-8') as f:
            users_data = json.load(f)

        print(f"\n{'='*20} VOLUME SYNCHRONIZATION ENGINE {'='*20}")

        for user_broker_id in users_data.keys():
            print(f"\n 👤 DEVELOPER ID: {user_broker_id}")
            print(f" └{'─'*45}")
            
            user_folder = os.path.join(DEV_PATH, user_broker_id)
            acc_mgmt_path = os.path.join(user_folder, "accountmanagement.json")

            if not os.path.exists(acc_mgmt_path):
                print(f"   ⚠️  Skipping: accountmanagement.json not found.")
                continue

            # 1. Identify strategy folders
            secondary_folders = []
            try:
                with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                    acc_data = json.load(f)
                poi = acc_data.get("chart", {}).get("define_candles", {}).get("entries_poi_condition", {})
                for app_val in poi.values():
                    if isinstance(app_val, dict):
                        for ent_val in app_val.values():
                            if isinstance(ent_val, dict) and ent_val.get("new_filename"):
                                path = os.path.join(user_folder, ent_val["new_filename"])
                                if path not in secondary_folders:
                                    secondary_folders.append(path)
            except Exception as e:
                print(f"   ❌ Error parsing account management: {e}")
                continue

            # 2. Process each strategy folder
            for strategy_folder in secondary_folders:
                folder_name = os.path.basename(strategy_folder)
                config_path = os.path.join(strategy_folder, "allowedsymbolsandvolumes.json")
                
                if not os.path.exists(config_path):
                    continue

                print(f"   📂 Strategy: {folder_name}")

                # Build Volume Lookup
                volume_map = {}
                try:
                    with open(config_path, 'r', encoding='utf-8') as f:
                        config_data = json.load(f)
                    for category in config_data.values():
                        if not isinstance(category, list): continue
                        for item in category:
                            symbol = str(item.get("symbol", "")).upper()
                            if not symbol: continue
                            if symbol not in volume_map: volume_map[symbol] = {}
                            for key, value in item.items():
                                if key.endswith("_specs") and isinstance(value, dict):
                                    tf = key.replace("_specs", "").upper()
                                    if "volume" in value:
                                        volume_map[symbol][tf] = float(value["volume"])
                except Exception as e:
                    print(f"     ❌ Config Error: {e}")
                    continue

                # 3. Apply to Order Files
                limit_files = glob.glob(os.path.join(strategy_folder, "**", "limit_orders.json"), recursive=True)
                for limit_path in limit_files:
                    if "risk_reward_" in limit_path: continue
                    
                    try:
                        with open(limit_path, 'r', encoding='utf-8') as f:
                            orders = json.load(f)
                    except: continue

                    modified = False
                    for order in orders:
                        sym = str(order.get('symbol', '')).upper()
                        tf = str(order.get('timeframe', '')).upper()
                        current_vol = order.get('volume')

                        if sym in volume_map and tf in volume_map[sym]:
                            appointed_volume = volume_map[sym][tf]
                            
                            if current_vol == appointed_volume:
                                print(f"     🔘 {sym} ({tf}): Already set to {appointed_volume}")
                            else:
                                order['volume'] = appointed_volume
                                modified = True
                                print(f"     ✅ {sym} ({tf}): Updated Volume {current_vol} ➡️  {appointed_volume}")
                        else:
                            print(f"     ⚠️  {sym} ({tf}): No volume mapping found in config.")

                    if modified:
                        with open(limit_path, 'w', encoding='utf-8') as f:
                            json.dump(orders, f, indent=4)

        print(f"\n{'='*22} SYNC PROCESS COMPLETE {'='*22}\n")
        return True

    except Exception as e:
        print(f" [!] Critical System Error: {e}")
        return False
        
def activate_usd_based_risk_on_empty_pricelevels():
    """
    Triggered when orders have no exit/target. 
    Enforces risk strictly within the scope of each 'new_filename' strategy folder.
    """
    try:
        if not os.path.exists(DEVELOPER_USERS) or os.path.getsize(DEVELOPER_USERS) == 0:
            print(f" [!] Error: Users file not found or empty: {DEVELOPER_USERS}")
            return False
            
        with open(DEVELOPER_USERS, 'r') as f:
            users_data = json.load(f)
        
        print(f"\n{'='*10} CHECKING EMPTY TARGET AND EXIT TO ENFORCE USD RISK {'='*10}")

        for user_broker_id in users_data.keys():
            user_folder = os.path.join(DEV_PATH, user_broker_id)
            print(f" [{user_broker_id}] 🔍 Checking for targetless orders...")

            # --- Identify Secondary Folders from Account Management ---
            acc_mgmt_path = os.path.join(user_folder, "accountmanagement.json")
            strategy_folders = []
            
            if os.path.exists(acc_mgmt_path):
                try:
                    with open(acc_mgmt_path, 'r') as f:
                        acc_data = json.load(f)
                    poi = acc_data.get("chart", {}).get("define_candles", {}).get("entries_poi_condition", {})
                    for app_val in poi.values():
                        if isinstance(app_val, dict):
                            for ent_val in app_val.values():
                                if isinstance(ent_val, dict) and ent_val.get("new_filename"):
                                    s_dir = os.path.join(user_folder, ent_val["new_filename"])
                                    if s_dir not in strategy_folders: strategy_folders.append(s_dir)
                except: continue

            # --- Process Each Strategy Folder Independently ---
            for s_folder in strategy_folders:
                folder_name = os.path.basename(s_folder)
                config_path = os.path.join(s_folder, "allowedsymbolsandvolumes.json")
                
                if not os.path.exists(config_path): continue
                
                # Build Local Lookup (Symbol -> Timeframe -> USD_Risk)
                local_risk_data = {}
                try:
                    with open(config_path, 'r') as f:
                        c_data = json.load(f)
                    for cat in c_data.values():
                        if not isinstance(cat, list): continue
                        for item in cat:
                            sym = str(item.get("symbol", "")).upper()
                            if not sym: continue
                            if sym not in local_risk_data: local_risk_data[sym] = {}
                            
                            for k, v in item.items():
                                if k.endswith("_specs") and isinstance(v, dict):
                                    tf = k.replace("_specs", "").upper()
                                    local_risk_data[sym][tf] = v.get("usd_risk", 0)
                except: continue

                # Scan Limit Orders in this specific folder
                limit_files = glob.glob(os.path.join(s_folder, "**", "limit_orders.json"), recursive=True)
                
                f_processed = 0
                f_enforced = 0
                
                for limit_path in limit_files:
                    if "risk_reward_" in limit_path: continue
                    try:
                        with open(limit_path, 'r') as f: orders = json.load(f)
                    except: continue
                    
                    modified = False
                    for order in orders:
                        sym = str(order.get('symbol', '')).upper()
                        tf = str(order.get('timeframe', '')).upper()
                        
                        # TRIGGER: Order has no exit and no target
                        is_missing = order.get('exit') in [0, "0", None] and \
                                     order.get('target') in [0, "0", None]
                        
                        if is_missing:
                            f_processed += 1
                            found_risk = local_risk_data.get(sym, {}).get(tf)
                            
                            if found_risk and found_risk > 0:
                                order['exit'] = 0
                                order['target'] = 0
                                order['usd_risk'] = found_risk
                                order['usd_based_risk_only'] = True
                                modified = True
                                f_enforced += 1
                            else:
                                print(f"    └─ [{folder_name}] ❌ Missing Config: {sym} {tf} has no exit/target & no risk value found.")

                    if modified:
                        with open(limit_path, 'w') as f:
                            json.dump(orders, f, indent=4)

                # Final folder-level report
                if f_processed > 0:
                    icon = "✅" if f_enforced == f_processed else "⚠️"
                    print(f"  └─ [{folder_name}] {icon} Found {f_processed} orders | Enforced: {f_enforced}")

        print(f"\n{'='*10} EMPTY TARGET SYNC COMPLETE {'='*10}\n")
        return True
    except Exception as e:
        print(f" [!] Critical Error: {e}")
        return False

def enforce_risk_on_option():
    """
    Synchronizes USD risk rules from strategy configs.
    Includes detailed warnings for skipped orders to explain Processed vs Enforced gaps.
    """
    activate_usd_based_risk_on_empty_pricelevels()
    try:
        # 1. Load User IDs
        if not os.path.exists(DEVELOPER_USERS) or os.path.getsize(DEVELOPER_USERS) == 0:
            print(f" [!] Error: Users file not found or empty: {DEVELOPER_USERS}")
            return False
            
        with open(DEVELOPER_USERS, 'r') as f:
            users_data = json.load(f)
        
        print(f"\n{'='*10} STARTING RISK ENFORCEMENT SYNC {'='*10}")

        for user_broker_id in users_data.keys():
            user_folder = os.path.join(DEV_PATH, user_broker_id)
            print(f" [{user_broker_id}] 🛡️  Scanning strategy folders...")

            # --- Identify Secondary Folders ---
            acc_mgmt_path = os.path.join(user_folder, "accountmanagement.json")
            secondary_folder_paths = []
            
            if os.path.exists(acc_mgmt_path):
                try:
                    with open(acc_mgmt_path, 'r') as f:
                        acc_data = json.load(f)
                    poi = acc_data.get("chart", {}).get("define_candles", {}).get("entries_poi_condition", {})
                    for app_val in poi.values():
                        if isinstance(app_val, dict):
                            for ent_val in app_val.values():
                                if isinstance(ent_val, dict) and ent_val.get("new_filename"):
                                    strategy_dir = os.path.join(user_folder, ent_val["new_filename"])
                                    if strategy_dir not in secondary_folder_paths:
                                        secondary_folder_paths.append(strategy_dir)
                except:
                    print(f"  └─ ❌ Error reading accountmanagement.json")
                    continue

            for strategy_folder in secondary_folder_paths:
                folder_name = os.path.basename(strategy_folder)
                config_path = os.path.join(strategy_folder, "allowedsymbolsandvolumes.json")
                
                if not os.path.exists(config_path):
                    continue
                
                try:
                    with open(config_path, 'r') as f: config_data = json.load(f)
                except: continue
                
                # Build Case-Insensitive Lookup
                risk_lookup = {}
                for category in config_data.values():
                    if not isinstance(category, list): continue
                    for item in category:
                        symbol = str(item.get("symbol", "")).upper()
                        if not symbol: continue
                        if symbol not in risk_lookup: risk_lookup[symbol] = {}
                        
                        for key, value in item.items():
                            if key.endswith("_specs") and isinstance(value, dict):
                                tf = key.replace("_specs", "").upper()
                                risk_lookup[symbol][tf] = {
                                    "enforce": str(value.get("enforce_usd_risk", "no")).lower() == "yes",
                                    "usd_risk": value.get("usd_risk", 0)
                                }

                # Apply to Orders
                limit_files = glob.glob(os.path.join(strategy_folder, "**", "limit_orders.json"), recursive=True)
                
                f_processed = 0
                f_enforced = 0
                
                for limit_path in limit_files:
                    if "risk_reward_" in limit_path: continue
                    try:
                        with open(limit_path, 'r') as f: orders = json.load(f)
                    except: continue
                    
                    modified = False
                    for order in orders:
                        f_processed += 1
                        raw_sym = str(order.get('symbol', '')).upper()
                        raw_tf = str(order.get('timeframe', '')).upper()
                        
                        # LOGIC CHECK
                        if raw_sym in risk_lookup:
                            if raw_tf in risk_lookup[raw_sym]:
                                rule = risk_lookup[raw_sym][raw_tf]
                                if rule["enforce"]:
                                    order['exit'] = 0
                                    order['target'] = 0
                                    order['usd_risk'] = rule["usd_risk"]
                                    order['usd_based_risk_only'] = True
                                    modified = True
                                    f_enforced += 1
                                # Note: If rule["enforce"] is False, we do nothing (order remains manual)
                            else:
                                print(f"    └─ ⚠️  {raw_sym} found, but timeframe {raw_tf} missing in config.")
                        else:
                            print(f"    └─ ⚠️  Symbol {raw_sym} not found in strategy config.")
                    
                    if modified:
                        with open(limit_path, 'w') as f:
                            json.dump(orders, f, indent=4)
                
                if f_processed > 0:
                    status_icon = "✅" if f_enforced > 0 else "🔘"
                    print(f"  └─ [{folder_name}] {status_icon} Processed: {f_processed} | Enforced: {f_enforced}")

        print(f"\n{'='*10} RISK ENFORCEMENT COMPLETE {'='*10}\n")
        return True

    except Exception as e:
        print(f" [!] Critical Error: {e}")
        return False     

def preprocess_limit_orders_with_broker_data():
    """
    Simplified Pre-processor:
    Focuses on Broker-level logging and hides long file paths.
    """
    if not os.path.exists(BROKER_DICT_PATH):
        print(f" [!] Error: Broker config missing at {BROKER_DICT_PATH}")
        return False

    # Load Normalization Map
    try:
        with open(NORMALIZE_SYMBOLS_PATH, 'r') as f:
            norm_map = json.load(f)
    except Exception as e:
        print(f" [!] Critical: Normalization map error: {e}")
        return False

    with open(BROKER_DICT_PATH, 'r') as f:
        broker_configs = json.load(f)

    print(f"\n{'='*10} STARTING BROKER PRE-PROCESSOR {'='*10}")

    for dev_broker_id, config in broker_configs.items():
        # --- MT5 CONNECTION ---
        if not mt5.initialize(
            path=config.get("TERMINAL_PATH", ""), 
            login=int(config.get("LOGIN_ID")), 
            password=config.get("PASSWORD"), 
            server=config.get("SERVER")
        ):
            print(f" [{dev_broker_id}] ❌ Connection Failed: {mt5.last_error()}")
            continue

        print(f" [{dev_broker_id}] 🟢 Connected to {config.get('SERVER')}")

        user_folder = os.path.join(DEV_PATH, dev_broker_id)
        limit_files = glob.glob(os.path.join(user_folder, "**", "limit_orders.json"), recursive=True)

        for file_path in limit_files:
            if "risk_reward_" in file_path:
                continue

            # Extract just the parent folder name (e.g., 'EURUSD_POI') instead of the full path
            folder_context = os.path.basename(os.path.dirname(file_path))

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    orders = json.load(f)
            except: continue

            file_changed = False
            for order in orders:
                raw_symbol = order.get('symbol')
                normalized_symbol = get_normalized_symbol(raw_symbol, norm_map)

                if not normalized_symbol:
                    print(f"  └─ [{folder_context}] ⚠️  Unknown symbol: {raw_symbol}")
                    continue

                mt5.symbol_select(normalized_symbol, True)
                info = mt5.symbol_info(normalized_symbol)

                if info is None:
                    print(f"  └─ [{folder_context}] ❓ {normalized_symbol} not on server")
                    continue

                # Update Logic
                if order['symbol'] != normalized_symbol or order.get('tick_size') != info.trade_tick_size:
                    order['symbol'] = normalized_symbol
                    order['tick_size'] = info.trade_tick_size
                    order['tick_value'] = info.trade_tick_value
                    file_changed = True

            if file_changed:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(orders, f, indent=4)
                print(f"  └─ [{folder_context}] ✅ Specs Updated")

        mt5.shutdown()

    print(f"{'='*10} PRE-PROCESSOR COMPLETE {'='*10}\n")
    return True

def validate_orders_with_live_volume():
    """
    Developer Version: Validates and fixes volumes in BOTH configuration files AND order files.
    For orders without volume, assigns the broker's minimum volume for that symbol.
    For config files, validates and adjusts volumes to meet broker constraints.
    """
    if not os.path.exists(BROKER_DICT_PATH):
        print(f" [!] Error: Broker dictionary not found.")
        return False

    # Load Normalization Map
    try:
        with open(NORMALIZE_SYMBOLS_PATH, 'r') as f:
            norm_map = json.load(f)
    except Exception as e:
        print(f" [!] Critical: Normalization map error: {e}")
        return False

    try:
        with open(BROKER_DICT_PATH, 'r') as f:
            broker_configs = json.load(f)
    except:
        return False

    print(f"\n{'='*10} PRE-CALCULATION VOLUME VALIDATION {'='*10}")
    overall_configs_updated = 0
    overall_orders_updated = 0
    overall_orders_fixed = 0

    for dev_broker_id, config in broker_configs.items():
        user_folder = os.path.join(DEV_PATH, dev_broker_id)
        broker_configs_updated = 0
        broker_orders_updated = 0
        broker_orders_fixed = 0
        symbols_fixed = []
        symbols_assigned = []
        orders_fixed_list = []

        # 1. MT5 Initialization
        if not mt5.initialize(
            path=config.get("TERMINAL_PATH", ""), 
            login=int(config.get("LOGIN_ID")), 
            password=config.get("PASSWORD"), 
            server=config.get("SERVER")
        ):
            print(f" [{dev_broker_id}] ❌ MT5 Init Failed")
            continue

        account_info = mt5.account_info()
        broker_server_name = account_info.server if account_info else config.get("SERVER")
        print(f" [{dev_broker_id}] 🔍 Checking Inputs: {broker_server_name}")

        # Step 1: Process CONFIGURATION FILES (allowedsymbolsandvolumes.json)
        config_search = os.path.join(user_folder, "**", "allowedsymbolsandvolumes.json")
        config_files = glob.glob(config_search, recursive=True)

        for config_file_path in config_files:
            try:
                file_changed = False
                with open(config_file_path, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)

                for category, items in config_data.items():
                    if not isinstance(items, list): 
                        continue
                    
                    for item in items:
                        raw_symbol = item.get("symbol")
                        if not raw_symbol:
                            continue
                            
                        symbol = get_normalized_symbol(raw_symbol, norm_map)
                        
                        # Select symbol in MT5 to get info
                        mt5.symbol_select(symbol, True)
                        info = mt5.symbol_info(symbol)
                        if info is None:
                            print(f"     ⚠️ Symbol {raw_symbol} (normalized: {symbol}) not available in broker")
                            continue

                        found_volume_specs = False
                        
                        for key, value in item.items():
                            if "_specs" in key and isinstance(value, dict):
                                found_volume_specs = True
                                
                                if "volume" not in value:
                                    # No volume specified - assign broker minimum
                                    default_volume = info.volume_min
                                    value["volume"] = default_volume
                                    value["vol_assigned"] = datetime.now().strftime("%H:%M")
                                    file_changed = True
                                    symbols_assigned.append(f"{symbol}(min:{default_volume})")
                                    print(f"     📝 [CONFIG] Assigned default volume {default_volume} to {symbol} ({key})")
                                else:
                                    # Volume exists - validate it
                                    current_vol = float(value.get("volume", 0.0))
                                    
                                    # --- MT5 CONSTRAINT LOGIC ---
                                    new_vol = max(current_vol, info.volume_min)
                                    step = info.volume_step
                                    if step > 0:
                                        # Floor to nearest step
                                        new_vol = round(math.floor(new_vol / step + 1e-9) * step, 2)
                                    
                                    if new_vol > info.volume_max:
                                        new_vol = info.volume_max

                                    # Check for change
                                    if abs(new_vol - current_vol) > 1e-7:
                                        value["volume"] = new_vol
                                        value["vol_validated"] = datetime.now().strftime("%H:%M")
                                        file_changed = True
                                        symbols_fixed.append(f"{symbol}({current_vol}->{new_vol})")
                                        print(f"     🔧 [CONFIG] Fixed volume for {symbol} ({key}): {current_vol} -> {new_vol}")
                        
                        # If no volume specs found at all for this symbol, create default specs
                        if not found_volume_specs:
                            default_tf = "1h_specs"
                            item[default_tf] = {
                                "volume": info.volume_min,
                                "vol_assigned": datetime.now().strftime("%H:%M")
                            }
                            file_changed = True
                            symbols_assigned.append(f"{symbol}(created:{info.volume_min})")
                            print(f"     📝 [CONFIG] Created default specs for {symbol} with volume {info.volume_min}")

                if file_changed:
                    with open(config_file_path, 'w', encoding='utf-8') as f:
                        json.dump(config_data, f, indent=4)
                    broker_configs_updated += 1
                    overall_configs_updated += 1

            except Exception as e:
                print(f"     ❌ Error processing config {os.path.basename(config_file_path)}: {e}")
                continue

        # Step 2: Process ORDER FILES (limit_orders.json)
        order_search = os.path.join(user_folder, "**", "limit_orders.json")
        order_files = glob.glob(order_search, recursive=True)

        for order_file_path in order_files:
            if "risk_reward_" in order_file_path:
                continue
                
            try:
                file_changed = False
                orders_fixed_in_file = 0
                
                with open(order_file_path, 'r', encoding='utf-8') as f:
                    orders = json.load(f)
                
                if not isinstance(orders, list):
                    continue
                
                for order in orders:
                    if not isinstance(order, dict):
                        continue
                    
                    raw_symbol = order.get('symbol', '')
                    if not raw_symbol:
                        continue
                    
                    # Check if volume exists
                    current_vol = order.get('volume')
                    
                    # Normalize symbol to get broker info
                    symbol = get_normalized_symbol(raw_symbol, norm_map)
                    
                    # Get symbol info from broker
                    mt5.symbol_select(symbol, True)
                    info = mt5.symbol_info(symbol)
                    
                    if info is None:
                        print(f"     ⚠️ [ORDERS] Symbol {raw_symbol} not available in broker, skipping")
                        continue
                    
                    # If no volume or volume is 0, assign minimum
                    if current_vol is None or current_vol == 0:
                        min_volume = info.volume_min
                        order['volume'] = min_volume
                        order['vol_assigned'] = datetime.now().strftime("%H:%M")
                        file_changed = True
                        orders_fixed_in_file += 1
                        broker_orders_fixed += 1
                        orders_fixed_list.append(f"{raw_symbol}(0->{min_volume})")
                        print(f"     📝 [ORDERS] Assigned volume {min_volume} to {raw_symbol}")
                    else:
                        # Validate existing volume against broker constraints
                        try:
                            current_vol = float(current_vol)
                        except (ValueError, TypeError):
                            # Invalid volume, replace with minimum
                            min_volume = info.volume_min
                            order['volume'] = min_volume
                            order['vol_fixed'] = datetime.now().strftime("%H:%M")
                            file_changed = True
                            orders_fixed_in_file += 1
                            broker_orders_fixed += 1
                            orders_fixed_list.append(f"{raw_symbol}(invalid->{min_volume})")
                            print(f"     🔧 [ORDERS] Fixed invalid volume for {raw_symbol} -> {min_volume}")
                            continue
                        
                        # Check if volume meets broker constraints
                        new_vol = max(current_vol, info.volume_min)
                        step = info.volume_step
                        if step > 0:
                            new_vol = round(math.floor(new_vol / step + 1e-9) * step, 2)
                        
                        if new_vol > info.volume_max:
                            new_vol = info.volume_max
                        
                        if abs(new_vol - current_vol) > 1e-7:
                            order['volume'] = new_vol
                            order['vol_validated'] = datetime.now().strftime("%H:%M")
                            file_changed = True
                            orders_fixed_in_file += 1
                            broker_orders_fixed += 1
                            orders_fixed_list.append(f"{raw_symbol}({current_vol}->{new_vol})")
                            print(f"     🔧 [ORDERS] Adjusted volume for {raw_symbol}: {current_vol} -> {new_vol}")
                
                if file_changed:
                    with open(order_file_path, 'w', encoding='utf-8') as f:
                        json.dump(orders, f, indent=4)
                    broker_orders_updated += 1
                    overall_orders_updated += 1
                    overall_orders_fixed += orders_fixed_in_file
                    
            except Exception as e:
                print(f"     ❌ Error processing orders {os.path.basename(order_file_path)}: {e}")
                continue

        # --- Summary Print ---
        summary_parts = []
        
        if symbols_fixed or symbols_assigned:
            config_summary = []
            if symbols_fixed:
                fix_summary = ", ".join(symbols_fixed[:2])
                more_fixes = f" (+{len(symbols_fixed)-2})" if len(symbols_fixed) > 2 else ""
                config_summary.append(f"Fixed: {fix_summary}{more_fixes}")
            if symbols_assigned:
                assign_summary = ", ".join(symbols_assigned[:2])
                more_assigns = f" (+{len(symbols_assigned)-2})" if len(symbols_assigned) > 2 else ""
                config_summary.append(f"Assigned: {assign_summary}{more_assigns}")
            summary_parts.append(f"Configs: {' | '.join(config_summary)}")
        
        if orders_fixed_list:
            orders_summary = ", ".join(orders_fixed_list[:3])
            more_orders = f" (+{len(orders_fixed_list)-3})" if len(orders_fixed_list) > 3 else ""
            summary_parts.append(f"Orders: {orders_summary}{more_orders}")
        
        if broker_configs_updated > 0 or broker_orders_updated > 0:
            print(f"  └─ ✅ Updated {broker_configs_updated} config files, {broker_orders_updated} order files ({broker_orders_fixed} orders fixed). {'. '.join(summary_parts)}")
        else:
            print(f"  └─ 🔘 All volumes are valid")

        mt5.shutdown()

    print(f"\n{'='*10} PRE-CALCULATION CHECK COMPLETE {'='*10}")
    return True

def calculate_symbols_orders():
    """
    Calculates Exit/Target prices for EVERY order in the limit_orders files.
    Strictly requires 'volume' to exist in the record; otherwise, skips the order.
    """
    try:
        # 1. Load User Data
        if not os.path.exists(DEVELOPER_USERS) or os.path.getsize(DEVELOPER_USERS) == 0:
            print(f" [!] Error: Users file not found: {DEVELOPER_USERS}")
            return False

        with open(DEVELOPER_USERS, 'r', encoding='utf-8') as f:
            users_data = json.load(f)

        print(f"\n{'='*10} FORCE-CALCULATING ALL ORDERS {'='*10}")

        # 2. Iterate through each User
        for dev_broker_id in users_data.keys():
            print(f" [{dev_broker_id}] 🧮 Processing all available orders...")
            
            user_folder = os.path.join(DEV_PATH, dev_broker_id)
            acc_mgmt_path = os.path.join(user_folder, "accountmanagement.json")

            if not os.path.exists(acc_mgmt_path):
                print(f"  └─ ⚠️  Account management file missing")
                continue

            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                acc_mgmt_data = json.load(f)
            
            rr_ratios = acc_mgmt_data.get("risk_reward_ratios", [1.0])
            
            # Find all limit_orders.json files
            limit_order_files = glob.glob(os.path.join(user_folder, "**", "limit_orders.json"), recursive=True)
            total_files_updated = 0

            for limit_path in limit_order_files:
                if "risk_reward_" in limit_path: 
                    continue
                
                try:
                    with open(limit_path, 'r', encoding='utf-8') as f:
                        original_orders = json.load(f)
                except: 
                    continue

                base_dir = os.path.dirname(limit_path)

                for current_rr in rr_ratios:
                    orders_copy = copy.deepcopy(original_orders)
                    updated_any_order = False
                    final_orders_to_save = []

                    for order in orders_copy:
                        try:
                            # --- STRICTOR DATA EXTRACTION ---
                            # Using .get() without a default for volume to check existence
                            raw_volume = order.get("volume")
                            if raw_volume is None or float(raw_volume) <= 0:
                                print(f"  └─ ❌ Skipping {order.get('symbol', 'Unknown')}: No volume found in record.")
                                continue 

                            volume = float(raw_volume)
                            entry = float(order['entry'])
                            rr_ratio = float(current_rr)
                            order_type = str(order.get('order_type', '')).upper()
                            tick_size = float(order.get('tick_size', 0.00001))
                            tick_value = float(order.get('tick_value', 1.0))
                            
                            digits = len(str(tick_size).split('.')[-1]) if tick_size < 1 else 0

                            # --- Calculation Logic ---
                            if order.get("usd_based_risk_only") is True:
                                risk_val = float(order.get("usd_risk", 0))
                                
                                if risk_val > 0:
                                    sl_dist = (risk_val * tick_size) / (tick_value * volume)
                                    tp_dist = sl_dist * rr_ratio

                                    if "BUY" in order_type:
                                        order["exit"] = round(entry - sl_dist, digits)
                                        order["target"] = round(entry + tp_dist, digits)
                                    else:
                                        order["exit"] = round(entry + sl_dist, digits)
                                        order["target"] = round(entry - tp_dist, digits)
                            else:
                                sl_price = float(order.get('exit', 0))
                                tp_price = float(order.get('target', 0))

                                if sl_price == 0 and tp_price > 0:
                                    risk_dist = abs(tp_price - entry) / rr_ratio
                                    order['exit'] = round(entry - risk_dist if "BUY" in order_type else entry + risk_dist, digits)
                                elif sl_price > 0:
                                    risk_dist = abs(entry - sl_price)
                                    order['target'] = round(entry + (risk_dist * rr_ratio) if "BUY" in order_type else entry - (risk_dist * rr_ratio), digits)

                            # --- Metadata Updates ---
                            order['risk_reward'] = rr_ratio
                            order['status'] = "Calculated"
                            order['calculated_at'] = datetime.now().strftime("%H:%M:%S")
                            
                            final_orders_to_save.append(order)
                            updated_any_order = True

                        except Exception:
                            continue

                    if updated_any_order:
                        target_out_dir = os.path.join(base_dir, f"risk_reward_{current_rr}")
                        os.makedirs(target_out_dir, exist_ok=True)
                        with open(os.path.join(target_out_dir, "limit_orders.json"), 'w', encoding='utf-8') as f:
                            json.dump(final_orders_to_save, f, indent=4)
                        total_files_updated += 1
            
            if total_files_updated > 0:
                print(f"  └─ ✅ Generated {total_files_updated} R:R specific files.")
            else:
                print(f"  └─ 🔘 No valid orders (with volume) were available to calculate.")

        print(f"{'='*10} CALCULATION COMPLETE {'='*10}\n")
        return True

    except Exception as e:
        print(f" [!] Critical Error: {e}")
        return False

def live_risk_reward_amounts_and_volume_scale():
    """
    Scans for limit orders and calculates volume based on USD risk targets.
    Logs each broker process clearly with a status summary.
    """
    if not os.path.exists(BROKER_DICT_PATH):
        print(f" [!] Error: Broker dictionary not found.")
        return False

    # Load Normalization Map
    try:
        with open(NORMALIZE_SYMBOLS_PATH, 'r') as f:
            norm_map = json.load(f)
    except Exception as e:
        print(f" [!] Critical: Normalization map error: {e}")
        return False

    with open(BROKER_DICT_PATH, 'r') as f:
        try:
            broker_configs = json.load(f)
        except:
            return False

    print(f"\n{'='*10} LIVE RISKS BUCKETING & VOLUME SCALING {'='*10}")

    for dev_broker_id, config in broker_configs.items():
        user_folder = os.path.join(DEV_PATH, dev_broker_id)
        
        # 1. MT5 Initialization
        if not mt5.initialize(
            path=config.get("TERMINAL_PATH", ""), 
            login=int(config.get("LOGIN_ID")), 
            password=config.get("PASSWORD"), 
            server=config.get("SERVER")
        ):
            print(f" [{dev_broker_id}] ❌ MT5 Init Failed")
            continue

        account_info = mt5.account_info()
        if account_info is None:
            print(f" [{dev_broker_id}] ❌ Connection Error")
            mt5.shutdown()
            continue
        
        broker_server_name = account_info.server
        acc_currency = account_info.currency
        print(f" [{dev_broker_id}] 🟢 {broker_server_name} ({acc_currency})")

        # 2. Load Risks
        acc_mgmt_path = os.path.join(user_folder, "accountmanagement.json")
        try:
            with open(acc_mgmt_path, 'r') as f:
                acc_mgmt_data = json.load(f)
            allowed_risks = acc_mgmt_data.get("RISKS", [])
            max_allowed_risk = max(allowed_risks) if allowed_risks else 50.0
        except:
            print(f"  └─ ⚠️  Account Config Missing")
            mt5.shutdown()
            continue

        # 3. Identify subfolders
        poi_conditions = acc_mgmt_data.get("chart", {}).get("define_candles", {}).get("entries_poi_condition", {})
        target_subfolders = [user_folder]
        for app_val in poi_conditions.values():
            if isinstance(app_val, dict):
                for ent_val in app_val.values():
                    if isinstance(ent_val, dict) and ent_val.get("new_filename"):
                        sub_path = os.path.join(user_folder, ent_val.get("new_filename"))
                        if os.path.exists(sub_path):
                            target_subfolders.append(sub_path)

        total_orders_calculated = 0
        buckets_found = set()

        for current_search_path in target_subfolders:
            limit_order_files = glob.glob(os.path.join(current_search_path, "**", "limit_orders.json"), recursive=True)
            
            for limit_path in limit_order_files:
                if "risk_reward_" not in limit_path or "_risk" in limit_path:
                    continue

                try:
                    with open(limit_path, 'r') as f: orders = json.load(f)
                except: continue

                risk_buckets = {}
                for order in orders:
                    if order.get('status') != "Calculated": continue

                    symbol = get_normalized_symbol(order.get('symbol'), norm_map)
                    if not symbol or not mt5.symbol_select(symbol, True): continue
                    
                    info = mt5.symbol_info(symbol)
                    if info is None: continue

                    entry, exit_p, target_p = float(order['entry']), float(order['exit']), float(order['target'])
                    current_volume = info.volume_min
                    filled_buckets = set()

                    # Volume loop
                    for _ in range(5000):
                        action = mt5.ORDER_TYPE_BUY if "BUY" in order['order_type'].upper() else mt5.ORDER_TYPE_SELL
                        sl_risk = mt5.order_calc_profit(action, symbol, current_volume, entry, exit_p)
                        
                        if sl_risk is None: # Fallback
                            sl_risk = -(abs(entry - exit_p) / info.trade_tick_size * info.trade_tick_value * current_volume)
                        
                        abs_risk = round(abs(sl_risk), 2)
                        if abs_risk > max_allowed_risk: break
                        
                        assigned_risk = None
                        if abs_risk < 1.00 and 0.5 in allowed_risks: assigned_risk = 0.5
                        elif int(math.floor(abs_risk)) in allowed_risks: assigned_risk = int(math.floor(abs_risk))
                        
                        if assigned_risk and assigned_risk not in filled_buckets:
                            risk_buckets.setdefault(assigned_risk, []).append({
                                **order,
                                'symbol': symbol,
                                f"{broker_server_name}_tick_size": info.trade_tick_size,
                                f"{broker_server_name}_tick_value": info.trade_tick_value,
                                'live_sl_risk_amount': abs_risk,
                                'calculated_at': datetime.now().strftime("%H:%M:%S")
                            })
                            filled_buckets.add(assigned_risk)
                            buckets_found.add(assigned_risk)
                            total_orders_calculated += 1
                        
                        current_volume += info.volume_step

                # Save Results
                if risk_buckets:
                    base_dir = os.path.dirname(limit_path)
                    for r_val, grouped in risk_buckets.items():
                        out_dir = os.path.join(base_dir, f"{r_val}usd_risk")
                        os.makedirs(out_dir, exist_ok=True)
                        out_file = os.path.join(out_dir, f"{r_val}usd_risk.json")
                        
                        existing = []
                        if os.path.exists(out_file):
                            try:
                                with open(out_file, 'r') as f: existing = json.load(f)
                            except: pass
                            
                        with open(out_file, 'w') as f:
                            json.dump(existing + grouped, f, indent=4)

        # Broker-level summary
        if total_orders_calculated > 0:
            bucket_str = ", ".join([f"{b}" for b in sorted(list(buckets_found))])
            print(f"  └─ ✅ Scaled {total_orders_calculated} orders into: {bucket_str} usd risks")
        else:
            print(f"  └─ 🔘 No pending orders found for scaling")

        mt5.shutdown()

    print(f"{'='*10} RISK SCALING COMPLETE {'='*10}\n")
    return True

def ajdust_order_price_closer_in_95cent_to_next_bucket():
    """
    Promotes fractional risk orders (e.g., $1.55) to the next whole bucket (e.g., $2.0).
    Processes folders silently per broker with a clean visual summary.
    """
    if not os.path.exists(DEV_PATH):
        print(f" [!] Error: DEV_PATH {DEV_PATH} does not exist.")
        return False

    print(f"\n{'='*10} PRICE RE-ADJUSTMENT PROMOTION {'='*10}")

    # Get all primary user directories
    user_folders = [f.path for f in os.scandir(DEV_PATH) if f.is_dir()]

    for user_folder in user_folders:
        dev_broker_id = os.path.basename(user_folder)
        acc_mgmt_path = os.path.join(user_folder, "accountmanagement.json")
        
        # 1. Validation & Initialization
        if not os.path.exists(acc_mgmt_path) or os.path.getsize(acc_mgmt_path) == 0:
            continue

        try:
            with open(acc_mgmt_path, 'r') as f:
                acc_mgmt_data = json.load(f)
        except:
            continue
        
        allowed_risks = acc_mgmt_data.get("RISKS", [])
        if not allowed_risks:
            continue

        # Indicator that we are starting this broker
        print(f" [{dev_broker_id}] ⚖️  Scaling fractional risks...")

        # 2. Identify strategy paths
        poi_conditions = acc_mgmt_data.get("chart", {}).get("define_candles", {}).get("entries_poi_condition", {})
        target_search_paths = [user_folder]
        for app_val in poi_conditions.values():
            if isinstance(app_val, dict):
                for ent_val in app_val.values():
                    if isinstance(ent_val, dict) and ent_val.get("new_filename"):
                        strat_path = os.path.join(user_folder, ent_val.get("new_filename"))
                        if os.path.exists(strat_path):
                            target_search_paths.append(strat_path)

        promotion_count = 0

        # 3. Process Folders
        for search_root in target_search_paths:
            risk_json_files = glob.glob(os.path.join(search_root, "**", "*usd_risk", "*usd_risk.json"), recursive=True)

            for file_path in risk_json_files:
                try:
                    with open(file_path, 'r') as f:
                        orders = json.load(f)
                except: continue

                if not isinstance(orders, list): continue

                for order in orders:
                    try:
                        sl_risk = order.get('live_sl_risk_amount', 0)
                        fractional_part = sl_risk - int(sl_risk)

                        # Promotion Logic (e.g., 1.51 becomes 2.0)
                        if fractional_part >= 0.95:
                            target_risk = float(math.ceil(sl_risk))
                            if target_risk not in allowed_risks: continue 

                            # Re-calculation Data
                            entry = float(order['entry'])
                            rr_ratio = float(order['risk_reward'])
                            tick_size = float(order['tick_size'])
                            tick_value = float(order['tick_value'])
                            tf = order['timeframe']
                            volume = float(order[f"{tf}_volume"])
                            
                            if tick_value == 0 or volume == 0: continue

                            # Precision & New Levels
                            tick_str = format(tick_size, 'f').rstrip('0').rstrip('.')
                            precision = len(tick_str.split('.')[1]) if '.' in tick_str else 0
                            risk_dist = (target_risk / (tick_value * volume)) * tick_size
                            
                            new_order = copy.deepcopy(order)
                            if "BUY" in order['order_type'].upper():
                                new_order['exit'] = round(entry - risk_dist, precision)
                                new_order['target'] = round(entry + (risk_dist * rr_ratio), precision)
                            else:
                                new_order['exit'] = round(entry + risk_dist, precision)
                                new_order['target'] = round(entry - (risk_dist * rr_ratio), precision)

                            new_order.update({
                                'live_sl_risk_amount': target_risk,
                                'live_tp_reward_amount': round(target_risk * rr_ratio, 2),
                                'status': "Adjusted_To_Next_Bucket",
                                'adjusted_at': datetime.now().strftime("%H:%M:%S")
                            })

                            # Save to Destination
                            parent_rr_dir = os.path.dirname(os.path.dirname(file_path))
                            new_bucket_folder = os.path.join(parent_rr_dir, f"{int(target_risk)}usd_risk")
                            os.makedirs(new_bucket_folder, exist_ok=True)
                            
                            target_json_path = os.path.join(new_bucket_folder, f"{int(target_risk)}usd_risk.json")
                            existing_data = []
                            if os.path.exists(target_json_path):
                                with open(target_json_path, 'r') as tf_file:
                                    try: existing_data = json.load(tf_file)
                                    except: pass
                            
                            existing_data.append(new_order)
                            with open(target_json_path, 'w') as tf_file:
                                json.dump(existing_data, tf_file, indent=4)
                            
                            promotion_count += 1
                    except: continue

        if promotion_count > 0:
            print(f"  └─ ✅ Promoted {promotion_count} orders to higher buckets")
        else:
            print(f"  └─ 🔘 No fractional risks required promotion")

    print(f"{'='*10} PROMOTION COMPLETE {'='*10}\n")
    return True

def fix_risk_buckets_according_to_orders_risk():
    """
    Developer Version: Identifies and fixes bucket violations in the DEV_PATH locally.
    Logic: Risk < $1.00 -> '0.5usd_risk', Risk >= $1.00 -> floor(risk) bucket.
    """
    print(f"\n{'='*15} 🛠️  BUCKET INTEGRITY REPAIR (DEV MODE) {'='*15}")
    print(f" >>> Scanning Developer Path: {DEV_PATH}")
    
    overall_moved = 0
    overall_files_fixed = 0
    broker_stats = []

    try:
        with open(BROKER_DICT_PATH, 'r') as f:
            broker_configs = json.load(f)
    except Exception as e:
        print(f" [!] Critical: Could not load broker configs: {e}")
        return False

    # Risk field key already exists in your JSON data
    risk_field = "live_sl_risk_amount"

    for dev_broker_id in broker_configs.keys():
        print(f" ⚙️  Processing Developer Broker: {dev_broker_id}...")
        
        user_folder = os.path.join(DEV_PATH, dev_broker_id)
        investor_moved = 0
        
        # Search for: **/risk_reward_*/*usd_risk/*.json
        search_pattern = os.path.join(user_folder, "**", "*usd_risk", "*.json")
        found_files = glob.glob(search_pattern, recursive=True)

        for target_file_path in found_files:
            try:
                filename = os.path.basename(target_file_path)
                try:
                    # Extracts '5.0' from '5usd_risk.json'
                    current_bucket_val = float(filename.replace('usd_risk.json', ''))
                except: continue

                with open(target_file_path, 'r', encoding='utf-8') as f:
                    entries = json.load(f)

                if not isinstance(entries, list): continue

                staying_entries = []
                file_changed = False

                for entry in entries:
                    live_risk_amt = entry.get(risk_field)
                    
                    if live_risk_amt is None:
                        staying_entries.append(entry)
                        continue

                    # --- HYBRID BUCKET LOGIC ---
                    if live_risk_amt < 1.0:
                        correct_bucket_val = 0.5
                    else:
                        correct_bucket_val = float(math.floor(live_risk_amt))

                    # Check for violation
                    if math.isclose(correct_bucket_val, current_bucket_val):
                        staying_entries.append(entry)
                    else:
                        # MIGRATION LOGIC
                        new_bucket_name = "0.5usd_risk" if correct_bucket_val == 0.5 else f"{int(correct_bucket_val)}usd_risk"
                        
                        # Navigate up from .../5usd_risk/5usd_risk.json to the RR folder
                        parent_rr_dir = os.path.dirname(os.path.dirname(target_file_path))
                        new_dir = os.path.join(parent_rr_dir, new_bucket_name)
                        os.makedirs(new_dir, exist_ok=True)
                        
                        new_file_path = os.path.join(new_dir, f"{new_bucket_name}.json")

                        # Append to target bucket
                        dest_data = []
                        if os.path.exists(new_file_path):
                            try:
                                with open(new_file_path, 'r', encoding='utf-8') as nf:
                                    dest_data = json.load(nf)
                            except: pass
                        
                        dest_data.append(entry)
                        with open(new_file_path, 'w', encoding='utf-8') as nf:
                            json.dump(dest_data, nf, indent=4)
                        
                        file_changed = True
                        investor_moved += 1
                        overall_moved += 1

                # Clean up the source file if items were moved out
                if file_changed:
                    with open(target_file_path, 'w', encoding='utf-8') as f:
                        json.dump(staying_entries, f, indent=4)
                    overall_files_fixed += 1

            except Exception:
                continue

        # Log completion for this broker
        status_icon = "🛠️" if investor_moved > 0 else "✨"
        print(f"  └─ {status_icon} Finished {dev_broker_id}: {investor_moved} shifts made.")
        broker_stats.append({"id": dev_broker_id, "moved": investor_moved})
    return True

def deduplicate_risk_bucket_orders():
    """
    Cleans up risk buckets by keeping only the most efficient order 
    (lowest risk) for each Symbol/Timeframe/Direction pair.
    """
    if not os.path.exists(DEV_PATH):
        print(f" [!] Error: DEV_PATH {DEV_PATH} does not exist.")
        return False

    print(f"\n{'='*10} RISK BUCKET DEDUPLICATION {'='*10}")

    # We first find all broker folders to group the output
    user_folders = [f.path for f in os.scandir(DEV_PATH) if f.is_dir()]

    for user_folder in user_folders:
        dev_broker_id = os.path.basename(user_folder)
        risk_json_files = glob.glob(os.path.join(user_folder, "**", "*usd_risk", "*usd_risk.json"), recursive=True)
        
        if not risk_json_files:
            continue

        print(f" [{dev_broker_id}] 🧹 Cleaning redundant orders...")
        total_removed = 0

        for file_path in risk_json_files:
            if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
                continue

            try:
                with open(file_path, 'r') as f:
                    orders = json.load(f)
            except:
                continue

            if not isinstance(orders, list) or not orders:
                continue

            initial_count = len(orders)
            # Key: (symbol, timeframe, order_type)
            best_orders = {}

            for order in orders:
                symbol = order.get('symbol')
                tf = order.get('timeframe', '1h')
                direction = order.get('order_type', '').upper()
                risk_amt = float(order.get('live_sl_risk_amount', 0))

                group_key = (symbol, tf, direction)

                if group_key not in best_orders:
                    best_orders[group_key] = order
                else:
                    # Keep the one with the LOWER risk amount (most conservative)
                    existing_risk = float(best_orders[group_key].get('live_sl_risk_amount', 0))
                    if risk_amt < existing_risk:
                        best_orders[group_key] = order

            unique_orders = list(best_orders.values())
            removed_in_file = initial_count - len(unique_orders)

            if removed_in_file > 0:
                try:
                    with open(file_path, 'w') as f:
                        json.dump(unique_orders, f, indent=4)
                    total_removed += removed_in_file
                except:
                    continue

        if total_removed > 0:
            print(f"  └─ ✅ Pruned {total_removed} redundant entries")
        else:
            print(f"  └─ 🔘 Risk buckets already optimized")

    print(f"{'='*10} DEDUPLICATION COMPLETE {'='*10}\n")
    return True

def sync_dev_investors():
    """
    Synchronizes investor accounts with developer strategy data.
    Logs each investor process clearly with a status summary.
    """
    def compact_json_format(data):
        """Custom formatter to keep lists on one line while indenting dictionaries."""
        res = json.dumps(data, indent=4)
        res = re.sub(r'\[\s+([^\[\]]+?)\s+\]', 
                    lambda m: "[" + ", ".join([line.strip() for line in m.group(1).splitlines()]).replace('"', '"') + "]", 
                    res)
        res = res.replace(",,", ",")
        return res

    try:
        # 1. Load Data
        if not all(os.path.exists(f) for f in [INVESTOR_USERS, DEVELOPER_USERS, DEFAULT_ACCOUNTMANAGEMENT]):
            print(" [!] Error: Configuration files missing.")
            return False

        with open(DEFAULT_ACCOUNTMANAGEMENT, 'r', encoding='utf-8') as f:
            default_acc_data = json.load(f)
            default_risk_mgmt = default_acc_data.get("account_balance_default_risk_management", {})

        with open(INVESTOR_USERS, 'r', encoding='utf-8') as f:
            investors_data = json.load(f)
        
        with open(DEVELOPER_USERS, 'r', encoding='utf-8') as f:
            developers_data = json.load(f)

        print(f"\n{'='*10} SYNCING INVESTOR ACCOUNTS {'='*10}")

        # 2. Iterate through Investors
        for inv_broker_id, inv_info in investors_data.items():
            print(f" [{inv_broker_id}] 🔄 Processing Sync...")

            invested_string = inv_info.get("INVESTED_WITH", "")
            inv_server = inv_info.get("SERVER", "")
            
            if "_" not in invested_string:
                print(f"  └─ ⚠️  Invalid 'INVESTED_WITH' format: {invested_string}")
                continue
            
            parts = invested_string.split("_", 1)
            dev_broker_id, target_strat_name = parts[0], parts[1]

            if dev_broker_id not in developers_data:
                print(f"  └─ ❌ Linked Dev {dev_broker_id} not found in database")
                continue

            # Broker Matching Logic
            dev_broker_name = developers_data[dev_broker_id].get("BROKER", "").lower()
            if dev_broker_name not in inv_server.lower():
                print(f"  └─ ❌ Broker Mismatch: Dev requires {dev_broker_name.upper()}")
                continue

            dev_user_folder = os.path.join(DEV_PATH, dev_broker_id)
            inv_user_folder = os.path.join(INV_PATH, inv_broker_id)
            dev_acc_path = os.path.join(dev_user_folder, "accountmanagement.json")
            inv_acc_path = os.path.join(inv_user_folder, "accountmanagement.json")

            # 3. Sync Account Management
            if os.path.exists(dev_acc_path):
                with open(dev_acc_path, 'r', encoding='utf-8') as f:
                    dev_acc_data = json.load(f)
                
                os.makedirs(inv_user_folder, exist_ok=True)
                inv_acc_data = {}
                if os.path.exists(inv_acc_path):
                    try:
                        with open(inv_acc_path, 'r', encoding='utf-8') as f:
                            inv_acc_data = json.load(f)
                    except: pass

                is_reset = inv_acc_data.get("reset_all", False)
                if is_reset: inv_acc_data = {"reset_all": False}
                
                needs_save = is_reset 
                keys_to_sync = ["RISKS", "risk_reward_ratios", "symbols_dictionary", "settings"]
                
                for key in keys_to_sync:
                    if key not in inv_acc_data or not inv_acc_data[key]:
                        inv_acc_data[key] = dev_acc_data.get(key, []) if key != "settings" else dev_acc_data.get(key, {})
                        needs_save = True

                if "account_balance_default_risk_management" not in inv_acc_data:
                    inv_acc_data["account_balance_default_risk_management"] = default_risk_mgmt
                    needs_save = True
                
                if needs_save:
                    with open(inv_acc_path, 'w', encoding='utf-8') as f:
                        f.write(compact_json_format(inv_acc_data))
                    print(f"  └─ ✅ accountmanagement.json synced")
            else:
                print(f"  └─ ⚠️  Dev accountmanagement.json missing")

            # 4. Clone Strategy Folder
            dev_strat_path = os.path.join(dev_user_folder, target_strat_name)
            inv_strat_path = os.path.join(inv_user_folder, target_strat_name)

            if os.path.exists(dev_strat_path):
                try:
                    if os.path.exists(inv_strat_path):
                        shutil.rmtree(inv_strat_path)
                    
                    # Selective copy/clean logic
                    shutil.copytree(dev_strat_path, inv_strat_path, dirs_exist_ok=True)
                    for item in os.listdir(inv_strat_path):
                        item_path = os.path.join(inv_strat_path, item)
                        if item == "pending_orders": continue
                        if os.path.isdir(item_path): shutil.rmtree(item_path)
                        else: os.remove(item_path)
                    
                    print(f"  └─ 📁 Strategy Cloned: {target_strat_name}")
                except Exception as e:
                    print(f"  └─ ❌ Folder Sync Error: {e}")
            else:
                print(f"  └─ ⚠️  Dev Strategy folder '{target_strat_name}' missing")

        print(f"{'='*10} INVESTOR SYNC COMPLETE {'='*10}\n")
        return True

    except Exception as e:
        print(f"\n [!] Enrichment Error: {e}")
        return False

def calculate_orders():
    purge_unauthorized_symbols()
    clean_risk_folders()
    backup_limit_orders()
    purge_unauthorized_symbols()
    provide_orders_volume()
    enforce_risk_on_option()
    preprocess_limit_orders_with_broker_data()
    validate_orders_with_live_volume()
    calculate_symbols_orders()
    live_risk_reward_amounts_and_volume_scale()
    ajdust_order_price_closer_in_95cent_to_next_bucket()
    fix_risk_buckets_according_to_orders_risk()
    deduplicate_risk_bucket_orders()
    deduplicate_risk_bucket_orders()
    sync_dev_investors()
    print(f"✅ Symbols order price levels calculation completed.")


if __name__ == "__main__":
   sync_dev_investors()
   