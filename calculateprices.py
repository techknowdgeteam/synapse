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
BROKER_DICT_PATH = r"C:\xampp\htdocs\chronedge\synarex\brokers.json"
SYMBOL_CATEGORY_PATH = r"C:\xampp\htdocs\chronedge\synarex\symbolscategory.json"
DEV_PATH = r"C:\xampp\htdocs\chronedge\synarex\usersdata\developers"
INV_PATH = r"C:\xampp\htdocs\chronedge\synarex\usersdata\investors"
DEFAULT_ACCOUNTMANAGEMENT = r"C:\xampp\htdocs\chronedge\synarex\default_accountmanagement.json"
INVESTOR_USERS = r"C:\xampp\htdocs\chronedge\synarex\usersdata\investors\investors.json"
DEVELOPER_USERS = r"C:\xampp\htdocs\chronedge\synarex\usersdata\developers\developers.json"
NORMALIZE_SYMBOLS_PATH = r"C:\xampp\htdocs\chronedge\synarex\symbols_normalization.json"



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

def provide_orders_volume(dev_broker_id):
    """
    Worker: Synchronizes specific volumes from allowedsymbolsandvolumes.json to limit_orders.json 
    for a single developer/broker ID.
    Assumes MT5 session is already initialized.
    Includes professional logging with change detection.
    """
    try:
        if not os.path.exists(DEVELOPER_USERS) or os.path.getsize(DEVELOPER_USERS) == 0:
            print(f" [!] Error: Users file not found: {DEVELOPER_USERS}")
            return False

        with open(DEVELOPER_USERS, 'r', encoding='utf-8') as f:
            users_data = json.load(f)

        # Verify the broker ID exists in users_data
        if dev_broker_id not in users_data:
            print(f" [{dev_broker_id}] ❌ Developer ID not found in {DEVELOPER_USERS}")
            return False

        print(f"\n{'='*20} VOLUME SYNCHRONIZATION ENGINE {'='*20}")
        print(f"\n 👤 DEVELOPER ID: {dev_broker_id}")
        print(f" └{'─'*45}")
        
        user_folder = os.path.join(DEV_PATH, dev_broker_id)
        acc_mgmt_path = os.path.join(user_folder, "accountmanagement.json")

        if not os.path.exists(acc_mgmt_path):
            print(f"   ⚠️  Skipping: accountmanagement.json not found for {dev_broker_id}.")
            return False

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
            print(f"   ❌ Error parsing account management for {dev_broker_id}: {e}")
            return False

        if not secondary_folders:
            print(f"   ⚠️  No strategy folders found for {dev_broker_id}")
            return False

        total_orders_processed = 0
        total_orders_updated = 0

        # 2. Process each strategy folder
        for strategy_folder in secondary_folders:
            folder_name = os.path.basename(strategy_folder)
            config_path = os.path.join(strategy_folder, "allowedsymbolsandvolumes.json")
            
            if not os.path.exists(config_path):
                print(f"   ⚠️  Strategy: {folder_name} - No allowedsymbolsandvolumes.json found")
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
                        if symbol not in volume_map: 
                            volume_map[symbol] = {}
                        for key, value in item.items():
                            if key.endswith("_specs") and isinstance(value, dict):
                                tf = key.replace("_specs", "").upper()
                                if "volume" in value:
                                    volume_map[symbol][tf] = float(value["volume"])
                
                if not volume_map:
                    print(f"     ⚠️  No volume mappings found in config")
                    continue
                    
            except Exception as e:
                print(f"     ❌ Config Error: {e}")
                continue

            # 3. Apply to Order Files
            limit_files = glob.glob(os.path.join(strategy_folder, "**", "limit_orders.json"), recursive=True)
            
            if not limit_files:
                print(f"     ⚠️  No limit_orders.json files found")
                continue
                
            for limit_path in limit_files:
                if "risk_reward_" in limit_path: 
                    continue
                
                try:
                    with open(limit_path, 'r', encoding='utf-8') as f:
                        orders = json.load(f)
                except Exception as e:
                    print(f"     ⚠️  Could not read {limit_path}: {e}")
                    continue

                if not orders:
                    continue

                modified = False
                folder_orders_processed = 0
                folder_orders_updated = 0
                
                for order in orders:
                    sym = str(order.get('symbol', '')).upper()
                    tf = str(order.get('timeframe', '')).upper()
                    current_vol = order.get('volume')
                    
                    folder_orders_processed += 1

                    if sym in volume_map and tf in volume_map[sym]:
                        appointed_volume = volume_map[sym][tf]
                        
                        if current_vol == appointed_volume:
                            print(f"     🔘 {sym} ({tf}): Already set to {appointed_volume}")
                        else:
                            order['volume'] = appointed_volume
                            modified = True
                            folder_orders_updated += 1
                            print(f"     ✅ {sym} ({tf}): Updated Volume {current_vol} ➡️  {appointed_volume}")
                    else:
                        print(f"     ⚠️  {sym} ({tf}): No volume mapping found in config.")

                if modified:
                    try:
                        with open(limit_path, 'w', encoding='utf-8') as f:
                            json.dump(orders, f, indent=4)
                        print(f"     📝 Saved changes to {os.path.basename(limit_path)}")
                    except Exception as e:
                        print(f"     ❌ Failed to save changes: {e}")
                
                total_orders_processed += folder_orders_processed
                total_orders_updated += folder_orders_updated

        # Summary for this broker
        if total_orders_processed > 0:
            print(f"\n  └─ 📊 Summary for {dev_broker_id}:")
            print(f"      Processed: {total_orders_processed} orders")
            print(f"      Updated: {total_orders_updated} orders")
            if total_orders_updated == 0:
                print(f"      ✅ All volumes already synchronized")
        else:
            print(f"\n  └─ ⚠️  No orders processed for {dev_broker_id}")

        print(f"\n{'='*22} SYNC PROCESS COMPLETE FOR {dev_broker_id} {'='*22}\n")
        return True

    except Exception as e:
        print(f" [!] Critical System Error for {dev_broker_id}: {e}")
        return False

def activate_usd_based_risk_on_empty_pricelevels(dev_broker_id):
    """
    Worker: Triggered when orders have no exit/target.
    Enforces risk strictly within the scope of each 'new_filename' strategy folder
    for a single developer/broker ID.
    Assumes MT5 session is already initialized.
    """
    try:
        if not os.path.exists(DEVELOPER_USERS) or os.path.getsize(DEVELOPER_USERS) == 0:
            print(f" [!] Error: Users file not found or empty: {DEVELOPER_USERS}")
            return False
            
        with open(DEVELOPER_USERS, 'r') as f:
            users_data = json.load(f)
        
        # Verify the broker ID exists in users_data
        if dev_broker_id not in users_data:
            print(f" [{dev_broker_id}] ❌ Developer ID not found in {DEVELOPER_USERS}")
            return False
        
        print(f"\n{'='*10} CHECKING EMPTY TARGET AND EXIT TO ENFORCE USD RISK {'='*10}")
        print(f" [{dev_broker_id}] 🔍 Checking for targetless orders...")

        user_folder = os.path.join(DEV_PATH, dev_broker_id)

        # --- Identify Secondary Folders from Account Management ---
        acc_mgmt_path = os.path.join(user_folder, "accountmanagement.json")
        strategy_folders = []
        
        if not os.path.exists(acc_mgmt_path):
            print(f"   ⚠️  accountmanagement.json not found for {dev_broker_id}")
            return False
            
        try:
            with open(acc_mgmt_path, 'r') as f:
                acc_data = json.load(f)
            poi = acc_data.get("chart", {}).get("define_candles", {}).get("entries_poi_condition", {})
            for app_val in poi.values():
                if isinstance(app_val, dict):
                    for ent_val in app_val.values():
                        if isinstance(ent_val, dict) and ent_val.get("new_filename"):
                            s_dir = os.path.join(user_folder, ent_val["new_filename"])
                            if s_dir not in strategy_folders: 
                                strategy_folders.append(s_dir)
        except Exception as e:
            print(f"   ❌ Error parsing account management: {e}")
            return False

        if not strategy_folders:
            print(f"   ⚠️  No strategy folders found for {dev_broker_id}")
            return False

        total_orders_processed = 0
        total_orders_enforced = 0
        folders_with_issues = []

        # --- Process Each Strategy Folder Independently ---
        for s_folder in strategy_folders:
            folder_name = os.path.basename(s_folder)
            config_path = os.path.join(s_folder, "allowedsymbolsandvolumes.json")
            
            if not os.path.exists(config_path):
                print(f"   ⚠️  [{folder_name}] No allowedsymbolsandvolumes.json found")
                continue
            
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
                        if sym not in local_risk_data: 
                            local_risk_data[sym] = {}
                        
                        for k, v in item.items():
                            if k.endswith("_specs") and isinstance(v, dict):
                                tf = k.replace("_specs", "").upper()
                                local_risk_data[sym][tf] = v.get("usd_risk", 0)
            except Exception as e:
                print(f"   ❌ [{folder_name}] Config Error: {e}")
                continue

            if not local_risk_data:
                print(f"   ⚠️  [{folder_name}] No risk data found in config")
                continue

            # Scan Limit Orders in this specific folder
            limit_files = glob.glob(os.path.join(s_folder, "**", "limit_orders.json"), recursive=True)
            
            if not limit_files:
                print(f"   ⚠️  [{folder_name}] No limit_orders.json files found")
                continue
                
            folder_processed = 0
            folder_enforced = 0
            folder_missing_config = []
            
            for limit_path in limit_files:
                if "risk_reward_" in limit_path: 
                    continue
                    
                try:
                    with open(limit_path, 'r') as f: 
                        orders = json.load(f)
                except Exception as e:
                    print(f"     ⚠️  Could not read {os.path.basename(limit_path)}: {e}")
                    continue
                
                if not orders:
                    continue
                    
                modified = False
                
                for order in orders:
                    sym = str(order.get('symbol', '')).upper()
                    tf = str(order.get('timeframe', '')).upper()
                    
                    # TRIGGER: Order has no exit and no target
                    is_missing = order.get('exit') in [0, "0", None, ""] and \
                                 order.get('target') in [0, "0", None, ""]
                    
                    if is_missing:
                        folder_processed += 1
                        found_risk = local_risk_data.get(sym, {}).get(tf)
                        
                        if found_risk and found_risk > 0:
                            order['exit'] = 0
                            order['target'] = 0
                            order['usd_risk'] = found_risk
                            order['usd_based_risk_only'] = True
                            modified = True
                            folder_enforced += 1
                            print(f"     ✅ [{folder_name}] Enforced ${found_risk} risk for {sym} {tf}")
                        else:
                            missing_key = f"{sym}_{tf}"
                            if missing_key not in folder_missing_config:
                                folder_missing_config.append(missing_key)
                            print(f"     ❌ [{folder_name}] Missing Config: {sym} {tf} has no exit/target & no risk value found.")

                if modified:
                    try:
                        with open(limit_path, 'w') as f:
                            json.dump(orders, f, indent=4)
                        print(f"     📝 [{folder_name}] Saved changes to {os.path.basename(limit_path)}")
                    except Exception as e:
                        print(f"     ❌ [{folder_name}] Failed to save changes: {e}")

            # Final folder-level report
            if folder_processed > 0:
                icon = "✅" if folder_enforced == folder_processed else "⚠️"
                print(f"  └─ [{folder_name}] {icon} Found {folder_processed} orders | Enforced: {folder_enforced}")
                
                if folder_missing_config:
                    print(f"      Missing risk config for: {', '.join(folder_missing_config[:5])}" + 
                          (f" and {len(folder_missing_config)-5} more" if len(folder_missing_config) > 5 else ""))
                    folders_with_issues.append(folder_name)
            else:
                print(f"  └─ [{folder_name}] 🔘 No targetless orders found")
            
            total_orders_processed += folder_processed
            total_orders_enforced += folder_enforced

        # Summary for this broker
        if total_orders_processed > 0:
            print(f"\n  └─ 📊 Summary for {dev_broker_id}:")
            print(f"      Total targetless orders found: {total_orders_processed}")
            print(f"      Successfully enforced risk on: {total_orders_enforced}")
            
            if total_orders_enforced == total_orders_processed:
                print(f"      ✅ All targetless orders have been configured with USD risk")
            elif folders_with_issues:
                print(f"      ⚠️  Some folders have missing risk configurations: {', '.join(folders_with_issues)}")
                print(f"      Please check allowedsymbolsandvolumes.json in these folders")
        else:
            print(f"\n  └─ 🔘 No targetless orders found for {dev_broker_id}")

        print(f"\n{'='*10} EMPTY TARGET SYNC COMPLETE FOR {dev_broker_id} {'='*10}\n")
        return True
        
    except Exception as e:
        print(f" [!] Critical Error for {dev_broker_id}: {e}")
        return False

def enforce_risks_on_option(dev_broker_id):
    """
    Worker: Synchronizes USD risk rules from strategy configs for a single developer/broker ID.
    Includes detailed warnings for skipped orders to explain Processed vs Enforced gaps.
    Assumes MT5 session is already initialized.
    """
    # First call the empty pricelevels function for this specific broker
    activate_usd_based_risk_on_empty_pricelevels(dev_broker_id)
    
    try:
        # 1. Load User IDs and validate
        if not os.path.exists(DEVELOPER_USERS) or os.path.getsize(DEVELOPER_USERS) == 0:
            print(f" [!] Error: Users file not found or empty: {DEVELOPER_USERS}")
            return False
            
        with open(DEVELOPER_USERS, 'r') as f:
            users_data = json.load(f)
        
        # Verify the broker ID exists in users_data
        if dev_broker_id not in users_data:
            print(f" [{dev_broker_id}] ❌ Developer ID not found in {DEVELOPER_USERS}")
            return False
        
        print(f"\n{'='*10} STARTING RISK ENFORCEMENT SYNC FOR {dev_broker_id} {'='*10}")
        print(f" [{dev_broker_id}] 🛡️  Scanning strategy folders...")

        user_folder = os.path.join(DEV_PATH, dev_broker_id)

        # --- Identify Secondary Folders from Account Management ---
        acc_mgmt_path = os.path.join(user_folder, "accountmanagement.json")
        secondary_folder_paths = []
        
        if not os.path.exists(acc_mgmt_path):
            print(f"   ⚠️  accountmanagement.json not found for {dev_broker_id}")
            return False
            
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
        except Exception as e:
            print(f"  └─ ❌ Error reading accountmanagement.json: {e}")
            return False

        if not secondary_folder_paths:
            print(f"   ⚠️  No strategy folders found for {dev_broker_id}")
            return False

        total_orders_processed = 0
        total_orders_enforced = 0
        folders_with_warnings = {}
        folders_skipped = []

        for strategy_folder in secondary_folder_paths:
            folder_name = os.path.basename(strategy_folder)
            config_path = os.path.join(strategy_folder, "allowedsymbolsandvolumes.json")
            
            if not os.path.exists(config_path):
                print(f"   ⚠️  [{folder_name}] No allowedsymbolsandvolumes.json found")
                folders_skipped.append(folder_name)
                continue
            
            # Load and parse config
            try:
                with open(config_path, 'r') as f: 
                    config_data = json.load(f)
            except Exception as e:
                print(f"   ❌ [{folder_name}] Failed to load config: {e}")
                folders_skipped.append(folder_name)
                continue
            
            # Build Case-Insensitive Lookup
            risk_lookup = {}
            config_symbols_found = 0
            
            for category in config_data.values():
                if not isinstance(category, list): 
                    continue
                for item in category:
                    symbol = str(item.get("symbol", "")).upper()
                    if not symbol: 
                        continue
                    if symbol not in risk_lookup: 
                        risk_lookup[symbol] = {}
                    
                    for key, value in item.items():
                        if key.endswith("_specs") and isinstance(value, dict):
                            tf = key.replace("_specs", "").upper()
                            enforce_value = str(value.get("enforce_usd_risk", "no")).lower()
                            risk_lookup[symbol][tf] = {
                                "enforce": enforce_value == "yes" or enforce_value == "true" or enforce_value == "1",
                                "usd_risk": value.get("usd_risk", 0)
                            }
                            config_symbols_found += 1

            if not risk_lookup:
                print(f"   ⚠️  [{folder_name}] No valid risk configurations found")
                folders_skipped.append(folder_name)
                continue

            # Apply to Orders
            limit_files = glob.glob(os.path.join(strategy_folder, "**", "limit_orders.json"), recursive=True)
            
            if not limit_files:
                print(f"   ⚠️  [{folder_name}] No limit_orders.json files found")
                continue
                
            folder_processed = 0
            folder_enforced = 0
            folder_warnings = {
                "missing_symbol": set(),
                "missing_timeframe": set(),
                "enforce_false": set(),
                "zero_risk": set()
            }
            
            for limit_path in limit_files:
                if "risk_reward_" in limit_path: 
                    continue
                    
                try:
                    with open(limit_path, 'r') as f: 
                        orders = json.load(f)
                except Exception as e:
                    print(f"     ⚠️  Could not read {os.path.basename(limit_path)}: {e}")
                    continue
                
                if not orders:
                    continue
                    
                modified = False
                
                for order in orders:
                    folder_processed += 1
                    raw_sym = str(order.get('symbol', '')).upper()
                    raw_tf = str(order.get('timeframe', '')).upper()
                    
                    # LOGIC CHECK
                    if raw_sym in risk_lookup:
                        if raw_tf in risk_lookup[raw_sym]:
                            rule = risk_lookup[raw_sym][raw_tf]
                            
                            if rule["enforce"]:
                                if rule["usd_risk"] > 0:
                                    # Valid enforcement rule found
                                    order['exit'] = 0
                                    order['target'] = 0
                                    order['usd_risk'] = rule["usd_risk"]
                                    order['usd_based_risk_only'] = True
                                    modified = True
                                    folder_enforced += 1
                                    print(f"     ✅ [{folder_name}] Enforced ${rule['usd_risk']} risk for {raw_sym} {raw_tf}")
                                else:
                                    # Enforce is Yes but risk value is 0 or missing
                                    folder_warnings["zero_risk"].add(f"{raw_sym} ({raw_tf})")
                                    print(f"     ⚠️  [{folder_name}] {raw_sym} {raw_tf}: Enforce=Yes but USD Risk=0")
                            else:
                                # Enforce is set to No - order remains manual
                                folder_warnings["enforce_false"].add(f"{raw_sym} ({raw_tf})")
                                # Not printing every occurrence to avoid spam, just tracking
                        else:
                            # Symbol exists but timeframe not found
                            folder_warnings["missing_timeframe"].add(f"{raw_sym} ({raw_tf})")
                            print(f"     ⚠️  [{folder_name}] {raw_sym} found, but timeframe {raw_tf} missing in config.")
                    else:
                        # Symbol not found in config
                        folder_warnings["missing_symbol"].add(raw_sym)
                        print(f"     ⚠️  [{folder_name}] Symbol {raw_sym} not found in strategy config.")
                
                if modified:
                    try:
                        with open(limit_path, 'w') as f:
                            json.dump(orders, f, indent=4)
                        print(f"     📝 [{folder_name}] Saved changes to {os.path.basename(limit_path)}")
                    except Exception as e:
                        print(f"     ❌ [{folder_name}] Failed to save changes: {e}")
            
            # Folder-level summary with detailed warnings
            if folder_processed > 0:
                status_icon = "✅" if folder_enforced > 0 else "🔘"
                print(f"  └─ [{folder_name}] {status_icon} Processed: {folder_processed} | Enforced: {folder_enforced}")
                
                # Show summary of warnings if any
                if any(folder_warnings.values()):
                    print(f"      📋 Warning Summary:")
                    if folder_warnings["missing_symbol"]:
                        print(f"        • Missing symbols: {', '.join(list(folder_warnings['missing_symbol'])[:3])}" + 
                              (f" and {len(folder_warnings['missing_symbol'])-3} more" if len(folder_warnings['missing_symbol']) > 3 else ""))
                    if folder_warnings["missing_timeframe"]:
                        print(f"        • Missing timeframes: {', '.join(list(folder_warnings['missing_timeframe'])[:3])}" + 
                              (f" and {len(folder_warnings['missing_timeframe'])-3} more" if len(folder_warnings['missing_timeframe']) > 3 else ""))
                    if folder_warnings["enforce_false"]:
                        print(f"        • Enforce=False: {', '.join(list(folder_warnings['enforce_false'])[:3])}" + 
                              (f" and {len(folder_warnings['enforce_false'])-3} more" if len(folder_warnings['enforce_false']) > 3 else ""))
                    if folder_warnings["zero_risk"]:
                        print(f"        • Zero risk values: {', '.join(list(folder_warnings['zero_risk'])[:3])}" + 
                              (f" and {len(folder_warnings['zero_risk'])-3} more" if len(folder_warnings['zero_risk']) > 3 else ""))
                    
                    folders_with_warnings[folder_name] = folder_warnings
            else:
                print(f"  └─ [{folder_name}] 🔘 No orders found to process")
            
            total_orders_processed += folder_processed
            total_orders_enforced += folder_enforced

        # Final summary for this broker
        print(f"\n  └─ 📊 RISK ENFORCEMENT SUMMARY FOR {dev_broker_id}:")
        print(f"      Total orders processed: {total_orders_processed}")
        print(f"      Total orders enforced: {total_orders_enforced}")
        
        if total_orders_processed > 0:
            enforcement_rate = (total_orders_enforced / total_orders_processed) * 100
            print(f"      Enforcement rate: {enforcement_rate:.1f}%")
        
        if folders_skipped:
            print(f"      ⚠️  Skipped folders: {', '.join(folders_skipped)}")
        
        if folders_with_warnings:
            print(f"      ⚠️  Folders with warnings: {', '.join(folders_with_warnings.keys())}")
            print(f"      Check individual folder logs above for details")
        elif total_orders_processed > 0:
            print(f"      ✅ All orders successfully processed with no warnings")

        print(f"\n{'='*10} RISK ENFORCEMENT COMPLETE FOR {dev_broker_id} {'='*10}\n")
        return True

    except Exception as e:
        print(f" [!] Critical Error for {dev_broker_id}: {e}")
        return False

def preprocess_limit_orders_with_broker_data(dev_broker_id):
    """
    Worker: Processes symbols for the currently active MT5 session.
    Assumes mt5.initialize() has already been called.
    """
    # Load Normalization Map
    try:
        with open(NORMALIZE_SYMBOLS_PATH, 'r') as f:
            norm_map = json.load(f)
    except Exception as e:
        print(f" [!] Normalization map error: {e}")
        return False

    user_folder = os.path.join(DEV_PATH, dev_broker_id)
    limit_files = glob.glob(os.path.join(user_folder, "**", "limit_orders.json"), recursive=True)
    
    # Caching specs for the current broker session
    broker_symbol_cache = {}

    for file_path in limit_files:
        if "risk_reward_" in file_path:
            continue

        folder_context = os.path.basename(os.path.dirname(file_path))
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                orders = json.load(f)
        except: 
            continue

        file_changed = False
        for order in orders:
            raw_symbol = order.get('symbol')
            normalized_symbol = get_normalized_symbol(raw_symbol, norm_map)

            if not normalized_symbol:
                continue

            # --- CACHING LOGIC ---
            if normalized_symbol not in broker_symbol_cache:
                mt5.symbol_select(normalized_symbol, True)
                info = mt5.symbol_info(normalized_symbol)
                
                if info is None:
                    broker_symbol_cache[normalized_symbol] = None
                    print(f"  └─ [{folder_context}] ❓ {normalized_symbol} missing on server")
                    continue
                
                broker_symbol_cache[normalized_symbol] = {
                    "tick_size": info.trade_tick_size,
                    "tick_value": info.trade_tick_value
                }

            specs = broker_symbol_cache[normalized_symbol]
            if specs is None: continue

            # Update Logic
            if (order.get('symbol') != normalized_symbol or 
                order.get('tick_size') != specs["tick_size"] or
                order.get('tick_value') != specs["tick_value"]):
                
                order['symbol'] = normalized_symbol
                order['tick_size'] = specs["tick_size"]
                order['tick_value'] = specs["tick_value"]
                file_changed = True

        if file_changed:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(orders, f, indent=4)
            print(f"  └─ [{folder_context}] ✅ Specs Updated")

    return True

def validate_orders_with_live_volume(dev_broker_id):
    """
    Worker: Validates and fixes volumes for a specific broker.
    Assumes an MT5 session is already active.
    """
    # Load Normalization Map
    try:
        with open(NORMALIZE_SYMBOLS_PATH, 'r') as f:
            norm_map = json.load(f)
    except Exception as e:
        print(f" [!] Normalization map error: {e}")
        return False

    user_folder = os.path.join(DEV_PATH, dev_broker_id)
    broker_configs_updated = 0
    broker_orders_updated = 0
    broker_orders_fixed = 0
    symbols_fixed = []
    symbols_assigned = []
    orders_fixed_list = []

    account_info = mt5.account_info()
    broker_server_name = account_info.server if account_info else "Unknown"
    print(f" [{dev_broker_id}] 🔍 Validating Volumes: {broker_server_name}")

    # --- Step 1: Process CONFIGURATION FILES ---
    config_files = glob.glob(os.path.join(user_folder, "**", "allowedsymbolsandvolumes.json"), recursive=True)
    for config_file_path in config_files:
        try:
            file_changed = False
            with open(config_file_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)

            for category, items in config_data.items():
                if not isinstance(items, list): continue
                
                for item in items:
                    raw_symbol = item.get("symbol")
                    if not raw_symbol: continue
                    symbol = get_normalized_symbol(raw_symbol, norm_map)
                    
                    mt5.symbol_select(symbol, True)
                    info = mt5.symbol_info(symbol)
                    if info is None: continue

                    found_volume_specs = False
                    for key, value in item.items():
                        if "_specs" in key and isinstance(value, dict):
                            found_volume_specs = True
                            
                            if "volume" not in value:
                                default_volume = info.volume_min
                                value["volume"] = default_volume
                                value["vol_assigned"] = datetime.now().strftime("%H:%M")
                                file_changed = True
                                symbols_assigned.append(f"{symbol}(min:{default_volume})")
                            else:
                                current_vol = float(value.get("volume", 0.0))
                                new_vol = max(current_vol, info.volume_min)
                                step = info.volume_step
                                if step > 0:
                                    new_vol = round(math.floor(new_vol / step + 1e-9) * step, 2)
                                if new_vol > info.volume_max:
                                    new_vol = info.volume_max

                                if abs(new_vol - current_vol) > 1e-7:
                                    value["volume"] = new_vol
                                    value["vol_validated"] = datetime.now().strftime("%H:%M")
                                    file_changed = True
                                    symbols_fixed.append(f"{symbol}({current_vol}->{new_vol})")
                    
                    if not found_volume_specs:
                        item["1h_specs"] = {"volume": info.volume_min, "vol_assigned": datetime.now().strftime("%H:%M")}
                        file_changed = True
                        symbols_assigned.append(f"{symbol}(created:{info.volume_min})")

            if file_changed:
                with open(config_file_path, 'w', encoding='utf-8') as f:
                    json.dump(config_data, f, indent=4)
                broker_configs_updated += 1

        except Exception as e:
            print(f"     ❌ Error in config {os.path.basename(config_file_path)}: {e}")

    # --- Step 2: Process ORDER FILES ---
    order_files = glob.glob(os.path.join(user_folder, "**", "limit_orders.json"), recursive=True)
    for order_file_path in order_files:
        if "risk_reward_" in order_file_path: continue
        try:
            file_changed = False
            with open(order_file_path, 'r', encoding='utf-8') as f:
                orders = json.load(f)
            
            for order in orders:
                raw_symbol = order.get('symbol', '')
                symbol = get_normalized_symbol(raw_symbol, norm_map)
                info = mt5.symbol_info(symbol)
                if info is None: continue
                
                current_vol = order.get('volume')
                
                # Logic to fix/assign volume
                if current_vol is None or current_vol == 0:
                    new_vol = info.volume_min
                    order['volume'] = new_vol
                    order['vol_assigned'] = datetime.now().strftime("%H:%M")
                    file_changed = True
                    broker_orders_fixed += 1
                    orders_fixed_list.append(f"{raw_symbol}(assigned:{new_vol})")
                else:
                    try:
                        v = float(current_vol)
                        new_vol = max(v, info.volume_min)
                        if info.volume_step > 0:
                            new_vol = round(math.floor(new_vol / info.volume_step + 1e-9) * info.volume_step, 2)
                        if new_vol > info.volume_max: new_vol = info.volume_max
                        
                        if abs(new_vol - v) > 1e-7:
                            order['volume'] = new_vol
                            order['vol_validated'] = datetime.now().strftime("%H:%M")
                            file_changed = True
                            broker_orders_fixed += 1
                            orders_fixed_list.append(f"{raw_symbol}({v}->{new_vol})")
                    except:
                        order['volume'] = info.volume_min
                        file_changed = True

            if file_changed:
                with open(order_file_path, 'w', encoding='utf-8') as f:
                    json.dump(orders, f, indent=4)
                broker_orders_updated += 1
        except Exception as e:
            print(f"     ❌ Error in order file: {e}")

    # Summary
    if broker_configs_updated > 0 or broker_orders_updated > 0:
        print(f"  └─ ✅ Updated {broker_configs_updated} configs, {broker_orders_updated} order files.")
    return True

def calculate_symbols_orders(dev_broker_id):
    """
    Worker: Calculates Exit/Target prices for EVERY order in the limit_orders files
    for a single developer/broker ID.
    Strictly requires 'volume' to exist in the record; otherwise, skips the order.
    Assumes MT5 session is already initialized.
    """
    try:
        # 1. Load User Data and validate
        if not os.path.exists(DEVELOPER_USERS) or os.path.getsize(DEVELOPER_USERS) == 0:
            print(f" [!] Error: Users file not found: {DEVELOPER_USERS}")
            return False

        with open(DEVELOPER_USERS, 'r', encoding='utf-8') as f:
            users_data = json.load(f)
        
        # Verify the broker ID exists in users_data
        if dev_broker_id not in users_data:
            print(f" [{dev_broker_id}] ❌ Developer ID not found in {DEVELOPER_USERS}")
            return False

        print(f"\n{'='*10} FORCE-CALCULATING ALL ORDERS FOR {dev_broker_id} {'='*10}")
        print(f" [{dev_broker_id}] 🧮 Processing all available orders...")
        
        user_folder = os.path.join(DEV_PATH, dev_broker_id)
        acc_mgmt_path = os.path.join(user_folder, "accountmanagement.json")

        if not os.path.exists(acc_mgmt_path):
            print(f"  └─ ⚠️  Account management file missing for {dev_broker_id}")
            return False

        # Load account management data for risk-reward ratios
        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                acc_mgmt_data = json.load(f)
        except Exception as e:
            print(f"  └─ ❌ Failed to load accountmanagement.json: {e}")
            return False
        
        rr_ratios = acc_mgmt_data.get("risk_reward_ratios", [1.0])
        print(f"  └─ 📊 Using R:R ratios: {rr_ratios}")
        
        # Find all limit_orders.json files
        limit_order_files = glob.glob(os.path.join(user_folder, "**", "limit_orders.json"), recursive=True)
        
        if not limit_order_files:
            print(f"  └─ ⚠️  No limit_orders.json files found for {dev_broker_id}")
            return False
            
        total_files_updated = 0
        total_orders_processed = 0
        total_orders_skipped = 0
        total_orders_calculated = 0
        
        skipped_orders_log = []

        for limit_path in limit_order_files:
            if "risk_reward_" in limit_path: 
                continue
            
            try:
                with open(limit_path, 'r', encoding='utf-8') as f:
                    original_orders = json.load(f)
            except Exception as e:
                print(f"  └─ ⚠️  Could not read {os.path.basename(limit_path)}: {e}")
                continue

            if not original_orders:
                print(f"  └─ ⚠️  Empty orders file: {os.path.basename(limit_path)}")
                continue

            base_dir = os.path.dirname(limit_path)
            file_orders_processed = 0
            file_orders_skipped = 0

            for current_rr in rr_ratios:
                orders_copy = copy.deepcopy(original_orders)
                updated_any_order = False
                final_orders_to_save = []
                file_rr_orders_processed = 0
                file_rr_orders_skipped = 0
                file_rr_orders_calculated = 0

                for order in orders_copy:
                    file_orders_processed += 1
                    file_rr_orders_processed += 1
                    
                    try:
                        # --- STRICT DATA EXTRACTION ---
                        # Using .get() without a default for volume to check existence
                        raw_volume = order.get("volume")
                        if raw_volume is None or float(raw_volume) <= 0:
                            skip_reason = f"No volume found in record"
                            skipped_orders_log.append({
                                'symbol': order.get('symbol', 'Unknown'),
                                'timeframe': order.get('timeframe', 'Unknown'),
                                'reason': skip_reason,
                                'rr_ratio': current_rr
                            })
                            file_orders_skipped += 1
                            file_rr_orders_skipped += 1
                            continue 

                        volume = float(raw_volume)
                        entry = float(order['entry'])
                        rr_ratio = float(current_rr)
                        order_type = str(order.get('order_type', '')).upper()
                        
                        # Get tick data with fallbacks
                        tick_size = float(order.get('tick_size', 0.00001))
                        tick_value = float(order.get('tick_value', 1.0))
                        
                        # Calculate digits for rounding
                        if tick_size < 1:
                            digits = len(str(tick_size).split('.')[-1])
                        else:
                            digits = 0

                        # --- Calculation Logic ---
                        if order.get("usd_based_risk_only") is True:
                            risk_val = float(order.get("usd_risk", 0))
                            
                            if risk_val > 0:
                                sl_dist = (risk_val * tick_size) / (tick_value * volume)
                                tp_dist = sl_dist * rr_ratio

                                if "BUY" in order_type:
                                    order["exit"] = round(entry - sl_dist, digits)
                                    order["target"] = round(entry + tp_dist, digits)
                                else:  # SELL
                                    order["exit"] = round(entry + sl_dist, digits)
                                    order["target"] = round(entry - tp_dist, digits)
                                
                                file_rr_orders_calculated += 1
                                print(f"     ✅ USD-based: {order.get('symbol')} {order_type} - Exit: {order['exit']}, Target: {order['target']}")
                                
                        else:
                            sl_price = float(order.get('exit', 0))
                            tp_price = float(order.get('target', 0))

                            if sl_price == 0 and tp_price > 0:
                                # Calculate exit from target
                                risk_dist = abs(tp_price - entry) / rr_ratio
                                if "BUY" in order_type:
                                    order['exit'] = round(entry - risk_dist, digits)
                                else:  # SELL
                                    order['exit'] = round(entry + risk_dist, digits)
                                file_rr_orders_calculated += 1
                                print(f"     ✅ Target-based: {order.get('symbol')} - Exit calculated: {order['exit']}")
                                
                            elif sl_price > 0:
                                # Calculate target from exit
                                risk_dist = abs(entry - sl_price)
                                if "BUY" in order_type:
                                    order['target'] = round(entry + (risk_dist * rr_ratio), digits)
                                else:  # SELL
                                    order['target'] = round(entry - (risk_dist * rr_ratio), digits)
                                file_rr_orders_calculated += 1
                                print(f"     ✅ Exit-based: {order.get('symbol')} - Target calculated: {order['target']}")
                            
                            else:
                                # No valid SL or target found
                                skip_reason = "Neither exit nor target provided for calculation"
                                skipped_orders_log.append({
                                    'symbol': order.get('symbol', 'Unknown'),
                                    'timeframe': order.get('timeframe', 'Unknown'),
                                    'reason': skip_reason,
                                    'rr_ratio': current_rr
                                })
                                file_rr_orders_skipped += 1
                                continue

                        # --- Metadata Updates ---
                        order['risk_reward'] = rr_ratio
                        order['status'] = "Calculated"
                        order['calculated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                        final_orders_to_save.append(order)
                        updated_any_order = True

                    except (ValueError, KeyError, TypeError) as e:
                        skip_reason = f"Data error: {str(e)}"
                        skipped_orders_log.append({
                            'symbol': order.get('symbol', 'Unknown'),
                            'timeframe': order.get('timeframe', 'Unknown'),
                            'reason': skip_reason,
                            'rr_ratio': current_rr
                        })
                        file_rr_orders_skipped += 1
                        continue

                if updated_any_order:
                    target_out_dir = os.path.join(base_dir, f"risk_reward_{current_rr}")
                    os.makedirs(target_out_dir, exist_ok=True)
                    out_path = os.path.join(target_out_dir, "limit_orders.json")
                    
                    try:
                        with open(out_path, 'w', encoding='utf-8') as f:
                            json.dump(final_orders_to_save, f, indent=4)
                        total_files_updated += 1
                        
                        # Log file-level stats
                        print(f"  └─ 📁 R:{current_rr} - Processed: {file_rr_orders_processed}, "
                              f"Calculated: {file_rr_orders_calculated}, Skipped: {file_rr_orders_skipped}")
                        
                        # Update global counters
                        total_orders_processed += file_rr_orders_processed
                        total_orders_calculated += file_rr_orders_calculated
                        total_orders_skipped += file_rr_orders_skipped
                        
                    except Exception as e:
                        print(f"  └─ ❌ Failed to save {out_path}: {e}")

        # Final summary for this broker
        print(f"\n  └─ 📊 CALCULATION SUMMARY FOR {dev_broker_id}:")
        print(f"      Total R:R ratios used: {len(rr_ratios)}")
        print(f"      Total files updated: {total_files_updated}")
        print(f"      Total orders processed: {total_orders_processed}")
        print(f"      Total orders calculated: {total_orders_calculated}")
        print(f"      Total orders skipped: {total_orders_skipped}")
        
        if total_orders_processed > 0:
            calculation_rate = (total_orders_calculated / total_orders_processed) * 100
            print(f"      Calculation rate: {calculation_rate:.1f}%")
        
        # Show top skipped order reasons if any
        if skipped_orders_log:
            reason_counts = {}
            for skip in skipped_orders_log[:50]:  # Limit analysis to first 50
                reason = skip['reason']
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
            
            if reason_counts:
                print(f"\n      ⚠️  Top skipped order reasons:")
                for reason, count in list(reason_counts.items())[:3]:
                    print(f"        • {reason}: {count} orders")
        
        if total_orders_calculated > 0:
            print(f"      ✅ Successfully calculated orders for {dev_broker_id}")
        else:
            print(f"      🔘 No orders could be calculated (missing volume or required data)")

        print(f"{'='*10} CALCULATION COMPLETE FOR {dev_broker_id} {'='*10}\n")
        return True

    except Exception as e:
        print(f" [!] Critical Error for {dev_broker_id}: {e}")
        return False

def live_risk_reward_amounts_and_volume_scale(dev_broker_id):
    """
    Worker: Processes a single broker account for risk bucketing and volume scaling.
    Assumes mt5.initialize() has already been called for the specific broker.
    """
    # Load Normalization Map
    try:
        with open(NORMALIZE_SYMBOLS_PATH, 'r') as f:
            norm_map = json.load(f)
    except Exception as e:
        print(f" [!] Critical: Normalization map error: {e}")
        return False

    user_folder = os.path.join(DEV_PATH, dev_broker_id)
    
    # Get account info from active MT5 session
    account_info = mt5.account_info()
    if account_info is None:
        print(f" [{dev_broker_id}] ❌ No active MT5 connection or account info unavailable")
        return False
    
    broker_server_name = account_info.server
    acc_currency = account_info.currency
    print(f" [{dev_broker_id}] 🟢 {broker_server_name} ({acc_currency})")

    # Load Risks from accountmanagement.json
    acc_mgmt_path = os.path.join(user_folder, "accountmanagement.json")
    try:
        with open(acc_mgmt_path, 'r') as f:
            acc_mgmt_data = json.load(f)
        allowed_risks = acc_mgmt_data.get("RISKS", [])
        max_allowed_risk = max(allowed_risks) if allowed_risks else 50.0
    except Exception as e:
        print(f"  └─ ⚠️  Account Config Missing or Invalid: {e}")
        return False

    # Identify subfolders based on POI conditions
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
                with open(limit_path, 'r') as f: 
                    orders = json.load(f)
            except Exception as e:
                print(f"  └─ ⚠️  Could not read {limit_path}: {e}")
                continue

            risk_buckets = {}
            
            for order in orders:
                if order.get('status') != "Calculated": 
                    continue

                symbol = get_normalized_symbol(order.get('symbol'), norm_map)
                if not symbol:
                    continue
                    
                # Use the active session's symbol info
                if not mt5.symbol_select(symbol, True): 
                    print(f"  └─ ⚠️  Could not select symbol {symbol}")
                    continue
                
                info = mt5.symbol_info(symbol)
                if info is None: 
                    print(f"  └─ ⚠️  Symbol info not available for {symbol}")
                    continue

                try:
                    entry = float(order['entry'])
                    exit_p = float(order['exit'])
                except (ValueError, KeyError) as e:
                    print(f"  └─ ⚠️  Invalid entry/exit values in order: {e}")
                    continue
                
                # Calculate price risk (distance in price)
                price_risk = abs(entry - exit_p)
                
                # Calculate risk per unit (1 standard lot/contract)
                risk_per_unit = (price_risk / info.trade_tick_size) * info.trade_tick_value
                
                if risk_per_unit <= 0:
                    continue
                
                # For each allowed risk bucket, calculate required volume
                for target_risk in allowed_risks:
                    # Skip if we already have this risk bucket assigned for this order
                    if any(existing_order.get('target_risk') == target_risk for existing_order in risk_buckets.get(target_risk, [])):
                        continue
                    
                    # Calculate volume needed for this risk target
                    target_volume = target_risk / risk_per_unit
                    
                    # Round to nearest valid volume step
                    volume_step = info.volume_step
                    min_volume = info.volume_min
                    max_volume = info.volume_max
                    
                    # Round to valid volume (ensure it's a multiple of volume_step)
                    if volume_step > 0:
                        steps = round(target_volume / volume_step)
                        valid_volume = steps * volume_step
                    else:
                        valid_volume = target_volume
                    
                    # Clamp to min/max volume
                    valid_volume = max(min_volume, min(valid_volume, max_volume))
                    
                    # Calculate actual risk with this rounded volume
                    actual_risk = risk_per_unit * valid_volume
                    
                    # Check if actual risk is within acceptable tolerance
                    risk_tolerance = target_risk * 0.2
                    risk_diff_percent = abs(actual_risk - target_risk) / target_risk * 100 if target_risk > 0 else 0
                    
                    # Accept if within tolerance OR if we're at min/max volume limits
                    volume_at_limit = (valid_volume == min_volume or valid_volume == max_volume)
                    
                    if actual_risk <= target_risk + risk_tolerance or volume_at_limit:
                        # Create the order with calculated volume
                        scaled_order = {
                            **order,
                            'symbol': symbol,
                            'volume': round(valid_volume, 2),
                            f"{broker_server_name}_tick_size": info.trade_tick_size,
                            f"{broker_server_name}_tick_value": info.trade_tick_value,
                            'live_sl_risk_amount': round(actual_risk, 2),
                            'target_risk': target_risk,
                            'risk_tolerance_used': round(risk_diff_percent, 2) if risk_diff_percent > 0 else 0,
                            'calculated_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        
                        # Initialize list for this risk bucket if needed
                        if target_risk not in risk_buckets:
                            risk_buckets[target_risk] = []
                        
                        risk_buckets[target_risk].append(scaled_order)
                        buckets_found.add(target_risk)
                        total_orders_calculated += 1
                        
                        # Print debug info for significant deviations
                        if risk_diff_percent > 10:
                            print(f"    └─ ⚠️  {symbol}: Target ${target_risk} risk, "
                                  f"actual ${round(actual_risk,2)} ({round(risk_diff_percent,1)}% diff) "
                                  f"with volume {round(valid_volume,2)}")

            # Save Results
            if risk_buckets:
                base_dir = os.path.dirname(limit_path)
                
                # First, clear existing risk bucket files to avoid duplicates
                for r_val in allowed_risks:
                    out_dir = os.path.join(base_dir, f"{r_val}usd_risk")
                    out_file = os.path.join(out_dir, f"{r_val}usd_risk.json")
                    if os.path.exists(out_file):
                        try:
                            os.remove(out_file)
                            # Also remove empty directory if needed
                            if os.path.exists(out_dir) and not os.listdir(out_dir):
                                os.rmdir(out_dir)
                        except Exception as e:
                            print(f"    └─ ⚠️  Could not remove old file {out_file}: {e}")
                
                # Save new scaled orders
                for r_val, grouped in risk_buckets.items():
                    out_dir = os.path.join(base_dir, f"{r_val}usd_risk")
                    os.makedirs(out_dir, exist_ok=True)
                    out_file = os.path.join(out_dir, f"{r_val}usd_risk.json")
                    
                    # Save with pretty formatting
                    with open(out_file, 'w') as f:
                        json.dump(grouped, f, indent=4)
                    
                    print(f"    └─ 📁 Saved {len(grouped)} orders to {r_val}usd_risk/")

    # Account-level summary
    if total_orders_calculated > 0:
        bucket_str = ", ".join([f"${b}" for b in sorted(list(buckets_found))])
        print(f"  └─ ✅ Scaled {total_orders_calculated} orders into risk buckets: {bucket_str}")
    else:
        print(f"  └─ 🔘 No pending orders found for scaling")

    return True

def ajdust_order_price_closer_in_95cent_to_next_bucket(dev_broker_id):
    """
    Worker: Promotes fractional risk orders (e.g., $1.55) to the next whole bucket (e.g., $2.0)
    for a single developer/broker ID.
    Processes folders silently with minimal visual output.
    Assumes MT5 session is already initialized.
    """
    if not os.path.exists(DEV_PATH):
        print(f" [!] Error: DEV_PATH {DEV_PATH} does not exist.")
        return False

    print(f"\n{'='*10} PRICE RE-ADJUSTMENT PROMOTION FOR {dev_broker_id} {'='*10}")

    user_folder = os.path.join(DEV_PATH, dev_broker_id)
    
    if not os.path.exists(user_folder):
        print(f" [{dev_broker_id}] ❌ User folder not found")
        return False
        
    acc_mgmt_path = os.path.join(user_folder, "accountmanagement.json")
    
    # 1. Validation & Initialization
    if not os.path.exists(acc_mgmt_path) or os.path.getsize(acc_mgmt_path) == 0:
        print(f" [{dev_broker_id}] ⚠️  accountmanagement.json missing or empty")
        return False

    try:
        with open(acc_mgmt_path, 'r') as f:
            acc_mgmt_data = json.load(f)
    except Exception as e:
        print(f" [{dev_broker_id}] ❌ Failed to load accountmanagement.json: {e}")
        return False
    
    allowed_risks = acc_mgmt_data.get("RISKS", [])
    if not allowed_risks:
        print(f" [{dev_broker_id}] ⚠️  No RISKS defined in accountmanagement.json")
        return False

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
    promotion_details = {
        'total_files_processed': 0,
        'total_orders_scanned': 0,
        'buckets_created': set()
    }

    # 3. Process Folders
    for search_root in target_search_paths:
        risk_json_files = glob.glob(os.path.join(search_root, "**", "*usd_risk", "*usd_risk.json"), recursive=True)
        
        promotion_details['total_files_processed'] += len(risk_json_files)

        for file_path in risk_json_files:
            try:
                with open(file_path, 'r') as f:
                    orders = json.load(f)
            except Exception:
                continue

            if not isinstance(orders, list):
                continue

            promotion_details['total_orders_scanned'] += len(orders)

            for order in orders:
                try:
                    sl_risk = order.get('live_sl_risk_amount', 0)
                    
                    # Skip if not a valid number
                    if not isinstance(sl_risk, (int, float)):
                        continue
                        
                    fractional_part = sl_risk - int(sl_risk)

                    # Promotion Logic (e.g., 1.51 becomes 2.0)
                    if fractional_part >= 0.95:
                        target_risk = float(math.ceil(sl_risk))
                        
                        # Check if target risk is allowed
                        if target_risk not in allowed_risks:
                            continue 

                        # Re-calculation Data
                        entry = float(order['entry'])
                        rr_ratio = float(order['risk_reward'])
                        tick_size = float(order['tick_size'])
                        tick_value = float(order['tick_value'])
                        
                        # Get volume - try different possible keys
                        volume = None
                        for vol_key in ['volume', f"{order.get('timeframe', '')}_volume"]:
                            if vol_key in order:
                                volume = float(order[vol_key])
                                break
                        
                        if tick_value == 0 or volume == 0 or volume is None:
                            continue

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
                            'adjusted_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })

                        # Save to Destination
                        parent_rr_dir = os.path.dirname(os.path.dirname(file_path))
                        new_bucket_folder = os.path.join(parent_rr_dir, f"{int(target_risk)}usd_risk")
                        os.makedirs(new_bucket_folder, exist_ok=True)
                        
                        target_json_path = os.path.join(new_bucket_folder, f"{int(target_risk)}usd_risk.json")
                        
                        # Load existing data if any
                        existing_data = []
                        if os.path.exists(target_json_path):
                            try:
                                with open(target_json_path, 'r') as tf_file:
                                    existing_data = json.load(tf_file)
                            except:
                                existing_data = []
                        
                        existing_data.append(new_order)
                        
                        with open(target_json_path, 'w') as tf_file:
                            json.dump(existing_data, tf_file, indent=4)
                        
                        promotion_count += 1
                        promotion_details['buckets_created'].add(int(target_risk))
                        
                except (ValueError, KeyError, TypeError) as e:
                    # Silently skip problematic orders
                    continue

    # Final summary (minimal)
    if promotion_count > 0:
        bucket_list = sorted(list(promotion_details['buckets_created']))
        bucket_str = ", ".join([f"${b}" for b in bucket_list])
        print(f"  └─ ✅ Promoted {promotion_count} orders to higher buckets: {bucket_str}")
        print(f"      (Scanned {promotion_details['total_orders_scanned']} orders across {promotion_details['total_files_processed']} files)")
    else:
        print(f"  └─ 🔘 No fractional risks required promotion")

    print(f"{'='*10} PROMOTION COMPLETE FOR {dev_broker_id} {'='*10}\n")
    return True

def fix_risk_buckets_according_to_orders_risk(dev_broker_id):
    """
    Worker: Identifies and fixes bucket violations for a single developer/broker ID.
    Logic: Risk < $1.00 -> '0.5usd_risk', Risk >= $1.00 -> floor(risk) bucket.
    Assumes MT5 session is already initialized.
    """
    print(f"\n{'='*15} 🛠️  BUCKET INTEGRITY REPAIR FOR {dev_broker_id} {'='*15}")
    
    # Risk field key already exists in your JSON data
    risk_field = "live_sl_risk_amount"

    print(f" ⚙️  Processing Developer Broker: {dev_broker_id}...")
    
    user_folder = os.path.join(DEV_PATH, dev_broker_id)
    
    if not os.path.exists(user_folder):
        print(f"  └─ ❌ User folder not found: {user_folder}")
        return False
        
    investor_moved = 0
    
    # Search for: **/risk_reward_*/*usd_risk/*.json
    search_pattern = os.path.join(user_folder, "**", "*usd_risk", "*.json")
    found_files = glob.glob(search_pattern, recursive=True)

    if not found_files:
        print(f"  └─ 🔘 No USD risk bucket files found")

    for target_file_path in found_files:
        try:
            filename = os.path.basename(target_file_path)
            try:
                # Extracts '5.0' from '5usd_risk.json'
                current_bucket_val = float(filename.replace('usd_risk.json', ''))
            except: 
                continue

            with open(target_file_path, 'r', encoding='utf-8') as f:
                entries = json.load(f)

            if not isinstance(entries, list): 
                continue

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
                        except: 
                            pass
                    
                    dest_data.append(entry)
                    with open(new_file_path, 'w', encoding='utf-8') as nf:
                        json.dump(dest_data, nf, indent=4)
                    
                    file_changed = True
                    investor_moved += 1

            # Clean up the source file if items were moved out
            if file_changed:
                with open(target_file_path, 'w', encoding='utf-8') as f:
                    json.dump(staying_entries, f, indent=4)

                # Optional: Remove file if empty
                if not staying_entries:
                    try:
                        os.remove(target_file_path)
                        # Try to remove parent directory if empty
                        parent_dir = os.path.dirname(target_file_path)
                        if os.path.exists(parent_dir) and not os.listdir(parent_dir):
                            os.rmdir(parent_dir)
                    except:
                        pass

        except Exception:
            continue

    # Log completion for this broker with original style
    status_icon = "🛠️" if investor_moved > 0 else "✨"
    print(f"  └─ {status_icon} Finished {dev_broker_id}: {investor_moved} shifts made.")
    
    return True

def deduplicate_risk_bucket_orders(dev_broker_id):
    """
    Worker: Cleans up risk buckets by keeping only the most efficient order 
    (lowest risk) for each Symbol/Timeframe/Direction pair for a single broker.
    Assumes MT5 session is already initialized.
    """
    if not os.path.exists(DEV_PATH):
        print(f" [!] Error: DEV_PATH {DEV_PATH} does not exist.")
        return False

    print(f"\n{'='*10} RISK BUCKET DEDUPLICATION FOR {dev_broker_id} {'='*10}")

    user_folder = os.path.join(DEV_PATH, dev_broker_id)
    
    if not os.path.exists(user_folder):
        print(f" [{dev_broker_id}] ❌ User folder not found: {user_folder}")
        return False
        
    risk_json_files = glob.glob(os.path.join(user_folder, "**", "*usd_risk", "*usd_risk.json"), recursive=True)
    
    if not risk_json_files:
        print(f" [{dev_broker_id}] 🔘 No USD risk bucket files found")
        return True

    print(f" [{dev_broker_id}] 🧹 Cleaning redundant orders...")
    total_removed = 0
    files_processed = 0
    files_modified = 0

    for file_path in risk_json_files:
        files_processed += 1
        
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            continue

        try:
            with open(file_path, 'r') as f:
                orders = json.load(f)
        except:
            print(f"  ⚠️  Could not read {os.path.basename(file_path)}")
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
            
            try:
                risk_amt = float(order.get('live_sl_risk_amount', 0))
            except (ValueError, TypeError):
                # Skip orders with invalid risk amount
                continue

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
                files_modified += 1
            except Exception as e:
                print(f"  ⚠️  Failed to save {os.path.basename(file_path)}: {e}")

    # Final summary with original print style
    if total_removed > 0:
        print(f"  └─ ✅ Pruned {total_removed} redundant entries across {files_modified} files")
        if files_processed > files_modified:
            print(f"      (Scanned {files_processed} files total)")
    else:
        print(f"  └─ 🔘 Risk buckets already optimized (scanned {files_processed} files)")

    print(f"{'='*10} DEDUPLICATION COMPLETE FOR {dev_broker_id} {'='*10}\n")
    return True

def sync_dev_investors(dev_broker_id):
    """
    Worker: Synchronizes investor accounts with developer strategy data for a single developer.
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

        print(f"\n{'='*10} SYNCING INVESTOR ACCOUNTS FOR DEVELOPER: {dev_broker_id} {'='*10}")

        # 2. Find investors linked to this developer
        linked_investors = []
        for inv_broker_id, inv_info in investors_data.items():
            invested_string = inv_info.get("INVESTED_WITH", "")
            if "_" in invested_string:
                parts = invested_string.split("_", 1)
                if parts[0] == dev_broker_id:
                    linked_investors.append((inv_broker_id, inv_info))

        if not linked_investors:
            print(f" [{dev_broker_id}] 🔘 No linked investors found")
            return True

        total_synced = 0
        synced_investors = []  # List to track successfully synced investors

        # 3. Process each linked investor
        for inv_broker_id, inv_info in linked_investors:
            inv_name = inv_info.get("NAME", inv_broker_id)  # Get investor name, fallback to ID if not found
            print(f" [{dev_broker_id}] 🔄 Processing Investor: {inv_name} ({inv_broker_id})...")

            invested_string = inv_info.get("INVESTED_WITH", "")
            inv_server = inv_info.get("SERVER", "")
            
            parts = invested_string.split("_", 1)
            target_strat_name = parts[1]

            # Broker Matching Logic
            dev_broker_name = developers_data[dev_broker_id].get("BROKER", "").lower()
            if dev_broker_name not in inv_server.lower():
                print(f"  └─ ❌ Broker Mismatch: Dev requires {dev_broker_name.upper()}")
                continue

            dev_user_folder = os.path.join(DEV_PATH, dev_broker_id)
            inv_user_folder = os.path.join(INV_PATH, inv_broker_id)
            dev_acc_path = os.path.join(dev_user_folder, "accountmanagement.json")
            inv_acc_path = os.path.join(inv_user_folder, "accountmanagement.json")

            # 4. Sync Account Management
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
                    print(f"  └─ ✅ accountmanagement.json synced for {inv_name}")
            else:
                print(f"  └─ ⚠️  Dev accountmanagement.json missing")
                continue

            # 5. Clone Strategy Folder - Only copy pending_orders folder and limit orders JSON files
            dev_strat_path = os.path.join(dev_user_folder, target_strat_name)
            inv_strat_path = os.path.join(inv_user_folder, target_strat_name)

            if os.path.exists(dev_strat_path):
                try:
                    # Create the investor strategy folder if it doesn't exist
                    os.makedirs(inv_strat_path, exist_ok=True)
                    
                    # Remove existing content in investor strategy folder (except we'll recreate selectively)
                    if os.path.exists(inv_strat_path):
                        for item in os.listdir(inv_strat_path):
                            item_path = os.path.join(inv_strat_path, item)
                            if os.path.isdir(item_path):
                                shutil.rmtree(item_path)
                            else:
                                os.remove(item_path)
                    
                    # Copy pending_orders folder if it exists
                    dev_pending_orders_path = os.path.join(dev_strat_path, "pending_orders")
                    if os.path.exists(dev_pending_orders_path) and os.path.isdir(dev_pending_orders_path):
                        inv_pending_orders_path = os.path.join(inv_strat_path, "pending_orders")
                        shutil.copytree(dev_pending_orders_path, inv_pending_orders_path)
                        print(f"  └─ 📁 Pending orders folder copied for {inv_name}")
                    
                    # Copy limit_orders.json if it exists
                    dev_limit_orders_path = os.path.join(dev_strat_path, "limit_orders.json")
                    if os.path.exists(dev_limit_orders_path) and os.path.isfile(dev_limit_orders_path):
                        inv_limit_orders_path = os.path.join(inv_strat_path, "limit_orders.json")
                        shutil.copy2(dev_limit_orders_path, inv_limit_orders_path)
                        print(f"  └─ 📄 limit_orders.json copied for {inv_name}")
                    
                    # Copy limit_orders_backup.json if it exists
                    dev_limit_orders_backup_path = os.path.join(dev_strat_path, "limit_orders_backup.json")
                    if os.path.exists(dev_limit_orders_backup_path) and os.path.isfile(dev_limit_orders_backup_path):
                        inv_limit_orders_backup_path = os.path.join(inv_strat_path, "limit_orders_backup.json")
                        shutil.copy2(dev_limit_orders_backup_path, inv_limit_orders_backup_path)
                        print(f"  └─ 📄 limit_orders_backup.json copied for {inv_name}")
                    
                    # Note: All risk reward x folders and other files are automatically excluded
                    # because we only copy specific items
                    
                    print(f"  └─ 📁 Strategy Synced for {inv_name}: {target_strat_name}")
                    total_synced += 1
                    synced_investors.append(inv_name)  # Add to synced investors list
                    
                except Exception as e:
                    print(f"  └─ ❌ Folder Sync Error for {inv_name}: {e}")
            else:
                print(f"  └─ ⚠️  Dev Strategy folder '{target_strat_name}' missing for {inv_name}")

        # Summary for this developer
        if total_synced > 0:
            print(f"  └─ ✅ Synced {total_synced} investor account(s):")
            for investor_name in synced_investors:
                print(f"      • {investor_name}")
        else:
            print(f"  └─ 🔘 No investors synced")

        print(f"{'='*10} INVESTOR SYNC COMPLETE FOR {dev_broker_id} {'='*10}\n")
        return True

    except Exception as e:
        print(f" [!] Enrichment Error for {dev_broker_id}: {e}")
        return False

def run_accounts():
    """
    Orchestrator: Handles broker connections and triggers 
    processing for each account sequentially.
    """
    #cleanups
    purge_unauthorized_symbols()
    backup_limit_orders()
    #--------
    if not os.path.exists(BROKER_DICT_PATH):
        print(f" [!] Error: Broker config missing at {BROKER_DICT_PATH}")
        return

    with open(BROKER_DICT_PATH, 'r') as f:
        broker_configs = json.load(f)

    print(f"\n{'='*10} STARTING GLOBAL ACCOUNT PROCESSING {'='*10}")

    for dev_broker_id, config in broker_configs.items():
        # --- MT5 CONNECTION (Handled here now) ---
        authorized = mt5.initialize(
            path=config.get("TERMINAL_PATH", ""), 
            login=int(config.get("LOGIN_ID")), 
            password=config.get("PASSWORD"), 
            server=config.get("SERVER")
        )

        if not authorized:
            print(f" [{dev_broker_id}] ❌ Connection Failed: {mt5.last_error()}")
            continue

        print(f" [{dev_broker_id}] 🟢 Connected & Authorized")

        # --- CALL ALL PROCESSORS ---
        # Pass only the ID, as the processors use it to find local files
        try:
            # Step 1: Preprocess limit orders with broker data
            print(f"\n [Step 1/11] Preprocessing limit orders with broker data...")
            preprocess_limit_orders_with_broker_data(dev_broker_id)
            
            # Step 2: Synchronize volumes from allowed symbols config
            print(f"\n [Step 2/11] Synchronizing volumes from configuration...")
            provide_orders_volume(dev_broker_id)
            
            # Step 3: Check for empty targets and enforce USD risk
            print(f"\n [Step 3/11] Checking for empty targets and enforcing USD risk...")
            activate_usd_based_risk_on_empty_pricelevels(dev_broker_id)
            
            # Step 4: Enforce risk based on configuration rules
            print(f"\n [Step 4/11] Enforcing risk rules from configuration...")
            enforce_risks_on_option(dev_broker_id)
            
            # Step 5: Validate orders with live volume
            print(f"\n [Step 5/11] Validating orders with live volume...")
            validate_orders_with_live_volume(dev_broker_id)
            
            # Step 6: Calculate exit/target prices for all orders
            print(f"\n [Step 6/11] Calculating exit/target prices for all orders...")
            calculate_symbols_orders(dev_broker_id)
            
            # Step 7: Calculate risk reward amounts and volume scale
            print(f"\n [Step 7/11] Calculating risk reward amounts and volume scaling...")
            live_risk_reward_amounts_and_volume_scale(dev_broker_id)
            
            # Step 8: Adjust fractional risk orders to next bucket
            print(f"\n [Step 8/11] Adjusting fractional risk orders to next bucket...")
            ajdust_order_price_closer_in_95cent_to_next_bucket(dev_broker_id)
            
            # Step 9: Fix any bucket violations
            print(f"\n [Step 9/11] Fixing bucket violations...")
            fix_risk_buckets_according_to_orders_risk(dev_broker_id)
            
            # Step 10: Deduplicate redundant orders in risk buckets (run once)
            print(f"\n [Step 10/11] Deduplicating redundant orders in risk buckets...")
            deduplicate_risk_bucket_orders(dev_broker_id)
            
            # Step 11: Sync investor accounts for this developer
            print(f"\n [Step 11/11] Syncing investor accounts...")
            sync_dev_investors(dev_broker_id)
            
            print(f"\n [{dev_broker_id}] ✅ All processing steps completed successfully.")
            
        except Exception as e:
            print(f" [{dev_broker_id}] ⚠️ Error during processing: {e}")

        # --- SHUTDOWN (Handled here now) ---
        mt5.shutdown()
        print(f" [{dev_broker_id}] ⚪ Connection Closed")

    print(f"\n{'='*10} ALL ACCOUNTS PROCESSED {'='*10}")
    return True

if __name__ == "__main__":
   run_accounts()
   