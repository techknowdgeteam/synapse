import os
import MetaTrader5 as mt5
import pandas as pd
import mplfinance as mpf
from datetime import datetime
import json
from PIL import Image
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
from datetime import timedelta
import traceback
import shutil
from datetime import datetime
import re
from pathlib import Path

INVESTOR_USERS = r"C:\xampp\htdocs\synapse\synarex\usersdata\investors\investors.json"
INV_PATH = r"C:\xampp\htdocs\synapse\synarex\usersdata\investors"
NORMALIZE_SYMBOLS_PATH = r"C:\xampp\htdocs\synapse\synarex\symbols_normalization.json"
DEFAULT_ACCOUNTMANAGEMENT = r"C:\xampp\htdocs\synapse\synarex\default_accountmanagement.json"

def load_investors_dictionary():
    BROKERS_JSON_PATH = r"C:\xampp\htdocs\synapse\synarex\usersdata\investors\investors.json"
    """Load brokers config from JSON file with error handling and fallback."""
    if not os.path.exists(BROKERS_JSON_PATH):
        print(f"CRITICAL: {BROKERS_JSON_PATH} NOT FOUND! Using empty config.", "CRITICAL")
        return {}

    try:
        with open(BROKERS_JSON_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Optional: Convert numeric strings back to int where needed
        for user_brokerid, cfg in data.items():
            if "LOGIN_ID" in cfg and isinstance(cfg["LOGIN_ID"], str):
                cfg["LOGIN_ID"] = cfg["LOGIN_ID"].strip()
            if "RISKREWARD" in cfg and isinstance(cfg["RISKREWARD"], (str, float)):
                cfg["RISKREWARD"] = int(cfg["RISKREWARD"])
        
        return data

    except json.JSONDecodeError as e:
        print(f"Invalid JSON in investors.json: {e}", "CRITICAL")
        return {}
    except Exception as e:
        print(f"Failed to load investors.json: {e}", "CRITICAL")
        return {}
usersdictionary = load_investors_dictionary()


def sort_orders():
    """
    Identifies investor directories and removes risk_reward folders 
    that are not explicitly allowed in their accountmanagement.json.
    """
    if not os.path.exists(INV_PATH):
        print(f"\n [!] Error: Investor path {INV_PATH} not found.")
        return False

    print(f"\n{'='*10} 🧹 INVESTOR ORDER FILTRATION STARTING {'='*10}")

    # 1. Identify all investor directories
    investor_ids = [f for f in os.listdir(INV_PATH) if os.path.isdir(os.path.join(INV_PATH, f))]
    
    if not investor_ids:
        print(" └─ 🔘 No investor directories found.")
        return False

    for inv_id in investor_ids:
        inv_root = os.path.join(INV_PATH, inv_id)
        acc_mgmt_path = os.path.join(inv_root, "accountmanagement.json")

        print(f" [{inv_id}] 🔍 Scanning configurations...")

        if not os.path.exists(acc_mgmt_path):
            print(f"  └─ ⚠️  Missing accountmanagement.json. Skipping.")
            continue

        # 2. Load and potentially update the config for THIS specific investor
        try:
            with open(acc_mgmt_path, 'r+', encoding='utf-8') as f:
                data = json.load(f)
                
                # Check if field exists; if not, add it with default [3]
                if "selected_risk_reward" not in data:
                    data["selected_risk_reward"] = [3]
                    f.seek(0)
                    json.dump(data, f, indent=4)
                    f.truncate()

                # Convert list to set of strings for fast lookup
                allowed_ratios = {str(r) for r in data.get("selected_risk_reward", [])}
                print(f"  └─ ✅ Allowed R:R Ratios: {', '.join(allowed_ratios)}")
                
        except Exception as e:
            print(f"  └─ ❌ Error processing config: {e}")
            continue

        # 3. Deep search using os.walk
        deleted_count = 0
        kept_count = 0

        for root, dirs, files in os.walk(inv_root, topdown=False):
            for dir_name in dirs:
                if dir_name.startswith("risk_reward_"):
                    # Extract the ratio suffix (the 'X' in 'risk_reward_X')
                    ratio_suffix = dir_name.replace("risk_reward_", "")

                    # 4. Filter logic
                    full_path = os.path.join(root, dir_name)
                    if ratio_suffix not in allowed_ratios:
                        try:
                            shutil.rmtree(full_path)
                            deleted_count += 1
                        except Exception as e:
                            print(f"    └─ ❗ Failed to delete {dir_name}: {e}")
                    else:
                        kept_count += 1

        # Investor Summary print
        if deleted_count > 0 or kept_count > 0:
            print(f"  └─ ✨ Cleanup complete: Kept {kept_count} | Removed {deleted_count} folders")
        else:
            print(f"  └─ 🔘 No risk_reward folders found to process.")

    print(f"\n{'='*10} 🏁 FILTRATION COMPLETED {'='*10}\n")
    return True

def debug_print_all_broker_symbols():
    """
    Connects to the currently active MT5 terminal and prints 
    every available symbol name to the console.
    """
    # Ensure MT5 is initialized (if not already)
    if not mt5.initialize():
        print(f"FAILED to initialize MT5: {mt5.last_error()}")
        return

    # Get all symbols from the terminal
    symbols = mt5.symbols_get()
    
    if symbols is None:
        print("No symbols found. This might be a connection issue.")
    else:
        print(f"\n{'='*40}")
        print(f"BROKER: {mt5.account_info().server if mt5.account_info() else 'Unknown'}")
        print(f"TOTAL SYMBOLS FOUND: {len(symbols)}")
        print(f"{'='*40}")
        
        # Extract names and sort them alphabetically for easier reading
        all_names = sorted([s.name for s in symbols])
        
        for i, name in enumerate(all_names, 1):
            print(f"{i}. {name}")
            
        print(f"{'='*40}\nEND OF LIST\n{'='*40}")

def get_normalized_symbol(record_symbol, norm_map):
    """
    Finds the correct broker symbol, prioritizing those that allow full trading.
    Checks for suffixes like '+', '.', 'm', '..', etc.
    """
    if not record_symbol:
        return None

    search_term = record_symbol.replace(" ", "").replace("_", "").replace(".", "").upper()
    norm_data = norm_map.get("NORMALIZATION", {})
    target_synonyms = []

    # 1. Identify potential base names from the normalization map
    for standard_key, synonyms in norm_data.items():
        clean_key = standard_key.replace("_", "").upper()
        clean_syns = [s.replace(" ", "").replace("_", "").replace("/", "").upper() for s in synonyms]
        
        if search_term == clean_key or search_term in clean_syns:
            target_synonyms = list(synonyms)
            break

    if not target_synonyms:
        target_synonyms = [record_symbol, search_term]

    # 2. Get all symbols from broker
    all_symbols = mt5.symbols_get()
    if not all_symbols:
        return None
    
    available_names = [s.name for s in all_symbols]

    # 3. Helper to check if a symbol is actually tradeable
    def is_tradeable(sym_name):
        info = mt5.symbol_info(sym_name)
        if info is None:
            mt5.symbol_select(sym_name, True)
            info = mt5.symbol_info(sym_name)
        # Check if symbol exists AND trade_mode allows full access (2)
        return info is not None and info.trade_mode == mt5.SYMBOL_TRADE_MODE_FULL

    # 4. Search Strategy:
    # First, try exact matches. If not tradeable, look for suffix variations.
    common_suffixes = ["+", "m", ".", "..", "#", "i", "z"]
    
    for option in target_synonyms:
        clean_opt = option.replace("/", "").upper()
        
        # Priority A: Check for exact match first
        if clean_opt in available_names and is_tradeable(clean_opt):
            return clean_opt
        
        # Priority B: Look for any broker symbol that starts with our option (e.g., AUDUSD -> AUDUSD+)
        for broker_name in available_names:
            if broker_name.upper().startswith(clean_opt):
                if is_tradeable(broker_name):
                    return broker_name

    # Priority C: Manual Suffix Append (Last Resort)
    for option in target_synonyms:
        clean_opt = option.replace("/", "").upper()
        for suffix in common_suffixes:
            test_name = f"{clean_opt}{suffix}"
            if test_name in available_names and is_tradeable(test_name):
                return test_name

    return None

def get_filling_mode(symbol):
    """Helper to detect the correct filling mode for the broker."""
    symbol_info = mt5.symbol_info(symbol)
    if not symbol_info:
        return mt5.ORDER_FILLING_IOC # Fallback
    
    # Corrected attribute names for bitwise checking
    filling_mode = symbol_info.filling_mode
    
    if filling_mode & mt5.SYMBOL_FILLING_FOK:
        return mt5.ORDER_FILLING_FOK
    elif filling_mode & mt5.SYMBOL_FILLING_IOC:
        return mt5.ORDER_FILLING_IOC
    else:
        # Most common for Deriv/Indices if FOK/IOC are restricted
        return mt5.ORDER_FILLING_RETURN
    
def deduplicate_orders():
    """
    Scans all risk bucket JSON files and removes duplicate orders based on:
    Symbol, Timeframe, Order Type, and Entry Price.
    """
    print(f"\n{'='*10} 🧹 DEDUPLICATING ORDERS {'='*10}")
    
    total_files_cleaned = 0
    total_duplicates_removed = 0
    inv_base_path = Path(INV_PATH)

    if not inv_base_path.exists():
        print(f" [!] Error: Investor path {INV_PATH} does not exist.")
        return False

    # 1. Iterate through all investor folders
    investor_folders = [f for f in inv_base_path.iterdir() if f.is_dir()]
    
    if not investor_folders:
        print(" └─ 🔘 No investor directories found for deduplication.")
        return False

    for inv_folder in investor_folders:
        inv_id = inv_folder.name
        print(f" [{inv_id}] 🔍 Checking for duplicate entries...")

        # 2. Search for all risk bucket JSON files
        search_pattern = "**/risk_reward_*/*usd_risk/*.json"
        order_files = list(inv_folder.rglob(search_pattern))
        
        investor_duplicates = 0
        investor_files_cleaned = 0

        for file_path in order_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    orders = json.load(f)

                if not orders:
                    continue

                original_count = len(orders)
                seen_orders = set()
                unique_orders = []

                for order in orders:
                    # Create a unique key based on Symbol, Timeframe, Type, and Entry
                    unique_key = (
                        str(order.get("symbol")).strip(),
                        str(order.get("timeframe")).strip(),
                        str(order.get("order_type")).strip(),
                        float(order.get("entry", 0))
                    )

                    if unique_key not in seen_orders:
                        seen_orders.add(unique_key)
                        unique_orders.append(order)
                
                # 3. Only write back if duplicates were actually found
                if len(unique_orders) < original_count:
                    removed = original_count - len(unique_orders)
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(unique_orders, f, indent=4)
                    
                    investor_duplicates += removed
                    investor_files_cleaned += 1
                    total_duplicates_removed += removed
                    total_files_cleaned += 1

            except Exception as e:
                print(f"  └─ ❌ Error processing {file_path.name}: {e}")

        # Summary for the current investor
        if investor_duplicates > 0:
            print(f"  └─ ✨ Cleaned {investor_files_cleaned} files | Removed {investor_duplicates} duplicates")
        else:
            print(f"  └─ ✅ No duplicates found in active risk buckets")

    # Final Global Summary
    print(f"\n{'='*10} DEDUPLICATION COMPLETE {'='*10}")
    if total_duplicates_removed > 0:
        print(f" Total Duplicates Purged: {total_duplicates_removed}")
        print(f" Total Files Modified:    {total_files_cleaned}")
    else:
        print(" Everything was already clean.")
    print(f"{'='*33}\n")
    
    return True

def check_limit_orders_risk():
    """
    Function 3: Validates live pending orders against the account's current risk bucket.
    Synchronized with the stable initialization logic of place_usd_orders.
    Only removes orders with risk HIGHER than allowed (lower risk orders are kept).
    """
    print(f"\n{'='*10} 🛡️  LIVE RISK AUDIT: PENDING ORDERS {'='*10}")

    # --- DATA INITIALIZATION ---
    try:
        if not os.path.exists(NORMALIZE_SYMBOLS_PATH):
            print(" [!] CRITICAL ERROR: Normalization map path missing.")
            return False
        with open(NORMALIZE_SYMBOLS_PATH, 'r') as f:
            norm_map = json.load(f)
    except Exception as e:
        print(f" [!] CRITICAL ERROR: Normalization map load failed: {e}")
        return False

    for user_brokerid, broker_cfg in usersdictionary.items():
        print(f" [{user_brokerid}] 🔍 Auditing live risk limits...")
        
        inv_root = Path(INV_PATH) / user_brokerid
        acc_mgmt_path = inv_root / "accountmanagement.json"

        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️  Account config missing. Skipping.")
            continue

        # --- LOAD RISK CONFIG ---
        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            risk_map = config.get("account_balance_default_risk_management", {})
        except Exception as e:
            print(f"  └─ ❌ Failed to read config: {e}")
            continue

        # --- MT5 INITIALIZATION ---
        mt5.shutdown() 
        login_id = int(broker_cfg['LOGIN_ID'])
        mt5_path = broker_cfg["TERMINAL_PATH"]
        
        if not mt5.initialize(path=mt5_path, timeout=180000):
            print(f"  └─ ❌ MT5 Init failed for {login_id}")
            continue

        if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
            print(f"  └─ ❌ Login failed for {login_id}")
            mt5.shutdown()
            continue
        
        acc_info = mt5.account_info()
        balance = acc_info.balance

        # Determine Primary Risk Value - FIXED: Keep as float
        primary_risk = None
        primary_risk_original = None
        for range_str, r_val in risk_map.items():
            try:
                raw_range = range_str.split("_")[0]
                low, high = map(float, raw_range.split("-"))
                if low <= balance <= high:
                    primary_risk_original = float(r_val)  # Store as float
                    primary_risk = float(r_val)  # Keep as float, don't convert to int
                    break
            except Exception as e:
                print(f"  └─ ⚠️  Error parsing range '{range_str}': {e}")
                continue

        if primary_risk is None:
            print(f"  └─ ⚠️  No risk mapping for balance ${balance:,.2f}")
            mt5.shutdown()
            continue

        print(f"  └─ 💰 Balance: ${balance:,.2f} | Target Risk: ${primary_risk:.2f}")

        # Check Live Pending Orders
        pending_orders = mt5.orders_get()
        orders_checked = 0
        orders_removed = 0
        orders_kept_lower = 0
        orders_kept_in_range = 0

        if pending_orders:
            for order in pending_orders:
                if order.type not in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT]:
                    continue

                orders_checked += 1
                calc_type = mt5.ORDER_TYPE_BUY if order.type == mt5.ORDER_TYPE_BUY_LIMIT else mt5.ORDER_TYPE_SELL
                sl_profit = mt5.order_calc_profit(calc_type, order.symbol, order.volume_initial, order.price_open, order.sl)
                
                if sl_profit is not None:
                    order_risk_usd = round(abs(sl_profit), 2)
                    
                    # Use a percentage-based threshold instead of absolute dollar difference
                    # For small balances, absolute differences can be misleading
                    risk_difference = order_risk_usd - primary_risk
                    
                    # For very small balances (like $2), a difference of $0.50 is significant
                    # Use a relative threshold: 20% of primary risk or $0.50, whichever is smaller
                    relative_threshold = max(0.50, primary_risk * 0.2)
                    
                    # Only remove if risk is significantly higher than allowed
                    if risk_difference > relative_threshold: 
                        print(f"    └─ 🗑️  PURGING: {order.symbol} (#{order.ticket}) - Risk too high")
                        print(f"       Risk: ${order_risk_usd:.2f} > Allowed: ${primary_risk:.2f} (Δ: ${risk_difference:.2f})")
                        
                        cancel_request = {
                            "action": mt5.TRADE_ACTION_REMOVE,
                            "order": order.ticket
                        }
                        result = mt5.order_send(cancel_request)
                        
                        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                            orders_removed += 1
                        else:
                            error_msg = result.comment if result else "No response"
                            print(f"       [!] Cancel failed: {error_msg}")
                    
                    elif order_risk_usd < primary_risk - relative_threshold:
                        # Lower risk - keep it (good for the account)
                        orders_kept_lower += 1
                        print(f"    └─ ✅ KEEPING: {order.symbol} (#{order.ticket}) - Lower risk than allowed")
                        print(f"       Risk: ${order_risk_usd:.2f} < Allowed: ${primary_risk:.2f} (Δ: ${primary_risk - order_risk_usd:.2f})")
                    
                    else:
                        # Within tolerance - keep it
                        orders_kept_in_range += 1
                        print(f"    └─ ✅ KEEPING: {order.symbol} (#{order.ticket}) - Risk within tolerance")
                        print(f"       Risk: ${order_risk_usd:.2f} vs Allowed: ${primary_risk:.2f} (Δ: ${abs(risk_difference):.2f})")
                else:
                    print(f"    └─ ⚠️  Could not calc risk for #{order.ticket}")

        # Broker final summary
        if orders_checked > 0:
            print(f"  └─ 📊 Audit Results:")
            print(f"       • Orders checked: {orders_checked}")
            if orders_kept_lower > 0:
                print(f"       • Kept (lower risk): {orders_kept_lower}")
            if orders_kept_in_range > 0:
                print(f"       • Kept (in tolerance): {orders_kept_in_range}")
            if orders_removed > 0:
                print(f"       • Removed (too high): {orders_removed}")
            else:
                print(f"       ✅ No orders needed removal")
        else:
            print(f"  └─ 🔘 No pending limit orders found.")

        mt5.shutdown()

    print(f"\n{'='*10} 🏁 RISK AUDIT COMPLETE {'='*10}\n")
    return True

def default_price_repair():
    """
    Synchronizes 'exit' and 'target' prices from limit_orders_backup.json 
    to all active risk bucket files ONLY if 'default_price' is set to true 
    in accountmanagement.json.
    """
    print(f"\n{'='*10} 🛠️  DEFAULT PRICE SYNCHRONIZATION {'='*10}")
    
    if not os.path.exists(INV_PATH):
        print(f" [!] Error: Investor path {INV_PATH} not found.")
        return False

    investor_ids = [f for f in os.listdir(INV_PATH) if os.path.isdir(os.path.join(INV_PATH, f))]

    for inv_id in investor_ids:
        print(f" [{inv_id}] 🔍 Checking authorization...")
        inv_root = Path(INV_PATH) / inv_id
        acc_mgmt_path = inv_root / "accountmanagement.json"
        
        # --- 1. PERMISSION CHECK ---
        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️  Missing accountmanagement.json. Skipping.")
            continue

        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            settings = config.get("settings", {})
            is_allowed = settings.get("default_price", False)
            
            if not is_allowed:
                print(f"  └─ 🔘 Default price repair disabled in settings.")
                continue
        except Exception as e:
            print(f"  └─ ❌ Config Read Error: {e}")
            continue

        # --- 2. BACKUP LOCATION ---
        backup_files = list(inv_root.rglob("limit_orders_backup.json"))
        if not backup_files:
            print(f"  └─ ⚠️  No limit_orders_backup.json found.")
            continue
            
        backup_path = backup_files[0]
        print(f"  └─ ✅ Authorized. Loading master data from backup...")

        # --- 3. LOAD MASTER DATA ---
        try:
            with open(backup_path, 'r', encoding='utf-8') as f:
                backup_entries = json.load(f)
            
            master_map = {}
            for b_entry in backup_entries:
                key = (b_entry.get("symbol"), b_entry.get("entry"), b_entry.get("order_type"))
                master_map[key] = b_entry
                
        except Exception as e:
            print(f"  └─ ❌ Error loading master data: {e}")
            continue

        # --- 4. APPLY UPDATES TO RISK BUCKETS ---
        risk_files = list(inv_root.rglob("*usd_risk.json"))
        total_orders_patched = 0
        files_modified = 0

        for target_file in risk_files:
            if target_file.name == "limit_orders_backup.json":
                continue

            file_changed = False
            try:
                with open(target_file, 'r', encoding='utf-8') as f:
                    active_entries = json.load(f)

                if not isinstance(active_entries, list):
                    continue

                for active_entry in active_entries:
                    key = (active_entry.get("symbol"), active_entry.get("entry"), active_entry.get("order_type"))
                    
                    if key in master_map:
                        backup_ref = master_map[key]
                        
                        # Apply Exit repair
                        b_exit = backup_ref.get("exit", 0)
                        if b_exit != 0 and active_entry.get("exit") != b_exit:
                            active_entry["exit"] = b_exit
                            file_changed = True
                            total_orders_patched += 1
                        
                        # Apply Target repair
                        b_target = backup_ref.get("target", 0)
                        if b_target != 0 and active_entry.get("target") != b_target:
                            active_entry["target"] = b_target
                            file_changed = True
                            total_orders_patched += 1

                if file_changed:
                    with open(target_file, 'w', encoding='utf-8') as f:
                        json.dump(active_entries, f, indent=4)
                    files_modified += 1

            except Exception as e:
                print(f"    └─ ❌ Error patching {target_file.name}: {e}")

        # Summary per investor
        if total_orders_patched > 0:
            print(f"  └─ ✨ Successfully repaired {total_orders_patched} prices across {files_modified} files.")
        else:
            print(f"  └─ ✅ All risk bucket prices are already synchronized.")

    print(f"\n{'='*10} 🏁 REPAIR COMPLETED {'='*10}\n")
    return True

def filter_unauthorized_symbols():
    """
    Verifies and filters risk entries based on allowed symbols defined in accountmanagement.json.
    Matches sanitized versions of symbols to handle broker suffixes (e.g., EURUSDm vs EURUSD).
    """
    print(f"\n{'='*10} 🛡️  SYMBOL AUTHORIZATION FILTER {'='*10}")

    def sanitize(sym):
        if not sym: return ""
        # Remove non-alphanumeric, uppercase, and strip trailing M/PRO suffixes
        clean = re.sub(r'[^a-zA-Z0-9]', '', str(sym)).upper()
        return re.sub(r'(PRO|M)$', '', clean)

    if not os.path.exists(INV_PATH):
        print(f" [!] Error: Investor path {INV_PATH} not found.")
        return False

    investor_ids = [f for f in os.listdir(INV_PATH) if os.path.isdir(os.path.join(INV_PATH, f))]

    for inv_id in investor_ids:
        print(f" [{inv_id}] 🔍 Verifying symbol permissions...")
        inv_folder = Path(INV_PATH) / inv_id
        acc_mgmt_path = inv_folder / "accountmanagement.json"
        
        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️  Account config missing. Skipping.")
            continue

        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Extract and sanitize the list of allowed symbols
            sym_dict = config.get("symbols_dictionary", {})
            allowed_sanitized = {sanitize(s) for sublist in sym_dict.values() for s in sublist}
            
            if not allowed_sanitized:
                print(f"  └─ 🔘 No symbols defined in dictionary. Skipping filter.")
                continue

            risk_files = list(inv_folder.rglob("*usd_risk.json"))
            total_removed = 0
            files_affected = 0

            for target_file in risk_files:
                try:
                    with open(target_file, 'r', encoding='utf-8') as f:
                        entries = json.load(f)
                    
                    if not isinstance(entries, list): continue

                    initial_count = len(entries)
                    # Filter: Keep only if the sanitized symbol exists in our allowed set
                    filtered = [e for e in entries if sanitize(e.get("symbol")) in allowed_sanitized]
                    
                    if len(filtered) != initial_count:
                        removed_here = initial_count - len(filtered)
                        total_removed += removed_here
                        files_affected += 1
                        with open(target_file, 'w', encoding='utf-8') as f:
                            json.dump(filtered, f, indent=4)
                except:
                    continue

            # Summary per investor
            if total_removed > 0:
                print(f"  └─ 🗑️  Purged {total_removed} unauthorized orders across {files_affected} buckets.")
            else:
                print(f"  └─ ✅ All {len(allowed_sanitized)} symbols are authorized.")

        except Exception as e:
            print(f"  └─ ❌ Error processing {inv_id}: {e}")

    print(f"\n{'='*10} 🏁 FILTERING COMPLETE {'='*10}\n")
    return True

def place_usd_orders():
    # --- SUB-FUNCTION 1: DATA INITIALIZATION ---
    def load_normalization_map():
        try:
            if not os.path.exists(NORMALIZE_SYMBOLS_PATH):
                return {}
            with open(NORMALIZE_SYMBOLS_PATH, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"❌ CRITICAL ERROR: Could not load normalization map: {e}")
            return None

    # --- SUB-FUNCTION 2: HISTORY ORDER CHECK ---
    def check_history_order_break(symbol, max_break_hours):
        """
        Check if there are any closed trades for the symbol within the max_break_hours window.
        Returns True if within break period (should skip), False if outside break period (can place).
        """
        if max_break_hours <= 0:
            return False  # No break limit, always allow
            
        try:
            # Get current time
            current_time = datetime.now()
            
            # Calculate the cutoff time (current time - max_break_hours)
            cutoff_time = current_time - timedelta(hours=max_break_hours)
            
            # Convert to timestamp for MT5
            cutoff_timestamp = int(cutoff_time.timestamp())
            current_timestamp = int(current_time.timestamp())
            
            # Get historical deals for the symbol within the time range
            # Using DEAL_ENTRY_OUT to get closed trades only
            history_deals = mt5.history_deals_get(cutoff_timestamp, current_timestamp, symbol=symbol)
            
            if not history_deals:
                return False  # No history found, can place order
                
            # Filter for closed trades only (deal entries that are exits)
            closed_trades = []
            for deal in history_deals:
                # Check if this is a trade exit (DEAL_ENTRY_OUT or DEAL_ENTRY_INOUT)
                if deal.entry in [mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_INOUT]:
                    # Verify it's a complete trade (has both entry and exit)
                    if deal.type in [mt5.DEAL_TYPE_BUY, mt5.DEAL_TYPE_SELL]:
                        closed_trades.append(deal)
            
            if closed_trades:
                # Sort by time (most recent first)
                closed_trades.sort(key=lambda x: x.time, reverse=True)
                most_recent = closed_trades[0]
                
                # Convert deal time to datetime for readable output
                deal_time = datetime.fromtimestamp(most_recent.time)
                hours_ago = (current_time - deal_time).total_seconds() / 3600
                
                print(f"      📊 History Check: Found {len(closed_trades)} closed trade(s) for {symbol}")
                print(f"        • Most recent: {most_recent.type} at {deal_time.strftime('%Y-%m-%d %H:%M:%S')} ({hours_ago:.1f} hours ago)")
                print(f"        • Volume: {most_recent.volume} | Profit: ${most_recent.profit:.2f}")
                print(f"        • Break period: {max_break_hours} hours - {'WITHIN' if hours_ago <= max_break_hours else 'OUTSIDE'} break window")
                
                # If most recent trade is within the break period, return True (should skip)
                return hours_ago <= max_break_hours
            else:
                print(f"      📊 History Check: No closed trades found for {symbol} in the last {max_break_hours} hours")
                return False  # No closed trades, can place order
                
        except Exception as e:
            print(f"      ⚠️  Error checking history for {symbol}: {e}")
            import traceback
            traceback.print_exc()
            return False  # On error, allow order (fail open)

    # --- SUB-FUNCTION 3: RISK & FILE AGGREGATION ---
    def collect_and_deduplicate_entries(inv_root, risk_map, balance, pull_lower, selected_rr, norm_map):
        primary_risk = None
        primary_risk_original = None  # Store original float value
        print(f"  ⚙️  Determining primary risk for balance: ${balance:,.2f}")
        
        for range_str, r_val in risk_map.items():
            try:
                raw_range = range_str.split("_")[0]
                low, high = map(float, raw_range.split("-"))
                if low <= balance <= high:
                    primary_risk_original = float(r_val)  # Store as float
                    # For folder structure, use the original decimal format
                    # If it's a whole number, use integer format, otherwise keep as decimal string
                    if primary_risk_original.is_integer():
                        primary_risk = str(int(primary_risk_original))
                    else:
                        primary_risk = str(primary_risk_original)  # Keep as "0.5" not "0_5"
                    print(f"  ✅ Balance ${balance:,.2f} in range ${low:,.2f}-${high:,.2f} → Risk Level: {primary_risk_original} (Folder: {primary_risk})")
                    break
            except Exception as e:
                print(f"  ⚠️  Error parsing risk range '{range_str}': {e}")
                continue

        if primary_risk is None:
            print(f"  ❌ No matching risk range found for balance: ${balance:,.2f}")
            return None, []

        # Build list of risk levels to scan
        risk_levels = []
        
        if pull_lower:
            # Get all risk values from the risk map that are <= primary_risk_original
            all_risk_values = []
            for range_str, r_val in risk_map.items():
                try:
                    val = float(r_val)
                    all_risk_values.append(val)
                except:
                    continue
            
            # Sort and filter risk values <= primary_risk_original
            all_risk_values.sort()
            risk_levels_float = [v for v in all_risk_values if v <= primary_risk_original]
            
            # Convert to folder name format (keep decimals, don't replace with underscores)
            for rv in risk_levels_float:
                if rv.is_integer():
                    risk_levels.append(str(int(rv)))
                else:
                    risk_levels.append(str(rv))  # Keep as "0.5", not "0_5"
            
            print(f"  📊 Pull lower enabled, scanning risk levels: {risk_levels_float} → Folders: {risk_levels}")
        else:
            # Just use the primary risk level
            risk_levels = [primary_risk]
            print(f"  📊 Scanning only primary risk level: {primary_risk_original} → Folder: {primary_risk}")

        unique_entries_dict = {}
        target_rr_folder = f"risk_reward_{selected_rr}"
        
        for r_val in risk_levels:
            # Use the risk value directly for folder name
            risk_folder_name = f"{r_val}usd_risk"
            risk_filename = f"{r_val}usd_risk.json"
            search_pattern = f"**/{target_rr_folder}/{risk_folder_name}/{risk_filename}"
            
            entries_found = 0
            files_found = list(inv_root.rglob(search_pattern))
            
            if files_found:
                print(f"    📁 Searching: {search_pattern}")
                print(f"    📁 Found {len(files_found)} file(s)")
                
            for path in files_found:
                if path.is_file():
                    try:
                        with open(path, 'r') as f:
                            data = json.load(f)
                            if isinstance(data, list):
                                entries_found += len(data)
                                for entry in data:
                                    symbol = get_normalized_symbol(entry["symbol"], norm_map)
                                    if symbol:  # Only add if symbol normalization succeeded
                                        key = f"{entry.get('timeframe','NA')}|{symbol}|{entry.get('order_type','NA')}|{round(float(entry['entry']), 5)}"
                                        if key not in unique_entries_dict:
                                            unique_entries_dict[key] = entry
                    except json.JSONDecodeError as e:
                        print(f"      ❌ Risk Level {r_val}: Invalid JSON format in {path.name} - {e}")
                    except Exception as e:
                        print(f"      ❌ Risk Level {r_val}: Failed to read file {path.name} - {e}")
            
            if entries_found > 0:
                print(f"    📁 Risk Level {r_val}: Found {entries_found} entries in {len(files_found)} file(s), {len(unique_entries_dict)} unique after dedup")
            else:
                if files_found:
                    print(f"    📁 Risk Level {r_val}: No valid entries found in {len(files_found)} file(s)")
                else:
                    print(f"    📁 Risk Level {r_val}: No files found matching pattern")
                
        print(f"  📈 TOTAL: {len(unique_entries_dict)} unique trading opportunities collected")
        return risk_levels, list(unique_entries_dict.values())
    
    # --- SUB-FUNCTION 4: BROKER CLEANUP (with history check) ---
    def cleanup_unauthorized_orders(all_entries, norm_map, max_break_hours):
        print("  🧹 Checking for unauthorized orders...")
        try:
            current_orders = mt5.orders_get()
            if not current_orders:
                print("  ✅ No pending orders found")
                return
            
            authorized_tickets = set()
            # First, mark tickets that match our entries as authorized
            for entry in all_entries:
                vol_key = next((k for k in entry.keys() if k.endswith("volume")), None)
                if not vol_key: continue
                
                e_symbol = get_normalized_symbol(entry["symbol"], norm_map)
                if not e_symbol: continue
                
                e_price = round(float(entry["entry"]), 5)
                e_vol = round(float(entry[vol_key]), 2)
                
                for order in current_orders:
                    if (order.symbol == e_symbol and 
                        round(order.price_open, 5) == e_price and 
                        round(order.volume_initial, 2) == e_vol):
                        authorized_tickets.add(order.ticket)
            
            # Now, check if any orders should be removed due to history break
            history_blocked_tickets = set()
            if max_break_hours > 0:
                print(f"  ⏰ Checking history break period ({max_break_hours} hours)...")
                for order in current_orders:
                    # Check if this symbol has recent history that would block it
                    if check_history_order_break(order.symbol, max_break_hours):
                        history_blocked_tickets.add(order.ticket)
                        print(f"    🚫 Blocking {order.symbol} order (Ticket: {order.ticket}) due to recent history")
            
            # Combine unauthorized tickets (not in authorized AND in history blocked)
            # But also remove any history blocked orders even if they're authorized
            tickets_to_remove = set()
            for order in current_orders:
                # Remove if not authorized OR if history blocked
                if order.ticket not in authorized_tickets or order.ticket in history_blocked_tickets:
                    tickets_to_remove.add(order.ticket)
            
            deleted_count = 0
            for order in current_orders:
                if order.ticket in tickets_to_remove:
                    reason = []
                    if order.ticket not in authorized_tickets:
                        reason.append("not in opportunity list")
                    if order.ticket in history_blocked_tickets:
                        reason.append("within history break period")
                    
                    print(f"    🗑️  Removing unauthorized order - Ticket: {order.ticket} | Symbol: {order.symbol} | Price: {order.price_open} | Reason: {', '.join(reason)}")
                    res = mt5.order_send({
                        "action": mt5.TRADE_ACTION_REMOVE,
                        "order": order.ticket
                    })
                    if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                        deleted_count += 1
                    else:
                        error_msg = res.comment if res else "No response"
                        error_code = res.retcode if res else "N/A"
                        print(f"      ❌ Failed to remove order {order.ticket}: {error_msg} (Code: {error_code})")
            
            if deleted_count > 0:
                print(f"  ✅ Removed {deleted_count} unauthorized order(s)")
            else:
                print(f"  ✅ All orders are authorized")
                
        except Exception as e:
            print(f"  ❌ Cleanup failed: {e}")

    # --- SUB-FUNCTION 5: ORDER EXECUTION (with history check) ---
    def execute_missing_orders(all_entries, norm_map, default_magic, selected_rr, trade_allowed, max_break_hours):
        if not trade_allowed:
            print("  ⚠️  AutoTrading is DISABLED - Orders will not be executed")
            return 0, 0, 0
            
        placed = failed = skipped = 0
        total = len(all_entries)
        
        print(f"  🚀 Executing {total} order(s)...")
        
        for idx, entry in enumerate(all_entries, 1):
            try:
                # Progress indicator with symbol
                symbol_orig = entry["symbol"]
                print(f"\n    [{idx}/{total}] Processing {symbol_orig}...")
                
                # Step 1: Symbol normalization
                symbol = get_normalized_symbol(symbol_orig, norm_map)
                if not symbol:
                    print(f"      ❌ FAIL: {symbol_orig} - Symbol not found in normalization map or not available on broker")
                    failed += 1
                    continue

                # Step 2: Select symbol in Market Watch
                if not mt5.symbol_select(symbol, True):
                    last_error = mt5.last_error()
                    print(f"      ❌ FAIL: {symbol} - Could not select symbol in Market Watch. Error: {last_error}")
                    failed += 1
                    continue

                # Step 3: Get symbol info
                symbol_info = mt5.symbol_info(symbol)
                if not symbol_info:
                    last_error = mt5.last_error()
                    print(f"      ❌ FAIL: {symbol} - Symbol info not available. Error: {last_error}")
                    failed += 1
                    continue
                
                # Step 4: Check symbol trade mode
                if symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_DISABLED:
                    print(f"      ❌ FAIL: {symbol} - Trading is disabled for this symbol")
                    failed += 1
                    continue
                elif symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_CLOSEONLY:
                    print(f"      ❌ FAIL: {symbol} - Only closing positions allowed (close-only mode)")
                    failed += 1
                    continue
                
                # Step 5: HISTORY BREAK CHECK - Skip if recent history exists
                if max_break_hours > 0:
                    if check_history_order_break(symbol, max_break_hours):
                        print(f"      ⏭️  SKIP: {symbol} - Recent trade history within {max_break_hours} hour break period")
                        skipped += 1
                        continue
                
                # Step 6: Get volume key
                vol_key = next((k for k in entry.keys() if k.endswith("volume")), None)
                if not vol_key:
                    print(f"      ❌ FAIL: {symbol_orig} - No volume field found in entry data")
                    failed += 1
                    continue
                
                # Step 7: Check for existing positions
                existing_pos = mt5.positions_get(symbol=symbol) or []
                if existing_pos:
                    pos_type = "BUY" if existing_pos[0].type == mt5.POSITION_TYPE_BUY else "SELL"
                    print(f"      ⏭️  SKIP: {symbol} - Existing {pos_type} position detected (Volume: {existing_pos[0].volume})")
                    skipped += 1
                    continue
                
                # Step 8: Check for existing orders
                existing_orders = mt5.orders_get(symbol=symbol) or []
                entry_price = round(float(entry["entry"]), symbol_info.digits)
                
                order_exists = False
                for order in existing_orders:
                    if round(order.price_open, symbol_info.digits) == entry_price:
                        order_type = "BUY LIMIT" if order.type == mt5.ORDER_TYPE_BUY_LIMIT else "SELL LIMIT"
                        print(f"      ⏭️  SKIP: {symbol} - {order_type} already exists at {entry_price} (Ticket: {order.ticket})")
                        order_exists = True
                        break
                
                if order_exists:
                    skipped += 1
                    continue

                # Step 9: Calculate and validate volume
                try:
                    volume = float(entry[vol_key])
                except (ValueError, TypeError) as e:
                    print(f"      ❌ FAIL: {symbol} - Invalid volume value '{entry[vol_key]}': {e}")
                    failed += 1
                    continue
                
                # Check minimum volume
                if volume < symbol_info.volume_min:
                    print(f"      ❌ FAIL: {symbol} - Volume {volume:.2f} below minimum {symbol_info.volume_min:.2f}")
                    failed += 1
                    continue
                
                # Check maximum volume
                if volume > symbol_info.volume_max:
                    print(f"      ❌ FAIL: {symbol} - Volume {volume:.2f} above maximum {symbol_info.volume_max:.2f}")
                    failed += 1
                    continue
                
                # Adjust volume step
                original_volume = volume
                if symbol_info.volume_step > 0:
                    volume = round(volume / symbol_info.volume_step) * symbol_info.volume_step
                    if volume != original_volume:
                        print(f"      📊 Volume adjusted: {original_volume:.2f} → {volume:.2f} (to match volume step {symbol_info.volume_step})")
                
                # Step 10: Validate prices
                try:
                    entry_price = round(float(entry["entry"]), symbol_info.digits)
                    sl_price = round(float(entry["exit"]), symbol_info.digits)
                    tp_price = round(float(entry["target"]), symbol_info.digits)
                except (ValueError, TypeError, KeyError) as e:
                    missing_field = str(e).split("'")[1] if "'" in str(e) else "unknown"
                    print(f"      ❌ FAIL: {symbol} - Missing or invalid price field: {missing_field}")
                    failed += 1
                    continue
                
                # Check price validity
                if entry_price <= 0 or sl_price <= 0 or tp_price <= 0:
                    print(f"      ❌ FAIL: {symbol} - Invalid prices (Entry: {entry_price}, SL: {sl_price}, TP: {tp_price})")
                    failed += 1
                    continue
                
                # Step 11: Determine order type
                order_type = entry.get("order_type", "").lower()
                if order_type == "buy_limit":
                    mt5_order_type = mt5.ORDER_TYPE_BUY_LIMIT
                    direction = "BUY LIMIT"
                elif order_type == "sell_limit":
                    mt5_order_type = mt5.ORDER_TYPE_SELL_LIMIT
                    direction = "SELL LIMIT"
                else:
                    print(f"      ❌ FAIL: {symbol} - Invalid order type '{order_type}' (expected 'buy_limit' or 'sell_limit')")
                    failed += 1
                    continue

                # Step 12: Prepare and send order
                request = {
                    "action": mt5.TRADE_ACTION_PENDING,
                    "symbol": symbol,
                    "volume": round(volume, 2),
                    "type": mt5_order_type,
                    "price": entry_price,
                    "sl": sl_price,
                    "tp": tp_price,
                    "magic": int(entry.get("magic", default_magic)),
                    "comment": f"RR{selected_rr}",
                    "type_time": mt5.ORDER_TIME_GTC,
                    "type_filling": mt5.ORDER_FILLING_IOC,
                }
                
                # Send order
                res = mt5.order_send(request)
                
                if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                    print(f"      ✅ SUCCESS: {direction} {symbol} @ {entry_price} | Vol: {volume:.2f} | Ticket: {res.order}")
                    placed += 1
                else:
                    # Detailed error mapping
                    error_code = res.retcode if res else "N/A"
                    error_msg = res.comment if res and res.comment else "No response"
                    
                    # Map common MT5 error codes to human-readable messages
                    error_map = {
                        10004: "Trade disabled",
                        10006: "No connection",
                        10007: "Too many requests",
                        10008: "Invalid price",
                        10009: "Invalid volume",
                        10010: "Market closed",
                        10011: "Insufficient money",
                        10012: "Price changed",
                        10013: "Off quotes",
                        10014: "Broker busy",
                        10015: "Requote",
                        10016: "Order locked",
                        10017: "Long positions only allowed",
                        10018: "Too many orders",
                        10019: "Pending orders limit reached",
                        10020: "Hedging prohibited",
                        10021: "Close-only mode",
                        10022: "FIFO rule violated",
                        10023: "Hedged position exists",
                        130: "Invalid stops",
                        134: "Insufficient funds",
                        135: "Price changed",
                        136: "Off quotes",
                        137: "Broker busy",
                        138: "Requote",
                        139: "Order locked",
                        140: "Invalid volume",
                        145: "Modification denied",
                        146: "No connection",
                        148: "Too many orders",
                        149: "Invalid order type",
                    }
                    
                    human_error = error_map.get(error_code, f"Unknown error ({error_code})")
                    print(f"      ❌ FAIL: {direction} {symbol} @ {entry_price} | Error: {human_error} | Details: {error_msg}")
                    failed += 1
                    
            except Exception as e:
                print(f"      💥 UNEXPECTED ERROR: {entry.get('symbol', 'Unknown')} - {str(e)}")
                import traceback
                traceback.print_exc()
                failed += 1
                
        # Summary
        if total > 0:
            success_rate = (placed / total) * 100 if total > 0 else 0
            print(f"\n    📊 Execution Summary: ✅ {placed} placed | ❌ {failed} failed | ⏭️  {skipped} skipped | Success Rate: {success_rate:.1f}%")
        return placed, failed, skipped

    # --- MAIN EXECUTION FLOW ---
    print("\n" + "="*80)
    print("🚀 STARTING USD ORDER PLACEMENT ENGINE")
    print("="*80)
    
    norm_map = load_normalization_map()
    if norm_map is None: return False

    total_investors = len(usersdictionary)
    processed = 0
    successful = 0

    for user_brokerid, broker_cfg in usersdictionary.items():
        processed += 1
        print(f"\n{'-'*80}")
        print(f"📋 INVESTOR [{processed}/{total_investors}]: {user_brokerid}")
        print(f"{'-'*80}")
        
        inv_root = Path(INV_PATH) / user_brokerid
        acc_mgmt_path = inv_root / "accountmanagement.json"
        
        if not acc_mgmt_path.exists():
            print(f"  ⚠️  Account management file not found - skipping")
            continue

        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Initialize MT5 connection
            print(f"  🔌 Connecting to MT5 terminal...")
            mt5.shutdown() 
            login_id = int(broker_cfg['LOGIN_ID'])
            mt5_path = broker_cfg["TERMINAL_PATH"]
            
            if not mt5.initialize(path=mt5_path, timeout=180000):
                error = mt5.last_error()
                print(f"  ❌ Failed to initialize MT5: {error}")
                continue

            # Login check
            acc = mt5.account_info()
            if acc is None or acc.login != login_id:
                print(f"  🔑 Logging into account {login_id}...")
                if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
                    error = mt5.last_error()
                    print(f"  ❌ Login failed: {error}")
                    continue
                print(f"  ✅ Successfully logged in")
            else:
                print(f"  ✅ Already logged in")

            # Extract settings
            settings = config.get("settings", {})
            pull_lower = settings.get("pull_orders_from_lower", False)
            max_history_break = settings.get("max_history_order_break", 0)  # Default to 0 (disabled)
            selected_rr = config.get("selected_risk_reward", [None])[0]
            risk_map = config.get("account_balance_default_risk_management", {})
            default_magic = config.get("magic_number", 123456)
            
            # Account info
            acc_info = mt5.account_info()
            term_info = mt5.terminal_info()
            
            if not acc_info:
                print(f"  ❌ Failed to get account info")
                continue
                
            print(f"\n  📊 Account Details:")
            print(f"    • Balance: ${acc_info.balance:,.2f}")
            print(f"    • Equity: ${acc_info.equity:,.2f}")
            print(f"    • Free Margin: ${acc_info.margin_free:,.2f}")
            print(f"    • Margin Level: {acc_info.margin_level:.2f}%" if acc_info.margin_level else "    • Margin Level: N/A")
            print(f"    • AutoTrading: {'✅ ENABLED' if term_info.trade_allowed else '❌ DISABLED'}")
            print(f"    • Risk/Reward: {selected_rr}")
            print(f"    • Max History Break: {max_history_break} hours {'(DISABLED)' if max_history_break <= 0 else ''}")

            # Stage 1: Risk determination and file loading
            print(f"\n  📁 STAGE 1: Scanning for trading opportunities")
            risk_lvls, all_entries = collect_and_deduplicate_entries(
                inv_root, risk_map, acc_info.balance, pull_lower, selected_rr, norm_map
            )
            
            if all_entries:
                # Stage 2: Cleanup (with history check)
                print(f"\n  🧹 STAGE 2: Order cleanup")
                cleanup_unauthorized_orders(all_entries, norm_map, max_history_break)
                
                # Stage 3: Execution (with history check)
                print(f"\n  🚀 STAGE 3: Order placement")
                p, f, s = execute_missing_orders(
                    all_entries, norm_map, default_magic, selected_rr, term_info.trade_allowed, max_history_break
                )
                
                if p > 0 or f > 0 or s > 0:
                    successful += 1
                    
                print(f"\n  📈 INVESTOR SUMMARY: {user_brokerid}")
                print(f"    • Orders Placed: {p}")
                print(f"    • Orders Failed: {f}")
                print(f"    • Orders Skipped: {s}")
                print(f"    • Total Processed: {len(all_entries)}")
            else:
                print(f"  ℹ️  No trading opportunities found")

        except json.JSONDecodeError as e:
            print(f"  ❌ Invalid JSON in account management file: {e}")
        except KeyError as e:
            print(f"  ❌ Missing required configuration key: {e}")
        except Exception as e:
            print(f"  💥 SYSTEM ERROR: {str(e)}")
            import traceback
            traceback.print_exc()
            continue
        
    mt5.shutdown()
    
    print("\n" + "="*80)
    print("✅ ORDER PLACEMENT COMPLETED")
    print(f"   Processed: {processed}/{total_investors} investors")
    print(f"   Successful: {successful} investors")
    print("="*80)
    
    return True

def place_orders_hedging():
    """
    Places hedge orders (opposite side) at the same entry points when place_orders_hedge is enabled.
    For each buy limit order, places a sell stop at same price with SL/TP switched.
    For each sell limit order, places a buy stop at same price with SL/TP switched.
    """
    print("\n" + "="*80)
    print("🛡️  HEDGING ENGINE: PLACING OPPOSITE ORDERS")
    print("="*80)
    
    # --- SUB-FUNCTION 1: DATA INITIALIZATION ---
    def load_normalization_map():
        try:
            if not os.path.exists(NORMALIZE_SYMBOLS_PATH):
                print(f"  [!] Normalization map not found at: {NORMALIZE_SYMBOLS_PATH}")
                return {}
            with open(NORMALIZE_SYMBOLS_PATH, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"  ❌ CRITICAL ERROR: Could not load normalization map: {e}")
            return None

    # --- SUB-FUNCTION 2: GET HEDGE ORDER TYPE ---
    def get_hedge_order_type(original_order_type):
        """
        Converts order type to its hedge counterpart:
        - BUY_LIMIT -> SELL_STOP
        - SELL_LIMIT -> BUY_STOP
        """
        if original_order_type == mt5.ORDER_TYPE_BUY_LIMIT:
            return mt5.ORDER_TYPE_SELL_STOP, "SELL STOP"
        elif original_order_type == mt5.ORDER_TYPE_SELL_LIMIT:
            return mt5.ORDER_TYPE_BUY_STOP, "BUY STOP"
        else:
            return None, None

    # --- SUB-FUNCTION 3: CHECK IF HEDGE ALREADY EXISTS ---
    def hedge_order_exists(symbol, entry_price, hedge_type, existing_orders):
        """Check if a hedge order already exists at the same price"""
        for order in existing_orders:
            if (order.symbol == symbol and 
                order.type == hedge_type and
                abs(order.price_open - entry_price) < 0.00001):  # Small epsilon for price comparison
                return order.ticket
        return None

    # --- SUB-FUNCTION 4: CALCULATE SWAPPED SL/TP ---
    def calculate_swapped_sl_tp(original_order, entry_price, symbol_info):
        """
        Swaps SL and TP positions for the hedge order:
        - For BUY_LIMIT (expecting price to go up): 
            SL is below entry, TP is above entry
        - For SELL_STOP hedge (expecting price to go down):
            SL should be above entry, TP below entry
        """
        if original_order.type == mt5.ORDER_TYPE_BUY_LIMIT:
            # Original BUY_LIMIT: SL below price, TP above price
            # Hedge SELL_STOP: SL above price, TP below price
            sl_distance = entry_price - original_order.sl  # Positive distance
            tp_distance = original_order.tp - entry_price  # Positive distance
            
            hedge_sl = entry_price + sl_distance  # SL above entry
            hedge_tp = entry_price - tp_distance  # TP below entry
            
        else:  # SELL_LIMIT
            # Original SELL_LIMIT: SL above price, TP below price
            # Hedge BUY_STOP: SL below price, TP above price
            sl_distance = original_order.sl - entry_price  # Positive distance
            tp_distance = entry_price - original_order.tp  # Positive distance
            
            hedge_sl = entry_price - sl_distance  # SL below entry
            hedge_tp = entry_price + tp_distance  # TP above entry
        
        # Round to symbol digits
        hedge_sl = round(hedge_sl, symbol_info.digits)
        hedge_tp = round(hedge_tp, symbol_info.digits)
        
        # Validate prices are positive
        if hedge_sl <= 0 or hedge_tp <= 0:
            return None, None
            
        return hedge_sl, hedge_tp

    # --- MAIN EXECUTION FLOW ---
    
    # Load normalization map
    norm_map = load_normalization_map()
    if norm_map is None:
        print("  ❌ Failed to load normalization map. Exiting.")
        return False

    total_investors = len(usersdictionary)
    processed = 0
    hedge_orders_placed = 0
    hedge_orders_skipped = 0
    hedge_orders_failed = 0

    for user_brokerid, broker_cfg in usersdictionary.items():
        processed += 1
        print(f"\n{'-'*80}")
        print(f"📋 INVESTOR [{processed}/{total_investors}]: {user_brokerid}")
        print(f"{'-'*80}")

        inv_root = Path(INV_PATH) / user_brokerid
        acc_mgmt_path = inv_root / "accountmanagement.json"

        if not acc_mgmt_path.exists():
            print(f"  ⚠️  Account management file not found - skipping")
            continue

        try:
            # Load configuration
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)

            # Check if hedging is enabled
            settings = config.get("settings", {})
            if not settings.get("place_orders_hedge", False):
                print(f"  ℹ️  Hedging is disabled for this investor (place_orders_hedge = false)")
                continue

            print(f"  ✅ Hedging is ENABLED")

            # Initialize MT5 connection
            print(f"  🔌 Connecting to MT5 terminal...")
            mt5.shutdown()
            login_id = int(broker_cfg['LOGIN_ID'])
            mt5_path = broker_cfg["TERMINAL_PATH"]

            if not mt5.initialize(path=mt5_path, timeout=180000):
                error = mt5.last_error()
                print(f"  ❌ Failed to initialize MT5: {error}")
                continue

            # Login
            acc = mt5.account_info()
            if acc is None or acc.login != login_id:
                print(f"  🔑 Logging into account {login_id}...")
                if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
                    error = mt5.last_error()
                    print(f"  ❌ Login failed: {error}")
                    continue
                print(f"  ✅ Successfully logged in")
            else:
                print(f"  ✅ Already logged in")

            # Get account and terminal info
            acc_info = mt5.account_info()
            term_info = mt5.terminal_info()

            if not acc_info:
                print(f"  ❌ Failed to get account info")
                continue

            if not term_info.trade_allowed:
                print(f"  ⚠️  AutoTrading is DISABLED - Cannot place hedge orders")
                continue

            print(f"\n  📊 Account Details:")
            print(f"    • Balance: ${acc_info.balance:,.2f}")
            print(f"    • Equity: ${acc_info.equity:,.2f}")
            print(f"    • AutoTrading: {'✅ ENABLED' if term_info.trade_allowed else '❌ DISABLED'}")

            # Get all pending orders
            pending_orders = mt5.orders_get()
            if not pending_orders:
                print(f"  ℹ️  No pending orders found to hedge")
                continue

            print(f"  🔍 Found {len(pending_orders)} pending order(s) to check for hedging")

            # Process each pending order
            investor_placed = 0
            investor_skipped = 0
            investor_failed = 0

            for order in pending_orders:
                # Skip if not a limit order (only hedge limit orders)
                if order.type not in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT]:
                    continue

                # Get symbol info
                symbol_info = mt5.symbol_info(order.symbol)
                if not symbol_info:
                    print(f"    ⚠️  Cannot get symbol info for {order.symbol}")
                    continue

                # Determine hedge order type
                hedge_type, hedge_type_name = get_hedge_order_type(order.type)
                if hedge_type is None:
                    print(f"    ⚠️  Cannot determine hedge type for order type: {order.type}")
                    continue

                # Check if hedge order already exists
                existing_orders = mt5.orders_get(symbol=order.symbol) or []
                existing_ticket = hedge_order_exists(order.symbol, order.price_open, hedge_type, existing_orders)
                
                if existing_ticket:
                    print(f"    ⏭️  SKIP: Hedge {hedge_type_name} already exists for {order.symbol} @ {order.price_open} (Ticket: {existing_ticket})")
                    investor_skipped += 1
                    continue

                # Calculate swapped SL and TP
                hedge_sl, hedge_tp = calculate_swapped_sl_tp(order, order.price_open, symbol_info)
                
                if hedge_sl is None or hedge_tp is None:
                    print(f"    ❌ FAIL: Invalid SL/TP calculation for {order.symbol} hedge")
                    investor_failed += 1
                    continue

                # Prepare hedge order request
                request = {
                    "action": mt5.TRADE_ACTION_PENDING,
                    "symbol": order.symbol,
                    "volume": order.volume_initial,  # Same volume as original
                    "type": hedge_type,
                    "price": order.price_open,  # Same entry price
                    "sl": hedge_sl,
                    "tp": hedge_tp,
                    "magic": order.magic,  # Same magic number
                    "comment": f"HEDGE_{order.ticket}",  # Reference to original order
                    "type_time": mt5.ORDER_TIME_GTC,
                    "type_filling": mt5.ORDER_FILLING_IOC,
                }

                # Send hedge order
                print(f"    🚀 Placing {hedge_type_name} hedge for {order.symbol} @ {order.price_open}")
                print(f"       Original: SL: {order.sl:.{symbol_info.digits}f}, TP: {order.tp:.{symbol_info.digits}f}")
                print(f"       Hedge:    SL: {hedge_sl:.{symbol_info.digits}f}, TP: {hedge_tp:.{symbol_info.digits}f}")
                
                res = mt5.order_send(request)

                if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                    print(f"       ✅ SUCCESS: Hedge placed - Ticket: {res.order}")
                    investor_placed += 1
                else:
                    error_code = res.retcode if res else "N/A"
                    error_msg = res.comment if res and res.comment else "No response"
                    
                    # Map common errors
                    error_map = {
                        10004: "Trade disabled",
                        10008: "Invalid price",
                        10009: "Invalid volume",
                        10014: "Broker busy",
                        130: "Invalid stops",
                        138: "Requote",
                        145: "Modification denied",
                    }
                    human_error = error_map.get(error_code, f"Unknown error ({error_code})")
                    print(f"       ❌ FAIL: {human_error} | Details: {error_msg}")
                    investor_failed += 1

            # Investor summary
            print(f"\n  📊 HEDGING SUMMARY FOR {user_brokerid}:")
            print(f"    • Hedge Orders Placed: {investor_placed}")
            print(f"    • Hedge Orders Skipped (already exist): {investor_skipped}")
            print(f"    • Hedge Orders Failed: {investor_failed}")
            
            hedge_orders_placed += investor_placed
            hedge_orders_skipped += investor_skipped
            hedge_orders_failed += investor_failed

        except json.JSONDecodeError as e:
            print(f"  ❌ Invalid JSON in account management file: {e}")
        except KeyError as e:
            print(f"  ❌ Missing required configuration key: {e}")
        except Exception as e:
            print(f"  💥 SYSTEM ERROR: {str(e)}")
            import traceback
            traceback.print_exc()
        finally:
            mt5.shutdown()

    return True  

def check_limit_orders_risk():
    """
    Function 3: Validates live pending orders against the account's current risk bucket.
    Synchronized with the stable initialization logic of place_usd_orders.
    Only removes orders with risk HIGHER than allowed (lower risk orders are kept).
    """
    print(f"\n{'='*10} 🛡️  LIVE RISK AUDIT: PENDING ORDERS {'='*10}")

    # --- DATA INITIALIZATION ---
    try:
        if not os.path.exists(NORMALIZE_SYMBOLS_PATH):
            print(" [!] CRITICAL ERROR: Normalization map path missing.")
            return False
        with open(NORMALIZE_SYMBOLS_PATH, 'r') as f:
            norm_map = json.load(f)
    except Exception as e:
        print(f" [!] CRITICAL ERROR: Normalization map load failed: {e}")
        return False

    for user_brokerid, broker_cfg in usersdictionary.items():
        print(f" [{user_brokerid}] 🔍 Auditing live risk limits...")
        
        inv_root = Path(INV_PATH) / user_brokerid
        acc_mgmt_path = inv_root / "accountmanagement.json"

        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️  Account config missing. Skipping.")
            continue

        # --- LOAD RISK CONFIG ---
        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            risk_map = config.get("account_balance_default_risk_management", {})
        except Exception as e:
            print(f"  └─ ❌ Failed to read config: {e}")
            continue

        # --- MT5 INITIALIZATION ---
        mt5.shutdown() 
        login_id = int(broker_cfg['LOGIN_ID'])
        mt5_path = broker_cfg["TERMINAL_PATH"]
        
        if not mt5.initialize(path=mt5_path, timeout=180000):
            print(f"  └─ ❌ MT5 Init failed for {login_id}")
            continue

        if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
            print(f"  └─ ❌ Login failed for {login_id}")
            mt5.shutdown()
            continue
        
        acc_info = mt5.account_info()
        balance = acc_info.balance

        # Determine Primary Risk Value - FIXED: Keep as float
        primary_risk = None
        primary_risk_original = None
        for range_str, r_val in risk_map.items():
            try:
                raw_range = range_str.split("_")[0]
                low, high = map(float, raw_range.split("-"))
                if low <= balance <= high:
                    primary_risk_original = float(r_val)  # Store as float
                    primary_risk = float(r_val)  # Keep as float, don't convert to int
                    break
            except Exception as e:
                print(f"  └─ ⚠️  Error parsing range '{range_str}': {e}")
                continue

        if primary_risk is None:
            print(f"  └─ ⚠️  No risk mapping for balance ${balance:,.2f}")
            mt5.shutdown()
            continue

        print(f"  └─ 💰 Balance: ${balance:,.2f} | Target Risk: ${primary_risk:.2f}")

        # Check Live Pending Orders
        pending_orders = mt5.orders_get()
        orders_checked = 0
        orders_removed = 0
        orders_kept_lower = 0
        orders_kept_in_range = 0

        if pending_orders:
            for order in pending_orders:
                if order.type not in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT]:
                    continue

                orders_checked += 1
                calc_type = mt5.ORDER_TYPE_BUY if order.type == mt5.ORDER_TYPE_BUY_LIMIT else mt5.ORDER_TYPE_SELL
                sl_profit = mt5.order_calc_profit(calc_type, order.symbol, order.volume_initial, order.price_open, order.sl)
                
                if sl_profit is not None:
                    order_risk_usd = round(abs(sl_profit), 2)
                    
                    # Use a percentage-based threshold instead of absolute dollar difference
                    # For small balances, absolute differences can be misleading
                    risk_difference = order_risk_usd - primary_risk
                    
                    # For very small balances (like $2), a difference of $0.50 is significant
                    # Use a relative threshold: 20% of primary risk or $0.50, whichever is smaller
                    relative_threshold = max(0.50, primary_risk * 0.2)
                    
                    # Only remove if risk is significantly higher than allowed
                    if risk_difference > relative_threshold: 
                        print(f"    └─ 🗑️  PURGING: {order.symbol} (#{order.ticket}) - Risk too high")
                        print(f"       Risk: ${order_risk_usd:.2f} > Allowed: ${primary_risk:.2f} (Δ: ${risk_difference:.2f})")
                        
                        cancel_request = {
                            "action": mt5.TRADE_ACTION_REMOVE,
                            "order": order.ticket
                        }
                        result = mt5.order_send(cancel_request)
                        
                        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                            orders_removed += 1
                        else:
                            error_msg = result.comment if result else "No response"
                            print(f"       [!] Cancel failed: {error_msg}")
                    
                    elif order_risk_usd < primary_risk - relative_threshold:
                        # Lower risk - keep it (good for the account)
                        orders_kept_lower += 1
                        print(f"    └─ ✅ KEEPING: {order.symbol} (#{order.ticket}) - Lower risk than allowed")
                        print(f"       Risk: ${order_risk_usd:.2f} < Allowed: ${primary_risk:.2f} (Δ: ${primary_risk - order_risk_usd:.2f})")
                    
                    else:
                        # Within tolerance - keep it
                        orders_kept_in_range += 1
                        print(f"    └─ ✅ KEEPING: {order.symbol} (#{order.ticket}) - Risk within tolerance")
                        print(f"       Risk: ${order_risk_usd:.2f} vs Allowed: ${primary_risk:.2f} (Δ: ${abs(risk_difference):.2f})")
                else:
                    print(f"    └─ ⚠️  Could not calc risk for #{order.ticket}")

        # Broker final summary
        if orders_checked > 0:
            print(f"  └─ 📊 Audit Results:")
            print(f"       • Orders checked: {orders_checked}")
            if orders_kept_lower > 0:
                print(f"       • Kept (lower risk): {orders_kept_lower}")
            if orders_kept_in_range > 0:
                print(f"       • Kept (in tolerance): {orders_kept_in_range}")
            if orders_removed > 0:
                print(f"       • Removed (too high): {orders_removed}")
            else:
                print(f"       ✅ No orders needed removal")
        else:
            print(f"  └─ 🔘 No pending limit orders found.")

        mt5.shutdown()

    print(f"\n{'='*10} 🏁 RISK AUDIT COMPLETE {'='*10}\n")
    return True
   
def cleanup_history_duplicates():
    """
    Scans history for the last 48 hours. If a position was closed, 
    any pending limit orders with the same first 3 digits in the price 
    are cancelled to prevent re-entry.
    """
    from datetime import datetime, timedelta
    print(f"\n{'='*10} 📜 HISTORY AUDIT: PREVENTING RE-ENTRY {'='*10}")

    for user_brokerid, broker_cfg in usersdictionary.items():
        print(f" [{user_brokerid}] 🔍 Checking 48h history for duplicates...")
        
        # --- MT5 INITIALIZATION ---
        mt5.shutdown() 
        login_id = int(broker_cfg['LOGIN_ID'])
        
        if not mt5.initialize(path=broker_cfg["TERMINAL_PATH"], timeout=180000):
            continue

        if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
            mt5.shutdown()
            continue

        # 1. Define the 48-hour window
        from_date = datetime.now() - timedelta(hours=48)
        to_date = datetime.now()

        # 2. Get Closed Positions (Deals)
        history_deals = mt5.history_deals_get(from_date, to_date)
        if history_deals is None:
            print(f"  └─ ⚠️ Could not access history for {login_id}")
            mt5.shutdown()
            continue

        # 3. Create a set of "Used Price Prefixes"
        # We store: (symbol, price_prefix)
        used_entries = set()
        for deal in history_deals:
            # Only look at actual trades (buy/sell) that were closed
            if deal.entry in [mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_INOUT]:
                # Extract first 3 significant digits of the price
                # We remove the decimal to handle 0.856 and 1901 uniformly
                clean_price = str(deal.price).replace('.', '')[:3]
                used_entries.add((deal.symbol, clean_price))

        if not used_entries:
            print(f"  └─ ✅ No closed orders found in last 48h.")
            mt5.shutdown()
            continue

        # 4. Check Current Pending Orders
        pending_orders = mt5.orders_get()
        removed_count = 0

        if pending_orders:
            for order in pending_orders:
                # Only target limit orders
                if order.type in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT]:
                    order_price_prefix = str(order.price_open).replace('.', '')[:3]
                    
                    # If this symbol + price prefix exists in history, kill the order
                    if (order.symbol, order_price_prefix) in used_entries:
                        print(f"  └─ 🚫 DUPLICATE FOUND: {order.symbol} at {order.price_open}")
                        print(f"     Match found in history (Prefix: {order_price_prefix}). Cancelling...")
                        
                        cancel_request = {
                            "action": mt5.TRADE_ACTION_REMOVE,
                            "order": order.ticket
                        }
                        res = mt5.order_send(cancel_request)
                        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                            removed_count += 1
                        else:
                            print(f"     ❌ Failed to cancel #{order.ticket}: {res.comment if res else 'No response'}")

        print(f"  └─ 📊 Cleanup Result: {removed_count} duplicate limit orders removed.")
        mt5.shutdown()

    print(f"\n{'='*10} 🏁 HISTORY AUDIT COMPLETE {'='*10}\n")
    return True

def limit_orders_reward_correction():
    """
    Function: Checks live pending limit orders and adjusts their take profit levels
    based on the selected risk-reward ratio from accountmanagement.json.
    Only executes if risk_reward_correction setting is True.
    """
    print(f"\n{'='*10} 📐 RISK-REWARD CORRECTION: PENDING ORDERS {'='*10}")

    # --- DATA INITIALIZATION ---
    try:
        if not os.path.exists(NORMALIZE_SYMBOLS_PATH):
            print(" [!] CRITICAL ERROR: Normalization map path missing.")
            return False
        with open(NORMALIZE_SYMBOLS_PATH, 'r') as f:
            norm_map = json.load(f)
    except Exception as e:
        print(f" [!] CRITICAL ERROR: Normalization map load failed: {e}")
        return False

    for user_brokerid, broker_cfg in usersdictionary.items():
        print(f" [{user_brokerid}] 🔍 Checking risk-reward configurations...")
        
        inv_root = Path(INV_PATH) / user_brokerid
        acc_mgmt_path = inv_root / "accountmanagement.json"

        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️  Account config missing. Skipping.")
            continue

        # --- LOAD CONFIG AND CHECK SETTINGS ---
        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Check if risk_reward_correction is enabled
            settings = config.get("settings", {})
            if not settings.get("risk_reward_correction", False):
                print(f"  └─ ⏭️  Risk-reward correction disabled in settings. Skipping.")
                continue
            
            # Get selected risk-reward ratios
            selected_rr = config.get("selected_risk_reward", [2])
            if not selected_rr:
                print(f"  └─ ⚠️  No risk-reward ratios selected. Using default [2]")
                selected_rr = [2]
            
            # Use the first ratio in the list (typically the preferred one)
            target_rr_ratio = float(selected_rr[0])
            print(f"  └─ ✅ Target R:R Ratio: 1:{target_rr_ratio}")
            
            # Get risk management mapping for balance-based risk
            risk_map = config.get("account_balance_default_risk_management", {})
            
        except Exception as e:
            print(f"  └─ ❌ Failed to read config: {e}")
            continue

        # --- MT5 INITIALIZATION ---
        mt5.shutdown() 
        login_id = int(broker_cfg['LOGIN_ID'])
        mt5_path = broker_cfg["TERMINAL_PATH"]
        
        if not mt5.initialize(path=mt5_path, timeout=180000):
            print(f"  └─ ❌ MT5 Init failed for {login_id}")
            continue

        if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
            print(f"  └─ ❌ Login failed for {login_id}")
            mt5.shutdown()
            continue
        
        acc_info = mt5.account_info()
        balance = acc_info.balance

        # --- DETERMINE PRIMARY RISK VALUE BASED ON BALANCE ---
        primary_risk = None
        for range_str, r_val in risk_map.items():
            try:
                raw_range = range_str.split("_")[0]
                low, high = map(float, raw_range.split("-"))
                if low <= balance <= high:
                    primary_risk = float(r_val)
                    break
            except Exception as e:
                print(f"  └─ ⚠️  Error parsing range '{range_str}': {e}")
                continue

        if primary_risk is None:
            print(f"  └─ ⚠️  No risk mapping for balance ${balance:,.2f}")
            mt5.shutdown()
            continue

        print(f"  └─ 💰 Balance: ${balance:,.2f} | Base Risk: ${primary_risk:.2f} | Target R:R: 1:{target_rr_ratio}")

        # --- CHECK AND ADJUST PENDING LIMIT ORDERS ---
        pending_orders = mt5.orders_get()
        orders_checked = 0
        orders_adjusted = 0
        orders_skipped = 0
        orders_error = 0

        if pending_orders:
            for order in pending_orders:
                if order.type not in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT]:
                    continue

                orders_checked += 1
                
                # Get symbol info for pip/point value and digit calculation
                symbol_info = mt5.symbol_info(order.symbol)
                if not symbol_info:
                    print(f"    └─ ⚠️  Cannot get symbol info for {order.symbol}")
                    continue

                # Determine order type for calculations
                is_buy = order.type == mt5.ORDER_TYPE_BUY_LIMIT
                calc_type = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
                
                # Calculate current risk (stop loss distance in money)
                if order.sl == 0:
                    print(f"    └─ ⚠️  Order #{order.ticket} has no SL set")
                    orders_skipped += 1
                    continue
                    
                sl_profit = mt5.order_calc_profit(calc_type, order.symbol, order.volume_initial, 
                                                  order.price_open, order.sl)
                
                if sl_profit is None:
                    print(f"    └─ ⚠️  Cannot calculate risk for order #{order.ticket}")
                    orders_skipped += 1
                    continue

                current_risk_usd = round(abs(sl_profit), 2)
                
                # Calculate required take profit based on risk and target R:R ratio
                # Target profit = risk * target_rr_ratio
                target_profit_usd = current_risk_usd * target_rr_ratio
                
                # Calculate the take profit price that would achieve this profit
                # For BUY orders: TP = Entry + (Profit / (Volume * Tick Value * Tick Size))
                # For SELL orders: TP = Entry - (Profit / (Volume * Tick Value * Tick Size))
                
                # Calculate price movement needed for target profit
                # Use tick value and tick size for more accurate calculation
                tick_value = symbol_info.trade_tick_value
                tick_size = symbol_info.trade_tick_size
                
                if tick_value > 0 and tick_size > 0:
                    # Calculate how many ticks we need to move to achieve target profit
                    # Profit = volume * ticks_moved * tick_value
                    # So ticks_moved = target_profit_usd / (volume * tick_value)
                    ticks_needed = target_profit_usd / (order.volume_initial * tick_value)
                    
                    # Convert ticks to price movement
                    price_move_needed = ticks_needed * tick_size
                    
                    # Round to symbol digits
                    digits = symbol_info.digits
                    price_move_needed = round(price_move_needed, digits)
                    
                    # Calculate new take profit price
                    if is_buy:
                        new_tp = round(order.price_open + price_move_needed, digits)
                    else:
                        new_tp = round(order.price_open - price_move_needed, digits)
                    
                    # Check if current TP is significantly different from calculated TP
                    # Use a small threshold (e.g., 5% of the move or 2 pips, whichever is larger)
                    current_move = abs(order.tp - order.price_open) if order.tp != 0 else 0
                    target_move = abs(new_tp - order.price_open)
                    
                    # Calculate threshold (10% of target move or 2 pips, whichever is larger)
                    pip_threshold = max(target_move * 0.1, symbol_info.point * 20)
                    
                    if order.tp == 0:
                        print(f"    └─ 📝 Order #{order.ticket} ({order.symbol}) - No TP set")
                        print(f"       Risk: ${current_risk_usd:.2f} | Target Profit: ${target_profit_usd:.2f}")
                        print(f"       Setting TP to {new_tp:.{digits}f}")
                        should_adjust = True
                    elif abs(current_move - target_move) > pip_threshold:
                        print(f"    └─ 📐 Order #{order.ticket} ({order.symbol}) - TP needs adjustment")
                        print(f"       Current TP: {order.tp:.{digits}f} (Move: {current_move:.{digits}f})")
                        print(f"       Target TP:  {new_tp:.{digits}f} (Move: {target_move:.{digits}f})")
                        print(f"       Risk: ${current_risk_usd:.2f} | Target Profit: ${target_profit_usd:.2f}")
                        should_adjust = True
                    else:
                        print(f"    └─ ✅ Order #{order.ticket} ({order.symbol}) - TP already correct")
                        print(f"       TP: {order.tp:.{digits}f} | Risk: ${current_risk_usd:.2f}")
                        orders_skipped += 1
                        continue
                    
                    if should_adjust:
                        # Prepare modification request
                        modify_request = {
                            "action": mt5.TRADE_ACTION_MODIFY,
                            "order": order.ticket,
                            "price": order.price_open,  # Keep original entry price
                            "sl": order.sl,  # Keep original stop loss
                            "tp": new_tp,  # New take profit
                        }
                        
                        # Send modification
                        result = mt5.order_send(modify_request)
                        
                        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                            orders_adjusted += 1
                            print(f"       ✅ TP adjusted successfully")
                        else:
                            orders_error += 1
                            error_msg = result.comment if result else f"Error code: {result.retcode if result else 'Unknown'}"
                            print(f"       ❌ Modification failed: {error_msg}")
                else:
                    print(f"    └─ ⚠️  Invalid tick values for {order.symbol}")
                    # Fallback method using profit calculation for a small price movement
                    try:
                        # Calculate point value by testing a small price movement
                        test_move = symbol_info.point * 10  # Test 10 points
                        if is_buy:
                            test_price = order.price_open + test_move
                        else:
                            test_price = order.price_open - test_move
                            
                        test_profit = mt5.order_calc_profit(calc_type, order.symbol, order.volume_initial, 
                                                            order.price_open, test_price)
                        
                        if test_profit and test_profit != 0:
                            # Calculate point value
                            point_value = abs(test_profit) / 10  # Per point value
                            
                            # Calculate price movement needed
                            price_move_needed = target_profit_usd / point_value * symbol_info.point
                            
                            digits = symbol_info.digits
                            price_move_needed = round(price_move_needed, digits)
                            
                            if is_buy:
                                new_tp = round(order.price_open + price_move_needed, digits)
                            else:
                                new_tp = round(order.price_open - price_move_needed, digits)
                            
                            print(f"    └─ ⚠️  Using fallback calculation for {order.symbol}")
                            
                            # Apply the modification (simplified - you can add the same logic as above)
                            if order.tp == 0 or abs(order.tp - new_tp) > symbol_info.point * 20:
                                modify_request = {
                                    "action": mt5.TRADE_ACTION_MODIFY,
                                    "order": order.ticket,
                                    "price": order.price_open,
                                    "sl": order.sl,
                                    "tp": new_tp,
                                }
                                
                                result = mt5.order_send(modify_request)
                                
                                if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                                    orders_adjusted += 1
                                    print(f"       ✅ TP adjusted using fallback method")
                                else:
                                    orders_error += 1
                                    print(f"       ❌ Fallback modification failed")
                            else:
                                orders_skipped += 1
                                print(f"       ✅ TP already correct (fallback check)")
                        else:
                            orders_skipped += 1
                            print(f"    └─ ⚠️  Cannot calculate using fallback method for {order.symbol}")
                    except Exception as e:
                        orders_error += 1
                        print(f"    └─ ❌ Fallback calculation error: {e}")

        # --- BROKER SUMMARY ---
        if orders_checked > 0:
            print(f"  └─ 📊 Risk-Reward Correction Results:")
            print(f"       • Orders checked: {orders_checked}")
            print(f"       • Orders adjusted: {orders_adjusted}")
            print(f"       • Orders skipped (already correct): {orders_skipped}")
            if orders_error > 0:
                print(f"       • Errors: {orders_error}")
            else:
                print(f"       ✅ All adjustments completed successfully")
        else:
            print(f"  └─ 🔘 No pending limit orders found.")

        mt5.shutdown()

    print(f"\n{'='*10} 🏁 RISK-REWARD CORRECTION COMPLETE {'='*10}\n")
    return True

def place_magnetic_orders():
    """
    Places magnetic orders by using stop losses of existing orders as entry points for new orders.
    Creates a chain of orders up to per_symbol_magnet_orders limit.
    
    For each order, it identifies the stop loss and places opposite type orders at that level:
    - If stop loss is below current price (for buy orders) -> place SELL_STOP and BUY_LIMIT at that level
    - If stop loss is above current price (for sell orders) -> place BUY_STOP and SELL_LIMIT at that level
    
    When adjust_magnet_orders_to_respect_current_price is True and price is not at magnetic level,
    it will place immediate market orders at current price and use those as new magnetic levels.
    """
    print("\n" + "="*80)
    print("🧲 MAGNETIC ORDERS ENGINE: CREATING ORDER CHAINS")
    print("="*80)
    
    # --- SUB-FUNCTION 1: LOAD CONFIGURATION ---
    def load_normalization_map():
        try:
            if not os.path.exists(NORMALIZE_SYMBOLS_PATH):
                print(f"  [!] Normalization map not found at: {NORMALIZE_SYMBOLS_PATH}")
                return {}
            with open(NORMALIZE_SYMBOLS_PATH, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"  ❌ CRITICAL ERROR: Could not load normalization map: {e}")
            return None

    # --- SUB-FUNCTION 2: GET OPPOSITE ORDER TYPES ---
    def get_magnetic_order_types(direction):
        """
        Returns the pair of opposite orders to place at a magnetic level.
        When price reaches a stop loss, we place both a stop and limit order.
        """
        if direction == "UP":  # Price expected to go up from this level
            return [
                (mt5.ORDER_TYPE_BUY_LIMIT, "BUY LIMIT"),   # Buy if price pulls back (below current)
                (mt5.ORDER_TYPE_SELL_STOP, "SELL STOP")    # Sell if price breaks up (above current)
            ]
        else:  # Price expected to go down from this level
            return [
                (mt5.ORDER_TYPE_SELL_LIMIT, "SELL LIMIT"), # Sell if price rallies (above current)
                (mt5.ORDER_TYPE_BUY_STOP, "BUY STOP")      # Buy if price breaks down (below current)
            ]

    # --- SUB-FUNCTION 3: CALCULATE MINIMUM STOP DISTANCE ---
    def get_min_stop_distance(symbol_info):
        """Get minimum allowed stop distance for the symbol"""
        # Try to get from symbol info
        if hasattr(symbol_info, 'trade_stops_level'):
            return symbol_info.trade_stops_level * symbol_info.point
        
        # Default to 20 points if not available
        return 20 * symbol_info.point

    # --- SUB-FUNCTION 4: CALCULATE ORDER PRICES ---
    def calculate_order_prices(entry_price, direction, symbol_info, risk_reward=2.0):
        """
        Calculates SL and TP for magnetic orders based on direction with broker constraints.
        """
        # Get minimum stop distance
        min_stop_distance = get_min_stop_distance(symbol_info)
        
        # Calculate base risk distance (use larger of ATR or min distance * 3)
        atr = calculate_atr(symbol_info.name)
        risk_distance = max(atr if atr and atr > 0 else min_stop_distance * 5, min_stop_distance * 3)
        
        # Round to symbol digits
        point = symbol_info.point
        digits = symbol_info.digits
        
        prices = {}
        
        if direction == "UP":  # For BUY_LIMIT (expecting price up)
            # BUY LIMIT: SL below entry, TP above entry
            buy_sl = entry_price - risk_distance
            buy_tp = entry_price + (risk_distance * risk_reward)
            
            # SELL STOP: SL above entry, TP below entry
            sell_sl = entry_price + risk_distance
            sell_tp = entry_price - (risk_distance * risk_reward)
            
            # Round and validate
            prices['buy_limit'] = {
                'sl': round(buy_sl, digits),
                'tp': round(buy_tp, digits)
            }
            prices['sell_stop'] = {
                'sl': round(sell_sl, digits),
                'tp': round(sell_tp, digits)
            }
            
        else:  # For SELL_LIMIT (expecting price down)
            # SELL LIMIT: SL above entry, TP below entry
            sell_sl = entry_price + risk_distance
            sell_tp = entry_price - (risk_distance * risk_reward)
            
            # BUY STOP: SL below entry, TP above entry
            buy_sl = entry_price - risk_distance
            buy_tp = entry_price + (risk_distance * risk_reward)
            
            # Round and validate
            prices['sell_limit'] = {
                'sl': round(sell_sl, digits),
                'tp': round(sell_tp, digits)
            }
            prices['buy_stop'] = {
                'sl': round(buy_sl, digits),
                'tp': round(buy_tp, digits)
            }
        
        return prices

    # --- SUB-FUNCTION 5: VALIDATE PENDING ORDER PRICE ---
    def validate_pending_order_price(order_type, entry_price, current_price, symbol_info):
        """
        Validate if the pending order price is valid relative to current market price
        """
        point = symbol_info.point
        min_distance = get_min_stop_distance(symbol_info)
        
        # Check based on order type
        if order_type == mt5.ORDER_TYPE_BUY_STOP:
            # BUY STOP must be above current price
            if entry_price <= current_price + point:
                return False, f"BUY STOP price ({entry_price}) must be ABOVE current price ({current_price})"
        
        elif order_type == mt5.ORDER_TYPE_SELL_STOP:
            # SELL STOP must be below current price
            if entry_price >= current_price - point:
                return False, f"SELL STOP price ({entry_price}) must be BELOW current price ({current_price})"
        
        elif order_type == mt5.ORDER_TYPE_BUY_LIMIT:
            # BUY LIMIT must be below current price
            if entry_price >= current_price - point:
                return False, f"BUY LIMIT price ({entry_price}) must be BELOW current price ({current_price})"
        
        elif order_type == mt5.ORDER_TYPE_SELL_LIMIT:
            # SELL LIMIT must be above current price
            if entry_price <= current_price + point:
                return False, f"SELL LIMIT price ({entry_price}) must be ABOVE current price ({current_price})"
        
        # Check minimum distance from current price
        price_distance = abs(entry_price - current_price)
        if price_distance < min_distance:
            return False, f"Entry price too close to current price (min distance: {min_distance/point:.0f} points)"
        
        return True, "Valid"

    # --- SUB-FUNCTION 6: VALIDATE STOP LEVELS ---
    def validate_stop_levels(entry_price, sl, tp, order_type, symbol_info):
        """
        Validate if stop levels meet broker requirements
        """
        min_distance = get_min_stop_distance(symbol_info)
        point = symbol_info.point
        
        # Check if SL and TP are valid
        if sl <= 0 or tp <= 0:
            return False, "SL/TP must be positive"
        
        # Check distance from entry
        sl_distance = abs(entry_price - sl)
        tp_distance = abs(entry_price - tp)
        
        if sl_distance < min_distance:
            return False, f"SL too close to entry (distance: {sl_distance/point:.0f} points, min: {min_distance/point:.0f})"
        
        if tp_distance < min_distance:
            return False, f"TP too close to entry (distance: {tp_distance/point:.0f} points, min: {min_distance/point:.0f})"
        
        # Check SL-TP distance
        if abs(tp - sl) < min_distance * 2:
            return False, "SL and TP too close to each other"
        
        return True, "Valid"

    # --- SUB-FUNCTION 7: CALCULATE ATR ---
    def calculate_atr(symbol, period=14):
        """Calculate ATR for risk management"""
        try:
            rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_H1, 0, period + 10)
            if rates is None or len(rates) < period + 1:
                return None
            
            tr_list = []
            for i in range(1, len(rates)):
                high = rates[i]['high']
                low = rates[i]['low']
                prev_close = rates[i-1]['close']
                
                tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
                tr_list.append(tr)
            
            return sum(tr_list[-period:]) / period if tr_list else None
        except Exception as e:
            return None

    # --- SUB-FUNCTION 8: CHECK ORDER EXISTS ---
    def order_exists_at_price(symbol, price, order_type, existing_orders):
        """Check if an order already exists at given price"""
        if not existing_orders:
            return None
            
        for order in existing_orders:
            try:
                if (order.symbol == symbol and 
                    order.type == order_type and
                    abs(order.price_open - price) < 0.0001 * price):  # 0.01% tolerance
                    return order.ticket
            except AttributeError:
                continue
        return None

    # --- SUB-FUNCTION 9: GET EXISTING STOP LOSS LEVELS ---
    def get_stop_loss_levels(positions, orders, current_prices):
        """Extract unique stop loss levels from existing positions and orders"""
        sl_levels = []
        seen_levels = set()  # To avoid duplicates
        
        # Process positions (open trades)
        if positions:
            for pos in positions:
                if pos.sl and pos.sl > 0:
                    # Get current price for this symbol
                    current_price = current_prices.get(pos.symbol, {}).get('mid', 0)
                    
                    # Determine direction based on SL position relative to current price
                    if pos.sl < current_price:
                        direction = "DOWN"  # SL is below current price
                    else:
                        direction = "UP"    # SL is above current price
                    
                    level_key = f"{pos.symbol}_{pos.sl}"
                    if level_key not in seen_levels:
                        seen_levels.add(level_key)
                        sl_levels.append({
                            'price': pos.sl,
                            'direction': direction,
                            'symbol': pos.symbol,
                            'volume': pos.volume,
                            'magic': pos.magic,
                            'source': 'position'
                        })
        
        # Process pending orders
        if orders:
            for order in orders:
                if order.sl and order.sl > 0:
                    # Get current price for this symbol
                    current_price = current_prices.get(order.symbol, {}).get('mid', 0)
                    
                    # Determine direction based on SL position relative to current price
                    if order.sl < current_price:
                        direction = "DOWN"  # SL is below current price
                    else:
                        direction = "UP"    # SL is above current price
                    
                    level_key = f"{order.symbol}_{order.sl}"
                    if level_key not in seen_levels:
                        seen_levels.add(level_key)
                        sl_levels.append({
                            'price': order.sl,
                            'direction': direction,
                            'symbol': order.symbol,
                            'volume': order.volume_initial,
                            'magic': order.magic,
                            'source': 'order'
                        })
        
        return sl_levels

    # --- SUB-FUNCTION 10: ADJUST PRICE TO BROKER REQUIREMENTS ---
    def adjust_to_broker_rules(price, symbol_info):
        """Adjust price to meet broker requirements (step size, etc.)"""
        if hasattr(symbol_info, 'trade_tick_size') and symbol_info.trade_tick_size > 0:
            tick_size = symbol_info.trade_tick_size
            price = round(price / tick_size) * tick_size
        
        return round(price, symbol_info.digits)

    # --- NEW SUB-FUNCTION 11: GET SUPPORTED FILLING MODE (FIXED) ---
    def get_supported_filling_mode(symbol_info):
        """
        Determine which filling mode is supported by the broker for the symbol
        Using the same logic as place_instant_order function
        """
        # Check if symbol_info has filling_mode attribute
        if hasattr(symbol_info, 'filling_mode'):
            mode = symbol_info.filling_mode
            
            # FOK if mode & 1
            if mode & 1:
                return mt5.ORDER_FILLING_FOK
            # IOC if mode & 2
            elif mode & 2:
                return mt5.ORDER_FILLING_IOC
            # Default to RETURN
            else:
                return mt5.ORDER_FILLING_RETURN
        
        # Default to RETURN if we can't determine
        return mt5.ORDER_FILLING_RETURN

    # --- NEW SUB-FUNCTION 12: PLACE IMMEDIATE MARKET ORDER (FIXED) ---
    def place_immediate_market_order(symbol, direction, volume, magic, comment, symbol_info, current_price):
        """
        Places an immediate market order when price is not at magnetic level.
        Returns the executed position if successful.
        Uses the same filling mode logic as place_instant_order
        """
        # Determine order type based on direction
        if direction == "UP":
            order_type = mt5.ORDER_TYPE_BUY
            order_type_name = "BUY"
            entry_price = symbol_info.ask
        else:
            order_type = mt5.ORDER_TYPE_SELL
            order_type_name = "SELL"
            entry_price = symbol_info.bid
        
        # Calculate risk distance based on ATR
        atr = calculate_atr(symbol)
        min_stop = get_min_stop_distance(symbol_info)
        risk_distance = atr if atr and atr > 0 else min_stop * 5
        
        # Ensure risk distance meets minimum requirements
        if risk_distance < min_stop * 2:
            risk_distance = min_stop * 3
        
        # Calculate SL and TP (using 1:2 risk-reward)
        if direction == "UP":
            sl = entry_price - risk_distance
            tp = entry_price + (risk_distance * 2)
        else:
            sl = entry_price + risk_distance
            tp = entry_price - (risk_distance * 2)
        
        # Adjust to broker rules
        sl = adjust_to_broker_rules(sl, symbol_info)
        tp = adjust_to_broker_rules(tp, symbol_info)
        
        # Validate stop levels
        is_valid, validation_msg = validate_stop_levels(entry_price, sl, tp, order_type, symbol_info)
        if not is_valid:
            print(f"      ❌ Market order validation failed: {validation_msg}")
            return None
        
        # Get supported filling mode (using the fixed function)
        filling_mode = get_supported_filling_mode(symbol_info)
        
        # Prepare market order request
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": volume,
            "type": order_type,
            "price": entry_price,
            "sl": sl,
            "tp": tp,
            "deviation": 20,
            "magic": magic,
            "comment": f"IMMEDIATE_MAGNET_{comment}",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": filling_mode,
        }
        
        # Map filling mode to string for display
        filling_mode_str = {
            mt5.ORDER_FILLING_FOK: "FOK",
            mt5.ORDER_FILLING_IOC: "IOC",
            mt5.ORDER_FILLING_RETURN: "RETURN"
        }.get(filling_mode, str(filling_mode))
        
        print(f"      🚀 Placing IMMEDIATE {order_type_name} market order for {symbol}")
        print(f"         Entry: {entry_price:.{symbol_info.digits}f}")
        print(f"         SL: {sl:.{symbol_info.digits}f} | TP: {tp:.{symbol_info.digits}f}")
        print(f"         Filling Mode: {filling_mode_str}")
        
        # Send order
        res = mt5.order_send(request)
        
        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
            print(f"         ✅ SUCCESS: Market order placed - Ticket: {res.order}")
            # Get the executed position
            position = mt5.positions_get(ticket=res.order)
            if position and len(position) > 0:
                return position[0]
        else:
            error_code = res.retcode if res else "N/A"
            error_comment = res.comment if res and hasattr(res, 'comment') else "No response"
            error_descr = mt5.last_error() if not res else ""
            
            print(f"         ❌ FAIL: Market order error {error_code} - {error_comment}")
            if error_descr:
                print(f"         Details: {error_descr}")
        
        return None

    # --- NEW SUB-FUNCTION 13: CHECK IF PRICE IS AT LEVEL ---
    def is_price_at_level(level_price, current_price, symbol_info, tolerance_pips=2):
        """
        Check if current price is at the magnetic level within tolerance
        """
        point = symbol_info.point
        min_distance = get_min_stop_distance(symbol_info)
        
        # Use tolerance of 2 pips or minimum stop distance, whichever is smaller
        tolerance = min(point * 10 * tolerance_pips, min_distance / 2)
        
        return abs(current_price - level_price) <= tolerance

    # --- MAIN EXECUTION FLOW ---
    
    # Load normalization map
    norm_map = load_normalization_map()
    if norm_map is None:
        print("  ❌ Failed to load normalization map. Exiting.")
        return False

    total_investors = len(usersdictionary)
    processed = 0
    total_magnetic_orders = 0
    total_immediate_orders = 0
    magnetic_orders_by_symbol = {}
    immediate_orders_by_symbol = {}

    for user_brokerid, broker_cfg in usersdictionary.items():
        processed += 1
        print(f"\n{'-'*80}")
        print(f"📋 INVESTOR [{processed}/{total_investors}]: {user_brokerid}")
        print(f"{'-'*80}")

        inv_root = Path(INV_PATH) / user_brokerid
        acc_mgmt_path = inv_root / "accountmanagement.json"

        if not acc_mgmt_path.exists():
            print(f"  ⚠️  Account management file not found - skipping")
            continue

        try:
            # Load configuration
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)

            # Get magnetic orders settings
            settings = config.get("settings", {})
            if not settings.get("activate_magnet_orders", False):
                print(f"  ℹ️  Magnetic orders are disabled for this investor")
                continue

            max_magnet_orders = settings.get("per_symbol_magnet_orders", 10)
            adjust_to_current = settings.get("adjust_magnet_orders_to_respect_current_price", True)
            
            print(f"  ✅ Magnetic orders ENABLED (Max {max_magnet_orders} per symbol)")
            print(f"  ✅ Adjust to current price: {'ENABLED' if adjust_to_current else 'DISABLED'}")

            # Initialize MT5 connection
            print(f"  🔌 Connecting to MT5 terminal...")
            mt5.shutdown()
            login_id = int(broker_cfg['LOGIN_ID'])
            mt5_path = broker_cfg["TERMINAL_PATH"]

            if not mt5.initialize(path=mt5_path, timeout=180000):
                error = mt5.last_error()
                print(f"  ❌ Failed to initialize MT5: {error}")
                continue

            # Login
            print(f"  🔑 Logging into account {login_id}...")
            if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
                error = mt5.last_error()
                print(f"  ❌ Login failed: {error}")
                continue
            print(f"  ✅ Successfully logged in")

            # Get account and terminal info
            acc_info = mt5.account_info()
            term_info = mt5.terminal_info()

            if not acc_info:
                print(f"  ❌ Failed to get account info")
                continue

            if not term_info.trade_allowed:
                print(f"  ⚠️  AutoTrading is DISABLED - Cannot place magnetic orders")
                continue

            print(f"\n  📊 Account Details:")
            print(f"    • Balance: ${acc_info.balance:,.2f}")
            print(f"    • Equity: ${acc_info.equity:,.2f}")
            print(f"    • AutoTrading: {'✅ ENABLED' if term_info.trade_allowed else '❌ DISABLED'}")

            # Get all positions and orders (convert tuples to lists)
            positions = list(mt5.positions_get() or [])
            pending_orders = list(mt5.orders_get() or [])
            
            print(f"  📊 Found {len(positions)} positions and {len(pending_orders)} pending orders")

            # Get current prices for all symbols
            symbols = set()
            for pos in positions:
                symbols.add(pos.symbol)
            for order in pending_orders:
                symbols.add(order.symbol)
            
            current_prices = {}
            for symbol in symbols:
                symbol_info = mt5.symbol_info(symbol)
                if symbol_info:
                    current_prices[symbol] = {
                        'bid': symbol_info.bid,
                        'ask': symbol_info.ask,
                        'mid': (symbol_info.bid + symbol_info.ask) / 2
                    }

            # Get stop loss levels to use as entry points
            sl_levels = get_stop_loss_levels(positions, pending_orders, current_prices)
            
            if not sl_levels:
                print(f"  ℹ️  No stop loss levels found to create magnetic orders")
                continue

            print(f"  🎯 Found {len(sl_levels)} stop loss levels to process")

            # Track orders per symbol
            symbol_order_count = {}
            investor_placed = 0
            investor_skipped = 0
            investor_failed = 0
            investor_immediate = 0

            # Process each stop loss level
            for sl_info in sl_levels:
                symbol = sl_info['symbol']
                
                # Initialize counter for this symbol
                if symbol not in symbol_order_count:
                    symbol_order_count[symbol] = 0
                    # Count existing magnetic orders for this symbol
                    for order in pending_orders:
                        if hasattr(order, 'comment') and order.comment and "MAGNET" in order.comment:
                            symbol_order_count[symbol] += 1

                # Check if we've reached the limit for this symbol
                if symbol_order_count[symbol] >= max_magnet_orders:
                    print(f"    ⏭️  SKIP {symbol}: Reached max magnetic orders ({max_magnet_orders})")
                    continue

                # Calculate how many more orders we can place for this symbol
                remaining_slots = max_magnet_orders - symbol_order_count[symbol]
                
                # Get symbol info and current price
                symbol_info = mt5.symbol_info(symbol)
                if not symbol_info:
                    print(f"    ⚠️  Cannot get symbol info for {symbol}")
                    continue
                
                current_price = current_prices.get(symbol, {}).get('mid', 0)
                if current_price == 0:
                    print(f"    ⚠️  Cannot get current price for {symbol}")
                    continue

                # Check if price is at the magnetic level
                price_at_level = is_price_at_level(sl_info['price'], current_price, symbol_info)
                
                # If adjust_to_current is True and price is NOT at level, place immediate market order
                if adjust_to_current and not price_at_level:
                    price_difference = abs(sl_info['price'] - current_price)
                    print(f"    📍 Price difference detected for {symbol}")
                    print(f"       Magnetic level: {sl_info['price']:.{symbol_info.digits}f}, Current: {current_price:.{symbol_info.digits}f}")
                    print(f"       Difference: {price_difference:.{symbol_info.digits}f}")
                    
                    # Place immediate market order
                    immediate_position = place_immediate_market_order(
                        symbol=symbol,
                        direction=sl_info['direction'],
                        volume=sl_info['volume'],
                        magic=sl_info['magic'],
                        comment=f"{sl_info['source']}_{symbol}",
                        symbol_info=symbol_info,
                        current_price=current_price
                    )
                    
                    if immediate_position:
                        investor_immediate += 1
                        
                        # Update global tracking
                        if symbol not in immediate_orders_by_symbol:
                            immediate_orders_by_symbol[symbol] = 0
                        immediate_orders_by_symbol[symbol] += 1
                        
                        # Use the immediate position's SL as new magnetic level
                        if immediate_position.sl and immediate_position.sl > 0:
                            print(f"       🔄 Using immediate order SL ({immediate_position.sl:.{symbol_info.digits}f}) as new magnetic level")
                            sl_info['price'] = immediate_position.sl
                            
                            # Re-determine direction based on new SL
                            if immediate_position.sl < current_price:
                                sl_info['direction'] = "DOWN"
                            else:
                                sl_info['direction'] = "UP"
                            
                            # Update volume to match immediate position
                            sl_info['volume'] = immediate_position.volume
                            
                            # Decrease remaining slots (immediate order counts toward limit)
                            remaining_slots -= 1
                            symbol_order_count[symbol] += 1
                            
                            print(f"       📊 Updated magnetic level: Price={sl_info['price']:.{symbol_info.digits}f}, Direction={sl_info['direction']}")
                            
                            # Refresh pending orders list to include new magnetic orders
                            pending_orders = list(mt5.orders_get() or [])
                        else:
                            print(f"       ⚠️  Immediate order has no SL, cannot create chain")
                            continue
                    else:
                        print(f"       ⚠️  Failed to place immediate order, proceeding with normal magnetic orders")
                        # Continue with normal magnetic order placement

                # Get magnetic order types based on direction
                order_types = get_magnetic_order_types(sl_info['direction'])
                
                # Calculate SL/TP levels for magnetic orders
                price_levels = calculate_order_prices(sl_info['price'], sl_info['direction'], symbol_info)

                # Place magnetic orders
                for order_type, type_name in order_types:
                    if remaining_slots <= 0:
                        break

                    # First validate if the entry price is valid for this order type
                    is_price_valid, price_msg = validate_pending_order_price(
                        order_type, sl_info['price'], current_price, symbol_info
                    )
                    
                    if not is_price_valid:
                        print(f"    ⏭️  SKIP: {type_name} - {price_msg}")
                        investor_skipped += 1
                        continue

                    # Check if order already exists at this level
                    existing_ticket = order_exists_at_price(symbol, sl_info['price'], order_type, pending_orders)
                    
                    if existing_ticket:
                        print(f"    ⏭️  SKIP: {type_name} already exists at {sl_info['price']} (Ticket: {existing_ticket})")
                        investor_skipped += 1
                        continue

                    # Determine SL and TP based on order type
                    try:
                        if order_type == mt5.ORDER_TYPE_BUY_LIMIT:
                            sl = price_levels['buy_limit']['sl']
                            tp = price_levels['buy_limit']['tp']
                        elif order_type == mt5.ORDER_TYPE_SELL_STOP:
                            sl = price_levels['sell_stop']['sl']
                            tp = price_levels['sell_stop']['tp']
                        elif order_type == mt5.ORDER_TYPE_SELL_LIMIT:
                            sl = price_levels['sell_limit']['sl']
                            tp = price_levels['sell_limit']['tp']
                        else:  # BUY_STOP
                            sl = price_levels['buy_stop']['sl']
                            tp = price_levels['buy_stop']['tp']
                    except KeyError:
                        print(f"    ❌ FAIL: Price levels not calculated correctly for {type_name}")
                        investor_failed += 1
                        continue

                    # Adjust prices to broker requirements
                    entry_price = adjust_to_broker_rules(sl_info['price'], symbol_info)
                    sl = adjust_to_broker_rules(sl, symbol_info)
                    tp = adjust_to_broker_rules(tp, symbol_info)

                    # Validate stop levels
                    is_valid, validation_msg = validate_stop_levels(entry_price, sl, tp, order_type, symbol_info)
                    
                    if not is_valid:
                        print(f"    ❌ FAIL: {type_name} - {validation_msg}")
                        print(f"       Entry: {entry_price}, SL: {sl}, TP: {tp}")
                        investor_failed += 1
                        continue

                    # Get supported filling mode for pending orders
                    filling_mode = get_supported_filling_mode(symbol_info)

                    # Prepare magnetic order request
                    request = {
                        "action": mt5.TRADE_ACTION_PENDING,
                        "symbol": symbol,
                        "volume": sl_info['volume'],
                        "type": order_type,
                        "price": entry_price,
                        "sl": sl,
                        "tp": tp,
                        "deviation": 20,
                        "magic": sl_info['magic'],
                        "comment": f"MAGNET_{sl_info['source']}_{symbol_order_count[symbol]}",
                        "type_time": mt5.ORDER_TIME_GTC,
                        "type_filling": filling_mode,
                    }

                    # Print order details
                    print(f"    🚀 Placing {type_name} magnetic order for {symbol}")
                    print(f"       Entry: {entry_price:.{symbol_info.digits}f}")
                    print(f"       SL: {sl:.{symbol_info.digits}f} | TP: {tp:.{symbol_info.digits}f}")
                    print(f"       Current Price: {current_price:.{symbol_info.digits}f} (Bid: {symbol_info.bid:.{symbol_info.digits}f}, Ask: {symbol_info.ask:.{symbol_info.digits}f})")
                    
                    # Send magnetic order
                    res = mt5.order_send(request)

                    if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                        print(f"       ✅ SUCCESS: Magnetic order placed - Ticket: {res.order}")
                        investor_placed += 1
                        symbol_order_count[symbol] += 1
                        remaining_slots -= 1
                        
                        # Add to global tracking
                        if symbol not in magnetic_orders_by_symbol:
                            magnetic_orders_by_symbol[symbol] = 0
                        magnetic_orders_by_symbol[symbol] += 1
                    else:
                        error_code = res.retcode if res else "N/A"
                        error_comment = res.comment if res and hasattr(res, 'comment') else "No response"
                        
                        print(f"       ❌ FAIL: Error {error_code}")
                        print(f"       Details: {error_comment}")
                        investor_failed += 1

            # Investor summary
            print(f"\n  📊 MAGNETIC ORDERS SUMMARY FOR {user_brokerid}:")
            print(f"    • Immediate Market Orders Placed: {investor_immediate}")
            print(f"    • Magnetic Pending Orders Placed: {investor_placed}")
            print(f"    • Magnetic Orders Skipped: {investor_skipped}")
            print(f"    • Magnetic Orders Failed: {investor_failed}")
            print(f"    • Total Orders Placed: {investor_immediate + investor_placed}")
            print(f"    • Orders Per Symbol: {symbol_order_count}")
            
            total_magnetic_orders += investor_placed
            total_immediate_orders += investor_immediate

        except json.JSONDecodeError as e:
            print(f"  ❌ Invalid JSON in account management file: {e}")
        except KeyError as e:
            print(f"  ❌ Missing required configuration key: {e}")
        except Exception as e:
            print(f"  💥 SYSTEM ERROR: {str(e)}")
            import traceback
            traceback.print_exc()
        finally:
            mt5.shutdown()

    # Final summary
    print("\n" + "="*80)
    print("📊 GLOBAL MAGNETIC ORDERS SUMMARY")
    print("="*80)
    print(f"Total Immediate Market Orders Placed: {total_immediate_orders}")
    print(f"Total Magnetic Pending Orders Placed: {total_magnetic_orders}")
    print(f"Total Combined Orders: {total_immediate_orders + total_magnetic_orders}")
    print(f"\nImmediate Orders By Symbol: {immediate_orders_by_symbol}")
    print(f"Magnetic Orders By Symbol: {magnetic_orders_by_symbol}")
    print("="*80)
    
    return True
    
def place_orders():
    sort_orders()
    deduplicate_orders()
    default_price_repair()
    filter_unauthorized_symbols()
    place_usd_orders()
    place_orders_hedging()
    check_limit_orders_risk()
    cleanup_history_duplicates()
    limit_orders_reward_correction()
    place_magnetic_orders()


if __name__ == "__main__":
   place_magnetic_orders()

