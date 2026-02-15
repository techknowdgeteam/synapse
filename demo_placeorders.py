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

        # Determine Primary Risk Value
        primary_risk = None
        for range_str, r_val in risk_map.items():
            try:
                raw_range = range_str.split("_")[0]
                low, high = map(float, raw_range.split("-"))
                if low <= balance <= high:
                    primary_risk = int(r_val)
                    break
            except: continue

        if primary_risk is None:
            print(f"  └─ ⚠️  No risk mapping for balance ${balance:,.2f}")
            mt5.shutdown()
            continue

        print(f"  └─ 💰 Balance: ${balance:,.2f} | Target Risk: ${primary_risk}")

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
                    
                    # Only remove if risk is significantly higher than allowed
                    if order_risk_usd - primary_risk > 1.0: 
                        print(f"    └─ 🗑️  PURGING: {order.symbol} (#{order.ticket}) - Risk too high")
                        print(f"       Risk: ${order_risk_usd} > Allowed: ${primary_risk}")
                        
                        cancel_request = {
                            "action": mt5.TRADE_ACTION_REMOVE,
                            "order": order.ticket
                        }
                        result = mt5.order_send(cancel_request)
                        
                        if result.retcode == mt5.TRADE_RETCODE_DONE:
                            orders_removed += 1
                        else:
                            print(f"       [!] Cancel failed: {result.comment}")
                    
                    elif order_risk_usd < primary_risk - 1.0:
                        # Lower risk - keep it (good for the account)
                        orders_kept_lower += 1
                        print(f"    └─ ✅ KEEPING: {order.symbol} (#{order.ticket}) - Lower risk than allowed")
                        print(f"       Risk: ${order_risk_usd} < Allowed: ${primary_risk}")
                    
                    else:
                        # Within tolerance - keep it
                        orders_kept_in_range += 1
                        print(f"    └─ ✅ KEEPING: {order.symbol} (#{order.ticket}) - Risk within tolerance")
                        print(f"       Risk: ${order_risk_usd} vs Allowed: ${primary_risk}")
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

def place_usd_orders_for_demo_accounts():
    """
    Process USD orders for DEMO accounts with enhanced visualization and logging.
    Maintains the exact demo account initialization logic while improving aesthetics.
    """
    # --- SUB-FUNCTION 1: DATA INITIALIZATION ---
    def load_normalization_map():
        """Load symbol normalization mapping with error handling"""
        try:
            if not os.path.exists(NORMALIZE_SYMBOLS_PATH):
                print("  ⚠️  No normalization map found - proceeding without symbol normalization")
                return {}
            with open(NORMALIZE_SYMBOLS_PATH, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"❌ CRITICAL ERROR: Could not load normalization map: {e}")
            return None

    # --- SUB-FUNCTION 2: RISK & FILE AGGREGATION ---
    def collect_entries(inv_root, risk_map, balance, pull_lower, selected_rr, norm_map):
        """Collect trading entries from risk folders (NO DEDUPLICATION)"""
        primary_risk = None
        print(f"  ⚙️  Determining primary risk for balance: ${balance:,.2f}")
        
        # Find primary risk level
        for range_str, r_val in risk_map.items():
            try:
                raw_range = range_str.split("_")[0]
                low, high = map(float, raw_range.split("-"))
                if low <= balance <= high:
                    primary_risk = int(r_val)
                    print(f"  ✅ Balance ${balance:,.2f} in range ${low:,.0f}-${high:,.0f} → Risk Level: {primary_risk}")
                    break
            except Exception as e:
                print(f"  ⚠️  Error parsing risk range '{range_str}': {e}")
                continue

        if primary_risk is None:
            print(f"  ❌ No matching risk range found for balance: ${balance:,.2f}")
            return None, []

        # Determine risk levels to scan
        risk_levels = [primary_risk]
        if pull_lower:
            start_lookback = max(1, primary_risk - 9)
            risk_levels = list(range(start_lookback, primary_risk + 1))
            print(f"  📊 Pull lower enabled, scanning risk levels: {risk_levels}")
        else:
            print(f"  📊 Scanning only primary risk level: {risk_levels}")

        all_entries = []  # Simple list, no deduplication
        target_rr_folder = f"risk_reward_{selected_rr}"
        
        # Scan each risk level
        for r_val in reversed(risk_levels):
            risk_folder_name = f"{r_val}usd_risk"
            risk_filename = f"{r_val}usd_risk.json"
            search_pattern = f"**/{target_rr_folder}/{risk_folder_name}/{risk_filename}"
            
            entries_found = 0
            for path in inv_root.rglob(search_pattern):
                if path.is_file():
                    try:
                        with open(path, 'r') as f:
                            data = json.load(f)
                            if isinstance(data, list):
                                entries_found += len(data)
                                # Add all entries without deduplication
                                all_entries.extend(data)
                    except json.JSONDecodeError as e:
                        print(f"      ❌ Risk Level {r_val}: Invalid JSON format - {e}")
                    except Exception as e:
                        print(f"      ❌ Risk Level {r_val}: Failed to read file - {e}")
            
            if entries_found > 0:
                print(f"    📁 Risk Level {r_val}: Found {entries_found} entries")
            else:
                print(f"    📁 Risk Level {r_val}: No entries found")
                
        print(f"  📈 TOTAL: {len(all_entries)} trading opportunities collected (no deduplication)")
        return risk_levels, all_entries

    # --- SUB-FUNCTION 3: BROKER CLEANUP ---
    def cleanup_unauthorized_orders(all_entries, norm_map):
        """Remove orders that don't match current trading opportunities"""
        print("  🧹 Checking for unauthorized orders...")
        try:
            current_orders = mt5.orders_get()
            if not current_orders:
                print("  ✅ No pending orders found")
                return
            
            authorized_tickets = set()
            for entry in all_entries:
                vol_key = next((k for k in entry.keys() if k.endswith("volume")), None)
                if not vol_key: continue
                
                e_symbol = get_normalized_symbol(entry["symbol"], norm_map)
                e_price = round(float(entry["entry"]), 5)
                e_vol = round(float(entry[vol_key]), 2)
                
                for order in current_orders:
                    if (order.symbol == e_symbol and 
                        round(order.price_open, 5) == e_price and 
                        round(order.volume_initial, 2) == e_vol):
                        authorized_tickets.add(order.ticket)
            
            deleted_count = 0
            for order in current_orders:
                if order.ticket not in authorized_tickets:
                    print(f"    🗑️  Removing unauthorized order - Ticket: {order.ticket} | Symbol: {order.symbol} | Price: {order.price_open}")
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

    # --- SUB-FUNCTION 4: ORDER EXECUTION ---
    def execute_missing_orders(all_entries, norm_map, default_magic, selected_rr, trade_allowed):
        """Place missing orders with comprehensive validation and detailed error mapping"""
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

                # Step 2: Select symbol in Market Watch (DEMO ACCOUNT LOGIC MAINTAINED)
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
                
                # Step 5: Get volume key
                vol_key = next((k for k in entry.keys() if k.endswith("volume")), None)
                if not vol_key:
                    print(f"      ❌ FAIL: {symbol_orig} - No volume field found in entry data")
                    failed += 1
                    continue
                
                # Step 6: Check for existing positions (DEMO ACCOUNT LOGIC MAINTAINED)
                existing_pos = mt5.positions_get(symbol=symbol) or []
                if existing_pos:
                    pos_type = "BUY" if existing_pos[0].type == mt5.POSITION_TYPE_BUY else "SELL"
                    print(f"      ⏭️  SKIP: {symbol} - Existing {pos_type} position detected (Volume: {existing_pos[0].volume})")
                    skipped += 1
                    continue
                
                # Step 7: Check for existing orders (DEMO ACCOUNT LOGIC MAINTAINED)
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

                # Step 8: Calculate and validate volume
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
                
                # Step 9: Validate prices
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
                
                # Step 10: Determine order type
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

                # Step 11: Prepare and send order (DEMO ACCOUNT LOGIC MAINTAINED)
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
                    # Detailed error mapping (copied from real account)
                    error_code = res.retcode if res else "N/A"
                    error_msg = res.comment if res and res.comment else "No response"
                    
                    # Comprehensive MT5 error code mapping
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
                traceback.print_exc()  # This will show the full stack trace for debugging
                failed += 1
                
        # Summary
        if total > 0:
            success_rate = (placed / total) * 100 if total > 0 else 0
            print(f"\n    📊 Execution Summary: ✅ {placed} placed | ❌ {failed} failed | ⏭️  {skipped} skipped | Success Rate: {success_rate:.1f}%")
        return placed, failed, skipped

    # --- MAIN EXECUTION FLOW ---
    print("\n" + "="*80)
    print("🎯 DEMO ACCOUNT USD ORDER PLACEMENT ENGINE")
    print("="*80)
    print("📌 Using DEMO account initialization logic")
    print("📌 Deduplication stage: REMOVED")
    print("📌 Comprehensive error mapping: ENABLED")
    print("="*80)
    
    # Load normalization map
    norm_map = load_normalization_map()
    if norm_map is None:
        return False

    total_investors = len(usersdictionary)
    processed = 0
    successful = 0

    # Process each investor
    for user_brokerid, broker_cfg in usersdictionary.items():
        processed += 1
        print(f"\n{'-'*80}")
        print(f"📋 DEMO INVESTOR [{processed}/{total_investors}]: {user_brokerid}")
        print(f"{'-'*80}")
        
        inv_root = Path(INV_PATH) / user_brokerid
        acc_mgmt_path = inv_root / "accountmanagement.json"
        
        if not acc_mgmt_path.exists():
            print(f"  ⚠️  Account management file not found - skipping")
            continue

        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # --- DEMO ACCOUNT INITIALIZATION (PRESERVED) ---
            print(f"  🔌 Initializing DEMO account connection...")
            mt5.shutdown()
            
            login_id = int(broker_cfg['LOGIN_ID'])
            mt5_path = broker_cfg["TERMINAL_PATH"]
            
            print(f"    • Terminal Path: {mt5_path}")
            print(f"    • Login ID: {login_id}")
            
            # Initialize MT5
            if not mt5.initialize(path=mt5_path, timeout=180000):
                error = mt5.last_error()
                print(f"  ❌ Failed to initialize MT5: {error}")
                continue

            # Check login status
            acc = mt5.account_info()
            if acc is None or acc.login != login_id:
                print(f"    🔑 Logging into DEMO account...")
                if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
                    error = mt5.last_error()
                    print(f"  ❌ DEMO login failed: {error}")
                    continue
                print(f"    ✅ Successfully logged into DEMO account")
            else:
                print(f"    ✅ Already logged into DEMO account")
            # --- END PRESERVED DEMO INITIALIZATION ---

            # Extract settings
            settings = config.get("settings", {})
            pull_lower = settings.get("pull_orders_from_lower", False)
            selected_rr = config.get("selected_risk_reward", [None])[0]
            risk_map = config.get("account_balance_default_risk_management", {})
            default_magic = config.get("magic_number", 123456)
            
            # Get account info
            acc_info = mt5.account_info()
            term_info = mt5.terminal_info()
            
            if not acc_info:
                print(f"  ❌ Failed to get account info")
                continue
                
            print(f"\n  📊 DEMO Account Details:")
            print(f"    • Balance: ${acc_info.balance:,.2f}")
            print(f"    • Equity: ${acc_info.equity:,.2f}")
            print(f"    • Free Margin: ${acc_info.margin_free:,.2f}")
            print(f"    • Margin Level: {acc_info.margin_level:.2f}%" if acc_info.margin_level else "    • Margin Level: N/A")
            print(f"    • AutoTrading: {'✅ ENABLED' if term_info.trade_allowed else '❌ DISABLED'}")
            print(f"    • Risk/Reward: {selected_rr}")
            print(f"    • Account Type: DEMO")

            # Stage 1: Risk determination and file loading (NO DEDUPLICATION)
            print(f"\n  📁 STAGE 1: Scanning for trading opportunities")
            risk_lvls, all_entries = collect_entries(
                inv_root, risk_map, acc_info.balance, pull_lower, selected_rr, norm_map
            )
            
            if all_entries:
                # Stage 2: Cleanup
                print(f"\n  🧹 STAGE 2: Order cleanup")
                cleanup_unauthorized_orders(all_entries, norm_map)
                
                # Stage 3: Execution
                print(f"\n  🚀 STAGE 3: Order placement")
                p, f, s = execute_missing_orders(
                    all_entries, norm_map, default_magic, selected_rr, term_info.trade_allowed
                )
                
                if p > 0 or f > 0 or s > 0:
                    successful += 1
                    
                print(f"\n  📈 DEMO INVESTOR SUMMARY: {user_brokerid}")
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
        
    # Shutdown MT5
    mt5.shutdown()
    
    print("\n" + "="*80)
    print("✅ DEMO ORDER PLACEMENT COMPLETED")
    print(f"   Processed: {processed}/{total_investors} DEMO investors")
    print(f"   Successful: {successful} DEMO investors")
    print("="*80)
    
    return True

def place_orders():
    sort_orders()
    deduplicate_orders()
    default_price_repair()
    place_usd_orders_for_demo_accounts()
    check_limit_orders_risk()

if __name__ == "__main__":
   place_orders()

