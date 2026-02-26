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
import math

INVESTOR_USERS = r"C:\xampp\htdocs\synapse\synarex\usersdata\investors\demoinvestors.json"
INV_PATH = r"C:\xampp\htdocs\synapse\synarex\usersdata\investors"
NORMALIZE_SYMBOLS_PATH = r"C:\xampp\htdocs\synapse\synarex\symbols_normalization.json"
DEFAULT_ACCOUNTMANAGEMENT = r"C:\xampp\htdocs\synapse\synarex\default_accountmanagement.json"

def load_investors_dictionary():
    BROKERS_JSON_PATH = r"C:\xampp\htdocs\synapse\synarex\usersdata\investors\demoinvestors.json"
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
        print(f"Invalid JSON in demoinvestors.json: {e}", "CRITICAL")
        return {}
    except Exception as e:
        print(f"Failed to load demoinvestors.json: {e}", "CRITICAL")
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

def place_orders_hedging_demo():
    """
    Places hedge orders (opposite side) at the same entry points when place_orders_hedge is enabled.
    For each buy limit order, places a sell stop at same price with SL/TP switched.
    For each sell limit order, places a buy stop at same price with SL/TP switched.
    
    DEMO VERSION: Uses the EXACT demo account initialization logic from place_usd_orders_for_demo_accounts()
    while maintaining all other hedging functionality.
    """

    print("\n" + "="*80)
    print("🛡️  HEDGING ENGINE: PLACING OPPOSITE ORDERS (DEMO VERSION)")
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
        print(f"📋 DEMO INVESTOR [{processed}/{total_investors}]: {user_brokerid}")
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

            # --- DEMO ACCOUNT INITIALIZATION (EXACT COPY FROM place_usd_orders_for_demo_accounts) ---
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
            # --- END EXACT DEMO INITIALIZATION COPY ---

            # Get account and terminal info
            acc_info = mt5.account_info()
            term_info = mt5.terminal_info()

            if not acc_info:
                print(f"  ❌ Failed to get account info")
                continue

            if not term_info.trade_allowed:
                print(f"  ⚠️  AutoTrading is DISABLED - Cannot place hedge orders")
                continue

            print(f"\n  📊 DEMO Account Details:")
            print(f"    • Balance: ${acc_info.balance:,.2f}")
            print(f"    • Equity: ${acc_info.equity:,.2f}")
            print(f"    • Free Margin: ${acc_info.margin_free:,.2f}")
            print(f"    • Margin Level: {acc_info.margin_level:.2f}%" if acc_info.margin_level else "    • Margin Level: N/A")
            print(f"    • AutoTrading: {'✅ ENABLED' if term_info.trade_allowed else '❌ DISABLED'}")
            print(f"    • Account Type: DEMO")

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
            print(f"\n  📊 HEDGING SUMMARY FOR {user_brokerid} (DEMO):")
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

    # Overall summary
    print(f"\n{'='*80}")
    print(f"📊 DEMO HEDGING ENGINE FINAL SUMMARY")
    print(f"{'='*80}")
    print(f"   Total Investors Processed: {processed}")
    print(f"   Total Hedge Orders Placed: {hedge_orders_placed}")
    print(f"   Total Hedge Orders Skipped: {hedge_orders_skipped}")
    print(f"   Total Hedge Orders Failed: {hedge_orders_failed}")
    print(f"{'='*80}")
    
    return True

def place_usd_orders_for_demo_accounts():
    """
    Process USD orders for DEMO accounts with enhanced visualization and logging.
    Modified to place orders from TARGET risk DOWN to lower risk (descending order).
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
        """Collect trading entries from risk folders with DESCENDING order when pull_lower is enabled"""
        primary_risk = None
        primary_risk_original = None  # Store original float value
        print(f"  ⚙️  Determining primary risk for balance: ${balance:,.2f}")
        
        # Find primary risk level
        for range_str, r_val in risk_map.items():
            try:
                raw_range = range_str.split("_")[0]
                low, high = map(float, raw_range.split("-"))
                if low <= balance <= high:
                    primary_risk_original = float(r_val)  # Store as float
                    # For folder structure, use the original decimal format
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

        # Determine risk levels to scan
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
            
            # REVERSE THE ORDER for descending (highest to lowest)
            risk_levels_float.reverse()
            
            # Convert to folder name format (keep decimals, don't replace with underscores)
            for rv in risk_levels_float:
                if rv.is_integer():
                    risk_levels.append(str(int(rv)))
                else:
                    risk_levels.append(str(rv))  # Keep as "0.5", not "0_5"
            
            print(f"  📊 Pull lower enabled (DESCENDING order), scanning risk levels: {risk_levels_float} → Folders: {risk_levels}")
        else:
            # Just use the primary risk level
            risk_levels = [primary_risk]
            print(f"  📊 Scanning only primary risk level: {primary_risk_original} → Folder: {primary_risk}")

        all_entries = []  # Simple list, no deduplication
        target_rr_folder = f"risk_reward_{selected_rr}"
        
        # Scan each risk level in the order specified (now descending when pull_lower=True)
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
                                # Add all entries without deduplication
                                all_entries.extend(data)
                    except json.JSONDecodeError as e:
                        print(f"      ❌ Risk Level {r_val}: Invalid JSON format in {path.name} - {e}")
                    except Exception as e:
                        print(f"      ❌ Risk Level {r_val}: Failed to read file {path.name} - {e}")
            
            if entries_found > 0:
                print(f"    📁 Risk Level {r_val}: Found {entries_found} entries in {len(files_found)} file(s)")
            else:
                if files_found:
                    print(f"    📁 Risk Level {r_val}: No valid entries found in {len(files_found)} file(s)")
                else:
                    print(f"    📁 Risk Level {r_val}: No files found matching pattern")
                
        print(f"  📈 TOTAL: {len(all_entries)} trading opportunities collected (no deduplication) - PROCESSING ORDER: {' → '.join(risk_levels)}")
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
                if not e_symbol: continue
                
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
                traceback.print_exc()
                failed += 1
                
        # Summary
        if total > 0:
            success_rate = (placed / total) * 100 if total > 0 else 0
            print(f"\n    📊 Execution Summary: ✅ {placed} placed | ❌ {failed} failed | ⏭️  {skipped} skipped | Success Rate: {success_rate:.1f}%")
        return placed, failed, skipped

    # --- MAIN EXECUTION FLOW ---
    print("\n" + "="*80)
    print("🎯 DEMO ACCOUNT USD ORDER PLACEMENT ENGINE (DESCENDING ORDER MODE)")
    print("="*80)
    print("📌 Using DEMO account initialization logic")
    print("📌 Deduplication stage: REMOVED")
    print("📌 Comprehensive error mapping: ENABLED")
    print("📌 ORDER PLACEMENT: TARGET risk → LOWER risk (descending)")
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
            if pull_lower:
                print(f"    • Order Mode: DESCENDING (Target → Lower)")

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
    print("✅ DEMO ORDER PLACEMENT COMPLETED (DESCENDING ORDER MODE)")
    print(f"   Processed: {processed}/{total_investors} DEMO investors")
    print(f"   Successful: {successful} DEMO investors")
    print("="*80)
    
    return True

def check_limit_orders_risk_demo():
    """
    Function 3: Validates live pending orders against the account's current risk bucket.
    DEMO VERSION: Uses the EXACT demo account initialization logic from place_usd_orders_for_demo_accounts()
    Only removes orders with risk HIGHER than allowed (lower risk orders are kept).
    """
    print(f"\n{'='*10} 🛡️  LIVE RISK AUDIT: PENDING ORDERS (DEMO VERSION) {'='*10}")

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
        print(f" [{user_brokerid}] 🔍 Auditing live risk limits (DEMO)...")
        
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

        # --- DEMO ACCOUNT INITIALIZATION (EXACT COPY FROM place_usd_orders_for_demo_accounts) ---
        print(f"  └─ 🔌 Initializing DEMO account connection...")
        mt5.shutdown()
        
        login_id = int(broker_cfg['LOGIN_ID'])
        mt5_path = broker_cfg["TERMINAL_PATH"]
        
        print(f"      • Terminal Path: {mt5_path}")
        print(f"      • Login ID: {login_id}")
        
        # Initialize MT5
        if not mt5.initialize(path=mt5_path, timeout=180000):
            error = mt5.last_error()
            print(f"  └─ ❌ Failed to initialize MT5: {error}")
            continue

        # Check login status
        acc = mt5.account_info()
        if acc is None or acc.login != login_id:
            print(f"      🔑 Logging into DEMO account...")
            if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
                error = mt5.last_error()
                print(f"  └─ ❌ DEMO login failed: {error}")
                continue
            print(f"      ✅ Successfully logged into DEMO account")
        else:
            print(f"      ✅ Already logged into DEMO account")
        # --- END EXACT DEMO INITIALIZATION COPY ---

        acc_info = mt5.account_info()
        if not acc_info:
            print(f"  └─ ❌ Failed to get account info")
            mt5.shutdown()
            continue
            
        balance = acc_info.balance

        # Get terminal info for additional details
        term_info = mt5.terminal_info()
        
        print(f"\n  └─ 📊 DEMO Account Details:")
        print(f"      • Balance: ${acc_info.balance:,.2f}")
        print(f"      • Equity: ${acc_info.equity:,.2f}")
        print(f"      • Free Margin: ${acc_info.margin_free:,.2f}")
        print(f"      • Margin Level: {acc_info.margin_level:.2f}%" if acc_info.margin_level else "      • Margin Level: N/A")
        print(f"      • AutoTrading: {'✅ ENABLED' if term_info.trade_allowed else '❌ DISABLED'}")
        print(f"      • Account Type: DEMO")

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

        print(f"\n  └─ 💰 Target Risk: ${primary_risk:.2f}")

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
            print(f"\n  └─ 📊 Audit Results for {user_brokerid} (DEMO):")
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

    print(f"\n{'='*10} 🏁 DEMO RISK AUDIT COMPLETE {'='*10}\n")
    return True

def cleanup_history_duplicates_demo():
    """
    Scans history for the last 48 hours. If a position was closed, 
    any pending limit orders with the same first 3 digits in the price 
    are cancelled to prevent re-entry.
    
    DEMO VERSION: Uses the EXACT demo account initialization logic from place_usd_orders_for_demo_accounts()
    """
    from datetime import datetime, timedelta
    print(f"\n{'='*10} 📜 HISTORY AUDIT: PREVENTING RE-ENTRY (DEMO VERSION) {'='*10}")

    for user_brokerid, broker_cfg in usersdictionary.items():
        print(f" [{user_brokerid}] 🔍 Checking 48h history for duplicates (DEMO)...")
        
        # --- DEMO ACCOUNT INITIALIZATION (EXACT COPY FROM place_usd_orders_for_demo_accounts) ---
        print(f"  └─ 🔌 Initializing DEMO account connection...")
        mt5.shutdown()
        
        login_id = int(broker_cfg['LOGIN_ID'])
        mt5_path = broker_cfg["TERMINAL_PATH"]
        
        print(f"      • Terminal Path: {mt5_path}")
        print(f"      • Login ID: {login_id}")
        
        # Initialize MT5
        if not mt5.initialize(path=mt5_path, timeout=180000):
            error = mt5.last_error()
            print(f"  └─ ❌ Failed to initialize MT5: {error}")
            continue

        # Check login status
        acc = mt5.account_info()
        if acc is None or acc.login != login_id:
            print(f"      🔑 Logging into DEMO account...")
            if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
                error = mt5.last_error()
                print(f"  └─ ❌ DEMO login failed: {error}")
                continue
            print(f"      ✅ Successfully logged into DEMO account")
        else:
            print(f"      ✅ Already logged into DEMO account")
        # --- END EXACT DEMO INITIALIZATION COPY ---

        # Get account and terminal info for display
        acc_info = mt5.account_info()
        term_info = mt5.terminal_info()
        
        if not acc_info:
            print(f"  └─ ❌ Failed to get account info")
            mt5.shutdown()
            continue

        print(f"\n  └─ 📊 DEMO Account Details:")
        print(f"      • Balance: ${acc_info.balance:,.2f}")
        print(f"      • Equity: ${acc_info.equity:,.2f}")
        print(f"      • Free Margin: ${acc_info.margin_free:,.2f}")
        print(f"      • Margin Level: {acc_info.margin_level:.2f}%" if acc_info.margin_level else "      • Margin Level: N/A")
        print(f"      • AutoTrading: {'✅ ENABLED' if term_info.trade_allowed else '❌ DISABLED'}")
        print(f"      • Account Type: DEMO")

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
                clean_price = str(deal.price).replace('.', '')[:4]
                used_entries.add((deal.symbol, clean_price))

        if not used_entries:
            print(f"  └─ ✅ No closed orders found in last 48h.")
            mt5.shutdown()
            continue

        print(f"  └─ 📋 Found {len(used_entries)} unique price prefixes in history")

        # 4. Check Current Pending Orders
        pending_orders = mt5.orders_get()
        removed_count = 0

        if pending_orders:
            print(f"  └─ 🔍 Scanning {len(pending_orders)} pending orders...")
            
            for order in pending_orders:
                # Only target limit orders
                if order.type in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT]:
                    order_price_prefix = str(order.price_open).replace('.', '')[:3]
                    
                    # If this symbol + price prefix exists in history, kill the order
                    if (order.symbol, order_price_prefix) in used_entries:
                        order_type = "BUY LIMIT" if order.type == mt5.ORDER_TYPE_BUY_LIMIT else "SELL LIMIT"
                        print(f"    └─ 🚫 DUPLICATE FOUND: {order.symbol} {order_type} at {order.price_open}")
                        print(f"       Match found in history (Prefix: {order_price_prefix}). Cancelling...")
                        
                        cancel_request = {
                            "action": mt5.TRADE_ACTION_REMOVE,
                            "order": order.ticket
                        }
                        res = mt5.order_send(cancel_request)
                        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                            removed_count += 1
                            print(f"       ✅ Successfully cancelled #{order.ticket}")
                        else:
                            error_msg = res.comment if res else "No response"
                            error_code = res.retcode if res else "N/A"
                            print(f"       ❌ Failed to cancel #{order.ticket}: {error_msg} (Code: {error_code})")

        print(f"\n  └─ 📊 DEMO Cleanup Result for {user_brokerid}:")
        print(f"      • History entries found: {len(used_entries)}")
        print(f"      • Duplicate orders removed: {removed_count}")
        if removed_count == 0 and used_entries:
            print(f"      • No matching pending orders found")
        
        mt5.shutdown()

    print(f"\n{'='*10} 🏁 DEMO HISTORY AUDIT COMPLETE {'='*10}\n")
    return True

def limit_orders_reward_correction_demo():
    """
    Function: Checks live pending limit orders and adjusts their take profit levels
    based on the selected risk-reward ratio from accountmanagement.json.
    Only executes if risk_reward_correction setting is True.
    
    DEMO VERSION: Uses the EXACT demo account initialization logic from place_usd_orders_for_demo_accounts()
    """
    print(f"\n{'='*10} 📐 RISK-REWARD CORRECTION: PENDING ORDERS (DEMO VERSION) {'='*10}")

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
        print(f" [{user_brokerid}] 🔍 Checking risk-reward configurations (DEMO)...")
        
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

        # --- DEMO ACCOUNT INITIALIZATION (EXACT COPY FROM place_usd_orders_for_demo_accounts) ---
        print(f"  └─ 🔌 Initializing DEMO account connection...")
        mt5.shutdown()
        
        login_id = int(broker_cfg['LOGIN_ID'])
        mt5_path = broker_cfg["TERMINAL_PATH"]
        
        print(f"      • Terminal Path: {mt5_path}")
        print(f"      • Login ID: {login_id}")
        
        # Initialize MT5
        if not mt5.initialize(path=mt5_path, timeout=180000):
            error = mt5.last_error()
            print(f"  └─ ❌ Failed to initialize MT5: {error}")
            continue

        # Check login status
        acc = mt5.account_info()
        if acc is None or acc.login != login_id:
            print(f"      🔑 Logging into DEMO account...")
            if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
                error = mt5.last_error()
                print(f"  └─ ❌ DEMO login failed: {error}")
                continue
            print(f"      ✅ Successfully logged into DEMO account")
        else:
            print(f"      ✅ Already logged into DEMO account")
        # --- END EXACT DEMO INITIALIZATION COPY ---

        acc_info = mt5.account_info()
        if not acc_info:
            print(f"  └─ ❌ Failed to get account info")
            mt5.shutdown()
            continue
            
        balance = acc_info.balance

        # Get terminal info for additional details
        term_info = mt5.terminal_info()
        
        print(f"\n  └─ 📊 DEMO Account Details:")
        print(f"      • Balance: ${acc_info.balance:,.2f}")
        print(f"      • Equity: ${acc_info.equity:,.2f}")
        print(f"      • Free Margin: ${acc_info.margin_free:,.2f}")
        print(f"      • Margin Level: {acc_info.margin_level:.2f}%" if acc_info.margin_level else "      • Margin Level: N/A")
        print(f"      • AutoTrading: {'✅ ENABLED' if term_info.trade_allowed else '❌ DISABLED'}")
        print(f"      • Account Type: DEMO")

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

        print(f"\n  └─ 💰 Balance: ${balance:,.2f} | Base Risk: ${primary_risk:.2f} | Target R:R: 1:{target_rr_ratio}")

        # --- CHECK AND ADJUST PENDING LIMIT ORDERS ---
        pending_orders = mt5.orders_get()
        orders_checked = 0
        orders_adjusted = 0
        orders_skipped = 0
        orders_error = 0

        if pending_orders:
            print(f"  └─ 🔍 Scanning {len(pending_orders)} pending orders...")
            
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
            print(f"\n  └─ 📊 DEMO Risk-Reward Correction Results for {user_brokerid}:")
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

    print(f"\n{'='*10} 🏁 DEMO RISK-REWARD CORRECTION COMPLETE {'='*10}\n")
    return True

def place_grid_trades_demo():
    """
    Place scale trades at 250-pip increment levels around current price.
    Uses simple rounding to nearest 000/250/500/750 levels.
    """
    print("\n" + "="*80)
    print("📊 SCALE TRADES PLACEMENT ENGINE (DEMO) - 250 INCREMENT LEVELS")
    print("="*80)
    
    # --- LOAD NORMALIZATION MAP ---
    try:
        if not os.path.exists(NORMALIZE_SYMBOLS_PATH):
            print("  ⚠️  No normalization map found - proceeding without symbol normalization")
            norm_map = {}
        else:
            with open(NORMALIZE_SYMBOLS_PATH, 'r') as f:
                norm_map = json.load(f)
                print(f"  ✅ Loaded normalization map with {len(norm_map)} symbols")
    except Exception as e:
        print(f"❌ CRITICAL ERROR: Could not load normalization map: {e}")
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
            
            # Check if scale trading is enabled
            settings = config.get("settings", {})
            if not settings.get("place_grid_trades", False):
                print(f"  ℹ️  Scale trading disabled in settings - skipping")
                continue
                
            print(f"  ✅ Scale trading ENABLED for this investor")
            
            # --- DEMO ACCOUNT INITIALIZATION ---
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

            # Get account info
            acc_info = mt5.account_info()
            term_info = mt5.terminal_info()
            
            if not acc_info:
                print(f"  ❌ Failed to get account info")
                continue
                
            print(f"\n  📊 DEMO Account Details:")
            print(f"    • Balance: ${acc_info.balance:,.2f}")
            print(f"    • Equity: ${acc_info.equity:,.2f}")
            print(f"    • AutoTrading: {'✅ ENABLED' if term_info.trade_allowed else '❌ DISABLED'}")
            
            if not term_info.trade_allowed:
                print(f"  ⚠️  AutoTrading is DISABLED - Scale trades will not be executed")
                mt5.shutdown()
                continue
            
            # Get symbols to trade from risk folders
            selected_rr = config.get("selected_risk_reward", [None])[0]
            if not selected_rr:
                print(f"  ⚠️  No risk/reward selected - skipping")
                mt5.shutdown()
                continue
                
            # Find all symbols that have risk files
            symbols_to_trade = set()
            target_rr_folder = f"risk_reward_{selected_rr}"
            
            # Scan for all risk level folders
            risk_folders = ["0.5usd_risk", "1usd_risk", "2usd_risk", "3usd_risk", "5usd_risk"]
            
            for risk_folder in risk_folders:
                search_pattern = f"**/{target_rr_folder}/{risk_folder}/*.json"
                for path in inv_root.rglob(search_pattern):
                    try:
                        with open(path, 'r') as f:
                            data = json.load(f)
                            if isinstance(data, list):
                                for entry in data:
                                    if "symbol" in entry:
                                        symbol_orig = entry["symbol"]
                                        # Normalize symbol
                                        symbol = get_normalized_symbol(symbol_orig, norm_map)
                                        if symbol:
                                            symbols_to_trade.add(symbol)
                    except Exception as e:
                        continue
            
            if not symbols_to_trade:
                print(f"  ⚠️  No tradable symbols found in risk files")
                mt5.shutdown()
                continue
                
            # Print ALL symbols found
            print(f"\n  📈 Found {len(symbols_to_trade)} symbols to place scale trades:")
            symbols_list = sorted(list(symbols_to_trade))
            for i, sym in enumerate(symbols_list, 1):
                print(f"    {i:2d}. {sym}")
            
            # Process each symbol for scale trades
            total_placed = 0
            total_failed = 0
            total_skipped = 0
            
            for symbol in symbols_list:
                print(f"\n  {'='*60}")
                print(f"  🔍 Processing {symbol}")
                print(f"  {'='*60}")
                
                # Select symbol in Market Watch
                if not mt5.symbol_select(symbol, True):
                    last_error = mt5.last_error()
                    print(f"    ❌ Could not select {symbol} - Error: {last_error}")
                    total_failed += 1
                    continue
                
                # Get symbol info
                symbol_info = mt5.symbol_info(symbol)
                if not symbol_info:
                    last_error = mt5.last_error()
                    print(f"    ❌ No symbol info for {symbol} - Error: {last_error}")
                    total_failed += 1
                    continue
                
                # Check if trading is allowed
                if symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_DISABLED:
                    print(f"    ❌ Trading disabled for {symbol} - skipping")
                    total_failed += 1
                    continue
                elif symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_CLOSEONLY:
                    print(f"    ❌ Only closing positions allowed for {symbol} - skipping")
                    total_failed += 1
                    continue
                
                # Get current market price
                tick = mt5.symbol_info_tick(symbol)
                if not tick:
                    last_error = mt5.last_error()
                    print(f"    ❌ No tick data for {symbol} - Error: {last_error}")
                    total_failed += 1
                    continue
                
                current_ask = tick.ask
                current_bid = tick.bid
                
                print(f"    📊 Current Price - Bid: {current_bid:.{symbol_info.digits}f} | Ask: {current_ask:.{symbol_info.digits}f}")
                
                # ===== SIMPLE ROUNDING METHOD - INCREMENT/DECREMENT DIRECTLY =====
                # Get the price as string to manipulate last digits
                ask_str = f"{current_ask:.{symbol_info.digits}f}"
                bid_str = f"{current_bid:.{symbol_info.digits}f}"
                
                # Split into integer and decimal parts
                if '.' in ask_str:
                    int_part, dec_part = ask_str.split('.')
                else:
                    int_part, dec_part = ask_str, ""
                
                # Determine the step based on digits
                if symbol_info.digits == 5 or symbol_info.digits == 3:
                    # For 5-digit forex: 250 pips = 0.02500
                    increment = 250 / (10 ** 3)  # 0.025
                    # Format string for display
                    inc_str = f"+0.{'0'*(symbol_info.digits-3)}250"
                elif symbol_info.digits == 4 or symbol_info.digits == 2:
                    # For 4-digit: 250 pips = 0.0250? Actually 250 pips = 0.025 for 4-digit too
                    increment = 250 / (10 ** 3)  # 0.025
                    inc_str = f"+0.0{symbol_info.digits-2}250"
                else:
                    # For indices/commodities, determine appropriate increment
                    # For DOW.N with 2 digits, 250 points = 2.50
                    increment = 250 / (10 ** (symbol_info.digits))
                    inc_str = f"+{increment:.{symbol_info.digits}f}"
                
                # Extract the last 3 digits of the decimal part
                if len(dec_part) >= 3:
                    last_three = int(dec_part[-3:])
                    
                    # Determine which range we're in and round
                    if 1 <= last_three <= 249:
                        # Range 001-249 → round to 250
                        # Replace last 3 digits with 250
                        base_part = dec_part[:-3] if len(dec_part) > 3 else ''
                        rounded_dec = base_part + "250"
                        range_desc = f"{last_three:03d} in 001-249 → rounded to 250"
                    elif 251 <= last_three <= 499:
                        # Range 251-499 → round to 500
                        base_part = dec_part[:-3] if len(dec_part) > 3 else ''
                        rounded_dec = base_part + "500"
                        range_desc = f"{last_three:03d} in 251-499 → rounded to 500"
                    elif 501 <= last_three <= 749:
                        # Range 501-749 → round to 750
                        base_part = dec_part[:-3] if len(dec_part) > 3 else ''
                        rounded_dec = base_part + "750"
                        range_desc = f"{last_three:03d} in 501-749 → rounded to 750"
                    else:
                        # Range 750-999 or 000 → round to 000 (next integer)
                        if last_three >= 750:
                            # Need to increment the part before last 3 digits
                            if len(dec_part) > 3:
                                base_part = dec_part[:-3]
                                if base_part:
                                    new_base = str(int(base_part) + 1).zfill(len(base_part))
                                    rounded_dec = new_base + "000"
                                else:
                                    # No digits before, increment integer part
                                    rounded_dec = "000"
                                    int_part = str(int(int_part) + 1)
                            else:
                                # Decimal part is exactly 3 digits, increment integer part
                                rounded_dec = "000"
                                int_part = str(int(int_part) + 1)
                        else:  # last_three == 0
                            # Already at 000
                            rounded_dec = dec_part
                    
                    # Build the rounded price
                    if len(dec_part) > 3:
                        rounded_price_str = f"{int_part}.{rounded_dec}"
                        # Ensure correct length
                        if len(rounded_dec) > symbol_info.digits:
                            rounded_dec = rounded_dec[:symbol_info.digits]
                            rounded_price_str = f"{int_part}.{rounded_dec}"
                    else:
                        # For exactly 3 decimal digits
                        rounded_price_str = f"{int_part}.{rounded_dec}"
                    
                    rounded_price = float(rounded_price_str)
                    
                else:
                    # Handle cases with fewer than 3 decimal digits
                    # Pad with zeros
                    while len(dec_part) < 3:
                        dec_part += '0'
                    last_three = int(dec_part[-3:])
                    
                    # Same logic as above but simpler
                    if last_three < 250:
                        rounded_price = float(int_part) + 0.250
                        range_desc = f"{last_three} → rounded to 250"
                    elif last_three < 500:
                        rounded_price = float(int_part) + 0.500
                        range_desc = f"{last_three} → rounded to 500"
                    elif last_three < 750:
                        rounded_price = float(int_part) + 0.750
                        range_desc = f"{last_three} → rounded to 750"
                    else:
                        rounded_price = float(int_part) + 1.000
                        range_desc = f"{last_three} → rounded to 000 (next)"
                
                print(f"    📊 Current price range: {range_desc}")
                print(f"    📊 Rounded price: {rounded_price:.{symbol_info.digits}f}")
                
                # Now simply add and subtract the increment
                # For 5-digit forex, increment should be 0.025
                # For 4-digit forex, increment should be 0.025 as well
                # For JPY pairs, increment should be 2.5
                
                # Calculate the correct increment based on instrument type
                if "JPY" in symbol or symbol_info.digits == 3 or symbol_info.digits == 2:
                    # JPY pairs or 3-digit instruments
                    # 250 pips = 2.5 for JPY (since 1 pip = 0.01)
                    grid_increment = 2.5
                elif symbol_info.digits == 5 or symbol_info.digits == 4:
                    # Forex pairs
                    # 250 pips = 0.025
                    grid_increment = 0.025
                else:
                    # Others - calculate based on digits
                    grid_increment = 250 / (10 ** symbol_info.digits)
                
                # Calculate levels by simple addition/subtraction
                upper_level = rounded_price + grid_increment
                lower_level = rounded_price - grid_increment
                
                # Round to correct digits
                upper_level = round(upper_level, symbol_info.digits)
                lower_level = round(lower_level, symbol_info.digits)
                
                # Ensure BUY is above ask and SELL is below bid
                if upper_level <= current_ask:
                    upper_level = round(upper_level + grid_increment, symbol_info.digits)
                    print(f"    ⚠️  Adjusting BUY up (was too close)")
                
                if lower_level >= current_bid:
                    lower_level = round(lower_level - grid_increment, symbol_info.digits)
                    print(f"    ⚠️  Adjusting SELL down (was too close)")
                
                print(f"    🎯 Grid Levels (250 pips = {grid_increment:.{symbol_info.digits}f}):")
                print(f"      • SELL STOP: {lower_level:.{symbol_info.digits}f} ({current_bid - lower_level:.{symbol_info.digits}f} below bid)")
                print(f"      • BUY STOP:  {upper_level:.{symbol_info.digits}f} ({upper_level - current_ask:.{symbol_info.digits}f} above ask)")
                
                # CHECK FOR EXISTING ORDERS
                existing_orders = mt5.orders_get(symbol=symbol) or ()
                
                buy_exists = False
                sell_exists = False
                buy_ticket = None
                sell_ticket = None
                
                if existing_orders:
                    for order in existing_orders:
                        # Check for BUY STOP at upper level (within 10% of step)
                        if order.type == mt5.ORDER_TYPE_BUY_STOP and abs(order.price_open - upper_level) < (grid_increment * 0.1):
                            print(f"    ⏭️  BUY STOP already exists at {order.price_open:.{symbol_info.digits}f} (Ticket: {order.ticket})")
                            buy_exists = True
                            buy_ticket = order.ticket
                        # Check for SELL STOP at lower level
                        elif order.type == mt5.ORDER_TYPE_SELL_STOP and abs(order.price_open - lower_level) < (grid_increment * 0.1):
                            print(f"    ⏭️  SELL STOP already exists at {order.price_open:.{symbol_info.digits}f} (Ticket: {order.ticket})")
                            sell_exists = True
                            sell_ticket = order.ticket
                
                # Calculate volume
                volume = 0.01  # Default minimum lot
                
                # Check volume constraints
                if volume < symbol_info.volume_min:
                    volume = symbol_info.volume_min
                if volume > symbol_info.volume_max:
                    volume = symbol_info.volume_max
                
                # Adjust to volume step
                if symbol_info.volume_step > 0:
                    volume = round(volume / symbol_info.volume_step) * symbol_info.volume_step
                
                # Check pending orders limit
                if not buy_exists or not sell_exists:
                    all_pending = mt5.orders_total()
                    if all_pending and all_pending >= 200:  # MT5 typical limit
                        print(f"    ⚠️  Pending orders limit reached ({all_pending}/200) - cannot place more")
                        total_failed += 1
                        continue
                
                # STEP 1: Place missing orders
                print(f"\n    📍 STEP 1: Placing missing orders...")
                
                # Place BUY STOP if not exists
                if not buy_exists:
                    buy_request = {
                        "action": mt5.TRADE_ACTION_PENDING,
                        "symbol": symbol,
                        "volume": volume,
                        "type": mt5.ORDER_TYPE_BUY_STOP,
                        "price": upper_level,
                        "sl": 0.0,
                        "tp": 0.0,
                        "magic": config.get("magic_number", 123456),
                        "comment": f"GRID_BUY_{selected_rr}",
                        "type_time": mt5.ORDER_TIME_GTC,
                        "type_filling": mt5.ORDER_FILLING_IOC,
                    }
                    
                    buy_result = mt5.order_send(buy_request)
                    if buy_result and buy_result.retcode == mt5.TRADE_RETCODE_DONE:
                        buy_ticket = buy_result.order
                        print(f"    ✅ BUY STOP placed at {upper_level:.{symbol_info.digits}f} (Ticket: {buy_ticket})")
                    else:
                        error_code = buy_result.retcode if buy_result else "N/A"
                        error_msg = buy_result.comment if buy_result and buy_result.comment else "No response"
                        
                        # MT5 error code mapping
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
                            10033: "Orders limit reached",
                            130: "Invalid stops",
                            134: "Insufficient funds",
                            135: "Price changed",
                            136: "Off quotes",
                            138: "Requote",
                            145: "Modification denied",
                        }
                        
                        human_error = error_map.get(error_code, f"Unknown error ({error_code})")
                        print(f"    ❌ BUY STOP failed: {human_error} | Details: {error_msg}")
                
                # Place SELL STOP if not exists
                if not sell_exists:
                    sell_request = {
                        "action": mt5.TRADE_ACTION_PENDING,
                        "symbol": symbol,
                        "volume": volume,
                        "type": mt5.ORDER_TYPE_SELL_STOP,
                        "price": lower_level,
                        "sl": 0.0,
                        "tp": 0.0,
                        "magic": config.get("magic_number", 123456),
                        "comment": f"GRID_SELL_{selected_rr}",
                        "type_time": mt5.ORDER_TIME_GTC,
                        "type_filling": mt5.ORDER_FILLING_IOC,
                    }
                    
                    sell_result = mt5.order_send(sell_request)
                    if sell_result and sell_result.retcode == mt5.TRADE_RETCODE_DONE:
                        sell_ticket = sell_result.order
                        print(f"    ✅ SELL STOP placed at {lower_level:.{symbol_info.digits}f} (Ticket: {sell_ticket})")
                    else:
                        error_code = sell_result.retcode if sell_result else "N/A"
                        error_msg = sell_result.comment if sell_result and sell_result.comment else "No response"
                        
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
                            10033: "Orders limit reached",
                            130: "Invalid stops",
                            134: "Insufficient funds",
                            135: "Price changed",
                            136: "Off quotes",
                            138: "Requote",
                            145: "Modification denied",
                        }
                        
                        human_error = error_map.get(error_code, f"Unknown error ({error_code})")
                        print(f"    ❌ SELL STOP failed: {human_error} | Details: {error_msg}")
                
                # STEP 2: Modify orders to use each other as stop loss
                if buy_ticket and sell_ticket:
                    print(f"\n    📍 STEP 2: Setting cross stop losses...")
                    
                    # Small delay if we placed new orders
                    if (buy_ticket and not buy_exists) or (sell_ticket and not sell_exists):
                        import time
                        time.sleep(1)
                    
                    # Get fresh order info
                    all_orders = mt5.orders_get() or ()
                    buy_order = None
                    sell_order = None
                    
                    for order in all_orders:
                        if order.ticket == buy_ticket:
                            buy_order = order
                        if order.ticket == sell_ticket:
                            sell_order = order
                    
                    # Modify BUY STOP
                    if buy_order:
                        if abs(buy_order.sl - lower_level) > (grid_increment * 0.1):
                            buy_modify_request = {
                                "action": mt5.TRADE_ACTION_MODIFY,
                                "order": buy_ticket,
                                "price": upper_level,
                                "sl": lower_level,
                                "tp": 0.0,
                            }
                            
                            modify_buy = mt5.order_send(buy_modify_request)
                            if modify_buy and modify_buy.retcode == mt5.TRADE_RETCODE_DONE:
                                print(f"    ✅ BUY STOP modified: SL set to {lower_level:.{symbol_info.digits}f}")
                            else:
                                error = modify_buy.retcode if modify_buy else "N/A"
                                print(f"    ❌ BUY STOP modification failed: {error}")
                        else:
                            print(f"    ✅ BUY STOP SL already correct at {lower_level:.{symbol_info.digits}f}")
                    
                    # Modify SELL STOP
                    if sell_order:
                        if abs(sell_order.sl - upper_level) > (grid_increment * 0.1):
                            sell_modify_request = {
                                "action": mt5.TRADE_ACTION_MODIFY,
                                "order": sell_ticket,
                                "price": lower_level,
                                "sl": upper_level,
                                "tp": 0.0,
                            }
                            
                            modify_sell = mt5.order_send(sell_modify_request)
                            if modify_sell and modify_sell.retcode == mt5.TRADE_RETCODE_DONE:
                                print(f"    ✅ SELL STOP modified: SL set to {upper_level:.{symbol_info.digits}f}")
                            else:
                                error = modify_sell.retcode if modify_sell else "N/A"
                                print(f"    ❌ SELL STOP modification failed: {error}")
                        else:
                            print(f"    ✅ SELL STOP SL already correct at {upper_level:.{symbol_info.digits}f}")
                    
                    print(f"\n    📊 Final Grid Configuration for {symbol}:")
                    print(f"      • BUY STOP  @ {upper_level:.{symbol_info.digits}f} | SL: {lower_level:.{symbol_info.digits}f}")
                    print(f"      • SELL STOP @ {lower_level:.{symbol_info.digits}f} | SL: {upper_level:.{symbol_info.digits}f}")
                    
                    if not buy_exists and not sell_exists:
                        total_placed += 2
                        print(f"    ✅ Both orders placed successfully")
                    elif (buy_exists and not sell_exists) or (not buy_exists and sell_exists):
                        total_placed += 1
                        print(f"    ✅ One new order placed")
                    
                elif buy_ticket or sell_ticket:
                    print(f"\n    ⚠️  Only one order exists - waiting for counterparty")
                    if buy_ticket and not sell_ticket:
                        print(f"      • BUY STOP exists at {upper_level:.{symbol_info.digits}f}, need SELL STOP at {lower_level:.{symbol_info.digits}f}")
                    if sell_ticket and not buy_ticket:
                        print(f"      • SELL STOP exists at {lower_level:.{symbol_info.digits}f}, need BUY STOP at {upper_level:.{symbol_info.digits}f}")
                    
                    if (buy_ticket and not buy_exists) or (sell_ticket and not sell_exists):
                        total_placed += 1
                else:
                    if buy_exists and sell_exists:
                        print(f"\n    ⏭️  Both orders already exist - no action needed")
                        total_skipped += 1
                    else:
                        print(f"\n    ❌ No orders placed for {symbol}")
                        total_failed += 1
            
            # Summary for this investor
            print(f"\n  {'='*60}")
            print(f"  📈 SCALE TRADES FINAL SUMMARY: {user_brokerid}")
            print(f"  {'='*60}")
            print(f"    • Total Symbols Processed: {len(symbols_list)}")
            print(f"    • Orders Placed: {total_placed}")
            print(f"    • Orders Skipped (already existed): {total_skipped}")
            print(f"    • Orders Failed: {total_failed}")
            if total_placed + total_failed > 0:
                success_rate = (total_placed / (total_placed + total_failed)) * 100
                print(f"    • Success Rate: {success_rate:.1f}%")
            
            if total_placed > 0:
                successful += 1
                print(f"  ✅ Investor processing completed with {total_placed} orders placed")
            else:
                print(f"  ⚠️  No new orders placed for this investor")
            
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
    
    print("\n" + "="*80)
    print("✅ SCALE TRADES PLACEMENT COMPLETED")
    print(f"   Processed: {processed}/{total_investors} DEMO investors")
    print(f"   Successful placements: {successful} investors")
    print("="*80)
    
    return True

def place_orders():
    sort_orders()
    deduplicate_orders() 
    default_price_repair()
    filter_unauthorized_symbols()
    place_usd_orders_for_demo_accounts()
    place_orders_hedging_demo()
    limit_orders_reward_correction_demo()
    cleanup_history_duplicates_demo()
    check_limit_orders_risk_demo()

if __name__ == "__main__":
   place_orders()

