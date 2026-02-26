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
import multiprocessing as mp
from pathlib import Path
import time
import random

INVESTOR_USERS = r"C:\xampp\htdocs\chronedge\synarex\usersdata\investors\investors.json"
INV_PATH = r"C:\xampp\htdocs\chronedge\synarex\usersdata\investors"
NORMALIZE_SYMBOLS_PATH = r"C:\xampp\htdocs\chronedge\synarex\symbols_normalization.json"
DEFAULT_ACCOUNTMANAGEMENT = r"C:\xampp\htdocs\chronedge\synarex\default_accountmanagement.json"
DEFAULT_PATH = r"C:\xampp\htdocs\chronedge\synarex"
NORM_FILE_PATH = Path(DEFAULT_PATH) / "symbols_normalization.json"

def load_investors_dictionary():
    BROKERS_JSON_PATH = r"C:\xampp\htdocs\chronedge\synarex\usersdata\investors\investors.json"
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

def get_normalized_symbol(record_symbol, risk_keys=None):
    """
    Standardizes symbols with a 'Broker-First' priority.
    If 'US OIL' is passed, it finds the USOIL family, then checks if the broker
    uses USOUSD, USOIL, or WTI.
    """
    if not record_symbol: return None

    NORM_PATH = Path(r"C:\xampp\htdocs\chronedge\synarex\symbols_normalization.json")
    
    def clean(s): 
        return str(s).replace(" ", "").replace("_", "").replace("/", "").replace(".", "").upper()

    search_term = clean(record_symbol)
    
    # 1. Load Normalization Map
    norm_data = {}
    if NORM_PATH.exists():
        try:
            with open(NORM_PATH, 'r', encoding='utf-8') as f:
                norm_data = json.load(f).get("NORMALIZATION", {})
        except: pass

    # 2. Find the "Family"
    target_family_key = None
    all_family_variants = []
    
    for std_key, synonyms in norm_data.items():
        family_variants = [clean(std_key)] + [clean(s) for s in synonyms]
        if any(search_term == v or search_term.startswith(v) or v.startswith(search_term) for v in family_variants):
            target_family_key = std_key
            all_family_variants = family_variants
            break

    # 3. IF RISK_KEYS ARE PROVIDED (For Risk Enforcement)
    if risk_keys:
        clean_risk_map = {clean(k): k for k in risk_keys}
        if target_family_key and clean(target_family_key) in clean_risk_map:
            return clean_risk_map[clean(target_family_key)]
        for v in all_family_variants:
            if v in clean_risk_map: return clean_risk_map[v]

    # 4. IF NO RISK_KEYS (For Populating Order Fields / MT5 Specs)
    # Check what the broker actually has in MarketWatch
    all_symbols = mt5.symbols_get()
    if all_symbols:
        broker_symbols = {clean(s.name): s.name for s in all_symbols}
        
        # Try to find which variant the broker uses
        for v in all_family_variants:
            if v in broker_symbols:
                return broker_symbols[v]
            # Handle suffixes (e.g., USOIL.m)
            for b_clean, b_raw in broker_symbols.items():
                if b_clean.startswith(v):
                    return b_raw

    # Fallback
    return target_family_key if target_family_key else record_symbol.upper()

def deduplicate_orders(inv_id=None):
    """
    Scans all pending_orders/limit_orders.json, pending_orders/limit_orders_backup.json, 
    and pending_orders/signals.json files and removes duplicate orders based on: 
    Symbol, Timeframe, Order Type, and Entry Price.
    
    Args:
        inv_id (str, optional): Specific investor ID to process. If None, processes all investors.
        This function does NOT require MT5.
    
    Returns:
        bool: True if any duplicates were removed, False otherwise
    """
    print(f"\n{'='*10} 🧹 DEDUPLICATING ORDERS {'='*10}")
    
    total_files_cleaned = 0
    total_duplicates_removed = 0
    total_limit_files_cleaned = 0
    total_signal_files_cleaned = 0
    total_limit_backup_files_cleaned = 0
    total_limit_duplicates = 0
    total_signal_duplicates = 0
    total_limit_backup_duplicates = 0
    inv_base_path = Path(INV_PATH)

    if not inv_base_path.exists():
        print(f" [!] Error: Investor path {INV_PATH} does not exist.")
        return False

    # Determine which investors to process
    if inv_id:
        inv_folder = inv_base_path / inv_id
        investor_folders = [inv_folder] if inv_folder.exists() else []
        if not investor_folders:
            print(f" [!] Error: Investor folder {inv_id} does not exist.")
            return False
    else:
        investor_folders = [f for f in inv_base_path.iterdir() if f.is_dir()]
    
    if not investor_folders:
        print(" └─ 🔘 No investor directories found for deduplication.")
        return False

    any_duplicates_removed = False

    for inv_folder in investor_folders:
        current_inv_id = inv_folder.name
        print(f"\n [{current_inv_id}] 🔍 Checking for duplicate entries...")

        # 2. Search for pending_orders folders
        pending_orders_folders = list(inv_folder.rglob("*/pending_orders/"))
        
        investor_limit_duplicates = 0
        investor_signal_duplicates = 0
        investor_limit_backup_duplicates = 0
        investor_limit_files_cleaned = 0
        investor_signal_files_cleaned = 0
        investor_limit_backup_files_cleaned = 0

        for pending_folder in pending_orders_folders:
            # Process limit_orders.json
            limit_file = pending_folder / "limit_orders.json"
            if limit_file.exists():
                try:
                    with open(limit_file, 'r', encoding='utf-8') as f:
                        orders = json.load(f)

                    if orders:
                        original_count = len(orders)
                        seen_orders = set()
                        unique_orders = []

                        for order in orders:
                            # Create a unique key based on Symbol, Timeframe, Order Type, and Entry
                            unique_key = (
                                str(order.get("symbol", "")).strip(),
                                str(order.get("timeframe", "")).strip(),
                                str(order.get("order_type", "")).strip(),
                                float(order.get("entry", 0))
                            )

                            if unique_key not in seen_orders:
                                seen_orders.add(unique_key)
                                unique_orders.append(order)
                        
                        # Only write back if duplicates were actually found
                        if len(unique_orders) < original_count:
                            removed = original_count - len(unique_orders)
                            with open(limit_file, 'w', encoding='utf-8') as f:
                                json.dump(unique_orders, f, indent=4)
                            
                            investor_limit_duplicates += removed
                            investor_limit_files_cleaned += 1
                            total_limit_duplicates += removed
                            total_limit_files_cleaned += 1
                            any_duplicates_removed = True
                            
                            folder_name = pending_folder.parent.name
                            print(f"  └─ 📄 {folder_name}/limit_orders.json - Removed {removed} duplicates")

                except Exception as e:
                    print(f"  └─ ❌ Error processing {limit_file}: {e}")

            # Process limit_orders_backup.json
            limit_backup_file = pending_folder / "limit_orders_backup.json"
            if limit_backup_file.exists():
                try:
                    with open(limit_backup_file, 'r', encoding='utf-8') as f:
                        backup_orders = json.load(f)

                    if backup_orders:
                        original_count = len(backup_orders)
                        seen_orders = set()
                        unique_backup_orders = []

                        for order in backup_orders:
                            # Create a unique key based on Symbol, Timeframe, Order Type, and Entry
                            unique_key = (
                                str(order.get("symbol", "")).strip(),
                                str(order.get("timeframe", "")).strip(),
                                str(order.get("order_type", "")).strip(),
                                float(order.get("entry", 0))
                            )

                            if unique_key not in seen_orders:
                                seen_orders.add(unique_key)
                                unique_backup_orders.append(order)
                        
                        # Only write back if duplicates were actually found
                        if len(unique_backup_orders) < original_count:
                            removed = original_count - len(unique_backup_orders)
                            with open(limit_backup_file, 'w', encoding='utf-8') as f:
                                json.dump(unique_backup_orders, f, indent=4)
                            
                            investor_limit_backup_duplicates += removed
                            investor_limit_backup_files_cleaned += 1
                            total_limit_backup_duplicates += removed
                            total_limit_backup_files_cleaned += 1
                            any_duplicates_removed = True
                            
                            folder_name = pending_folder.parent.name
                            print(f"  └─ 📄 {folder_name}/limit_orders_backup.json - Removed {removed} duplicates")

                except Exception as e:
                    print(f"  └─ ❌ Error processing {limit_backup_file}: {e}")

            # Process signals.json
            signals_file = pending_folder / "signals.json"
            if signals_file.exists():
                try:
                    with open(signals_file, 'r', encoding='utf-8') as f:
                        signals = json.load(f)

                    if signals:
                        original_count = len(signals)
                        seen_orders = set()
                        unique_signals = []

                        for signal in signals:
                            # Create a unique key based on Symbol, Timeframe, Order Type, and Entry
                            unique_key = (
                                str(signal.get("symbol", "")).strip(),
                                str(signal.get("timeframe", "")).strip(),
                                str(signal.get("order_type", "")).strip(),
                                float(signal.get("entry", 0))
                            )

                            if unique_key not in seen_orders:
                                seen_orders.add(unique_key)
                                unique_signals.append(signal)
                        
                        # Only write back if duplicates were actually found
                        if len(unique_signals) < original_count:
                            removed = original_count - len(unique_signals)
                            with open(signals_file, 'w', encoding='utf-8') as f:
                                json.dump(unique_signals, f, indent=4)
                            
                            investor_signal_duplicates += removed
                            investor_signal_files_cleaned += 1
                            total_signal_duplicates += removed
                            total_signal_files_cleaned += 1
                            any_duplicates_removed = True
                            
                            folder_name = pending_folder.parent.name
                            print(f"  └─ 📄 {folder_name}/signals.json - Removed {removed} duplicates")

                except Exception as e:
                    print(f"  └─ ❌ Error processing {signals_file}: {e}")

        # Summary for the current investor
        if investor_limit_duplicates > 0 or investor_signal_duplicates > 0 or investor_limit_backup_duplicates > 0:
            print(f"\n  └─ ✨ Investor {current_inv_id} Cleanup Summary:")
            if investor_limit_duplicates > 0:
                print(f"      • limit_orders.json: Cleaned {investor_limit_files_cleaned} files | Removed {investor_limit_duplicates} duplicates")
            if investor_limit_backup_duplicates > 0:
                print(f"      • limit_orders_backup.json: Cleaned {investor_limit_backup_files_cleaned} files | Removed {investor_limit_backup_duplicates} duplicates")
            if investor_signal_duplicates > 0:
                print(f"      • signals.json: Cleaned {investor_signal_files_cleaned} files | Removed {investor_signal_duplicates} duplicates")
        else:
            print(f"  └─ ✅ No duplicates found in any order files")

    # Final Global Summary
    print(f"\n{'='*10} DEDUPLICATION COMPLETE {'='*10}")
    
    total_files_cleaned = total_limit_files_cleaned + total_signal_files_cleaned + total_limit_backup_files_cleaned
    total_duplicates_removed = total_limit_duplicates + total_signal_duplicates + total_limit_backup_duplicates
    
    if total_duplicates_removed > 0:
        print(f" Total Duplicates Purged: {total_duplicates_removed}")
        print(f" Total Files Modified:    {total_files_cleaned}")
        print(f"\n Breakdown by file type:")
        print(f"   • limit_orders.json:        {total_limit_files_cleaned} files | {total_limit_duplicates} duplicates")
        print(f"   • limit_orders_backup.json: {total_limit_backup_files_cleaned} files | {total_limit_backup_duplicates} duplicates")
        print(f"   • signals.json:             {total_signal_files_cleaned} files | {total_signal_duplicates} duplicates")
    else:
        print(" ✅ Everything was already clean - no duplicates found!")
    print(f"{'='*33}\n")
    
    return any_duplicates_removed

def filter_unauthorized_symbols(inv_id=None):
    """
    Verifies and filters pending order files based on allowed symbols defined in accountmanagement.json.
    Now filters both limit_orders.json and signals.json files, removing any entries with unauthorized symbols.
    Matches sanitized versions of symbols to handle broker suffixes (e.g., EURUSDm vs EURUSD).
    
    Args:
        inv_id (str, optional): Specific investor ID to process. If None, processes all investors.
        This function does NOT require MT5.
    
    Returns:
        bool: True if any unauthorized symbols were removed, False otherwise
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

    # Determine which investors to process
    if inv_id:
        investor_ids = [inv_id]
    else:
        investor_ids = [f for f in os.listdir(INV_PATH) if os.path.isdir(os.path.join(INV_PATH, f))]
    
    if not investor_ids:
        print(" └─ 🔘 No investor directories found for filtering.")
        return False

    total_files_cleaned = 0
    total_entries_removed = 0
    total_limit_files_cleaned = 0
    total_signal_files_cleaned = 0
    total_limit_removed = 0
    total_signal_removed = 0
    any_symbols_removed = False

    for current_inv_id in investor_ids:
        print(f"\n [{current_inv_id}] 🔍 Verifying symbol permissions...")
        inv_folder = Path(INV_PATH) / current_inv_id
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

            print(f"  └─ ✅ Found {len(allowed_sanitized)} authorized symbols")

            # Search for pending_orders folders
            pending_orders_folders = list(inv_folder.rglob("*/pending_orders/"))
            
            investor_limit_removed = 0
            investor_signal_removed = 0
            investor_limit_files_cleaned = 0
            investor_signal_files_cleaned = 0

            for pending_folder in pending_orders_folders:
                # Process limit_orders.json
                limit_file = pending_folder / "limit_orders.json"
                if limit_file.exists():
                    try:
                        with open(limit_file, 'r', encoding='utf-8') as f:
                            orders = json.load(f)

                        if orders and isinstance(orders, list):
                            original_count = len(orders)
                            
                            # Filter: Keep only if the sanitized symbol exists in our allowed set
                            filtered_orders = [
                                order for order in orders 
                                if sanitize(order.get("symbol", "")) in allowed_sanitized
                            ]
                            
                            # Only write back if entries were actually removed
                            if len(filtered_orders) < original_count:
                                removed = original_count - len(filtered_orders)
                                with open(limit_file, 'w', encoding='utf-8') as f:
                                    json.dump(filtered_orders, f, indent=4)
                                
                                investor_limit_removed += removed
                                investor_limit_files_cleaned += 1
                                total_limit_removed += removed
                                total_limit_files_cleaned += 1
                                any_symbols_removed = True
                                
                                folder_name = pending_folder.parent.name
                                print(f"    └─ 📄 {folder_name}/limit_orders.json - Removed {removed} unauthorized entries")
                            elif original_count > 0:
                                folder_name = pending_folder.parent.name
                                print(f"    └─ ✅ {folder_name}/limit_orders.json - All symbols authorized ({original_count} entries)")

                    except Exception as e:
                        print(f"    └─ ❌ Error processing {limit_file}: {e}")

                # Process signals.json
                signals_file = pending_folder / "signals.json"
                if signals_file.exists():
                    try:
                        with open(signals_file, 'r', encoding='utf-8') as f:
                            signals = json.load(f)

                        if signals and isinstance(signals, list):
                            original_count = len(signals)
                            
                            # Filter: Keep only if the sanitized symbol exists in our allowed set
                            filtered_signals = [
                                signal for signal in signals 
                                if sanitize(signal.get("symbol", "")) in allowed_sanitized
                            ]
                            
                            # Only write back if entries were actually removed
                            if len(filtered_signals) < original_count:
                                removed = original_count - len(filtered_signals)
                                with open(signals_file, 'w', encoding='utf-8') as f:
                                    json.dump(filtered_signals, f, indent=4)
                                
                                investor_signal_removed += removed
                                investor_signal_files_cleaned += 1
                                total_signal_removed += removed
                                total_signal_files_cleaned += 1
                                any_symbols_removed = True
                                
                                folder_name = pending_folder.parent.name
                                print(f"    └─ 📄 {folder_name}/signals.json - Removed {removed} unauthorized entries")
                            elif original_count > 0:
                                folder_name = pending_folder.parent.name
                                print(f"    └─ ✅ {folder_name}/signals.json - All symbols authorized ({original_count} entries)")

                    except Exception as e:
                        print(f"    └─ ❌ Error processing {signals_file}: {e}")

            # Summary for the current investor
            if investor_limit_removed > 0 or investor_signal_removed > 0:
                print(f"\n  └─ ✨ Investor {current_inv_id} Filter Summary:")
                if investor_limit_removed > 0:
                    print(f"      • limit_orders.json: Cleaned {investor_limit_files_cleaned} files | Removed {investor_limit_removed} unauthorized entries")
                if investor_signal_removed > 0:
                    print(f"      • signals.json: Cleaned {investor_signal_files_cleaned} files | Removed {investor_signal_removed} unauthorized entries")
            else:
                # Check if any files were found at all
                if pending_orders_folders:
                    print(f"  └─ ✅ All symbols in order files are authorized")
                else:
                    print(f"  └─ 🔘 No pending_orders folders found")

        except Exception as e:
            print(f"  └─ ❌ Error processing {current_inv_id}: {e}")

    # Final Global Summary
    print(f"\n{'='*10} SYMBOL FILTERING COMPLETE {'='*10}")
    
    total_files_cleaned = total_limit_files_cleaned + total_signal_files_cleaned
    total_entries_removed = total_limit_removed + total_signal_removed
    
    if total_entries_removed > 0:
        print(f" Total Unauthorized Entries Removed: {total_entries_removed}")
        print(f" Total Files Modified:               {total_files_cleaned}")
        print(f"\n Breakdown by file type:")
        print(f"   • limit_orders.json:   {total_limit_files_cleaned} files | {total_limit_removed} entries removed")
        print(f"   • signals.json:        {total_signal_files_cleaned} files | {total_signal_removed} entries removed")
    else:
        if total_files_cleaned == 0:
            print(" ✅ No files needed filtering - all symbols were already authorized!")
        else:
            print(" ✅ All files checked and verified - no unauthorized symbols found!")
    print(f"{'='*39}\n")
    
    return any_symbols_removed

def filter_unauthorized_timeframes(inv_id=None):
    """
    Verifies and filters pending order files based on restricted timeframes defined in accountmanagement.json.
    Now filters both limit_orders.json and signals.json files, removing any entries with restricted timeframes.
    Matches the 'timeframe' key in order files against the 'restrict_order_from_timeframe' setting.
    
    Args:
        inv_id (str, optional): Specific investor ID to process. If None, processes all investors.
        This function does NOT require MT5.
    
    Returns:
        bool: True if any restricted timeframes were removed, False otherwise
    """
    print(f"\n{'='*10} 🛡️  TIMEFRAME AUTHORIZATION FILTER {'='*10}")

    def sanitize_tf(tf):
        if not tf: return ""
        # Ensure uniform comparison (lowercase, stripped)
        return str(tf).strip().lower()

    if not os.path.exists(INV_PATH):
        print(f" [!] Error: Investor path {INV_PATH} not found.")
        return False

    # Determine which investors to process
    if inv_id:
        investor_ids = [inv_id]
    else:
        investor_ids = [f for f in os.listdir(INV_PATH) if os.path.isdir(os.path.join(INV_PATH, f))]
    
    if not investor_ids:
        print(" └─ 🔘 No investor directories found for filtering.")
        return False

    total_files_cleaned = 0
    total_entries_removed = 0
    total_limit_files_cleaned = 0
    total_signal_files_cleaned = 0
    total_limit_removed = 0
    total_signal_removed = 0
    any_timeframes_removed = False

    for current_inv_id in investor_ids:
        print(f"\n [{current_inv_id}] 🔍 Checking timeframe restrictions...")
        inv_folder = Path(INV_PATH) / current_inv_id
        acc_mgmt_path = inv_folder / "accountmanagement.json"
        
        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️  Account config missing. Skipping.")
            continue

        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Extract restriction setting
            # Supports: "5m" OR ["1m", "5m"]
            raw_restrictions = config.get("settings", {}).get("restrict_order_from_timeframe", [])
            
            if isinstance(raw_restrictions, str):
                # Handle comma separated strings or single strings
                restricted_list = [s.strip() for s in raw_restrictions.split(',')]
            elif isinstance(raw_restrictions, list):
                restricted_list = raw_restrictions
            else:
                restricted_list = []

            restricted_set = {sanitize_tf(t) for t in restricted_list if t}

            if not restricted_set:
                print(f"  └─ ✅ No timeframe restrictions active.")
                continue

            print(f"  └─ 🚫 Restricted timeframes: {', '.join(restricted_set)}")

            # Search for pending_orders folders
            pending_orders_folders = list(inv_folder.rglob("*/pending_orders/"))
            
            investor_limit_removed = 0
            investor_signal_removed = 0
            investor_limit_files_cleaned = 0
            investor_signal_files_cleaned = 0

            for pending_folder in pending_orders_folders:
                # Process limit_orders.json
                limit_file = pending_folder / "limit_orders.json"
                if limit_file.exists():
                    try:
                        with open(limit_file, 'r', encoding='utf-8') as f:
                            orders = json.load(f)

                        if orders and isinstance(orders, list):
                            original_count = len(orders)
                            
                            # Filter: Keep only if the entry's timeframe is NOT in the restricted set
                            filtered_orders = [
                                order for order in orders 
                                if sanitize_tf(order.get("timeframe")) not in restricted_set
                            ]
                            
                            # Only write back if entries were actually removed
                            if len(filtered_orders) < original_count:
                                removed = original_count - len(filtered_orders)
                                with open(limit_file, 'w', encoding='utf-8') as f:
                                    json.dump(filtered_orders, f, indent=4)
                                
                                investor_limit_removed += removed
                                investor_limit_files_cleaned += 1
                                total_limit_removed += removed
                                total_limit_files_cleaned += 1
                                any_timeframes_removed = True
                                
                                folder_name = pending_folder.parent.name
                                print(f"    └─ 📄 {folder_name}/limit_orders.json - Removed {removed} restricted timeframe entries")
                            elif original_count > 0:
                                folder_name = pending_folder.parent.name
                                print(f"    └─ ✅ {folder_name}/limit_orders.json - All timeframes authorized ({original_count} entries)")

                    except Exception as e:
                        print(f"    └─ ❌ Error processing {limit_file}: {e}")

                # Process signals.json
                signals_file = pending_folder / "signals.json"
                if signals_file.exists():
                    try:
                        with open(signals_file, 'r', encoding='utf-8') as f:
                            signals = json.load(f)

                        if signals and isinstance(signals, list):
                            original_count = len(signals)
                            
                            # Filter: Keep only if the entry's timeframe is NOT in the restricted set
                            filtered_signals = [
                                signal for signal in signals 
                                if sanitize_tf(signal.get("timeframe")) not in restricted_set
                            ]
                            
                            # Only write back if entries were actually removed
                            if len(filtered_signals) < original_count:
                                removed = original_count - len(filtered_signals)
                                with open(signals_file, 'w', encoding='utf-8') as f:
                                    json.dump(filtered_signals, f, indent=4)
                                
                                investor_signal_removed += removed
                                investor_signal_files_cleaned += 1
                                total_signal_removed += removed
                                total_signal_files_cleaned += 1
                                any_timeframes_removed = True
                                
                                folder_name = pending_folder.parent.name
                                print(f"    └─ 📄 {folder_name}/signals.json - Removed {removed} restricted timeframe entries")
                            elif original_count > 0:
                                folder_name = pending_folder.parent.name
                                print(f"    └─ ✅ {folder_name}/signals.json - All timeframes authorized ({original_count} entries)")

                    except Exception as e:
                        print(f"    └─ ❌ Error processing {signals_file}: {e}")

            # Summary for the current investor
            if investor_limit_removed > 0 or investor_signal_removed > 0:
                print(f"\n  └─ ✨ Investor {current_inv_id} Filter Summary:")
                if investor_limit_removed > 0:
                    print(f"      • limit_orders.json: Cleaned {investor_limit_files_cleaned} files | Removed {investor_limit_removed} restricted entries")
                if investor_signal_removed > 0:
                    print(f"      • signals.json: Cleaned {investor_signal_files_cleaned} files | Removed {investor_signal_removed} restricted entries")
                print(f"     (Blocked timeframes: {', '.join(restricted_set)})")
            else:
                # Check if any files were found at all
                if pending_orders_folders:
                    print(f"  └─ ✅ All timeframes in order files are authorized")
                else:
                    print(f"  └─ 🔘 No pending_orders folders found")

        except Exception as e:
            print(f"  └─ ❌ Error processing {current_inv_id}: {e}")

    # Final Global Summary
    print(f"\n{'='*10} TIMEFRAME FILTERING COMPLETE {'='*10}")
    
    total_files_cleaned = total_limit_files_cleaned + total_signal_files_cleaned
    total_entries_removed = total_limit_removed + total_signal_removed
    
    if total_entries_removed > 0:
        print(f" Total Restricted Entries Removed: {total_entries_removed}")
        print(f" Total Files Modified:              {total_files_cleaned}")
        print(f"\n Breakdown by file type:")
        print(f"   • limit_orders.json:   {total_limit_files_cleaned} files | {total_limit_removed} entries removed")
        print(f"   • signals.json:        {total_signal_files_cleaned} files | {total_signal_removed} entries removed")
    else:
        if total_files_cleaned == 0:
            print(" ✅ No files needed filtering - no restricted timeframes found!")
        else:
            print(" ✅ All files checked and verified - no restricted timeframes found!")
    print(f"{'='*41}\n")
    
    return any_timeframes_removed

def populate_orders_missing_fields(inv_id=None, callback_function=None):
    print(f"\n{'='*10} 📊 POPULATING ORDER FIELDS {'='*10}")
    
    total_files_updated = 0
    total_orders_updated = 0
    total_symbols_normalized = 0
    inv_base_path = Path(INV_PATH)

    investor_folders = [inv_base_path / inv_id] if inv_id else [f for f in inv_base_path.iterdir() if f.is_dir()]
    
    for inv_folder in investor_folders:
        current_inv_id = inv_folder.name
        print(f" [{current_inv_id}] 🔍 Processing orders...")

        # Local Cache for this investor to prevent redundant lookups
        # Format: { "raw_symbol": {"broker_sym": "normalized", "info": mt5_obj} }
        resolution_cache = {}

        order_files = list(inv_folder.rglob("*/pending_orders/limit_orders.json"))
        if not order_files: continue
            
        broker_cfg = usersdictionary.get(current_inv_id)
        if not broker_cfg: continue
            
        server = broker_cfg.get('SERVER', '')
        broker_prefix = server.split('-')[0].split('.')[0].lower() if server else 'broker'
        v_field, ts_field, tv_field = f"{broker_prefix}_volume", f"{broker_prefix}_tick_size", f"{broker_prefix}_tick_value"

        for file_path in order_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    orders = json.load(f)
                if not orders: continue
                
                modified = False
                for order in orders:
                    raw_symbol = order.get("symbol")
                    if not raw_symbol: continue

                    # Check Cache First
                    if raw_symbol in resolution_cache:
                        res = resolution_cache[raw_symbol]
                        broker_symbol = res['broker_sym']
                        symbol_info = res['info']
                    else:
                        # Perform mapping only once
                        broker_symbol = get_normalized_symbol(raw_symbol)
                        symbol_info = mt5.symbol_info(broker_symbol)
                        
                        resolution_cache[raw_symbol] = {'broker_sym': broker_symbol, 'info': symbol_info}
                        
                        # Detailed Log only on first discovery
                        if symbol_info:
                            if broker_symbol != raw_symbol:
                                print(f"    └─ ✅ {raw_symbol} -> {broker_symbol} (Mapped & Cached)")
                                total_symbols_normalized += 1
                        else:
                            print(f"    └─ ❌ MT5: '{broker_symbol}' (from '{raw_symbol}') not found in MarketWatch")

                    if symbol_info:
                        order['symbol'] = broker_symbol
                        
                        # Cleanup and Update
                        for key in list(order.keys()):
                            if any(x in key.lower() for x in ['volume', 'tick_size', 'tick_value']) and key not in [v_field, ts_field, tv_field]:
                                del order[key]

                        order[v_field] = symbol_info.volume_min
                        order[ts_field] = symbol_info.trade_tick_size
                        order[tv_field] = symbol_info.trade_tick_value
                        total_orders_updated += 1
                        modified = True

                if modified:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(orders, f, indent=4)
                    total_files_updated += 1

            except Exception as e:
                print(f"  └─ ❌ Error: {e}")

    print(f"\n{'='*10} POPULATION COMPLETE {'='*10}")
    print(f" Total Orders Updated:      {total_orders_updated}")
    print(f" Total Symbols Normalized:  {total_symbols_normalized}")
    return True

def activate_usd_based_risk_on_empty_pricelevels(inv_id=None):
    print(f"\n{'='*10} 📊 INVESTOR EMPTY TARGET CHECK - USD RISK ENFORCEMENT {'='*10}")
    
    total_orders_processed = 0
    total_orders_enforced = 0
    total_files_updated = 0
    inv_base_path = Path(INV_PATH)

    if not inv_base_path.exists():
        print(f" [!] Error: Investor path {INV_PATH} does not exist.")
        return False

    investor_folders = [inv_base_path / inv_id] if inv_id else [f for f in inv_base_path.iterdir() if f.is_dir()]
    
    for inv_folder in investor_folders:
        current_inv_id = inv_folder.name
        print(f"\n [{current_inv_id}] 🔍 Processing empty target check...")

        # Cache for risk mappings to avoid re-calculating family logic 1000s of times
        risk_map_cache = {}

        broker_cfg = usersdictionary.get(current_inv_id)
        if not broker_cfg:
            print(f"  └─ ❌ No broker config found")
            continue
        
        broker_name = broker_cfg.get('BROKER_NAME', '').lower() or \
                      broker_cfg.get('SERVER', 'default').split('-')[0].split('.')[0].lower()

        default_config_path = Path(DEFAULT_PATH) / f"{broker_name}_default_allowedsymbolsandvolumes.json"
        
        risk_lookup = {}
        if default_config_path.exists():
            try:
                with open(default_config_path, 'r', encoding='utf-8') as f:
                    default_config = json.load(f)
                    for category, items in default_config.items():
                        if not isinstance(items, list): continue
                        for item in items:
                            sym = str(item.get("symbol", "")).upper()
                            if sym:
                                risk_lookup[sym] = {
                                    k.replace("_specs", "").upper(): v.get("usd_risk", 0)
                                    for k, v in item.items() if k.endswith("_specs")
                                }
                print(f"  └─ ✅ Loaded risk config for {len(risk_lookup)} symbols")
            except Exception as e:
                print(f"  └─ ❌ Risk config error: {e}")
                continue

        known_risk_symbols = list(risk_lookup.keys())
        order_files = list(inv_folder.rglob("*/pending_orders/limit_orders.json"))
        signals_files = list(inv_folder.rglob("*/signals/signals.json"))
        
        for file_list, label in [(order_files, "LIMITS"), (signals_files, "SIGNALS")]:
            for file_path in file_list:
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    if not data: continue
                    modified = False
                    
                    for item in data:
                        if item.get('exit') in [0, "0", None, 0.0] and \
                           item.get('target') in [0, "0", None, 0.0]:
                            
                            total_orders_processed += 1
                            raw_sym = str(item.get('symbol', '')).upper()
                            raw_tf = str(item.get('timeframe', '')).upper()
                            
                            # Cache Logic for Risk Mapping
                            if raw_sym not in risk_map_cache:
                                matched_sym = get_normalized_symbol(raw_sym, risk_keys=known_risk_symbols)
                                risk_map_cache[raw_sym] = matched_sym
                                
                                # Print mapping only once per symbol type
                                if matched_sym not in risk_lookup:
                                    print(f"      ❌ [{label}] {raw_sym}: Not in risk config (Mapped as: {matched_sym})")
                            else:
                                matched_sym = risk_map_cache[raw_sym]
                            
                            if matched_sym in risk_lookup:
                                tf_risks = risk_lookup[matched_sym]
                                risk_value = tf_risks.get(raw_tf, 0)
                                
                                if risk_value > 0:
                                    item.update({
                                        'exit': 0, 'target': 0, 'usd_risk': risk_value,
                                        'usd_based_risk_only': True, 'symbol': matched_sym
                                    })
                                    modified = True
                                    total_orders_enforced += 1
                                    
                                    # Logic to avoid spamming the same enforcement 1000 times in logs
                                    # Only log the first time we enforce this symbol/tf pair for this file
                                    if f"{raw_sym}_{raw_tf}" not in risk_map_cache:
                                        print(f"      ✅ [{label}] {matched_sym} ({raw_sym}) {raw_tf}: Enforced ${risk_value} risk")
                                        risk_map_cache[f"{raw_sym}_{raw_tf}"] = True
                                
                    if modified:
                        with open(file_path, 'w', encoding='utf-8') as f:
                            json.dump(data, f, indent=4)
                        total_files_updated += 1

                except Exception as e:
                    print(f"    └─ ❌ Error processing {file_path.name}: {e}")

    print(f"\n{'='*10} ENFORCEMENT COMPLETE {'='*10}")
    print(f" Total Targetless Found:  {total_orders_processed}")
    print(f" Total Risk Enforced:    {total_orders_enforced}")
    print(f" Files Updated:          {total_files_updated}")
    
    return total_orders_enforced > 0

def enforce_investors_risk(inv_id=None):
    """
    Enforces risk rules for investors based on accountmanagement.json settings.
    Enhanced with Smart Normalization Caching and optimized lookup logic.
    """
    print(f"\n{'='*10} 📊 SMART INVESTOR RISK ENFORCEMENT {'='*10}")
    
    total_orders_processed = 0
    total_orders_enforced = 0
    total_files_updated = 0
    total_symbols_normalized = 0
    inv_base_path = Path(INV_PATH)

    if not inv_base_path.exists():
        print(f" [!] Error: Investor path {INV_PATH} does not exist.")
        return False

    investor_folders = [inv_base_path / inv_id] if inv_id else [f for f in inv_base_path.iterdir() if f.is_dir()]
    
    if not investor_folders:
        print(" └─ 🔘 No investor directories found.")
        return False

    any_orders_enforced = False
    
    for inv_folder in investor_folders:
        current_inv_id = inv_folder.name
        
        # --- INVESTOR LOCAL CACHE ---
        # Stores: { "RAW_SYM": {"matched": "NORM_SYM", "is_norm": True/False, "risk": {TF_DATA}} }
        resolution_cache = {}
        
        print(f"\n [{current_inv_id}] 🔍 Initializing smart enforcement...")

        # 1. Load accountmanagement.json
        acc_mgmt_path = inv_folder / "accountmanagement.json"
        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️  accountmanagement.json not found, skipping")
            continue

        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                acc_mgmt_data = json.load(f)
            
            enforce_default = acc_mgmt_data.get("settings", {}).get("enforce_default_usd_risk", False)
            print(f"  └─ 🎯 Master Switch: {enforce_default}")
            
            if not enforce_default:
                print(f"  └─ ⏭️  Master switch is OFF - skipping")
                continue
        except Exception as e:
            print(f"  └─ ❌ Failed to load accountmanagement.json: {e}")
            continue

        # 2. Get Broker and Config Path
        broker_cfg = usersdictionary.get(current_inv_id)
        if not broker_cfg:
            continue
        
        broker_name = broker_cfg.get('BROKER_NAME', '').lower() or \
                      broker_cfg.get('SERVER', 'default').split('-')[0].split('.')[0].lower()

        default_config_path = Path(DEFAULT_PATH) / f"{broker_name}_default_allowedsymbolsandvolumes.json"
        if not default_config_path.exists():
            print(f"  └─ ❌ Default config not found: {default_config_path.name}")
            continue
        
        # 3. Build Risk Lookup Table
        risk_lookup = {}
        try:
            with open(default_config_path, 'r', encoding='utf-8') as f:
                default_config = json.load(f)
                for category, items in default_config.items():
                    if not isinstance(items, list): continue
                    for item in items:
                        sym = str(item.get("symbol", "")).upper()
                        if sym:
                            risk_lookup[sym] = {
                                k.replace("_specs", "").upper(): {
                                    "volume": v.get("volume", 0.01),
                                    "usd_risk": v.get("usd_risk", 0)
                                } for k, v in item.items() if k.endswith("_specs")
                            }
            known_risk_symbols = list(risk_lookup.keys())
            print(f"  └─ ✅ Loaded risk config for {len(risk_lookup)} symbols")
        except Exception as e:
            print(f"  └─ ❌ Failed to parse default config: {e}")
            continue

        # 4. Gather Files
        order_files = list(inv_folder.rglob("*/pending_orders/limit_orders.json"))
        signals_files = list(inv_folder.rglob("*/signals/signals.json"))
        
        investor_orders_enforced = 0
        investor_files_updated = 0
        
        # 5. Process Unified Pipeline
        for file_list, label in [(order_files, "LIMITS"), (signals_files, "SIGNALS")]:
            for file_path in file_list:
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    if not data: continue
                    
                    modified = False
                    for item in data:
                        total_orders_processed += 1
                        raw_sym = str(item.get('symbol', '')).upper()
                        raw_tf = str(item.get('timeframe', '')).upper()
                        
                        # --- SMART RESOLUTION LOGIC ---
                        if raw_sym not in resolution_cache:
                            # Helper does the heavy lifting: USOUSD -> USOIL
                            matched_sym = get_normalized_symbol(raw_sym, risk_keys=known_risk_symbols)
                            was_normalized = (matched_sym != raw_sym)
                            
                            # Cache the result
                            resolution_cache[raw_sym] = {
                                "matched": matched_sym,
                                "is_norm": was_normalized,
                                "risk_data": risk_lookup.get(matched_sym, {})
                            }
                            
                            # Log first-time discovery
                            if was_normalized and matched_sym in risk_lookup:
                                print(f"    └─ ✅ Normalized: {raw_sym} -> {matched_sym}")
                                total_symbols_normalized += 1
                        
                        res = resolution_cache[raw_sym]
                        matched_sym = res["matched"]
                        tf_data = res["risk_data"].get(raw_tf)

                        if tf_data and tf_data["usd_risk"] > 0:
                            # Apply Enforcement
                            item.update({
                                'exit': 0,
                                'target': 0,
                                'usd_risk': tf_data["usd_risk"],
                                'usd_based_risk_only': True,
                                'symbol': matched_sym
                            })
                            
                            # Update volume if specified
                            if tf_data["volume"] > 0:
                                for key in list(item.keys()):
                                    if 'volume' in key.lower():
                                        item[key] = tf_data["volume"]
                                        break
                                        
                            modified = True
                            investor_orders_enforced += 1
                            total_orders_enforced += 1
                        
                    if modified:
                        with open(file_path, 'w', encoding='utf-8') as f:
                            json.dump(data, f, indent=4)
                        investor_files_updated += 1
                        total_files_updated += 1
                        
                except Exception as e:
                    print(f"    └─ ❌ Error in {file_path.name}: {e}")

        # Summary for this investor
        if investor_orders_enforced > 0:
            any_orders_enforced = True
            print(f"  └─ 📊 {current_inv_id} Complete: Enforced {investor_orders_enforced} orders across {investor_files_updated} files.")

    # Final Global Summary
    print(f"\n{'='*10} RISK ENFORCEMENT COMPLETE {'='*10}")
    print(f" Total Files Updated:   {total_files_updated}")
    print(f" Total Enforced:        {total_orders_enforced} / {total_orders_processed}")
    print(f" Symbols Normalized:    {total_symbols_normalized}")
    print(f"{'='*50}\n")
    
    return any_orders_enforced
    
def calculate_investor_symbols_orders(inv_id=None, callback_function=None):
    """
    Calculates Exit/Target prices for ALL orders in limit_orders.json files for investors.
    Uses the selected_risk_reward value from accountmanagement.json for each investor.
    
    Args:
        inv_id (str, optional): Specific investor ID to process. If None, processes all investors.
        callback_function (callable, optional): A function to call with the opened file data.
            The callback will receive (inv_id, file_path, orders_list) parameters.
    
    Returns:
        bool: True if any orders were calculated, False otherwise
    """
    print(f"\n{'='*10} 📊 CALCULATING INVESTOR ORDER PRICES {'='*10}")
    
    total_files_updated = 0
    total_orders_processed = 0
    total_orders_calculated = 0
    total_orders_skipped = 0
    total_symbols_normalized = 0
    inv_base_path = Path(INV_PATH)

    if not inv_base_path.exists():
        print(f" [!] Error: Investor path {INV_PATH} does not exist.")
        return False

    # Determine which investors to process
    if inv_id:
        inv_folder = inv_base_path / inv_id
        investor_folders = [inv_folder] if inv_folder.exists() else []
        if not investor_folders:
            print(f" [!] Error: Investor folder {inv_id} does not exist.")
            return False
    else:
        investor_folders = [f for f in inv_base_path.iterdir() if f.is_dir()]
    
    if not investor_folders:
        print(" └─ 🔘 No investor directories found.")
        return False

    any_orders_calculated = False

    for inv_folder in investor_folders:
        current_inv_id = inv_folder.name
        print(f" [{current_inv_id}] 🔍 Processing orders...")

        # --- INVESTOR LOCAL CACHE for symbol normalization ---
        resolution_cache = {}

        # 1. Load accountmanagement.json to get selected_risk_reward
        acc_mgmt_path = inv_folder / "accountmanagement.json"
        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️  accountmanagement.json not found for {current_inv_id}, skipping")
            continue

        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                acc_mgmt_data = json.load(f)
            
            # Get selected_risk_reward value (default to 1.0 if not found)
            selected_rr = acc_mgmt_data.get("selected_risk_reward", [1.0])
            if isinstance(selected_rr, list) and len(selected_rr) > 0:
                rr_ratio = float(selected_rr[0])
            else:
                rr_ratio = float(selected_rr) if selected_rr else 1.0
            
            print(f"  └─ 📊 Using selected R:R ratio: {rr_ratio}")
            
        except Exception as e:
            print(f"  └─ ❌ Failed to load accountmanagement.json: {e}")
            continue

        # 2. Get broker config for potential symbol mapping context
        broker_cfg = usersdictionary.get(current_inv_id)
        if not broker_cfg:
            print(f"  └─ ⚠️  No broker config found for {current_inv_id}")
            # Continue anyway as normalization might still work

        # 3. Find all limit_orders.json files
        order_files = list(inv_folder.rglob("*/pending_orders/limit_orders.json"))
        
        if not order_files:
            print(f"  └─ 🔘 No limit order files found")
            continue
            
        investor_files_updated = 0
        investor_orders_processed = 0
        investor_orders_calculated = 0
        investor_orders_skipped = 0
        
        # Process each file individually
        for file_path in order_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    orders = json.load(f)
                
                if not orders:
                    continue
                
                # Call callback function if provided with the original data
                if callback_function:
                    try:
                        callback_function(current_inv_id, file_path, orders)
                    except Exception as e:
                        print(f"    └─ ⚠️  Callback error for {file_path.name}: {e}")
                
                # Track original orders for this file
                original_count = len(orders)
                investor_orders_processed += original_count
                
                # Process each order in this file
                orders_updated = False
                file_orders_calculated = 0
                file_orders_skipped = 0
                
                for order in orders:
                    try:
                        # --- SYMBOL NORMALIZATION with Caching ---
                        raw_symbol = order.get("symbol", "")
                        if not raw_symbol:
                            file_orders_skipped += 1
                            continue
                        
                        # Check Cache First
                        if raw_symbol in resolution_cache:
                            normalized_symbol = resolution_cache[raw_symbol]
                        else:
                            # Perform mapping only once
                            normalized_symbol = get_normalized_symbol(raw_symbol)
                            resolution_cache[raw_symbol] = normalized_symbol
                            
                            # Log normalization on first discovery
                            if normalized_symbol != raw_symbol:
                                print(f"    └─ ✅ {raw_symbol} -> {normalized_symbol} (Mapped & Cached)")
                                total_symbols_normalized += 1
                        
                        # Update the symbol in the order
                        if normalized_symbol:
                            order['symbol'] = normalized_symbol
                        
                        # --- CHECK FOR USD-BASED RISK FIRST (doesn't require volume) ---
                        if order.get("usd_based_risk_only") is True:
                            risk_val = float(order.get("usd_risk", 0))
                            
                            if risk_val > 0:
                                # For USD-based, we need volume but it might be named differently
                                # Try to find volume field
                                volume_value = None
                                for key, value in order.items():
                                    if 'volume' in key.lower() and isinstance(value, (int, float)):
                                        volume_value = float(value)
                                        break
                                
                                if volume_value is None or volume_value <= 0:
                                    print(f"      ⚠️  USD-based order missing volume for {order.get('symbol', 'Unknown')}, skipping")
                                    file_orders_skipped += 1
                                    continue
                                
                                # Find tick_size field
                                tick_size_value = None
                                for key, value in order.items():
                                    if 'tick_size' in key.lower() and isinstance(value, (int, float)):
                                        tick_size_value = float(value)
                                        break
                                
                                if tick_size_value is None or tick_size_value <= 0:
                                    tick_size_value = 0.00001
                                    print(f"      ⚠️  No tick_size found for {order.get('symbol', 'Unknown')}, using default")
                                
                                # Find tick_value field
                                tick_value_value = None
                                for key, value in order.items():
                                    if 'tick_value' in key.lower() and isinstance(value, (int, float)):
                                        tick_value_value = float(value)
                                        break
                                
                                if tick_value_value is None or tick_value_value <= 0:
                                    tick_value_value = 1.0
                                    print(f"      ⚠️  No tick_value found for {order.get('symbol', 'Unknown')}, using default")
                                
                                # Extract required order data
                                entry = float(order.get('entry', 0))
                                if entry == 0:
                                    file_orders_skipped += 1
                                    continue
                                    
                                order_type = str(order.get('order_type', '')).upper()
                                
                                # Calculate digits for rounding based on tick_size
                                if tick_size_value < 1:
                                    digits = len(str(tick_size_value).split('.')[-1])
                                else:
                                    digits = 0
                                
                                # Calculate using USD risk
                                sl_dist = (risk_val * tick_size_value) / (tick_value_value * volume_value)
                                tp_dist = sl_dist * rr_ratio
                                
                                if "BUY" in order_type:
                                    order["exit"] = round(entry - sl_dist, digits)
                                    order["target"] = round(entry + tp_dist, digits)
                                elif "SELL" in order_type:
                                    order["exit"] = round(entry + sl_dist, digits)
                                    order["target"] = round(entry - tp_dist, digits)
                                else:
                                    file_orders_skipped += 1
                                    continue
                                
                                file_orders_calculated += 1
                                any_orders_calculated = True
                                
                                # Update metadata
                                order['risk_reward'] = rr_ratio
                                order['status'] = "Calculated"
                                order['calculated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                orders_updated = True
                                continue  # Skip the rest of the processing for this order
                            else:
                                file_orders_skipped += 1
                                continue
                        
                        # --- NON-USD BASED ORDERS (require volume) ---
                        # Check for required volume field
                        volume_field = None
                        volume_value = None
                        
                        for key, value in order.items():
                            if 'volume' in key.lower() and isinstance(value, (int, float)):
                                volume_field = key
                                volume_value = float(value)
                                break
                        
                        if volume_value is None or volume_value <= 0:
                            file_orders_skipped += 1
                            continue
                        
                        # Find tick_size field
                        tick_size_field = None
                        tick_size_value = None
                        
                        for key, value in order.items():
                            if 'tick_size' in key.lower() and isinstance(value, (int, float)):
                                tick_size_field = key
                                tick_size_value = float(value)
                                break
                        
                        if tick_size_value is None or tick_size_value <= 0:
                            tick_size_value = 0.00001
                            print(f"      ⚠️  No tick_size found for {order.get('symbol', 'Unknown')}, using default")
                        
                        # Find tick_value field
                        tick_value_field = None
                        tick_value_value = None
                        
                        for key, value in order.items():
                            if 'tick_value' in key.lower() and isinstance(value, (int, float)):
                                tick_value_field = key
                                tick_value_value = float(value)
                                break
                        
                        if tick_value_value is None or tick_value_value <= 0:
                            tick_value_value = 1.0
                            print(f"      ⚠️  No tick_value found for {order.get('symbol', 'Unknown')}, using default")
                        
                        # Extract required order data
                        entry = float(order.get('entry', 0))
                        if entry == 0:
                            file_orders_skipped += 1
                            continue
                            
                        order_type = str(order.get('order_type', '')).upper()
                        
                        # Calculate digits for rounding based on tick_size
                        if tick_size_value < 1:
                            digits = len(str(tick_size_value).split('.')[-1])
                        else:
                            digits = 0
                        
                        # Standard calculation based on exit or target
                        sl_price = float(order.get('exit', 0))
                        tp_price = float(order.get('target', 0))
                        
                        # Case 1: Target provided, need to calculate exit
                        if sl_price == 0 and tp_price > 0:
                            risk_dist = abs(tp_price - entry) / rr_ratio
                            if "BUY" in order_type:
                                order['exit'] = round(entry - risk_dist, digits)
                            elif "SELL" in order_type:
                                order['exit'] = round(entry + risk_dist, digits)
                            else:
                                file_orders_skipped += 1
                                continue
                            
                            file_orders_calculated += 1
                            any_orders_calculated = True
                        
                        # Case 2: Exit provided, need to calculate target
                        elif sl_price > 0:
                            risk_dist = abs(entry - sl_price)
                            if "BUY" in order_type:
                                order['target'] = round(entry + (risk_dist * rr_ratio), digits)
                            elif "SELL" in order_type:
                                order['target'] = round(entry - (risk_dist * rr_ratio), digits)
                            else:
                                file_orders_skipped += 1
                                continue
                            
                            file_orders_calculated += 1
                            any_orders_calculated = True
                            print(f"      ✅ Exit-based: {order.get('symbol')} - Target calculated: {order['target']}")
                        
                        # Case 3: Neither exit nor target provided, skip
                        else:
                            file_orders_skipped += 1
                            continue
                        
                        # --- METADATA UPDATES ---
                        order['risk_reward'] = rr_ratio
                        order['status'] = "Calculated"
                        order['calculated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        orders_updated = True
                        
                    except (ValueError, KeyError, TypeError, ZeroDivisionError) as e:
                        file_orders_skipped += 1
                        print(f"      ⚠️  Error processing order {order.get('symbol', 'Unknown')}: {e}")
                        continue
                
                # Save the updated orders back to the same file
                if orders_updated:
                    try:
                        with open(file_path, 'w', encoding='utf-8') as f:
                            json.dump(orders, f, indent=4)
                        
                        investor_files_updated += 1
                        total_files_updated += 1
                        
                        # Update counters
                        investor_orders_calculated += file_orders_calculated
                        investor_orders_skipped += file_orders_skipped
                        
                        print(f"    └─ 📁 {file_path.parent.name}/limit_orders.json: "
                              f"Processed: {original_count}, Calculated: {file_orders_calculated}, "
                              f"Skipped: {file_orders_skipped}")
                        
                    except Exception as e:
                        print(f"    └─ ❌ Failed to save {file_path}: {e}")
                
            except Exception as e:
                print(f"    └─ ❌ Error reading {file_path}: {e}")
                continue
        
        # Summary for current investor
        if investor_orders_processed > 0:
            total_orders_processed += investor_orders_processed
            total_orders_calculated += investor_orders_calculated
            total_orders_skipped += investor_orders_skipped
            
            print(f"  └─ ✨ Investor {current_inv_id} Summary:")
            print(f"      Files updated: {investor_files_updated}")
            print(f"      Orders processed: {investor_orders_processed}")
            print(f"      Orders calculated: {investor_orders_calculated}")
            print(f"      Orders skipped: {investor_orders_skipped}")
            
            if investor_orders_processed > 0:
                calc_rate = (investor_orders_calculated / investor_orders_processed) * 100
                print(f"      Calculation rate: {calc_rate:.1f}%")
        else:
            print(f"  └─ ⚠️  No orders processed for {current_inv_id}")

    # Final Global Summary
    print(f"\n{'='*10} INVESTOR CALCULATION COMPLETE {'='*10}")
    if total_orders_processed > 0:
        print(f" Total Files Modified:    {total_files_updated}")
        print(f" Total Orders Processed:  {total_orders_processed}")
        print(f" Total Orders Calculated: {total_orders_calculated}")
        print(f" Total Orders Skipped:    {total_orders_skipped}")
        print(f" Symbols Normalized:      {total_symbols_normalized}")
        
        if total_orders_processed > 0:
            overall_rate = (total_orders_calculated / total_orders_processed) * 100
            print(f" Overall Calculation Rate: {overall_rate:.1f}%")
    else:
        print(" No orders were processed.")
    
    return any_orders_calculated

def live_usd_risk_and_scaling(inv_id=None, callback_function=None):
    """
    Calculates and populates the live USD risk for all orders in pending_orders/limit_orders.json files.
    Scales volume to meet account balance risk requirements from accountmanagement.json.
    Moves qualifying orders to signals.json when risk is within tolerance.
    
    Args:
        inv_id (str, optional): Specific investor ID to process. If None, processes all investors.
        callback_function (callable, optional): A function to call with the opened file data.
            The callback will receive (inv_id, file_path, orders_list) parameters.
        MT5 should already be initialized and logged in for this investor.
    
    Returns:
        bool: True if any orders were processed, False otherwise
    """
    print(f"\n{'='*10} 💰 CALCULATING LIVE USD RISK WITH VOLUME SCALING {'='*10}")
    
    total_files_updated = 0
    total_orders_updated = 0
    total_risk_usd = 0.0
    total_signals_created = 0
    total_symbols_normalized = 0
    inv_base_path = Path(INV_PATH)

    if not inv_base_path.exists():
        print(f" [!] Error: Investor path {INV_PATH} does not exist.")
        return False

    # Determine which investors to process
    if inv_id:
        inv_folder = inv_base_path / inv_id
        investor_folders = [inv_folder] if inv_folder.exists() else []
        if not investor_folders:
            print(f" [!] Error: Investor folder {inv_id} does not exist.")
            return False
    else:
        investor_folders = [f for f in inv_base_path.iterdir() if f.is_dir()]
    
    if not investor_folders:
        print(" └─ 🔘 No investor directories found.")
        return False

    any_orders_processed = False

    for inv_folder in investor_folders:
        current_inv_id = inv_folder.name
        print(f"\n [{current_inv_id}] 🔍 Processing USD risk calculations with volume scaling...")

        # --- INVESTOR LOCAL CACHE for symbol normalization ---
        resolution_cache = {}

        # 1. Load account management data
        account_mgmt_path = inv_folder / "accountmanagement.json"
        risk_ranges = {}
        account_balance = None
        
        if account_mgmt_path.exists():
            try:
                with open(account_mgmt_path, 'r', encoding='utf-8') as f:
                    account_data = json.load(f)
                risk_ranges = account_data.get('account_balance_default_risk_management', {})
                print(f"  └─ 📊 Loaded risk management ranges: {len(risk_ranges)} ranges")
            except Exception as e:
                print(f"  └─ ⚠️  Could not load accountmanagement.json: {e}")
        else:
            print(f"  └─ ⚠️  No accountmanagement.json found, skipping risk-based scaling")
            continue

        # 2. Get broker config for this investor
        broker_cfg = usersdictionary.get(current_inv_id)
        if not broker_cfg:
            print(f"  └─ ❌ No broker config found for {current_inv_id}")
            continue
        
        # Get account balance directly from broker (MT5 should already be initialized)
        account_info = mt5.account_info()
        if account_info:
            account_balance = account_info.balance
            print(f"  └─ 💵 Live account balance: ${account_balance:,.2f}")
        else:
            print(f"  └─ ⚠️  Could not fetch account balance from broker")
            continue
        
        # Determine required risk from balance using ranges
        required_risk = 0
        tolerance_min = 0
        tolerance_max = 0
        
        # Parse risk ranges to find matching balance
        for range_str, risk_value in risk_ranges.items():
            try:
                # Parse range like "10-20.99_risk"
                if '_risk' in range_str:
                    range_part = range_str.replace('_risk', '')
                    if '-' in range_part:
                        min_val, max_val = map(float, range_part.split('-'))
                        if min_val <= account_balance <= max_val:
                            required_risk = float(risk_value)
                            # Set tolerance: required_risk + up to 0.99
                            tolerance_min = required_risk
                            tolerance_max = required_risk + 0.99
                            print(f"  └─ 🎯 Balance ${account_balance:,.2f} falls in range {range_part}")
                            print(f"  └─ 🎯 Required risk: ${required_risk:.2f} (tolerance: ${tolerance_min:.2f} - ${tolerance_max:.2f})")
                            break
            except Exception as e:
                continue
        
        if required_risk == 0:
            print(f"  └─ ⚠️  No matching risk range found for balance ${account_balance:,.2f}")
            continue
        
        # 3. Find all limit_orders.json files
        order_files = list(inv_folder.rglob("*/pending_orders/limit_orders.json"))
        
        if not order_files:
            print(f"  └─ 🔘 No limit order files found")
            continue
            
        investor_files_updated = 0
        investor_orders_updated = 0
        investor_risk_usd = 0.0
        investor_signals_count = 0
        
        # Get broker prefix for field names
        broker_prefix = broker_cfg.get('BROKER_NAME', '').lower()
        if not broker_prefix:
            server = broker_cfg.get('SERVER', '')
            broker_prefix = server.split('-')[0].split('.')[0].lower() if server else 'broker'
        
        print(f"  └─ 🏷️  Using broker prefix: '{broker_prefix}' for field names")
        
        # Process each file
        signals_orders = []  # Collect orders that meet risk criteria for signals.json
        
        for file_path in order_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    orders = json.load(f)
                
                if not orders:
                    continue
                
                # Call callback if provided
                if callback_function:
                    try:
                        callback_function(current_inv_id, file_path, orders)
                    except Exception as e:
                        print(f"    └─ ⚠️  Callback error: {e}")
                
                orders_modified = False
                file_risk_total = 0.0
                file_signals = []
                
                # Process each order
                for order in orders:
                    # --- SYMBOL NORMALIZATION with Caching ---
                    raw_symbol = order.get("symbol", "")
                    if not raw_symbol:
                        continue
                    
                    # Check Cache First
                    if raw_symbol in resolution_cache:
                        normalized_symbol = resolution_cache[raw_symbol]
                    else:
                        # Perform mapping only once
                        normalized_symbol = get_normalized_symbol(raw_symbol)
                        resolution_cache[raw_symbol] = normalized_symbol
                        
                        # Log normalization on first discovery
                        if normalized_symbol != raw_symbol:
                            print(f"    └─ ✅ {raw_symbol} -> {normalized_symbol} (Mapped & Cached)")
                            total_symbols_normalized += 1
                    
                    # Update the symbol in the order
                    if normalized_symbol:
                        order['symbol'] = normalized_symbol
                        symbol = normalized_symbol
                    else:
                        symbol = raw_symbol
                    
                    # Skip if required fields are missing
                    volume_field = f"{broker_prefix}_volume"
                    tick_size_field = f"{broker_prefix}_tick_size"
                    tick_value_field = f"{broker_prefix}_tick_value"
                    
                    # Get current volume (might be scaled from previous runs)
                    current_volume = order.get(volume_field)
                    tick_size = order.get(tick_size_field)
                    tick_value = order.get(tick_value_field)
                    
                    if None in (current_volume, tick_size, tick_value):
                        print(f"    └─ ⚠️  Missing broker fields for {symbol}, skipping")
                        continue
                    
                    # Get current market price
                    symbol_info = mt5.symbol_info(symbol)
                    
                    if not symbol_info:
                        print(f"    └─ ⚠️  Could not fetch current price for {symbol}, skipping")
                        continue
                    
                    entry_price = order.get("entry")
                    exit_price = order.get("exit")
                    
                    if not entry_price or not exit_price:
                        continue
                    
                    # Calculate stop loss distance in price terms
                    stop_distance_pips = abs(entry_price - exit_price)
                    
                    # Calculate number of ticks in the stop loss
                    ticks_in_stop = stop_distance_pips / tick_size if tick_size > 0 else 0
                    
                    # Get volume step (minimum volume increment)
                    volume_step = symbol_info.volume_step
                    min_volume = symbol_info.volume_min
                    max_volume = symbol_info.volume_max
                    
                    # SCALING LOGIC: Scale volume to meet required risk
                    best_volume = current_volume
                    best_risk = 0
                    previous_volume = current_volume
                    previous_risk = 0
                    
                    # Start with current volume or min volume
                    test_volume = current_volume if current_volume >= min_volume else min_volume
                    
                    # Calculate risk with current volume
                    test_risk = test_volume * ticks_in_stop * tick_value
                    
                    print(f"    └─ 📈 {symbol}: Starting volume {test_volume} -> risk ${test_risk:.2f}")
                    
                    # If current risk is already above tolerance, try scaling down
                    if test_risk > tolerance_max:
                        print(f"      └─ ⬇️  Risk too high (${test_risk:.2f} > ${tolerance_max:.2f}), scaling down...")
                        # Scale down until risk is within tolerance or below
                        while test_risk > tolerance_max and test_volume > min_volume:
                            previous_volume = test_volume
                            previous_risk = test_risk
                            test_volume = max(min_volume, test_volume - volume_step)
                            test_risk = test_volume * ticks_in_stop * tick_value
                            print(f"         Volume: {test_volume:.3f} -> risk ${test_risk:.2f}")
                        
                        if test_risk <= tolerance_max and test_risk >= tolerance_min * 0.5:  # Allow slightly below
                            best_volume = test_volume
                            best_risk = test_risk
                            print(f"      └─ ✅ Found suitable volume: {best_volume:.3f} (risk ${best_risk:.2f})")
                        else:
                            # If we can't get within tolerance, use the smallest volume
                            best_volume = min_volume
                            best_risk = min_volume * ticks_in_stop * tick_value
                            print(f"      └─ ⚠️  Using minimum volume: {best_volume:.3f} (risk ${best_risk:.2f})")
                    
                    # If current risk is below required, scale up
                    elif test_risk < tolerance_min:
                        print(f"      └─ ⬆️  Risk too low (${test_risk:.2f} < ${tolerance_min:.2f}), scaling up...")
                        previous_volume = test_volume
                        previous_risk = test_risk
                        
                        while test_risk < tolerance_min and test_volume < max_volume:
                            previous_volume = test_volume
                            previous_risk = test_risk
                            test_volume = min(max_volume, test_volume + volume_step)
                            test_risk = test_volume * ticks_in_stop * tick_value
                            print(f"         Volume: {test_volume:.3f} -> risk ${test_risk:.2f}")
                        
                        # Check if we overshot
                        if test_risk > tolerance_max:
                            # Use previous volume that was within/below tolerance
                            best_volume = previous_volume
                            best_risk = previous_risk
                            print(f"      └─ ✅ Using previous volume: {best_volume:.3f} (risk ${best_risk:.2f}) to avoid overshoot")
                        elif test_risk >= tolerance_min:
                            best_volume = test_volume
                            best_risk = test_risk
                            print(f"      └─ ✅ Found suitable volume: {best_volume:.3f} (risk ${best_risk:.2f})")
                        else:
                            best_volume = test_volume
                            best_risk = test_risk
                            print(f"      └─ ⚠️  Using max volume reached: {best_volume:.3f} (risk ${best_risk:.2f})")
                    
                    # If already within tolerance
                    else:
                        best_volume = test_volume
                        best_risk = test_risk
                        print(f"      └─ ✅ Already within tolerance: volume {best_volume:.3f} (risk ${best_risk:.2f})")
                    
                    # Update order with scaled volume and risk
                    if best_volume != current_volume:
                        order[volume_field] = round(best_volume, 2)  # Round to 2 decimals for volume
                        print(f"      └─ 📊 Volume updated: {current_volume:.3f} -> {best_volume:.3f}")
                    
                    # Always update risk fields
                    order["risk_in_usd"] = round(best_risk, 2)
                    order["risk_calculated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    order["required_risk_target"] = required_risk
                    order["risk_tolerance_min"] = round(tolerance_min, 2)
                    order["risk_tolerance_max"] = round(tolerance_max, 2)
                    order["account_balance_at_calc"] = round(account_balance, 2)
                    order["current_bid"] = round(symbol_info.bid, 6) if hasattr(symbol_info, 'bid') else None
                    order["current_ask"] = round(symbol_info.ask, 6) if hasattr(symbol_info, 'ask') else None
                    
                    orders_modified = True
                    investor_orders_updated += 1
                    total_orders_updated += 1
                    file_risk_total += best_risk
                    
                    # Check if order meets criteria for signals.json
                    # Risk must be within tolerance and > 0
                    if best_risk >= tolerance_min * 0.5 and best_risk <= tolerance_max and best_risk > 0:
                        # Create a copy for signals to avoid reference issues
                        signal_order = order.copy()
                        signal_order["moved_to_signals_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        file_signals.append(signal_order)
                        investor_signals_count += 1
                        total_signals_created += 1
                        print(f"      └─ 🟢 Qualified for signals.json (risk ${best_risk:.2f})")
                
                # Save modified limit orders file
                if orders_modified:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(orders, f, indent=4)
                    
                    investor_files_updated += 1
                    total_files_updated += 1
                    investor_risk_usd += file_risk_total
                    total_risk_usd += file_risk_total
                    any_orders_processed = True
                    
                    print(f"  └─ 📁 {file_path.parent.name}: Updated {len([o for o in orders if 'risk_in_usd' in o])} orders | File risk total: ${file_risk_total:.2f}")
                    
                    # Save signals.json in the same pending_orders directory
                    if file_signals:
                        signals_path = file_path.parent / "signals.json"
                        try:
                            # Load existing signals if any
                            existing_signals = []
                            if signals_path.exists():
                                with open(signals_path, 'r', encoding='utf-8') as f:
                                    existing_signals = json.load(f)
                            
                            # Merge new signals with existing (avoid duplicates by checking symbol/entry/exit)
                            existing_keys = {(s.get('symbol'), s.get('entry'), s.get('exit')) for s in existing_signals}
                            for signal in file_signals:
                                signal_key = (signal.get('symbol'), signal.get('entry'), signal.get('exit'))
                                if signal_key not in existing_keys:
                                    existing_signals.append(signal)
                            
                            with open(signals_path, 'w', encoding='utf-8') as f:
                                json.dump(existing_signals, f, indent=4)
                            
                            print(f"  └─ 📊 signals.json: Added {len(file_signals)} new signals | Total: {len(existing_signals)}")
                        except Exception as e:
                            print(f"  └─ ❌ Error writing signals.json: {e}")
                
            except Exception as e:
                print(f"  └─ ❌ Error processing {file_path}: {e}")
                continue
        
        # Summary for current investor
        if investor_orders_updated > 0:
            print(f"\n  └─ {'='*40}")
            print(f"  └─ ✨ Investor {current_inv_id} Summary:")
            print(f"  └─    Files Modified:     {investor_files_updated}")
            print(f"  └─    Orders Updated:     {investor_orders_updated}")
            print(f"  └─    Total Risk:         ${investor_risk_usd:,.2f}")
            print(f"  └─    Signals Created:    {investor_signals_count}")
            print(f"  └─    Symbols Normalized: {total_symbols_normalized}")
            print(f"  └─    Required Risk:      ${required_risk:.2f} (tolerance: ${tolerance_min:.2f}-${tolerance_max:.2f})")
            print(f"  └─ {'='*40}")
        else:
            print(f"  └─ ⚠️  No orders were updated")

    # Final Global Summary
    print(f"\n{'='*10} USD RISK CALCULATION COMPLETE {'='*10}")
    if total_orders_updated > 0:
        print(f" Total Files Modified:     {total_files_updated}")
        print(f" Total Orders Updated:     {total_orders_updated}")
        print(f" Total USD Risk:           ${total_risk_usd:,.2f}")
        print(f" Average Risk per Order:   ${total_risk_usd / total_orders_updated:,.2f}")
        print(f" Total Signals Created:    {total_signals_created}")
        print(f" Total Symbols Normalized: {total_symbols_normalized}")
    else:
        print(" No orders were processed.")
    
    return any_orders_processed

def apply_default_prices(inv_id=None, callback_function=None):
    """
    Applies default prices from limit_orders_backup.json to signals.json when default_price is true.
    Copies exit/target prices from backup to matching orders in signals.json, handling symbol normalization.
    
    Args:
        inv_id (str, optional): Specific investor ID to process. If None, processes all investors.
        callback_function (callable, optional): A function to call with the opened file data.
            The callback will receive (inv_id, backup_file_path, signals_file_path, modifications) parameters.
    
    Returns:
        bool: True if any orders were modified, False otherwise
    """
    print(f"\n{'='*10} 💰 APPLYING DEFAULT PRICES FROM BACKUP {'='*10}")
    
    total_orders_modified = 0
    total_files_updated = 0
    total_symbols_normalized = 0
    inv_base_path = Path(INV_PATH)

    if not inv_base_path.exists():
        print(f" [!] Error: Investor path {INV_PATH} does not exist.")
        return False

    # Determine which investors to process
    if inv_id:
        inv_folder = inv_base_path / inv_id
        investor_folders = [inv_folder] if inv_folder.exists() else []
        if not investor_folders:
            print(f" [!] Error: Investor folder {inv_id} does not exist.")
            return False
    else:
        investor_folders = [f for f in inv_base_path.iterdir() if f.is_dir()]
    
    if not investor_folders:
        print(" └─ 🔘 No investor directories found.")
        return False

    any_orders_modified = False

    for inv_folder in investor_folders:
        current_inv_id = inv_folder.name
        print(f"\n [{current_inv_id}] 🔍 Checking default price setting...")

        # --- INVESTOR LOCAL CACHE for symbol normalization ---
        resolution_cache = {}

        # 1. Load accountmanagement.json to check default_price setting
        account_mgmt_path = inv_folder / "accountmanagement.json"
        if not account_mgmt_path.exists():
            print(f"  └─ ⚠️  accountmanagement.json not found, skipping")
            continue

        try:
            with open(account_mgmt_path, 'r', encoding='utf-8') as f:
                account_data = json.load(f)
            
            settings = account_data.get('settings', {})
            default_price_enabled = settings.get('default_price', False)
            
            if not default_price_enabled:
                print(f"  └─ ⏭️  default_price is FALSE - skipping investor (set to true to apply default prices)")
                continue
                
            print(f"  └─ ✅ default_price is TRUE - will apply prices from backup")
            
        except Exception as e:
            print(f"  └─ ❌ Error reading accountmanagement.json: {e}")
            continue

        # 2. Load broker config for symbol handling
        broker_cfg = usersdictionary.get(current_inv_id)
        if not broker_cfg:
            print(f"  └─ ❌ No broker config found for {current_inv_id}")
            continue

        # 3. Find all limit_orders_backup.json files
        backup_files = list(inv_folder.rglob("*/pending_orders/limit_orders_backup.json"))
        
        if not backup_files:
            print(f"  └─ 🔘 No limit_orders_backup.json files found")
            continue
        
        print(f"  └─ 📁 Found {len(backup_files)} backup files to process")

        investor_orders_modified = 0
        investor_files_updated = 0
        investor_symbols_normalized = 0

        # 4. Process each backup file
        for backup_path in backup_files:
            folder_path = backup_path.parent.parent  # Gets the strategy folder (e.g., double-levels)
            signals_path = backup_path.parent / "signals.json"  # Same directory as backup
            
            # Check if signals.json exists
            if not signals_path.exists():
                print(f"  └─ ⚠️  No signals.json found in {backup_path.parent} (same folder as backup), skipping")
                continue
            
            print(f"\n  └─ 📂 Processing folder: {folder_path.name}")
            print(f"      ├─ Backup: {backup_path.name}")
            print(f"      └─ Signals: {signals_path.name}")
            
            try:
                # Load backup orders
                with open(backup_path, 'r', encoding='utf-8') as f:
                    backup_orders = json.load(f)
                
                # Load signals
                with open(signals_path, 'r', encoding='utf-8') as f:
                    signals = json.load(f)
                
                if not backup_orders:
                    print(f"    └─ ⚠️  Empty backup file")
                    continue
                    
                if not signals:
                    print(f"    └─ ⚠️  Empty signals file")
                    continue
                
                # Create lookup dictionaries for backup orders with multiple matching strategies
                backup_lookup = {}  # (symbol, timeframe, order_type) -> order
                
                print(f"    └─ 📊 Processing {len(backup_orders)} backup orders and {len(signals)} signals")
                
                # First, let's analyze what's in the backup for AUDCAD 15m sell_limit
                audcad_backups = []
                for order in backup_orders:
                    # --- SYMBOL NORMALIZATION for backup symbols ---
                    raw_symbol = str(order.get('symbol', '')).upper()
                    if not raw_symbol:
                        continue
                    
                    # Check Cache First for backup symbol
                    if raw_symbol in resolution_cache:
                        normalized_symbol = resolution_cache[raw_symbol]
                    else:
                        # Perform mapping only once
                        normalized_symbol = get_normalized_symbol(raw_symbol)
                        resolution_cache[raw_symbol] = normalized_symbol
                        
                        # Log normalization on first discovery
                        if normalized_symbol != raw_symbol:
                            print(f"      └─ ✅ Backup: {raw_symbol} -> {normalized_symbol} (Mapped & Cached)")
                            investor_symbols_normalized += 1
                            total_symbols_normalized += 1
                    
                    # Use normalized symbol for lookup
                    symbol = normalized_symbol if normalized_symbol else raw_symbol
                    timeframe = str(order.get('timeframe', '')).upper()
                    order_type = str(order.get('order_type', '')).lower()
                    
                    if symbol == 'AUDCAD' and timeframe == '15M' and order_type == 'sell_limit':
                        audcad_backups.append(order)
                        print(f"      └─ 📌 Found AUDCAD 15M sell_limit in backup with exit: {order.get('exit')}")
                    
                    if symbol and timeframe and order_type:
                        # Store with normalized symbol
                        key = (symbol, timeframe, order_type)
                        backup_lookup[key] = order
                
                print(f"    └─ 📊 Created lookup for {len(backup_lookup)} backup orders")
                
                # Process signals and apply default prices
                modified = False
                signals_modified_count = 0
                modifications_log = []
                
                for signal in signals:
                    # --- SYMBOL NORMALIZATION for signal symbols ---
                    raw_signal_symbol = str(signal.get('symbol', '')).upper()
                    if not raw_signal_symbol:
                        continue
                    
                    # Check Cache First for signal symbol
                    if raw_signal_symbol in resolution_cache:
                        signal_symbol = resolution_cache[raw_signal_symbol]
                    else:
                        # Perform mapping only once
                        signal_symbol = get_normalized_symbol(raw_signal_symbol)
                        resolution_cache[raw_signal_symbol] = signal_symbol
                        
                        # Log normalization on first discovery
                        if signal_symbol != raw_signal_symbol:
                            print(f"      └─ ✅ Signal: {raw_signal_symbol} -> {signal_symbol} (Mapped & Cached)")
                            investor_symbols_normalized += 1
                            total_symbols_normalized += 1
                    
                    signal_timeframe = str(signal.get('timeframe', '')).upper()
                    signal_type = str(signal.get('order_type', '')).lower()
                    
                    if not all([signal_symbol, signal_timeframe, signal_type]):
                        print(f"      └─ ⚠️  Signal missing required fields: {signal}")
                        continue
                    
                    # Special debug for AUDCAD+ 15M (now normalized to AUDCAD)
                    if raw_signal_symbol == 'AUDCAD+' and signal_timeframe == '15M' and signal_type == 'sell_limit':
                        print(f"      └─ 🔍 DEBUG: Processing AUDCAD+ 15M sell_limit signal (normalized to {signal_symbol})")
                        print(f"          Current exit: {signal.get('exit')}, target: {signal.get('target')}")
                    
                    # Try to find matching backup order
                    matched_backup = None
                    match_method = None
                    
                    # Method 1: Direct symbol match with normalized symbols
                    backup_key = (signal_symbol, signal_timeframe, signal_type)
                    if backup_key in backup_lookup:
                        matched_backup = backup_lookup[backup_key]
                        match_method = "direct match"
                        if raw_signal_symbol == 'AUDCAD+':
                            print(f"      └─ ✓ Found direct match for normalized AUDCAD")
                    
                    if matched_backup:
                        # Check if we need to update any prices
                        updates_made = False
                        
                        # Get backup values
                        backup_exit = matched_backup.get('exit', 0)
                        backup_target = matched_backup.get('target', 0)
                        
                        # Current signal values
                        current_exit = signal.get('exit', 0)
                        current_target = signal.get('target', 0)
                        
                        update_details = []
                        
                        # Apply backup exit if not zero and different from current
                        if backup_exit != 0 and backup_exit != current_exit:
                            signal['exit'] = backup_exit
                            updates_made = True
                            update_details.append(f"exit: {current_exit} -> {backup_exit}")
                        
                        # Apply backup target if not zero and different from current
                        if backup_target != 0 and backup_target != current_target:
                            signal['target'] = backup_target
                            updates_made = True
                            update_details.append(f"target: {current_target} -> {backup_target}")
                        
                        if updates_made:
                            # Add metadata about the update
                            signal['price_updated_from_backup'] = True
                            signal['price_updated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            signal['backup_match_method'] = match_method
                            signal['original_symbol'] = raw_signal_symbol
                            
                            signals_modified_count += 1
                            investor_orders_modified += 1
                            total_orders_modified += 1
                            any_orders_modified = True
                            modified = True
                            
                            modifications_log.append({
                                'symbol': raw_signal_symbol,
                                'normalized_symbol': signal_symbol,
                                'timeframe': signal_timeframe,
                                'type': signal_type,
                                'updates': {
                                    'exit': backup_exit if backup_exit != 0 else None,
                                    'target': backup_target if backup_target != 0 else None
                                },
                                'match_method': match_method,
                                'update_details': ', '.join(update_details)
                            })
                            
                            print(f"      └─ 🔄 [{raw_signal_symbol} -> {signal_symbol}] {', '.join(update_details)} [{match_method}]")
                        else:
                            if raw_signal_symbol == 'AUDCAD+':
                                print(f"      └─ ✓ AUDCAD+ already has correct prices (exit={current_exit}, target={current_target})")
                    else:
                        # Debug: Show unmatched signals with more detail
                        if raw_signal_symbol == 'AUDCAD+':
                            print(f"      └─ ❌ FAILED to find match for AUDCAD+ 15M sell_limit (normalized to {signal_symbol})")
                            print(f"          Looking for backup_key: ({signal_symbol}, {signal_timeframe}, {signal_type})")
                            
                            # Show all available backup keys
                            print(f"          Available backup keys:")
                            for (bsym, btf, btype) in list(backup_lookup.keys())[:10]:
                                if btf == signal_timeframe and btype == signal_type:
                                    print(f"            • ({bsym}, {btf}, {btype})")
                        else:
                            print(f"      └─ ⚠️  No backup match for: {raw_signal_symbol} -> {signal_symbol} ({signal_timeframe}, {signal_type})")
                
                # Save modified signals file
                if modified:
                    try:
                        with open(signals_path, 'w', encoding='utf-8') as f:
                            json.dump(signals, f, indent=4)
                        
                        investor_files_updated += 1
                        total_files_updated += 1
                        
                        print(f"    └─ 📝 Updated {signals_modified_count} orders in signals.json")
                        
                        # Call callback if provided
                        if callback_function:
                            try:
                                callback_function(current_inv_id, backup_path, signals_path, modifications_log)
                            except Exception as e:
                                print(f"    └─ ⚠️  Callback error: {e}")
                        
                        # Show summary of modifications
                        if modifications_log:
                            print(f"    └─ 📋 Modification Summary:")
                            for mod in modifications_log[:5]:  # Show first 5
                                norm_info = f" -> {mod['normalized_symbol']}" if mod['symbol'] != mod['normalized_symbol'] else ""
                                print(f"      • {mod['symbol']}{norm_info} ({mod['timeframe']}): {mod['update_details']} [{mod['match_method']}]")
                            if len(modifications_log) > 5:
                                print(f"      • ... and {len(modifications_log) - 5} more")
                    
                    except Exception as e:
                        print(f"    └─ ❌ Error saving signals.json: {e}")
                else:
                    print(f"    └─ ✓ No price updates needed for signals in {folder_path.name}")
                
            except Exception as e:
                print(f"  └─ ❌ Error processing {backup_path}: {e}")
                continue

        # Investor summary
        if investor_orders_modified > 0:
            print(f"\n  └─ {'='*40}")
            print(f"  └─ ✨ Investor {current_inv_id} Summary:")
            print(f"  └─    Folders Processed:   {len(backup_files)}")
            print(f"  └─    Signals Files Updated: {investor_files_updated}")
            print(f"  └─    Orders Modified:     {investor_orders_modified}")
            if investor_symbols_normalized > 0:
                print(f"  └─    Symbols Normalized:   {investor_symbols_normalized}")
            print(f"  └─ {'='*40}")
        else:
            print(f"\n  └─ ⚠️  No modifications made for {current_inv_id}")

    # Final Global Summary
    print(f"\n{'='*10} DEFAULT PRICE APPLICATION COMPLETE {'='*10}")
    if total_orders_modified > 0:
        print(f" Total Files Updated:       {total_files_updated}")
        print(f" Total Orders Modified:     {total_orders_modified}")
        if total_symbols_normalized > 0:
            print(f" Total Symbols Normalized:  {total_symbols_normalized}")
        print(f"\n ✓ Default prices successfully applied from backup files")
    else:
        print(" No orders were modified.")
        print(" └─ Possible reasons:")
        print("    • default_price is false in accountmanagement.json")
        print("    • No matching orders found between backup and signals")
        print("    • All exit/target prices already match backup values")
        print("    • No limit_orders_backup.json files found")
    
    return any_orders_modified

def place_usd_orders(inv_id=None):
    """
    Places pending orders from signals.json files for investors.
    
    Args:
        inv_id (str, optional): Specific investor ID to process. If None, processes all investors.
        MT5 should already be initialized and logged in for this investor.
    
    Returns:
        bool: True if any orders were placed, False otherwise
    """
    # --- SUB-FUNCTION 2: COLLECT ORDERS FROM SIGNALS.JSON ---
    def collect_orders_from_signals(inv_root, resolution_cache):
        """
        Collects ALL orders from pending_orders/signals.json files regardless of risk/reward.
        
        Args:
            inv_root: Path to investor root folder
            resolution_cache: Symbol normalization cache dictionary
            
        Returns:
            list: All collected orders
        """
        print(f"  📁 Scanning for signals.json files...")
        
        # Search pattern for signals.json in any pending_orders folder
        signals_files = list(inv_root.rglob("*/pending_orders/signals.json"))
        
        if not signals_files:
            print(f"  📁 No signals.json files found")
            return []
        
        print(f"  📁 Found {len(signals_files)} signals.json file(s)")
        
        entries_list = []
        total_entries_found = 0
        file_symbols_normalized = 0
        
        for signals_path in signals_files:
            if not signals_path.is_file():
                continue
                
            folder_name = signals_path.parent.parent.name
            try:
                with open(signals_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if not isinstance(data, list):
                    print(f"    📁 {folder_name}: Invalid format (not a list)")
                    continue
                
                # Take ALL entries - no filtering by risk_reward
                print(f"    📁 {folder_name}: Found {len(data)} entries")
                
                file_processed = 0
                for entry in data:
                    # --- SYMBOL NORMALIZATION with Caching ---
                    raw_symbol = entry.get("symbol", "")
                    if not raw_symbol:
                        print(f"      ⚠️  Skipping entry with no symbol")
                        continue
                    
                    # Check Cache First
                    if raw_symbol in resolution_cache:
                        normalized_symbol = resolution_cache[raw_symbol]
                    else:
                        # Perform mapping only once
                        normalized_symbol = get_normalized_symbol(raw_symbol)
                        resolution_cache[raw_symbol] = normalized_symbol
                        
                        # Log normalization on first discovery
                        if normalized_symbol != raw_symbol:
                            print(f"      ✅ {raw_symbol} -> {normalized_symbol} (Mapped & Cached)")
                            file_symbols_normalized += 1
                    
                    if not normalized_symbol:
                        print(f"      ⚠️  Skipping {raw_symbol} - Could not normalize symbol")
                        continue
                    
                    # Update symbol in entry
                    entry['symbol'] = normalized_symbol
                    
                    # Add entry without deduplication
                    entries_list.append(entry)
                    file_processed += 1
                    total_entries_found += 1
                
                if file_processed < len(data):
                    print(f"      📊 {folder_name}: Added {file_processed}/{len(data)} entries after normalization")
                    
            except json.JSONDecodeError as e:
                print(f"    ❌ {folder_name}: Invalid JSON - {e}")
            except Exception as e:
                print(f"    ❌ {folder_name}: Error reading file - {e}")
        
        print(f"\n  📈 TOTAL: {total_entries_found} entries collected from all signals.json files")
        if file_symbols_normalized > 0:
            print(f"  📈 Symbols normalized during collection: {file_symbols_normalized}")
        
        return entries_list

    # --- SUB-FUNCTION 3: CHECK EXISTING POSITIONS/ORDERS ---
    def check_existing_positions_and_orders(symbol, entry_price, digits):
        """
        Check if there's already an active position or pending order at the given entry price.
        
        Returns:
            tuple: (exists, type_string) where type_string describes what exists
        """
        # Check existing POSITIONS (open trades)
        positions = mt5.positions_get(symbol=symbol) or []
        for position in positions:
            if round(position.price_open, digits) == entry_price:
                position_type = "BUY" if position.type == mt5.POSITION_TYPE_BUY else "SELL"
                return True, f"{position_type} position already open at {entry_price} (Ticket: {position.ticket})"
        
        # Check existing PENDING ORDERS (limit orders)
        orders = mt5.orders_get(symbol=symbol) or []
        for order in orders:
            if round(order.price_open, digits) == entry_price:
                order_type = "BUY LIMIT" if order.type == mt5.ORDER_TYPE_BUY_LIMIT else "SELL LIMIT"
                return True, f"{order_type} already pending at {entry_price} (Ticket: {order.ticket})"
        
        return False, ""

    # --- SUB-FUNCTION 4: ORDER EXECUTION ---
    def execute_missing_orders(all_entries, resolution_cache, default_magic, trade_allowed):
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
                
                # Step 1: Symbol normalization (already done during collection, but verify)
                symbol = entry["symbol"]
                
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
                
                # Step 5: Get volume key
                vol_key = next((k for k in entry.keys() if k.endswith("volume")), None)
                if not vol_key:
                    print(f"      ❌ FAIL: {symbol_orig} - No volume field found in entry data")
                    failed += 1
                    continue
                
                # Step 6: Check for existing POSITIONS or PENDING ORDERS at the same price
                entry_price = round(float(entry["entry"]), symbol_info.digits)
                exists, exists_msg = check_existing_positions_and_orders(symbol, entry_price, symbol_info.digits)
                
                if exists:
                    print(f"      ⏭️  SKIP: {symbol} - {exists_msg}")
                    skipped += 1
                    continue

                # Step 7: Calculate and validate volume
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
                
                # Step 8: Validate prices
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
                
                # Step 9: Determine order type
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

                # Step 10: Get risk_reward value for comment
                rr_value = entry.get("risk_reward", "?")
                
                # Step 11: Prepare and send order
                request = {
                    "action": mt5.TRADE_ACTION_PENDING,
                    "symbol": symbol,
                    "volume": round(volume, 2),
                    "type": mt5_order_type,
                    "price": entry_price,
                    "sl": sl_price,
                    "tp": tp_price,
                    "magic": int(entry.get("magic", default_magic)),
                    "comment": f"RR{rr_value}",
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
                        10009: "Invalid stops",
                        10010: "Invalid volume",
                        10011: "Market closed",
                        10012: "Insufficient money",
                        10013: "Price changed",
                        10014: "Off quotes",
                        10015: "Broker busy",
                        10016: "Requote",
                        10017: "Order locked",
                        10018: "Long positions only allowed",
                        10019: "Too many orders",
                        10020: "Pending orders limit reached",
                        10021: "Hedging prohibited",
                        10022: "Close-only mode",
                        10023: "FIFO rule violated",
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
    
    # No need to load normalization map - we'll use the helper function

    # Determine which investors to process
    if inv_id:
        investor_ids = [inv_id]
    else:
        investor_ids = list(usersdictionary.keys())
    
    if not investor_ids:
        print(" └─ 🔘 No investors found.")
        return False

    total_investors = len(investor_ids)
    processed = 0
    successful = 0
    any_orders_placed = False
    global_symbols_normalized = 0

    for user_brokerid in investor_ids:
        processed += 1
        print(f"\n{'-'*80}")
        print(f"📋 INVESTOR [{processed}/{total_investors}]: {user_brokerid}")
        print(f"{'-'*80}")
        
        # --- INVESTOR LOCAL CACHE for symbol normalization ---
        resolution_cache = {}
        
        broker_cfg = usersdictionary.get(user_brokerid)
        if not broker_cfg:
            print(f"  ❌ No broker config found for {user_brokerid}")
            continue
        
        inv_root = Path(INV_PATH) / user_brokerid
        acc_mgmt_path = inv_root / "accountmanagement.json"
        
        if not acc_mgmt_path.exists():
            print(f"  ⚠️  Account management file not found - skipping")
            continue

        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Extract settings
            default_magic = config.get("magic_number", 123456)
            
            # Account info (MT5 should already be initialized)
            acc_info = mt5.account_info()
            term_info = mt5.terminal_info()
            
            if not acc_info:
                print(f"  ❌ Failed to get account info - MT5 might not be initialized")
                continue
                
            print(f"\n  📊 Account Details:")
            print(f"    • Balance: ${acc_info.balance:,.2f}")
            print(f"    • Equity: ${acc_info.equity:,.2f}")
            print(f"    • Free Margin: ${acc_info.margin_free:,.2f}")
            print(f"    • Margin Level: {acc_info.margin_level:.2f}%" if acc_info.margin_level else "    • Margin Level: N/A")
            print(f"    • AutoTrading: {'✅ ENABLED' if term_info.trade_allowed else '❌ DISABLED'}")

            # Stage 1: Collect orders from signals.json files (ALL orders, no deduplication)
            print(f"\n  📁 STAGE 1: Collecting ALL orders from signals.json")
            all_entries = collect_orders_from_signals(
                inv_root, resolution_cache
            )
            
            # Track global normalization stats
            global_symbols_normalized += len([k for k, v in resolution_cache.items() if v != k])
            
            if all_entries:
                # Stage 2: Execution
                print(f"\n  🚀 STAGE 2: Order placement")
                p, f, s = execute_missing_orders(
                    all_entries, resolution_cache, default_magic, term_info.trade_allowed
                )
                
                if p > 0:
                    any_orders_placed = True
                    successful += 1
                    
                print(f"\n  📈 INVESTOR SUMMARY: {user_brokerid}")
                print(f"    • Orders Placed: {p}")
                print(f"    • Orders Failed: {f}")
                print(f"    • Orders Skipped: {s}")
                print(f"    • Total Processed: {len(all_entries)}")
                
                # Show normalization stats for this investor
                investor_normalized = len([k for k, v in resolution_cache.items() if v != k])
                if investor_normalized > 0:
                    print(f"    • Symbols Normalized: {investor_normalized}")
            else:
                print(f"  ℹ️  No trading opportunities found in signals.json")

        except json.JSONDecodeError as e:
            print(f"  ❌ Invalid JSON in account management file: {e}")
        except KeyError as e:
            print(f"  ❌ Missing required configuration key: {e}")
        except Exception as e:
            print(f"  💥 SYSTEM ERROR: {str(e)}")
            import traceback
            traceback.print_exc()
            continue
    
    print("\n" + "="*80)
    print("✅ ORDER PLACEMENT COMPLETED")
    print(f"   Processed: {processed}/{total_investors} investors")
    print(f"   Successful: {successful} investors")
    if global_symbols_normalized > 0:
        print(f"   Total Symbols Normalized: {global_symbols_normalized}")
    print("="*80)
    
    return any_orders_placed

def pending_orders_reward_correction(inv_id=None):
    """
    Function: Checks live pending orders (limit and stop) and adjusts their take profit levels
    based on the selected risk-reward ratio from accountmanagement.json.
    Only executes if risk_reward_correction setting is True.
    
    Args:
        inv_id (str, optional): Specific investor ID to process. If None, processes all investors.
        MT5 should already be initialized and logged in for this investor.
    
    Returns:
        bool: True if any orders were adjusted, False otherwise
    """
    print(f"\n{'='*10} 📐 RISK-REWARD CORRECTION: PENDING ORDERS {'='*10}")

    # --- DATA INITIALIZATION ---
    # No need to load normalization map - we'll use the helper function

    # Determine which investors to process
    if inv_id:
        investor_ids = [inv_id]
    else:
        investor_ids = list(usersdictionary.keys())
    
    if not investor_ids:
        print(" └─ 🔘 No investors found.")
        return False

    any_orders_adjusted = False
    total_orders_checked = 0
    total_orders_adjusted = 0
    total_orders_skipped = 0
    total_orders_error = 0
    total_symbols_normalized = 0

    for user_brokerid in investor_ids:
        print(f"\n [{user_brokerid}] 🔍 Checking risk-reward configurations...")
        
        # --- INVESTOR LOCAL CACHE for symbol normalization ---
        resolution_cache = {}
        
        inv_root = Path(INV_PATH) / user_brokerid
        acc_mgmt_path = inv_root / "accountmanagement.json"
        broker_cfg = usersdictionary.get(user_brokerid)

        if not broker_cfg:
            print(f"  └─ ❌ No broker config found for {user_brokerid}")
            continue

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
                selected_rr = [3]
            
            # Use the first ratio in the list (typically the preferred one)
            target_rr_ratio = float(selected_rr[0])
            print(f"  └─ ✅ Target R:R Ratio: 1:{target_rr_ratio}")
            
            # Get risk management mapping for balance-based risk
            risk_map = config.get("account_balance_default_risk_management", {})
            
        except Exception as e:
            print(f"  └─ ❌ Failed to read config: {e}")
            continue

        # --- GET ACCOUNT INFO (MT5 should already be initialized) ---
        acc_info = mt5.account_info()
        if not acc_info:
            print(f"  └─ ❌ Could not fetch account info - MT5 might not be initialized")
            continue
            
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
            continue

        print(f"  └─ 💰 Balance: ${balance:,.2f} | Base Risk: ${primary_risk:.2f} | Target R:R: 1:{target_rr_ratio}")

        # --- CHECK AND ADJUST ALL PENDING ORDERS ---
        # Get all pending orders (both limit and stop)
        pending_orders = mt5.orders_get()
        investor_orders_checked = 0
        investor_orders_adjusted = 0
        investor_orders_skipped = 0
        investor_orders_error = 0
        investor_symbols_normalized = 0

        if pending_orders:
            for order in pending_orders:
                # Check if it's any type of pending order (limit or stop)
                # MT5 pending order types:
                # ORDER_TYPE_BUY_LIMIT, ORDER_TYPE_SELL_LIMIT, 
                # ORDER_TYPE_BUY_STOP, ORDER_TYPE_SELL_STOP,
                # ORDER_TYPE_BUY_STOP_LIMIT, ORDER_TYPE_SELL_STOP_LIMIT
                if order.type not in [
                    mt5.ORDER_TYPE_BUY_LIMIT, 
                    mt5.ORDER_TYPE_SELL_LIMIT,
                    mt5.ORDER_TYPE_BUY_STOP, 
                    mt5.ORDER_TYPE_SELL_STOP,
                    mt5.ORDER_TYPE_BUY_STOP_LIMIT, 
                    mt5.ORDER_TYPE_SELL_STOP_LIMIT
                ]:
                    continue

                investor_orders_checked += 1
                total_orders_checked += 1
                
                # Get order type description for logging
                order_type_names = {
                    mt5.ORDER_TYPE_BUY_LIMIT: "BUY LIMIT",
                    mt5.ORDER_TYPE_SELL_LIMIT: "SELL LIMIT",
                    mt5.ORDER_TYPE_BUY_STOP: "BUY STOP",
                    mt5.ORDER_TYPE_SELL_STOP: "SELL STOP",
                    mt5.ORDER_TYPE_BUY_STOP_LIMIT: "BUY STOP LIMIT",
                    mt5.ORDER_TYPE_SELL_STOP_LIMIT: "SELL STOP LIMIT"
                }
                order_type_name = order_type_names.get(order.type, f"UNKNOWN({order.type})")
                
                # --- SYMBOL NORMALIZATION for order symbol ---
                raw_symbol = order.symbol
                
                # Check Cache First
                if raw_symbol in resolution_cache:
                    normalized_symbol = resolution_cache[raw_symbol]
                else:
                    # Perform mapping only once
                    normalized_symbol = get_normalized_symbol(raw_symbol)
                    resolution_cache[raw_symbol] = normalized_symbol
                    
                    # Log normalization on first discovery
                    if normalized_symbol != raw_symbol:
                        print(f"    └─ ✅ Order symbol normalized: {raw_symbol} -> {normalized_symbol} (Mapped & Cached)")
                        investor_symbols_normalized += 1
                        total_symbols_normalized += 1
                
                # Use normalized symbol for symbol info lookup
                lookup_symbol = normalized_symbol if normalized_symbol else raw_symbol
                
                # Get symbol info for pip/point value and digit calculation
                symbol_info = mt5.symbol_info(lookup_symbol)
                if not symbol_info:
                    print(f"    └─ ⚠️  Cannot get symbol info for {lookup_symbol} (from {raw_symbol})")
                    investor_orders_skipped += 1
                    total_orders_skipped += 1
                    continue

                # Determine the direction for profit calculation based on order type
                # For pending orders, the actual trade direction when triggered:
                # BUY_* orders become BUY positions, SELL_* orders become SELL positions
                is_buy_direction = order.type in [
                    mt5.ORDER_TYPE_BUY_LIMIT, 
                    mt5.ORDER_TYPE_BUY_STOP, 
                    mt5.ORDER_TYPE_BUY_STOP_LIMIT
                ]
                
                calc_type = mt5.ORDER_TYPE_BUY if is_buy_direction else mt5.ORDER_TYPE_SELL
                
                # Calculate current risk (stop loss distance in money)
                if order.sl == 0:
                    print(f"    └─ ⚠️  {order_type_name} #{order.ticket} ({raw_symbol}) has no SL set")
                    investor_orders_skipped += 1
                    total_orders_skipped += 1
                    continue
                    
                sl_profit = mt5.order_calc_profit(calc_type, lookup_symbol, order.volume_initial, 
                                                  order.price_open, order.sl)
                
                if sl_profit is None:
                    print(f"    └─ ⚠️  Cannot calculate risk for {order_type_name} #{order.ticket}")
                    investor_orders_skipped += 1
                    total_orders_skipped += 1
                    continue

                current_risk_usd = round(abs(sl_profit), 2)
                
                # Calculate required take profit based on risk and target R:R ratio
                # Target profit = risk * target_rr_ratio
                target_profit_usd = current_risk_usd * target_rr_ratio
                
                # Calculate the take profit price that would achieve this profit
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
                    
                    # Calculate new take profit price based on order direction
                    if is_buy_direction:
                        # For BUY pending orders, TP should be above entry
                        new_tp = round(order.price_open + price_move_needed, digits)
                    else:
                        # For SELL pending orders, TP should be below entry
                        new_tp = round(order.price_open - price_move_needed, digits)
                    
                    # Check if current TP is significantly different from calculated TP
                    current_move = abs(order.tp - order.price_open) if order.tp != 0 else 0
                    target_move = abs(new_tp - order.price_open)
                    
                    # Calculate threshold (10% of target move or 2 pips, whichever is larger)
                    pip_threshold = max(target_move * 0.1, symbol_info.point * 20)
                    
                    if order.tp == 0:
                        print(f"    └─ 📝 {order_type_name} #{order.ticket} ({raw_symbol}) - No TP set")
                        print(f"       Entry: {order.price_open:.{digits}f} | Risk: ${current_risk_usd:.2f}")
                        print(f"       Target Profit: ${target_profit_usd:.2f} | Setting TP to {new_tp:.{digits}f}")
                        should_adjust = True
                    elif abs(current_move - target_move) > pip_threshold:
                        print(f"    └─ 📐 {order_type_name} #{order.ticket} ({raw_symbol}) - TP needs adjustment")
                        print(f"       Entry: {order.price_open:.{digits}f}")
                        print(f"       Current TP: {order.tp:.{digits}f} (Move: {current_move:.{digits}f})")
                        print(f"       Target TP:  {new_tp:.{digits}f} (Move: {target_move:.{digits}f})")
                        print(f"       Risk: ${current_risk_usd:.2f} | Target Profit: ${target_profit_usd:.2f}")
                        should_adjust = True
                    else:
                        print(f"    └─ ✅ {order_type_name} #{order.ticket} ({raw_symbol}) - TP already correct")
                        print(f"       TP: {order.tp:.{digits}f} | Risk: ${current_risk_usd:.2f}")
                        investor_orders_skipped += 1
                        total_orders_skipped += 1
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
                            investor_orders_adjusted += 1
                            total_orders_adjusted += 1
                            any_orders_adjusted = True
                            print(f"       ✅ TP adjusted successfully")
                        else:
                            investor_orders_error += 1
                            total_orders_error += 1
                            error_msg = result.comment if result else f"Error code: {result.retcode if result else 'Unknown'}"
                            print(f"       ❌ Modification failed: {error_msg}")
                else:
                    print(f"    └─ ⚠️  Invalid tick values for {lookup_symbol}")
                    # Fallback method using profit calculation for a small price movement
                    try:
                        # Calculate point value by testing a small price movement
                        test_move = symbol_info.point * 10  # Test 10 points
                        if is_buy_direction:
                            test_price = order.price_open + test_move
                        else:
                            test_price = order.price_open - test_move
                            
                        test_profit = mt5.order_calc_profit(calc_type, lookup_symbol, order.volume_initial, 
                                                            order.price_open, test_price)
                        
                        if test_profit and test_profit != 0:
                            # Calculate point value
                            point_value = abs(test_profit) / 10  # Per point value
                            
                            # Calculate price movement needed
                            price_move_needed = target_profit_usd / point_value * symbol_info.point
                            
                            digits = symbol_info.digits
                            price_move_needed = round(price_move_needed, digits)
                            
                            if is_buy_direction:
                                new_tp = round(order.price_open + price_move_needed, digits)
                            else:
                                new_tp = round(order.price_open - price_move_needed, digits)
                            
                            print(f"    └─ ⚠️  Using fallback calculation for {raw_symbol}")
                            
                            # Check if adjustment is needed
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
                                    investor_orders_adjusted += 1
                                    total_orders_adjusted += 1
                                    any_orders_adjusted = True
                                    print(f"       ✅ TP adjusted using fallback method")
                                else:
                                    investor_orders_error += 1
                                    total_orders_error += 1
                                    print(f"       ❌ Fallback modification failed")
                            else:
                                investor_orders_skipped += 1
                                total_orders_skipped += 1
                                print(f"       ✅ TP already correct (fallback check)")
                        else:
                            investor_orders_skipped += 1
                            total_orders_skipped += 1
                            print(f"    └─ ⚠️  Cannot calculate using fallback method for {raw_symbol}")
                    except Exception as e:
                        investor_orders_error += 1
                        total_orders_error += 1
                        print(f"    └─ ❌ Fallback calculation error: {e}")

        # --- INVESTOR SUMMARY ---
        if investor_orders_checked > 0:
            print(f"\n  └─ 📊 Risk-Reward Correction Results for {user_brokerid}:")
            print(f"       • Orders checked: {investor_orders_checked}")
            print(f"       • Orders adjusted: {investor_orders_adjusted}")
            print(f"       • Orders skipped: {investor_orders_skipped}")
            if investor_symbols_normalized > 0:
                print(f"       • Symbols normalized: {investor_symbols_normalized}")
            if investor_orders_error > 0:
                print(f"       • Errors: {investor_orders_error}")
            else:
                print(f"       ✅ All adjustments completed successfully")
        else:
            print(f"  └─ 🔘 No pending orders found for {user_brokerid}.")

    # --- GLOBAL SUMMARY ---
    print(f"\n{'='*10} 🏁 RISK-REWARD CORRECTION COMPLETE {'='*10}")
    if total_orders_checked > 0:
        print(f" Total orders checked:   {total_orders_checked}")
        print(f" Total orders adjusted:  {total_orders_adjusted}")
        print(f" Total orders skipped:   {total_orders_skipped}")
        if total_symbols_normalized > 0:
            print(f" Total symbols normalized: {total_symbols_normalized}")
        if total_orders_error > 0:
            print(f" Total errors:           {total_orders_error}")
        else:
            print(f" ✅ All operations completed without errors")
    else:
        print(" No pending orders found across all investors.")
    print(f"{'='*50}\n")
    
    return any_orders_adjusted

def check_pending_orders_risk(inv_id=None):
    """
    Function 3: Validates live pending orders against the account's current risk bucket.
    Tolerance: Allows any order risk up to (Target Risk + $0.99).
    Works for small risks (0.10) and large risks (1.00+) identically.
    Now checks ALL pending order types (limit, stop, stop-limit).
    
    Args:
        inv_id (str, optional): Specific investor ID to process. If None, processes all investors.
        MT5 should already be initialized and logged in for this investor.
    
    Returns:
        bool: True if any orders were removed, False otherwise
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

    # Determine which investors to process
    if inv_id:
        investor_ids = [inv_id]
    else:
        investor_ids = list(usersdictionary.keys())
    
    if not investor_ids:
        print(" └─ 🔘 No investors found.")
        return False

    any_orders_removed = False

    for user_brokerid in investor_ids:
        print(f" [{user_brokerid}] 🔍 Auditing live risk limits...")
        
        inv_root = Path(INV_PATH) / user_brokerid
        acc_mgmt_path = inv_root / "accountmanagement.json"
        broker_cfg = usersdictionary.get(user_brokerid)

        if not broker_cfg:
            print(f"  └─ ❌ No broker config found for {user_brokerid}")
            continue

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

        # --- GET ACCOUNT INFO (MT5 should already be initialized) ---
        acc_info = mt5.account_info()
        if not acc_info:
            print(f"  └─ ❌ Could not fetch account info - MT5 might not be initialized")
            continue
            
        balance = acc_info.balance

        # Determine Primary Risk Value
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
            continue

        # Calculate the absolute ceiling
        max_allowed_risk = round(primary_risk + 0.99, 2)
        print(f"  └─ 💰 Balance: ${balance:,.2f} | Target: ${primary_risk:.2f} | Max Allowed: ${max_allowed_risk:.2f}")

        # --- CHECK ALL PENDING ORDERS ---
        # Get all pending orders (limit, stop, stop-limit)
        pending_orders = mt5.orders_get()
        orders_checked = 0
        orders_removed = 0
        orders_kept = 0
        orders_skipped = 0
        orders_error = 0

        if pending_orders:
            # Dictionary for better order type logging
            order_type_names = {
                mt5.ORDER_TYPE_BUY_LIMIT: "BUY LIMIT",
                mt5.ORDER_TYPE_SELL_LIMIT: "SELL LIMIT",
                mt5.ORDER_TYPE_BUY_STOP: "BUY STOP",
                mt5.ORDER_TYPE_SELL_STOP: "SELL STOP",
                mt5.ORDER_TYPE_BUY_STOP_LIMIT: "BUY STOP LIMIT",
                mt5.ORDER_TYPE_SELL_STOP_LIMIT: "SELL STOP LIMIT"
            }

            for order in pending_orders:
                # Check if it's any type of pending order
                if order.type not in [
                    mt5.ORDER_TYPE_BUY_LIMIT, 
                    mt5.ORDER_TYPE_SELL_LIMIT,
                    mt5.ORDER_TYPE_BUY_STOP, 
                    mt5.ORDER_TYPE_SELL_STOP,
                    mt5.ORDER_TYPE_BUY_STOP_LIMIT, 
                    mt5.ORDER_TYPE_SELL_STOP_LIMIT
                ]:
                    continue

                orders_checked += 1
                
                # Get order type for logging
                order_type_name = order_type_names.get(order.type, f"UNKNOWN({order.type})")
                
                # Determine the direction for profit calculation
                # BUY_* orders become long positions, SELL_* orders become short positions
                is_buy_direction = order.type in [
                    mt5.ORDER_TYPE_BUY_LIMIT, 
                    mt5.ORDER_TYPE_BUY_STOP, 
                    mt5.ORDER_TYPE_BUY_STOP_LIMIT
                ]
                
                calc_type = mt5.ORDER_TYPE_BUY if is_buy_direction else mt5.ORDER_TYPE_SELL
                
                # Skip orders without stop loss
                if order.sl == 0:
                    print(f"    └─ ⚠️  {order_type_name} #{order.ticket} ({order.symbol}) - No SL set, cannot calculate risk")
                    orders_skipped += 1
                    continue
                
                # Calculate the potential loss (risk) at Stop Loss
                sl_profit = mt5.order_calc_profit(
                    calc_type, 
                    order.symbol, 
                    order.volume_initial, 
                    order.price_open, 
                    order.sl
                )
                
                if sl_profit is not None:
                    order_risk_usd = round(abs(sl_profit), 2)
                    
                    # LOGIC: If order risk > Target + 0.99, remove it.
                    # Use a tiny epsilon (0.0001) to avoid float precision errors
                    if order_risk_usd > (max_allowed_risk + 0.0001): 
                        print(f"    └─ 🗑️  PURGING: {order_type_name} {order.symbol} (#{order.ticket})")
                        print(f"       Risk: ${order_risk_usd:.2f} > Max: ${max_allowed_risk:.2f}")
                        
                        cancel_request = {
                            "action": mt5.TRADE_ACTION_REMOVE,
                            "order": order.ticket
                        }
                        result = mt5.order_send(cancel_request)
                        
                        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                            orders_removed += 1
                            any_orders_removed = True
                            print(f"       ✅ Order removed successfully")
                        else:
                            orders_error += 1
                            error_msg = result.comment if result else f"Error code: {result.retcode if result else 'Unknown'}"
                            print(f"       ❌ Cancel failed: {error_msg}")
                    
                    else:
                        # Within tolerance (even if target is 0.10 and risk is 1.09)
                        orders_kept += 1
                        print(f"    └─ ✅ KEEPING: {order_type_name} {order.symbol} (#{order.ticket}) - Risk: ${order_risk_usd:.2f} (Under ${max_allowed_risk:.2f})")
                else:
                    orders_error += 1
                    print(f"    └─ ❌ Could not calculate risk for {order_type_name} #{order.ticket}")

        # Final terminal summary
        print(f"  └─ 📊 Audit Results:")
        print(f"       • Orders checked: {orders_checked}")
        print(f"       • Orders kept (within risk): {orders_kept}")
        print(f"       • Orders removed (over risk): {orders_removed}")
        if orders_skipped > 0:
            print(f"       • Orders skipped (no SL): {orders_skipped}")
        if orders_error > 0:
            print(f"       • Errors: {orders_error}")

    print(f"\n{'='*10} 🏁 RISK AUDIT COMPLETE {'='*10}\n")
    return any_orders_removed

def history_closed_orders_removal_in_pendingorders(inv_id=None):
    """
    Scans history for the last 48 hours. If a position was closed, 
    any pending limit orders with the same first 4 digits in the price 
    are cancelled to prevent re-entry.
    
    Args:
        inv_id (str, optional): Specific investor ID to process. If None, processes all investors.
        MT5 should already be initialized and logged in for this investor.
    
    Returns:
        bool: True if any orders were removed, False otherwise
    """
    from datetime import datetime, timedelta
    print(f"\n{'='*10} 📜 HISTORY AUDIT: PREVENTING RE-ENTRY {'='*10}")

    # Determine which investors to process
    if inv_id:
        investor_ids = [inv_id]
    else:
        investor_ids = list(usersdictionary.keys())
    
    if not investor_ids:
        print(" └─ 🔘 No investors found.")
        return False

    any_orders_removed = False

    for user_brokerid in investor_ids:
        print(f" [{user_brokerid}] 🔍 Checking 48h history for duplicates...")
        
        broker_cfg = usersdictionary.get(user_brokerid)
        if not broker_cfg:
            print(f"  └─ ❌ No broker config found for {user_brokerid}")
            continue

        # 1. Define the 48-hour window
        from_date = datetime.now() - timedelta(hours=48)
        to_date = datetime.now()

        # 2. Get Closed Positions (Deals)
        history_deals = mt5.history_deals_get(from_date, to_date)
        if history_deals is None:
            print(f"  └─ ⚠️ Could not access history for {user_brokerid}")
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
            continue

        # 4. Check Current Pending Orders
        pending_orders = mt5.orders_get()
        removed_count = 0
        orders_checked = 0

        if pending_orders:
            for order in pending_orders:
                # Only target limit orders
                if order.type in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT]:
                    orders_checked += 1
                    order_price_prefix = str(order.price_open).replace('.', '')[:4]
                    
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
                            any_orders_removed = True
                            print(f"     ✅ Order #{order.ticket} cancelled successfully")
                        else:
                            error_msg = res.comment if res else f"Error code: {res.retcode if res else 'Unknown'}"
                            print(f"     ❌ Failed to cancel #{order.ticket}: {error_msg}")

        print(f"  └─ 📊 Cleanup Result: {removed_count} duplicate limit orders removed out of {orders_checked} checked.")

    print(f"\n{'='*10} 🏁 HISTORY AUDIT COMPLETE {'='*10}\n")
    return any_orders_removed

def process_single_investor(inv_folder):
    """
    WORKER FUNCTION: Handles the entire pipeline for ONE investor.
    Each process calls this independently.
    """
    inv_id = inv_folder.name
    # Results dictionary to pass back to the main process for statistics
    account_stats = {"inv_id": inv_id, "success": False, "details": {}}
    
    # 1. Get broker config
    broker_cfg = usersdictionary.get(inv_id)
    if not broker_cfg:
        print(f" [{inv_id}] ❌ No broker config found")
        return account_stats

    # --- ISOLATION START ---
    # Give a small random offset to avoid exact simultaneous initialization hits on the OS
    time.sleep(random.uniform(0.1, 2.0)) 
    
    login_id = int(broker_cfg['LOGIN_ID'])
    mt5_path = broker_cfg["TERMINAL_PATH"]

    try:
        # Initialize and Login (Local to this process)
        if not mt5.initialize(path=mt5_path, timeout=180000):
            print(f" [{inv_id}] ❌ MT5 Init failed at {mt5_path}")
            return account_stats

        if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
            print(f" [{inv_id}] ❌ Login failed")
            mt5.shutdown()
            return account_stats

        # --- RUN ALL SEQUENTIAL STEPS FOR THIS BROKER ---
        # Note: All your functions (deduplicate_orders, etc.) must accept inv_id
        deduplicate_orders(inv_id=inv_id)
        filter_unauthorized_symbols(inv_id=inv_id)
        filter_unauthorized_timeframes(inv_id=inv_id)
        populate_orders_missing_fields(inv_id=inv_id)
        activate_usd_based_risk_on_empty_pricelevels(inv_id=inv_id)
        enforce_investors_risk(inv_id=inv_id)
        calculate_investor_symbols_orders(inv_id=inv_id)
        live_usd_risk_and_scaling(inv_id=inv_id)
        apply_default_prices(inv_id=inv_id)
        place_usd_orders(inv_id=inv_id)
        pending_orders_reward_correction(inv_id=inv_id)
        check_pending_orders_risk(inv_id=inv_id)
        history_closed_orders_removal_in_pendingorders(inv_id=inv_id)

        mt5.shutdown()
        account_stats["success"] = True
        print(f" [{inv_id}] ✅ Processing complete")
        
    except Exception as e:
        print(f" [{inv_id}] ❌ Critical Error: {e}")
        mt5.shutdown()
    
    return account_stats

def place_orders_parallel():
    """
    ORCHESTRATOR: Spawns multiple processes to handle investors in parallel.
    """
    print(f"\n{'='*10} 🚀 STARTING MULTIPROCESSING ENGINE {'='*10}")
    
    inv_base_path = Path(INV_PATH)
    investor_folders = [f for f in inv_base_path.iterdir() if f.is_dir()]
    
    if not investor_folders:
        print(" └─ 🔘 No investor directories found.")
        return False

    # Create a pool based on the number of accounts (or CPU cores)
    # This will run 'process_single_investor' for all folders at the same time
    with mp.Pool(processes=len(investor_folders)) as pool:
        results = pool.map(process_single_investor, investor_folders)

    # Summary logic
    successful = sum(1 for r in results if r["success"])
    print(f"\n{'='*10} PARALLEL PROCESSING COMPLETE {'='*10}")
    print(f" Total: {len(results)} | Successful: {successful} | Failed: {len(results)-successful}")
    return successful > 0

if __name__ == "__main__":
    place_orders_parallel()


