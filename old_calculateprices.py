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

BROKER_DICT_PATH = r"C:\xampp\htdocs\chronedge\synarex\ohlc.json"
USERS_PATH = r"C:\xampp\htdocs\chronedge\synarex\users.json"
SYMBOL_CATEGORY_PATH = r"C:\xampp\htdocs\chronedge\synarex\symbolscategory.json"
DEV_PATH = r"C:\xampp\htdocs\chronedge\synarex\usersdata\developers"

DICTATOR_PATH = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbolscategory\symbolscategory.json"
SYMBOLSTICK_PATH = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbolstick\symbolstick.json"

def scale_orders_proportionally():
    from datetime import datetime
    from pathlib import Path
    import json
    import os
    from collections import defaultdict

    BROKERS_JSON_PATH = r"C:\xampp\htdocs\chronedge\synarex\users.json"
    brokers_dir = os.path.dirname(BROKERS_JSON_PATH)
    developers_functions_path = os.path.join(brokers_dir, "developers_functions.json")
    
    if not os.path.exists(BROKERS_JSON_PATH) or not os.path.exists(developers_functions_path):
        print("[ERROR] Required JSON files missing.")
        return False
    
    try:
        with open(BROKERS_JSON_PATH, 'r', encoding='utf-8') as f:
            usersdictionary = json.load(f)
        with open(developers_functions_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to load configuration: {e}")
        return False
    
    total_promoted = 0
    
    for user_brokerid, items in data.items():
        if not isinstance(items, list):
            continue
        filename = next((item[len("filename:"):].strip() for item in items if str(item).strip().startswith("filename:")), None)
        if not filename or not filename.endswith(".py"):
            continue
        if user_brokerid not in usersdictionary:
            continue
        
        cfg = usersdictionary[user_brokerid]
        base_folder = cfg.get("BASE_FOLDER")
        if not base_folder:
            continue
        
        developer_folder = os.path.join(base_folder, "..", "developers", user_brokerid)
        developer_path = Path(developer_folder).resolve()
        if not developer_path.is_dir():
            continue
        
        print(f"[BROKER] {user_brokerid}")
        
        calc_risk_dirs = list(developer_path.rglob("calculatedrisk"))
        files_generated_this_broker = 0
        
        for calc_risk_dir in calc_risk_dirs:
            temp_dir = calc_risk_dir.parent
            am_files = list(temp_dir.glob("*_accountmanagement.json"))
            if not am_files:
                continue
            prefixed_am = am_files[0]
            
            am_name = prefixed_am.stem
            base_stem = am_name.replace("_accountmanagement", "").rstrip("_").lower()
            if not base_stem:
                continue
            
            # Load RISKS from accountmanagement
            try:
                with open(prefixed_am, 'r', encoding='utf-8') as f:
                    am_data = json.load(f)
                raw_risks = am_data.get("RISKS", [])
                risks_list = sorted(set(float(r) for r in raw_risks if isinstance(r, (int, float)) or str(r).replace('.', '', 1).isdigit()))
                if not risks_list:
                    continue
            except:
                continue
            
            # Find all category files in base (lowest) risk folders
            base_category_files = defaultdict(dict)  # risk_val -> {category: {"folder": ..., "file": ...}}
            actual_filenames = {}  # category -> filename

            for risk_folder in calc_risk_dir.iterdir():
                if not risk_folder.is_dir() or not risk_folder.name.startswith("risk_") or not risk_folder.name.endswith("_usd"):
                    continue
                try:
                    risk_str = risk_folder.name[5:-4].replace("_", ".")
                    risk_val = float(risk_str)
                except:
                    continue

                for file in risk_folder.iterdir():
                    if not file.name.endswith(".json") or "_accountmanagement" in file.name:
                        continue
                    # Extract category from filename: e.g., "eurusd_forex.json" -> "forex"
                    parts = file.stem.split("_")
                    if len(parts) < 2:
                        continue
                    category = parts[-1].lower()
                    expected_prefix = f"{base_stem}_"
                    if not file.stem.lower().startswith(expected_prefix):
                        continue

                    base_category_files[risk_val][category] = {"folder": risk_folder, "file": file}
                    actual_filenames[category] = file.name

            if not base_category_files:
                continue

            base_risks = sorted(base_category_files.keys())

            # Load base orders for all categories and risks
            base_orders_by_risk_category = defaultdict(lambda: defaultdict(list))  # risk -> category -> [orders]

            for risk_val, category_info in base_category_files.items():
                for category, info in category_info.items():
                    try:
                        with open(info["file"], 'r', encoding='utf-8') as f:
                            structure = json.load(f)
                        orders_section = structure.get("orders", {})
                        for market_key, market_data in orders_section.items():
                            if market_key in ["total_orders", "total_markets"]:
                                continue
                            for tf, order_list in market_data.items():
                                if not isinstance(order_list, list):
                                    continue
                                for order in order_list:
                                    order_copy = order.copy()
                                    order_copy["_original_risk"] = risk_val
                                    order_copy["_category"] = category
                                    base_orders_by_risk_category[risk_val][category].append(order_copy)
                    except:
                        continue

            # Promote to higher risks for each category independently
            target_risks = [r for r in risks_list if r > min(base_risks)]

            for target_risk in target_risks:
                target_folder_name = f"risk_{str(target_risk).replace('.', '_')}_usd"
                target_folder = calc_risk_dir / target_folder_name
                target_folder.mkdir(parents=True, exist_ok=True)

                for category, filename in actual_filenames.items():
                    out_file = target_folder / filename

                    all_promoted_orders = []
                    for source_risk in base_risks:
                        if source_risk >= target_risk:
                            continue
                        scale_factor = target_risk / source_risk
                        for orig in base_orders_by_risk_category[source_risk][category]:
                            promoted = orig.copy()
                            promoted["volume"] = round(promoted["volume"] * scale_factor, 2)
                            promoted["riskusd_amount"] = target_risk
                            promoted["promoted_from"] = source_risk
                            promoted["calculated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            clean_order = {k: v for k, v in promoted.items() if not k.startswith("_")}
                            all_promoted_orders.append(clean_order)
                            total_promoted += 1

                    if not all_promoted_orders:
                        continue

                    # Rebuild structure per category
                    markets_dict = defaultdict(lambda: defaultdict(list))
                    for order in all_promoted_orders:
                        markets_dict[order["market_name"]][order["timeframe"]].append(order)

                    orders_structure = {
                        "orders": {
                            "total_orders": len(all_promoted_orders),
                            "total_markets": len(markets_dict)
                        }
                    }
                    orders_dict = orders_structure["orders"]

                    for market_key, tf_dict in markets_dict.items():
                        sample_order = next(iter(tf_dict.values()))[0]
                        orders_dict[market_key] = {
                            "category": category,
                            "broker": user_brokerid,
                            "tick_size": sample_order.get("tick_size", 0.00001),
                            "tick_value": sample_order.get("tick_value", 0.0),
                            **tf_dict
                        }

                    try:
                        with open(out_file, 'w', encoding='utf-8') as f:
                            json.dump(orders_structure, f, indent=2, ensure_ascii=False)
                        files_generated_this_broker += 1
                    except:
                        pass
        
        if files_generated_this_broker > 0:
            print(f" → Generated {files_generated_this_broker} JSON file(s)\n")
        else:
            print(" → No output files generated\n")
    
    return True

def check_risk_integrity():
    from pathlib import Path
    import json
    import os
    import shutil
    from collections import defaultdict

    BASE_DEVELOPERS_DICT = r"C:\xampp\htdocs\chronedge\synarex\users.json"
    DEVELOPERS_FUNCTIONS = os.path.join(os.path.dirname(BASE_DEVELOPERS_DICT), "developers_functions.json")

    if not os.path.exists(BASE_DEVELOPERS_DICT) or not os.path.exists(DEVELOPERS_FUNCTIONS):
        print("[ERROR] Required configuration files missing.")
        return False

    try:
        with open(BASE_DEVELOPERS_DICT, 'r', encoding='utf-8') as f:
            usersdictionary = json.load(f)
        with open(DEVELOPERS_FUNCTIONS, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to load configuration: {e}")
        return False

    RISK_LEVELS = [0.5, 1.0, 2.0, 3.0, 4.0, 8.0, 16.0]
    RISK_FOLDERS = {r: f"risk_{str(r).replace('.', '_')}_usd" for r in RISK_LEVELS}

    total_removed_invalid_placement = 0
    total_files_cleaned = 0
    total_risk_folders_removed = 0

    for user_brokerid, items in data.items():
        if not isinstance(items, list):
            continue
        filename = next((item[len("filename:"):].strip() for item in items if str(item).strip().startswith("filename:")), None)
        if not filename or not filename.endswith(".py"):
            continue
        if user_brokerid not in usersdictionary:
            continue

        cfg = usersdictionary[user_brokerid]
        base_folder = cfg.get("BASE_FOLDER")
        if not base_folder:
            continue

        developer_folder = os.path.join(base_folder, "..", "developers", user_brokerid)
        developer_path = Path(developer_folder).resolve()
        if not developer_path.is_dir():
            continue

        print(f"[BROKER] {user_brokerid}")

        base_volumes = {}  # (stem, market_key) -> (risk, volume)
        calc_risk_dirs = list(developer_path.rglob("calculatedrisk"))
        files_cleaned_this_broker = 0
        risk_folders_removed_this_broker = 0
        surviving_risk_folders = set()

        # First pass: collect base volumes from non-promoted orders
        for calc_risk_dir in calc_risk_dirs:
            temp_dir = calc_risk_dir.parent
            am_files = list(temp_dir.glob("*_accountmanagement.json"))
            if not am_files:
                continue
            base_stem = am_files[0].stem.replace("_accountmanagement", "").rstrip("_")

            for risk in RISK_LEVELS:
                risk_folder = calc_risk_dir / RISK_FOLDERS.get(risk, "")
                if not risk_folder.is_dir():
                    continue

                category_files = [f for f in risk_folder.glob("*.json") if not "_accountmanagement" in f.name]
                for file_path in category_files:
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            structure = json.load(f)
                        orders_section = structure.get("orders", {})
                        for market_key, market_data in orders_section.items():
                            if market_key in ["total_orders", "total_markets"]:
                                continue
                            for tf, order_list in market_data.items():
                                if not isinstance(order_list, list):
                                    continue
                                for order in order_list:
                                    if "promoted_from" in order:
                                        continue
                                    key = (base_stem, market_key)
                                    current_risk = order.get("riskusd_amount")
                                    volume = order.get("volume", 0)
                                    if key not in base_volumes or current_risk < base_volumes[key][0]:
                                        base_volumes[key] = (current_risk, volume)
                    except:
                        pass

        # Second & Third pass: clean files and remove empty folders
        for calc_risk_dir in calc_risk_dirs:
            for risk in RISK_LEVELS:
                risk_folder = calc_risk_dir / RISK_FOLDERS.get(risk, "")
                if not risk_folder.is_dir():
                    continue

                category_files = [f for f in risk_folder.glob("*.json") if not "_accountmanagement" in f.name]
                if not category_files:
                    continue

                has_valid_content = False

                for file_path in category_files:
                    stem = file_path.stem.rsplit("_", 1)[0] if "_" in file_path.stem else file_path.stem
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            original_structure = json.load(f)
                        orders_section = original_structure.get("orders", {})
                    except:
                        continue

                    modified = False
                    valid_orders_by_market_tf = defaultdict(lambda: defaultdict(list))

                    for market_key, market_data in orders_section.items():
                        if market_key in ["total_orders", "total_markets"]:
                            continue
                        for tf, order_list in market_data.items():
                            if not isinstance(order_list, list):
                                continue
                            valid_orders = []
                            for order in order_list:
                                entry_risk = order.get("riskusd_amount")
                                volume = order.get("volume", 0)
                                promoted_from = order.get("promoted_from")
                                remove = False

                                if entry_risk != risk:
                                    remove = True
                                    total_removed_invalid_placement += 1

                                elif promoted_from is not None:
                                    try:
                                        src_risk = float(promoted_from)
                                    except:
                                        src_risk = None
                                    if src_risk is not None and src_risk < risk:
                                        scale = risk / src_risk
                                        key = (stem, market_key)
                                        if key in base_volumes:
                                            base_risk, base_vol = base_volumes[key]
                                            if abs(base_risk - src_risk) < 1e-8:
                                                expected_vol = round(base_vol * scale, 2)
                                                if abs(volume - expected_vol) > 1e-6:
                                                    remove = True

                                if remove:
                                    modified = True
                                else:
                                    valid_orders.append(order)

                            if valid_orders:
                                valid_orders_by_market_tf[market_key][tf] = valid_orders

                    if modified:
                        new_total_orders = sum(len(lst) for market in valid_orders_by_market_tf.values() for lst in market.values())
                        new_total_markets = len(valid_orders_by_market_tf)
                        new_structure = {"orders": {"total_orders": new_total_orders, "total_markets": new_total_markets}}

                        for market_key, tf_dict in valid_orders_by_market_tf.items():
                            if tf_dict:
                                sample = next(iter(tf_dict.values()))[0]
                                new_structure["orders"][market_key] = {
                                    "category": sample.get("category", "unknown"),
                                    "broker": user_brokerid,
                                    "tick_size": sample.get("tick_size", 0.00001),
                                    "tick_value": sample.get("tick_value", 0.0),
                                    **tf_dict
                                }

                        if new_total_markets > 0:
                            with open(file_path, 'w', encoding='utf-8') as f:
                                json.dump(new_structure, f, indent=2, ensure_ascii=False)
                            files_cleaned_this_broker += 1
                            total_files_cleaned += 1
                            has_valid_content = True
                        else:
                            os.remove(file_path)
                            total_files_cleaned += 1
                    else:
                        has_valid_content = True

                if has_valid_content:
                    surviving_risk_folders.add(str(risk_folder.resolve()))

        # Remove empty risk folders
        for calc_risk_dir in calc_risk_dirs:
            for risk in RISK_LEVELS:
                risk_folder = calc_risk_dir / RISK_FOLDERS.get(risk, "")
                if not risk_folder.is_dir():
                    continue
                if str(risk_folder.resolve()) not in surviving_risk_folders:
                    if not any(risk_folder.glob("*.json")):
                        shutil.rmtree(risk_folder)
                        risk_folders_removed_this_broker += 1
                        total_risk_folders_removed += 1

        total_actions = files_cleaned_this_broker + risk_folders_removed_this_broker
        if total_actions > 0:
            print(f" → Processed: {files_cleaned_this_broker} file(s) cleaned, {risk_folders_removed_this_broker} empty risk level(s) removed\n")
        else:
            print(" → No changes needed\n")

    return True

def remove_non_allowed_symbol_orders():
    from pathlib import Path
    import json
    import os
    from collections import defaultdict

    BROKERS_JSON_PATH = r"C:\xampp\htdocs\chronedge\synarex\users.json"
    brokers_dir = os.path.dirname(BROKERS_JSON_PATH)
    developers_functions_path = os.path.join(brokers_dir, "developers_functions.json")
    
    if not os.path.exists(BROKERS_JSON_PATH) or not os.path.exists(developers_functions_path):
        print("[ERROR] Required JSON files missing.")
        return False

    try:
        with open(BROKERS_JSON_PATH, 'r', encoding='utf-8') as f:
            usersdictionary = json.load(f)
        with open(developers_functions_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to load configuration: {e}")
        return False

    total_files_cleaned = 0
    total_orders_removed = 0

    for user_brokerid, items in data.items():
        if not isinstance(items, list):
            continue
        filename = next((item[len("filename:"):].strip() for item in items if str(item).strip().startswith("filename:")), None)
        if not filename or not filename.endswith(".py"):
            continue
        if user_brokerid not in usersdictionary:
            continue

        cfg = usersdictionary[user_brokerid]
        base_folder = cfg.get("BASE_FOLDER")
        if not base_folder:
            continue

        developer_folder = os.path.join(base_folder, "..", "developers", user_brokerid)
        developer_path = Path(developer_folder).resolve()
        if not developer_path.is_dir():
            continue

        print(f"[BROKER] {user_brokerid}")

        files_cleaned_this_broker = 0
        orders_removed_this_broker = 0

        calc_risk_dirs = list(developer_path.rglob("calculatedrisk"))
        
        for calc_risk_dir in calc_risk_dirs:
            temp_dir = calc_risk_dir.parent
            asv_files = list(temp_dir.glob("*_allowedsymbolsandvolumes.json"))
            if not asv_files:
                continue
            asv_path = asv_files[0]
            
            try:
                with open(asv_path, 'r', encoding='utf-8') as f:
                    asv_data = json.load(f)
            except:
                continue

            # NEW LOGIC:
            # Build allowed symbols per category
            # - If limited == True  → only symbols in "allowed" list
            # - If limited == False → allow ALL symbols (no restriction)
            allowed = defaultdict(set)  # cat -> set of allowed normalized symbols
            restricted_categories = set()  # categories that are in whitelist mode

            for cat, cfg in asv_data.items():
                cat_lower = cat.lower()
                limited = cfg.get("limited", False)
                allowed_list = cfg.get("allowed", [])

                if limited:
                    restricted_categories.add(cat_lower)
                    for item in allowed_list:
                        sym = " ".join(item.get("symbol", "").replace("_", " ").split()).upper()
                        if sym:
                            allowed[cat_lower].add(sym)
                # else: limited == False → do nothing, all symbols allowed for this category

            for risk_folder in calc_risk_dir.iterdir():
                if not risk_folder.is_dir() or not risk_folder.name.startswith("risk_") or not risk_folder.name.endswith("_usd"):
                    continue

                for file in risk_folder.glob("*.json"):
                    if "_accountmanagement" in file.name:
                        continue
                    try:
                        with open(file, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                    except:
                        continue

                    orders = data.get("orders", {})
                    modified = False
                    new_markets = {}

                    for market, mdata in orders.items():
                        if market in ["total_orders", "total_markets"]:
                            continue
                        cat = mdata.get("category", "").lower()
                        sym_norm = " ".join(market.replace("_", " ").split()).upper()

                        # Only restrict if the category is in limited=True mode
                        if cat in restricted_categories and sym_norm not in allowed[cat]:
                            orders_removed_this_broker += sum(len(ol) for ol in mdata.values() if isinstance(ol, list))
                            modified = True
                            continue

                        new_markets[market] = mdata

                    if modified:
                        new_total_orders = sum(len(ol) for m in new_markets.values() for ol in m.values() if isinstance(ol, list))
                        new_total_markets = len(new_markets)

                        new_data = {
                            "orders": {
                                "total_orders": new_total_orders,
                                "total_markets": new_total_markets,
                                **new_markets
                            }
                        }

                        try:
                            with open(file, 'w', encoding='utf-8') as f:
                                json.dump(new_data, f, indent=2, ensure_ascii=False)
                            files_cleaned_this_broker += 1
                        except:
                            pass

        if files_cleaned_this_broker > 0:
            print(f" → Cleaned {files_cleaned_this_broker} file(s), removed {orders_removed_this_broker} order(s)\n")
        else:
            print(" → No changes\n")

        total_files_cleaned += files_cleaned_this_broker
        total_orders_removed += orders_removed_this_broker

    print(f"[SUMMARY] Files cleaned: {total_files_cleaned}, Orders removed: {total_orders_removed}")
    return True

def filter_orders_by_timeframe():
    from pathlib import Path
    import json
    import os

    BROKERS_JSON_PATH = r"C:\xampp\htdocs\chronedge\synarex\users.json"
    brokers_dir = os.path.dirname(BROKERS_JSON_PATH)
    developers_functions_path = os.path.join(brokers_dir, "developers_functions.json")

    if not os.path.exists(BROKERS_JSON_PATH) or not os.path.exists(developers_functions_path):
        print("[ERROR] Required JSON files missing.")
        return False

    try:
        with open(BROKERS_JSON_PATH, 'r', encoding='utf-8') as f:
            usersdictionary = json.load(f)
        with open(developers_functions_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to load configuration: {e}")
        return False

    # Comprehensive timeframe mapping (add more if needed in the future)
    TF_MAP = {
        "15m": "15m", "m15": "15m", "15min": "15m",
        "30m": "30m", "m30": "30m",
        "45m": "45m", "m45": "45m",
        "1h": "1h", "h1": "1h", "60m": "1h",
        "2h": "2h", "h2": "2h",
        "4h": "4h", "h4": "4h",
        "1d": "1d", "d1": "1d",
        "1w": "1w", "w1": "1w",
        "1mn": "1mn", "mn1": "1mn",
        # Additional common ones to prevent surprises
        "5m": "5m", "m5": "5m",
        "1m": "1m", "m1": "1m",
        "3m": "3m", "m3": "3m"
    }

    total_files_cleaned = 0
    total_orders_removed = 0

    for user_brokerid, items in data.items():
        if not isinstance(items, list):
            continue
        filename = next((item[len("filename:"):].strip() for item in items if str(item).strip().startswith("filename:")), None)
        if not filename or not filename.endswith(".py"):
            continue
        if user_brokerid not in usersdictionary:
            continue

        cfg = usersdictionary[user_brokerid]
        base_folder = cfg.get("BASE_FOLDER")
        if not base_folder:
            continue

        developer_folder = os.path.join(base_folder, "..", "developers", user_brokerid)
        developer_path = Path(developer_folder).resolve()
        if not developer_path.is_dir():
            continue

        print(f"[BROKER] {user_brokerid}")

        files_cleaned_this_broker = 0
        orders_removed_this_broker = 0

        calc_risk_dirs = list(developer_path.rglob("calculatedrisk"))

        for calc_risk_dir in calc_risk_dirs:
            temp_dir = calc_risk_dir.parent
            am_files = list(temp_dir.glob("*_accountmanagement.json"))
            if not am_files:
                continue

            try:
                with open(am_files[0], 'r', encoding='utf-8') as f:
                    settings = json.load(f).get("settings", {})
                raw_tfs = settings.get("order_timeframes", ["all"])
                if not isinstance(raw_tfs, list):
                    raw_tfs = [raw_tfs]
            except Exception as e:
                print(f"   [WARN] Could not read accountmanagement: {e}")
                continue

            allowed_tfs = set()
            all_allowed = False
            for tf in raw_tfs:
                tf_str = str(tf).strip().lower()
                if tf_str == "all":
                    all_allowed = True
                    break
                norm = TF_MAP.get(tf_str, tf_str)  # use mapped value or keep original normalized
                allowed_tfs.add(norm)

            if all_allowed:
                continue  # nothing to filter

            for risk_folder in calc_risk_dir.iterdir():
                if not risk_folder.is_dir() or not risk_folder.name.startswith("risk_") or not risk_folder.name.endswith("_usd"):
                    continue

                for file in risk_folder.glob("*.json"):
                    if "_accountmanagement" in file.name:
                        continue

                    try:
                        with open(file, 'r', encoding='utf-8') as f:
                            file_data = json.load(f)
                    except Exception:
                        continue

                    orders = file_data.get("orders", {})
                    if not isinstance(orders, dict):
                        continue

                    modified = False
                    new_markets = {}

                    for market, mdata in orders.items():
                        if market in ["total_orders", "total_markets"]:
                            continue
                        if not isinstance(mdata, dict):
                            continue

                        # Keep only allowed timeframes
                        new_tfs = {}
                        for tf, order_list in mdata.items():
                            if not isinstance(order_list, list):
                                continue

                            tf_str = str(tf).strip().lower()
                            norm_tf = TF_MAP.get(tf_str, tf_str)

                            if norm_tf not in allowed_tfs:
                                removed_count = len(order_list)
                                orders_removed_this_broker += removed_count
                                modified = True
                                # Optional debug print
                                # print(f"   Removing {tf} ({removed_count} orders) from {file.name}")
                                continue

                            # Keep the original key name (as in source file)
                            new_tfs[tf] = order_list

                        if new_tfs:  # only add market if it still has allowed timeframes
                            # Preserve essential metadata fields
                            new_market_data = {
                                "category": mdata.get("category"),
                                "broker": mdata.get("broker"),
                                "tick_size": mdata.get("tick_size"),
                                "tick_value": mdata.get("tick_value"),
                                # Add any other metadata fields you know exist and want to keep
                            }
                            # Remove None values to avoid clutter
                            new_market_data = {k: v for k, v in new_market_data.items() if v is not None}
                            # Add back only the allowed timeframes
                            new_market_data.update(new_tfs)
                            new_markets[market] = new_market_data

                    if modified:
                        # Recalculate totals
                        new_total_orders = sum(len(ol) for m in new_markets.values() for ol in m.values() if isinstance(ol, list))
                        new_total_markets = len(new_markets)

                        new_file_data = {
                            "orders": {
                                "total_orders": new_total_orders,
                                "total_markets": new_total_markets,
                                **new_markets
                            }
                        }

                        try:
                            if new_total_markets > 0:
                                with open(file, 'w', encoding='utf-8') as f:
                                    json.dump(new_file_data, f, indent=2, ensure_ascii=False)
                                files_cleaned_this_broker += 1
                            else:
                                # No markets left → delete the file
                                file.unlink()
                        except Exception as e:
                            print(f"   [ERROR] Failed to write/delete {file}: {e}")

        if files_cleaned_this_broker > 0 or orders_removed_this_broker > 0:
            print(f" → Cleaned {files_cleaned_this_broker} file(s), removed {orders_removed_this_broker} order(s)\n")
        else:
            print(" → No changes\n")

        total_files_cleaned += files_cleaned_this_broker
        total_orders_removed += orders_removed_this_broker

    if total_files_cleaned > 0 or total_orders_removed > 0:
        print(f"[SUMMARY] Files cleaned: {total_files_cleaned}, Orders removed: {total_orders_removed}")
    else:
        print("[SUMMARY] No files needed cleaning.")
    return True

def remove_disabled_orders():
    from pathlib import Path
    import json
    import os

    BROKERS_JSON_PATH = r"C:\xampp\htdocs\chronedge\synarex\users.json"
    brokers_dir = os.path.dirname(BROKERS_JSON_PATH)
    developers_functions_path = os.path.join(brokers_dir, "developers_functions.json")
    
    if not os.path.exists(BROKERS_JSON_PATH) or not os.path.exists(developers_functions_path):
        print("[ERROR] Required JSON files missing.")
        return False

    try:
        with open(BROKERS_JSON_PATH, 'r', encoding='utf-8') as f:
            usersdictionary = json.load(f)
        with open(developers_functions_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to load configuration: {e}")
        return False

    for user_brokerid, items in data.items():
        if not isinstance(items, list):
            continue
        filename = next((item[len("filename:"):].strip() for item in items if str(item).strip().startswith("filename:")), None)
        if not filename or not filename.endswith(".py"):
            continue
        if user_brokerid not in usersdictionary:
            continue

        cfg = usersdictionary[user_brokerid]
        base_folder = cfg.get("BASE_FOLDER")
        if not base_folder:
            continue

        developer_folder = os.path.join(base_folder, "..", "developers", user_brokerid)
        developer_path = Path(developer_folder).resolve()
        if not developer_path.is_dir():
            continue

        print(f"[BROKER] {user_brokerid}")

        files_cleaned_this_broker = 0
        orders_removed_this_broker = 0

        calc_risk_dirs = list(developer_path.rglob("calculatedrisk"))

        for calc_risk_dir in calc_risk_dirs:
            temp_dir = calc_risk_dir.parent

            am_files = list(temp_dir.glob("*_accountmanagement.json"))
            disable_mode = "sameordertype"
            if am_files:
                try:
                    with open(am_files[0], 'r', encoding='utf-8') as f:
                        settings = json.load(f).get("settings", {})
                    mode = settings.get("disable_orders", "").strip().lower()
                    if mode in ["sameordertype", "allordertype"]:
                        disable_mode = mode
                except:
                    pass

            disabled_files = list(temp_dir.glob("*_disabledorders.json"))
            if not disabled_files:
                continue

            try:
                with open(disabled_files[0], 'r', encoding='utf-8') as f:
                    disabled_data = json.load(f)
            except:
                continue

            disabled = set()
            for section in ["OPEN_POSITIONS", "PENDING_POSITIONS", "HISTORY_ORDERS"]:
                for sym, info in disabled_data.get(section, {}).items():
                    sym_norm = " ".join(sym.replace("_", " ").split()).upper()
                    order_type = info.get("limit_order")
                    if order_type in ["buy_limit", "sell_limit"]:
                        if disable_mode == "allordertype":
                            disabled.add(sym_norm)
                        else:
                            disabled.add((sym_norm, order_type))

            if not disabled:
                continue

            for risk_folder in calc_risk_dir.iterdir():
                if not risk_folder.is_dir() or not risk_folder.name.startswith("risk_") or not risk_folder.name.endswith("_usd"):
                    continue

                for file in risk_folder.glob("*.json"):
                    if "_accountmanagement" in file.name or "_disabledorders" in file.name:
                        continue
                    try:
                        with open(file, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                    except:
                        continue

                    orders = data.get("orders", {})
                    modified = False
                    new_markets = {}

                    for market, mdata in orders.items():
                        if market in ["total_orders", "total_markets"]:
                            continue
                        new_tfs = {}
                        for tf, order_list in mdata.items():
                            if not isinstance(order_list, list):
                                continue
                            valid = []
                            for order in order_list:
                                order_type = order.get("limit_order")
                                if order_type not in ["buy_limit", "sell_limit"]:
                                    valid.append(order)
                                    continue
                                sym_norm = " ".join(market.replace("_", " ").split()).upper()
                                should_remove = (
                                    (disable_mode == "allordertype" and sym_norm in disabled) or
                                    (disable_mode == "sameordertype" and (sym_norm, order_type) in disabled)
                                )
                                if should_remove:
                                    orders_removed_this_broker += 1
                                    modified = True
                                    continue
                                valid.append(order)
                            if valid:
                                new_tfs[tf] = valid

                        if new_tfs:
                            new_markets[market] = {**mdata, **new_tfs}

                    if modified:
                        new_total_orders = sum(len(ol) for m in new_markets.values() for ol in m.values() if isinstance(ol, list))
                        new_total_markets = len(new_markets)

                        if new_total_markets > 0:
                            new_data = {
                                "orders": {
                                    "total_orders": new_total_orders,
                                    "total_markets": new_total_markets,
                                    **new_markets
                                }
                            }
                            try:
                                with open(file, 'w', encoding='utf-8') as f:
                                    json.dump(new_data, f, indent=2, ensure_ascii=False)
                                files_cleaned_this_broker += 1
                            except:
                                pass
                        else:
                            try:
                                file.unlink()
                            except:
                                pass

        if files_cleaned_this_broker > 0:
            print(f" → Cleaned {files_cleaned_this_broker} file(s), removed {orders_removed_this_broker} order(s)\n")
        else:
            print(" → No changes\n")

    return True  

def calculate_forex_sl_tp_market_old():
    from datetime import datetime
    from pathlib import Path
    import json
    import os

    # --- Path Configurations ---
    USERS_JSON_PATH = r"C:\xampp\htdocs\chronedge\synarex\users.json"
    DEV_BASE_PATH = r"C:\xampp\htdocs\chronedge\synarex\usersdata\developers"
    SYMBOL_CATEGORY_PATH = r"C:\xampp\htdocs\chronedge\synarex\symbolscategory.json"

    # Load Required Base Files
    if not all(os.path.exists(p) for p in [USERS_JSON_PATH, SYMBOL_CATEGORY_PATH]):
        print("[ERROR] Base configuration files missing.")
        return False

    try:
        with open(USERS_JSON_PATH, 'r', encoding='utf-8') as f:
            users_dict = json.load(f)
        with open(SYMBOL_CATEGORY_PATH, 'r', encoding='utf-8') as f:
            category_data = json.load(f)
        
        # Identify which symbols are forex
        forex_symbols = set(category_data.get("forex", []))
    except Exception as e:
        print(f"[ERROR] Failed to load initial data: {e}")
        return False

    # Iterate through each broker/user in users.json
    for user_brokerid in users_dict.keys():
        dev_path = os.path.join(DEV_BASE_PATH, user_brokerid)
        limit_orders_path = os.path.join(dev_path, "limit_orders.json")
        asv_path = os.path.join(dev_path, "allowedsymbolsandvolumes.json")

        # Skip if necessary files don't exist for this specific developer
        if not os.path.exists(limit_orders_path):
            continue

        print(f"[PROCESS] Checking Broker: {user_brokerid}")

        # Load Allowed Symbols and Volumes
        symbol_rules = {}
        if os.path.exists(asv_path):
            try:
                with open(asv_path, 'r', encoding='utf-8') as f:
                    asv_content = json.load(f)
                    forex_cfg = asv_content.get("forex", {})
                    # If limited is true, we only use symbols in the allowed list
                    if forex_cfg.get("limited", False):
                        for item in forex_cfg.get("allowed", []):
                            sym = item.get("symbol", "").upper()
                            symbol_rules[sym] = {
                                "volume": float(item.get("volume", 0.01)),
                                "risk": float(item.get("risk", 4.0))
                            }
                    else:
                        # If not limited, we'll use a default later if symbol not in rules
                        pass
            except Exception as e:
                print(f"  → Warning: Could not parse ASV for {user_brokerid}: {e}")

        # Load existing limit_orders.json
        try:
            with open(limit_orders_path, 'r', encoding='utf-8') as f:
                orders_list = json.load(f)
        except Exception as e:
            print(f"  → Error reading limit_orders.json: {e}")
            continue

        updated_orders = []
        changes_made = False

        for order in orders_list:
            symbol = order.get("symbol", "").upper()
            
            # 1. Filter: Must be a Forex symbol according to symbolscategory.json
            if symbol not in forex_symbols:
                updated_orders.append(order)
                continue

            # 2. Check ASV rules if "limited" was true
            # (If symbol_rules is populated but symbol isn't in it, we skip calculation)
            if symbol_rules and symbol not in symbol_rules:
                updated_orders.append(order)
                continue

            # Get Volume and Risk (Prefer ASV, fallback to defaults)
            rule = symbol_rules.get(symbol, {"volume": 0.01, "risk": 4.0})
            volume = rule["volume"]
            risk_usd = rule["risk"]

            # Calculation Logic
            try:
                entry = float(order.get("entry", 0))
                tick_size = float(order.get("tick_size", 0.00001))
                tick_value = float(order.get("tick_value", 0))
                rr_ratio = float(order.get("risk_reward", 1))
                order_type = order.get("order_type", "")

                if tick_value <= 0 or entry <= 0:
                    updated_orders.append(order)
                    continue

                # Math for SL and TP
                pip_size = 10 * tick_size
                pip_value_usd = tick_value * volume * (pip_size / tick_size)
                
                if pip_value_usd <= 0:
                    updated_orders.append(order)
                    continue

                sl_pips = risk_usd / pip_value_usd
                tp_pips = sl_pips * rr_ratio
                
                # Determine decimals for rounding
                digits = 5 if tick_size <= 1e-5 else 3

                if order_type == "buy_limit":
                    sl_price = round(entry - (sl_pips * pip_size), digits)
                    tp_price = round(entry + (tp_pips * pip_size), digits)
                elif order_type == "sell_limit":
                    sl_price = round(entry + (sl_pips * pip_size), digits)
                    tp_price = round(entry - (tp_pips * pip_size), digits)
                else:
                    updated_orders.append(order)
                    continue

                # Update the original order structure
                order["exit"] = sl_price
                order["target"] = tp_price
                # Optional: tracking metadata
                order["calculated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                updated_orders.append(order)
                changes_made = True

            except Exception as e:
                print(f"  → Failed to calculate for {symbol}: {e}")
                updated_orders.append(order)

        # Save back to the original file path without restructuring
        if changes_made:
            try:
                with open(limit_orders_path, 'w', encoding='utf-8') as f:
                    json.dump(updated_orders, f, indent=4, ensure_ascii=False)
                print(f"  → Successfully updated SL/TP for {user_brokerid}")
            except Exception as e:
                print(f"  → Error saving file: {e}")
        else:
            print(f"  → No forex calculations needed for {user_brokerid}")

    return True

def calculate_basketindices_sl_tp_market():
    from datetime import datetime
    from pathlib import Path
    from collections import defaultdict
    import json
    import os

    BROKERS_JSON_PATH = r"C:\xampp\htdocs\chronedge\synarex\users.json"
    brokers_dir = os.path.dirname(BROKERS_JSON_PATH)
    developers_functions_path = os.path.join(brokers_dir, "developers_functions.json")
    
    if not os.path.exists(BROKERS_JSON_PATH) or not os.path.exists(developers_functions_path):
        print("[ERROR] Required JSON files missing.")
        return False

    try:
        with open(BROKERS_JSON_PATH, 'r', encoding='utf-8') as f:
            usersdictionary = json.load(f)
        with open(developers_functions_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to load configuration: {e}")
        return False

    # Assuming SYMBOLSTICK_DATA is defined globally or loaded elsewhere in your project
    global SYMBOLSTICK_DATA

    for user_brokerid, items in data.items():
        if not isinstance(items, list):
            continue
        filename = next((item[len("filename:"):].strip() for item in items if str(item).strip().startswith("filename:")), None)
        if not filename or not filename.endswith(".py"):
            continue
        if user_brokerid not in usersdictionary:
            continue

        cfg = usersdictionary[user_brokerid]
        base_folder = cfg.get("BASE_FOLDER")
        if not base_folder:
            continue

        developer_folder = os.path.join(base_folder, "..", "developers", user_brokerid)
        developer_path = Path(developer_folder).resolve()
        if not developer_path.is_dir():
            continue

        temp_files = list(developer_path.rglob("*_temp.json"))
        if not temp_files:
            continue

        print(f"[BROKER] {user_brokerid}")

        broker_clean = ''.join(c for c in user_brokerid if not c.isdigit()).lower()
        files_saved_this_broker = 0

        for temp_path in temp_files:
            temp_name = temp_path.name.lower()
            original_stem = temp_name[:-10] if temp_name.endswith('_temp.json') else temp_path.stem
            temp_dir = temp_path.parent
            calc_risk_dir = temp_dir / "calculatedrisk"
            calc_risk_dir.mkdir(exist_ok=True)

            # Clean existing basket indices output files
            for risk_folder in calc_risk_dir.iterdir():
                if risk_folder.is_dir() and risk_folder.name.startswith("risk_") and risk_folder.name.endswith("_usd"):
                    for basket_file in risk_folder.glob("*_basketindices.json"):
                        try:
                            basket_file.unlink()
                        except:
                            pass

            # Load *_allowedsymbolsandvolumes.json
            prefixed_asv = temp_dir / f"{original_stem}_allowedsymbolsandvolumes.json"
            if not prefixed_asv.is_file():
                continue
            try:
                with open(prefixed_asv, 'r', encoding='utf-8') as f:
                    asv_data = json.load(f)
            except:
                continue

            basket_config = asv_data.get("basket_indices", {})

            # NEW LOGIC (same as updated forex version):
            # - limited == True  → only process symbols listed in "allowed"
            # - limited == False → allow ALL basket indices symbols found in temp file
            limited = basket_config.get("limited", False)
            allowed_list = basket_config.get("allowed", [])

            if limited and not allowed_list:
                # limited=True but nothing allowed → no processing
                continue

            # Build symbol_rules only for limited mode
            symbol_rules = {}
            if limited:
                for item in allowed_list:
                    sym = " ".join(item.get("symbol", "").replace("_", " ").split()).upper()
                    vol = float(item.get("volume", 0))
                    risk = float(item.get("risk", 0))
                    if vol > 0 and risk > 0:
                        symbol_rules[sym] = {"volume": vol, "risk_usd": risk}

            # Load account management settings
            prefixed_am = temp_dir / f"{original_stem}_accountmanagement.json"
            if not prefixed_am.is_file():
                continue
            try:
                with open(prefixed_am, 'r', encoding='utf-8') as f:
                    am_data = json.load(f)
            except:
                continue

            settings = am_data.get("settings", {})
            calc_sl = str(settings.get("calculate_stoploss", "")).strip().lower() == "yes"
            calc_tp = str(settings.get("calculate_takeprofit", "")).strip().lower() == "yes"
            calculation_type = str(settings.get("calculation_type", "default")).strip().lower()

            try:
                rr_ratio_raw = settings.get("risk_reward_ratio", 3.0)
                rr_ratio = float(rr_ratio_raw)
                if rr_ratio <= 0:
                    rr_ratio = 3.0
            except:
                rr_ratio = 3.0

            record_from = str(settings.get("record_orders_from", "all")).strip().lower()
            orders_per_sym_str = str(settings.get("orders_per_symbol", "all")).strip().lower()
            max_orders_per_sym = None if orders_per_sym_str == "all" else int(orders_per_sym_str) if orders_per_sym_str.isdigit() else None

            # Load orders
            try:
                with open(temp_path, 'r', encoding='utf-8') as f:
                    content = json.load(f)
                orders_section = content.get("orders", {})
            except:
                continue

            results_by_risk = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
            basket_found = False
            symbol_orders = defaultdict(list)

            for market_key, market_data in orders_section.items():
                if market_key in ["total_orders", "total_markets"] or market_data.get("category", "").lower() != "basket_indices":
                    continue

                basket_found = True
                norm_market = " ".join(market_key.replace("_", " ").split()).upper()

                # Determine volume and risk
                if limited:
                    rule = symbol_rules.get(norm_market)
                    if not rule:
                        continue
                    volume_to_use = rule["volume"]
                    risk_usd_to_use = rule["risk_usd"]
                else:
                    # limited=False → allow all basket indices with defaults
                    volume_to_use = 0.01    # adjust default volume if needed
                    risk_usd_to_use = 4.0   # adjust default risk if needed

                tick_info = SYMBOLSTICK_DATA.get(norm_market, {})
                if tick_info.get("broker") != broker_clean:
                    tick_info = {}
                tick_size = tick_info.get("tick_size", 0.001)
                tick_value = tick_info.get("tick_value", 0.1)
                pip_size = 10 * tick_size
                pip_value_usd = tick_value * volume_to_use * (pip_size / tick_size)
                if pip_value_usd <= 0:
                    continue

                sl_pips = risk_usd_to_use / pip_value_usd
                tp_pips = sl_pips * rr_ratio

                for tf, order_list in market_data.items():
                    if not isinstance(order_list, list):
                        continue
                    for order in order_list:
                        order_type = order.get("order_type") or order.get("limit_order")
                        if order_type not in ["buy_limit", "sell_limit"] or "entry_price" not in order:
                            continue
                        try:
                            entry_price = float(order["entry_price"])
                            exit_price = float(order.get("exit_price")) if order.get("exit_price") is not None else None
                            profit_price = float(order.get("profit_price")) if order.get("profit_price") is not None else None
                        except:
                            continue

                        order_info = {
                            "tf": tf,
                            "order_type": order_type,
                            "entry_price": entry_price,
                            "exit_price": exit_price,
                            "profit_price": profit_price,
                            "market_key": market_key,
                            "volume_to_use": volume_to_use,
                            "risk_usd_to_use": risk_usd_to_use,
                            "sl_pips": sl_pips,
                            "tp_pips": tp_pips,
                            "tick_size": tick_size,
                            "pip_size": pip_size
                        }
                        symbol_orders[norm_market].append(order_info)

            # Filtering (same as before)
            filtered_symbol_orders = {}
            for norm_market, orders in symbol_orders.items():
                filtered = orders

                if record_from != "all":
                    buys = [o for o in orders if o["order_type"] == "buy_limit"]
                    sells = [o for o in orders if o["order_type"] == "sell_limit"]
                    if record_from == "buys_only":
                        filtered = buys
                    elif record_from == "sells_only":
                        filtered = sells

                if max_orders_per_sym is not None and len(filtered) > max_orders_per_sym:
                    buys_f = [o for o in filtered if o["order_type"] == "buy_limit"]
                    sells_f = [o for o in filtered if o["order_type"] == "sell_limit"]
                    selected = []
                    if buys_f:
                        selected.extend(sorted(buys_f, key=lambda x: x["entry_price"])[:max_orders_per_sym])
                    if sells_f and len(selected) < max_orders_per_sym:
                        remaining = max_orders_per_sym - len(selected)
                        selected.extend(sorted(sells_f, key=lambda x: x["entry_price"], reverse=True)[:remaining])
                    filtered = selected

                filtered_symbol_orders[norm_market] = filtered

            # Coordination mode TP
            coordinated_tp = {}
            if calculation_type == "coordination":
                for norm_market, orders in filtered_symbol_orders.items():
                    if len(orders) < 2:
                        continue
                    buys = [o for o in orders if o["order_type"] == "buy_limit"]
                    sells = [o for o in orders if o["order_type"] == "sell_limit"]
                    buy_prices = sorted([o["entry_price"] for o in buys])
                    sell_prices = sorted([o["entry_price"] for o in sells])
                    rr_distance_pips = orders[0]["sl_pips"] * rr_ratio

                    for sell_order in sells:
                        candidates = [p for p in buy_prices if p < sell_order["entry_price"]]
                        if candidates:
                            closest_buy = max(candidates)
                            distance = sell_order["entry_price"] - closest_buy
                            if distance >= rr_distance_pips * orders[0]["pip_size"]:
                                coordinated_tp[(sell_order["tf"], sell_order["entry_price"], "sell_limit")] = closest_buy - orders[0]["tick_size"]

                    for buy_order in buys:
                        candidates = [p for p in sell_prices if p > buy_order["entry_price"]]
                        if candidates:
                            closest_sell = min(candidates)
                            distance = closest_sell - buy_order["entry_price"]
                            if distance >= rr_distance_pips * orders[0]["pip_size"]:
                                coordinated_tp[(buy_order["tf"], buy_order["entry_price"], "buy_limit")] = closest_sell + orders[0]["tick_size"]

            # Build final orders
            for norm_market, orders in filtered_symbol_orders.items():
                if not orders:
                    continue
                sample = orders[0]
                digits = len(str(sample["tick_size"]).split('.')[-1]) if '.' in str(sample["tick_size"]) else 0
                digits = max(digits, 2)

                for ord_info in orders:
                    entry_price = ord_info["entry_price"]
                    order_type = ord_info["order_type"]
                    tf = ord_info["tf"]
                    manual_sl = ord_info["exit_price"]
                    manual_tp = ord_info["profit_price"]
                    market_key = ord_info["market_key"]

                    sl_price = None
                    tp_price = None

                    if calc_sl:
                        sl_price = entry_price - (ord_info["sl_pips"] * ord_info["pip_size"]) if order_type == "buy_limit" else entry_price + (ord_info["sl_pips"] * ord_info["pip_size"])
                    elif manual_sl is not None:
                        sl_price = manual_sl

                    key = (tf, entry_price, order_type)
                    if calculation_type == "coordination" and key in coordinated_tp:
                        tp_price = coordinated_tp[key]
                    elif calc_tp:
                        tp_price = entry_price + (ord_info["tp_pips"] * ord_info["pip_size"]) if order_type == "buy_limit" else entry_price - (ord_info["tp_pips"] * ord_info["pip_size"])
                    elif manual_tp is not None:
                        tp_price = manual_tp
                    else:
                        tp_price = entry_price + (ord_info["tp_pips"] * ord_info["pip_size"]) if order_type == "buy_limit" else entry_price - (ord_info["tp_pips"] * ord_info["pip_size"])

                    if sl_price is not None:
                        sl_price = round(sl_price, digits)
                    tp_price = round(tp_price, digits)

                    calc_order = {
                        "market_name": market_key,
                        "timeframe": tf,
                        "limit_order": order_type,
                        "entry_price": entry_price,
                        "sl_price": sl_price,
                        "tp_price": tp_price,
                        "volume": ord_info["volume_to_use"],
                        "riskusd_amount": ord_info["risk_usd_to_use"],
                        "rr_ratio": rr_ratio,
                        "sl_pips": round(ord_info["sl_pips"], 2),
                        "tp_pips": round(ord_info["tp_pips"], 2),
                        "calculated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "broker": user_brokerid
                    }

                    risk_usd = ord_info["risk_usd_to_use"]
                    results_by_risk[risk_usd][market_key][tf].append(calc_order)

            if not basket_found or not results_by_risk:
                continue

            # Save output files
            for risk_usd, markets_dict in results_by_risk.items():
                risk_folder_name = f"risk_{str(risk_usd).replace('.', '_')}_usd"
                risk_folder = calc_risk_dir / risk_folder_name
                risk_folder.mkdir(parents=True, exist_ok=True)

                orders_structure = {
                    "orders": {
                        "total_orders": sum(len(orders) for market in markets_dict.values() for orders in market.values()),
                        "total_markets": len(markets_dict)
                    }
                }
                orders_dict = orders_structure["orders"]

                for market_key, tf_dict in markets_dict.items():
                    norm_market = " ".join(market_key.replace("_", " ").split()).upper()
                    tick_info = SYMBOLSTICK_DATA.get(norm_market, {})
                    if tick_info.get("broker") != broker_clean:
                        tick_info = {}
                    orders_dict[market_key] = {
                        "category": "basket_indices",
                        "broker": user_brokerid,
                        "tick_size": tick_info.get("tick_size", 0.001),
                        "tick_value": tick_info.get("tick_value", 0.1),
                        **tf_dict
                    }

                out_file = risk_folder / f"{original_stem}_basketindices.json"
                try:
                    with open(out_file, 'w', encoding='utf-8') as f:
                        json.dump(orders_structure, f, indent=2, ensure_ascii=False)
                    files_saved_this_broker += 1
                except Exception as e:
                    print(f"Failed to save {out_file}: {e}")

        if files_saved_this_broker > 0:
            print(f" → Generated {files_saved_this_broker} basket indices JSON file(s)\n")
        else:
            print(" → No basket indices output files generated\n")

    return True 

def calculate_synthetics_sl_tp_market():
    from datetime import datetime
    from pathlib import Path
    from collections import defaultdict
    import json
    import os

    BROKERS_JSON_PATH = r"C:\xampp\htdocs\chronedge\synarex\users.json"
    brokers_dir = os.path.dirname(BROKERS_JSON_PATH)
    developers_functions_path = os.path.join(brokers_dir, "developers_functions.json")
    
    if not os.path.exists(BROKERS_JSON_PATH) or not os.path.exists(developers_functions_path):
        print("[ERROR] Required JSON files missing.")
        return False

    try:
        with open(BROKERS_JSON_PATH, 'r', encoding='utf-8') as f:
            usersdictionary = json.load(f)
        with open(developers_functions_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to load configuration: {e}")
        return False

    # Assumes SYMBOLSTICK_DATA is available globally
    global SYMBOLSTICK_DATA

    for user_brokerid, items in data.items():
        if not isinstance(items, list):
            continue
        filename = next((item[len("filename:"):].strip() for item in items if str(item).strip().startswith("filename:")), None)
        if not filename or not filename.endswith(".py"):
            continue
        if user_brokerid not in usersdictionary:
            continue

        cfg = usersdictionary[user_brokerid]
        base_folder = cfg.get("BASE_FOLDER")
        if not base_folder:
            continue

        developer_folder = os.path.join(base_folder, "..", "developers", user_brokerid)
        developer_path = Path(developer_folder).resolve()
        if not developer_path.is_dir():
            continue

        temp_files = list(developer_path.rglob("*_temp.json"))
        if not temp_files:
            continue

        print(f"[BROKER] {user_brokerid}")

        broker_clean = ''.join(c for c in user_brokerid if not c.isdigit()).lower()
        files_saved_this_broker = 0

        for temp_path in temp_files:
            temp_name = temp_path.name.lower()
            original_stem = temp_name[:-10] if temp_name.endswith('_temp.json') else temp_path.stem
            temp_dir = temp_path.parent
            calc_risk_dir = temp_dir / "calculatedrisk"
            calc_risk_dir.mkdir(exist_ok=True)

            # Clean existing synthetics output files
            for risk_folder in calc_risk_dir.iterdir():
                if risk_folder.is_dir() and risk_folder.name.startswith("risk_") and risk_folder.name.endswith("_usd"):
                    for synth_file in risk_folder.glob("*_synthetics.json"):
                        try:
                            synth_file.unlink()
                        except:
                            pass

            # Load *_allowedsymbolsandvolumes.json
            prefixed_asv = temp_dir / f"{original_stem}_allowedsymbolsandvolumes.json"
            if not prefixed_asv.is_file():
                continue
            try:
                with open(prefixed_asv, 'r', encoding='utf-8') as f:
                    asv_data = json.load(f)
            except:
                continue

            synthetics_config = asv_data.get("synthetics", {})

            # NEW LOGIC (consistent with forex and basketindices):
            # - limited == True  → only process symbols listed in "allowed"
            # - limited == False → allow ALL synthetics symbols found in temp file
            limited = synthetics_config.get("limited", False)
            allowed_list = synthetics_config.get("allowed", [])

            if limited and not allowed_list:
                # limited=True but nothing listed → no synthetics processing
                continue

            # Build symbol_rules only in limited mode
            symbol_rules = {}
            if limited:
                for item in allowed_list:
                    sym = " ".join(item.get("symbol", "").replace("_", " ").split()).upper()
                    vol = float(item.get("volume", 0))
                    risk = float(item.get("risk", 0))
                    if vol > 0 and risk > 0:
                        symbol_rules[sym] = {"volume": vol, "risk_usd": risk}

            # Load account management settings
            prefixed_am = temp_dir / f"{original_stem}_accountmanagement.json"
            if not prefixed_am.is_file():
                continue
            try:
                with open(prefixed_am, 'r', encoding='utf-8') as f:
                    am_data = json.load(f)
            except:
                continue

            settings = am_data.get("settings", {})
            calc_sl = str(settings.get("calculate_stoploss", "")).strip().lower() == "yes"
            calc_tp = str(settings.get("calculate_takeprofit", "")).strip().lower() == "yes"
            calculation_type = str(settings.get("calculation_type", "default")).strip().lower()

            try:
                rr_ratio_raw = settings.get("risk_reward_ratio", 3.0)
                rr_ratio = float(rr_ratio_raw)
                if rr_ratio <= 0:
                    rr_ratio = 3.0
            except:
                rr_ratio = 3.0

            record_from = str(settings.get("record_orders_from", "all")).strip().lower()
            orders_per_sym_str = str(settings.get("orders_per_symbol", "all")).strip().lower()
            max_orders_per_sym = None if orders_per_sym_str == "all" else int(orders_per_sym_str) if orders_per_sym_str.isdigit() else None

            # Load orders
            try:
                with open(temp_path, 'r', encoding='utf-8') as f:
                    content = json.load(f)
                orders_section = content.get("orders", {})
            except:
                continue

            results_by_risk = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
            synthetics_found = False
            symbol_orders = defaultdict(list)

            for market_key, market_data in orders_section.items():
                if market_key in ["total_orders", "total_markets"] or market_data.get("category", "").lower() != "synthetics":
                    continue

                synthetics_found = True
                norm_market = " ".join(market_key.replace("_", " ").split()).upper()

                # Determine volume and risk based on mode
                if limited:
                    rule = symbol_rules.get(norm_market)
                    if not rule:
                        continue
                    volume_to_use = rule["volume"]
                    risk_usd_to_use = rule["risk_usd"]
                else:
                    # limited=False → allow all synthetics symbols with defaults
                    volume_to_use = 0.01    # default volume for synthetics (adjust if needed)
                    risk_usd_to_use = 4.0   # default risk in USD (adjust if needed)

                tick_info = SYMBOLSTICK_DATA.get(norm_market, {})
                if tick_info.get("broker") != broker_clean:
                    tick_info = {}
                tick_size = tick_info.get("tick_size", 0.01)
                tick_value = tick_info.get("tick_value", 0.01)
                pip_size = 10 * tick_size
                pip_value_usd = tick_value * volume_to_use * (pip_size / tick_size)
                if pip_value_usd <= 0:
                    continue

                sl_pips = risk_usd_to_use / pip_value_usd
                tp_pips = sl_pips * rr_ratio

                for tf, order_list in market_data.items():
                    if not isinstance(order_list, list):
                        continue
                    for order in order_list:
                        order_type = order.get("order_type") or order.get("limit_order")
                        if order_type not in ["buy_limit", "sell_limit"] or "entry_price" not in order:
                            continue
                        try:
                            entry_price = float(order["entry_price"])
                            exit_price = float(order.get("exit_price")) if order.get("exit_price") is not None else None
                            profit_price = float(order.get("profit_price")) if order.get("profit_price") is not None else None
                        except:
                            continue

                        order_info = {
                            "tf": tf,
                            "order_type": order_type,
                            "entry_price": entry_price,
                            "exit_price": exit_price,
                            "profit_price": profit_price,
                            "market_key": market_key,
                            "volume_to_use": volume_to_use,
                            "risk_usd_to_use": risk_usd_to_use,
                            "sl_pips": sl_pips,
                            "tp_pips": tp_pips,
                            "tick_size": tick_size,
                            "pip_size": pip_size
                        }
                        symbol_orders[norm_market].append(order_info)

            # Filtering: buys/sells only, max orders per symbol
            filtered_symbol_orders = {}
            for norm_market, orders in symbol_orders.items():
                filtered = orders

                if record_from != "all":
                    buys = [o for o in orders if o["order_type"] == "buy_limit"]
                    sells = [o for o in orders if o["order_type"] == "sell_limit"]
                    if record_from == "buys_only":
                        filtered = buys
                    elif record_from == "sells_only":
                        filtered = sells

                if max_orders_per_sym is not None and len(filtered) > max_orders_per_sym:
                    buys_f = [o for o in filtered if o["order_type"] == "buy_limit"]
                    sells_f = [o for o in filtered if o["order_type"] == "sell_limit"]
                    selected = []
                    if buys_f:
                        selected.extend(sorted(buys_f, key=lambda x: x["entry_price"])[:max_orders_per_sym])
                    if sells_f and len(selected) < max_orders_per_sym:
                        remaining = max_orders_per_sym - len(selected)
                        selected.extend(sorted(sells_f, key=lambda x: x["entry_price"], reverse=True)[:remaining])
                    filtered = selected

                filtered_symbol_orders[norm_market] = filtered

            # Coordination mode for TP
            coordinated_tp = {}
            if calculation_type == "coordination":
                for norm_market, orders in filtered_symbol_orders.items():
                    if len(orders) < 2:
                        continue
                    buys = [o for o in orders if o["order_type"] == "buy_limit"]
                    sells = [o for o in orders if o["order_type"] == "sell_limit"]
                    buy_prices = sorted([o["entry_price"] for o in buys])
                    sell_prices = sorted([o["entry_price"] for o in sells])
                    rr_distance_pips = orders[0]["sl_pips"] * rr_ratio

                    for sell_order in sells:
                        candidates = [p for p in buy_prices if p < sell_order["entry_price"]]
                        if candidates:
                            closest_buy = max(candidates)
                            distance = sell_order["entry_price"] - closest_buy
                            if distance >= rr_distance_pips * orders[0]["pip_size"]:
                                coordinated_tp[(sell_order["tf"], sell_order["entry_price"], "sell_limit")] = closest_buy - orders[0]["tick_size"]

                    for buy_order in buys:
                        candidates = [p for p in sell_prices if p > buy_order["entry_price"]]
                        if candidates:
                            closest_sell = min(candidates)
                            distance = closest_sell - buy_order["entry_price"]
                            if distance >= rr_distance_pips * orders[0]["pip_size"]:
                                coordinated_tp[(buy_order["tf"], buy_order["entry_price"], "buy_limit")] = closest_sell + orders[0]["tick_size"]

            # Build final calculated orders
            for norm_market, orders in filtered_symbol_orders.items():
                if not orders:
                    continue
                sample = orders[0]
                digits = len(str(sample["tick_size"]).split('.')[-1]) if '.' in str(sample["tick_size"]) else 0
                digits = max(digits, 2)

                for ord_info in orders:
                    entry_price = ord_info["entry_price"]
                    order_type = ord_info["order_type"]
                    tf = ord_info["tf"]
                    manual_sl = ord_info["exit_price"]
                    manual_tp = ord_info["profit_price"]
                    market_key = ord_info["market_key"]

                    sl_price = None
                    tp_price = None

                    if calc_sl:
                        sl_price = entry_price - (ord_info["sl_pips"] * ord_info["pip_size"]) if order_type == "buy_limit" else entry_price + (ord_info["sl_pips"] * ord_info["pip_size"])
                    elif manual_sl is not None:
                        sl_price = manual_sl

                    key = (tf, entry_price, order_type)
                    if calculation_type == "coordination" and key in coordinated_tp:
                        tp_price = coordinated_tp[key]
                    elif calc_tp:
                        tp_price = entry_price + (ord_info["tp_pips"] * ord_info["pip_size"]) if order_type == "buy_limit" else entry_price - (ord_info["tp_pips"] * ord_info["pip_size"])
                    elif manual_tp is not None:
                        tp_price = manual_tp
                    else:
                        tp_price = entry_price + (ord_info["tp_pips"] * ord_info["pip_size"]) if order_type == "buy_limit" else entry_price - (ord_info["tp_pips"] * ord_info["pip_size"])

                    if sl_price is not None:
                        sl_price = round(sl_price, digits)
                    tp_price = round(tp_price, digits)

                    calc_order = {
                        "market_name": market_key,
                        "timeframe": tf,
                        "limit_order": order_type,
                        "entry_price": entry_price,
                        "sl_price": sl_price,
                        "tp_price": tp_price,
                        "volume": ord_info["volume_to_use"],
                        "riskusd_amount": ord_info["risk_usd_to_use"],
                        "rr_ratio": rr_ratio,
                        "sl_pips": round(ord_info["sl_pips"], 2),
                        "tp_pips": round(ord_info["tp_pips"], 2),
                        "calculated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "broker": user_brokerid
                    }

                    risk_usd = ord_info["risk_usd_to_use"]
                    results_by_risk[risk_usd][market_key][tf].append(calc_order)

            if not synthetics_found or not results_by_risk:
                continue

            # Save to risk folders
            for risk_usd, markets_dict in results_by_risk.items():
                risk_folder_name = f"risk_{str(risk_usd).replace('.', '_')}_usd"
                risk_folder = calc_risk_dir / risk_folder_name
                risk_folder.mkdir(parents=True, exist_ok=True)

                orders_structure = {
                    "orders": {
                        "total_orders": sum(len(orders) for market in markets_dict.values() for orders in market.values()),
                        "total_markets": len(markets_dict)
                    }
                }
                orders_dict = orders_structure["orders"]

                for market_key, tf_dict in markets_dict.items():
                    norm_market = " ".join(market_key.replace("_", " ").split()).upper()
                    tick_info = SYMBOLSTICK_DATA.get(norm_market, {})
                    if tick_info.get("broker") != broker_clean:
                        tick_info = {}
                    orders_dict[market_key] = {
                        "category": "synthetics",
                        "broker": user_brokerid,
                        "tick_size": tick_info.get("tick_size", 0.01),
                        "tick_value": tick_info.get("tick_value", 0.01),
                        **tf_dict
                    }

                out_file = risk_folder / f"{original_stem}_synthetics.json"
                try:
                    with open(out_file, 'w', encoding='utf-8') as f:
                        json.dump(orders_structure, f, indent=2, ensure_ascii=False)
                    files_saved_this_broker += 1
                except Exception as e:
                    print(f"Failed to save {out_file}: {e}")

        if files_saved_this_broker > 0:
            print(f" → Generated {files_saved_this_broker} synthetics JSON file(s)\n")
        else:
            print(" → No synthetics output files generated\n")

    return True

def calculate_energies_sl_tp_market():
    from datetime import datetime
    from pathlib import Path
    from collections import defaultdict
    import json
    import os

    BROKERS_JSON_PATH = r"C:\xampp\htdocs\chronedge\synarex\users.json"
    brokers_dir = os.path.dirname(BROKERS_JSON_PATH)
    developers_functions_path = os.path.join(brokers_dir, "developers_functions.json")
    
    if not os.path.exists(BROKERS_JSON_PATH) or not os.path.exists(developers_functions_path):
        print("[ERROR] Required JSON files missing.")
        return False

    try:
        with open(BROKERS_JSON_PATH, 'r', encoding='utf-8') as f:
            usersdictionary = json.load(f)
        with open(developers_functions_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to load configuration: {e}")
        return False

    # Assumes SYMBOLSTICK_DATA is available globally (used in forex/synthetics/basket)
    global SYMBOLSTICK_DATA

    for user_brokerid, items in data.items():
        if not isinstance(items, list):
            continue
        filename = next((item[len("filename:"):].strip() for item in items if str(item).strip().startswith("filename:")), None)
        if not filename or not filename.endswith(".py"):
            continue
        if user_brokerid not in usersdictionary:
            continue

        cfg = usersdictionary[user_brokerid]
        base_folder = cfg.get("BASE_FOLDER")
        if not base_folder:
            continue

        developer_folder = os.path.join(base_folder, "..", "developers", user_brokerid)
        developer_path = Path(developer_folder).resolve()
        if not developer_path.is_dir():
            continue

        temp_files = list(developer_path.rglob("*_temp.json"))
        if not temp_files:
            continue

        print(f"[BROKER] {user_brokerid}")

        broker_clean = ''.join(c for c in user_brokerid if not c.isdigit()).lower()
        files_saved_this_broker = 0

        for temp_path in temp_files:
            temp_name = temp_path.name.lower()
            original_stem = temp_name[:-10] if temp_name.endswith('_temp.json') else temp_path.stem
            temp_dir = temp_path.parent
            calc_risk_dir = temp_dir / "calculatedrisk"
            calc_risk_dir.mkdir(exist_ok=True)

            # Clean existing energies output files
            for risk_folder in calc_risk_dir.iterdir():
                if risk_folder.is_dir() and risk_folder.name.startswith("risk_") and risk_folder.name.endswith("_usd"):
                    for energy_file in risk_folder.glob("*_energies.json"):
                        try:
                            energy_file.unlink()
                        except:
                            pass

            # Load *_allowedsymbolsandvolumes.json
            prefixed_asv = temp_dir / f"{original_stem}_allowedsymbolsandvolumes.json"
            if not prefixed_asv.is_file():
                continue
            try:
                with open(prefixed_asv, 'r', encoding='utf-8') as f:
                    asv_data = json.load(f)
            except:
                continue

            energies_config = asv_data.get("energies", {})

            # NEW LOGIC: same as forex/synthetics/basket
            limited = energies_config.get("limited", False)
            allowed_list = energies_config.get("allowed", [])

            if limited and not allowed_list:
                continue  # limited=True but empty allowed → no processing

            symbol_rules = {}
            if limited:
                for item in allowed_list:
                    sym = " ".join(item.get("symbol", "").replace("_", " ").split()).upper()
                    vol = float(item.get("volume", 0))
                    risk = float(item.get("risk", 0))
                    if vol > 0 and risk > 0:
                        symbol_rules[sym] = {"volume": vol, "risk_usd": risk}

            # Load account management
            prefixed_am = temp_dir / f"{original_stem}_accountmanagement.json"
            if not prefixed_am.is_file():
                continue
            try:
                with open(prefixed_am, 'r', encoding='utf-8') as f:
                    am_data = json.load(f)
            except:
                continue

            settings = am_data.get("settings", {})
            calc_sl = str(settings.get("calculate_stoploss", "")).strip().lower() == "yes"
            calc_tp = str(settings.get("calculate_takeprofit", "")).strip().lower() == "yes"
            calculation_type = str(settings.get("calculation_type", "default")).strip().lower()

            try:
                rr_ratio_raw = settings.get("risk_reward_ratio", 3.0)
                rr_ratio = float(rr_ratio_raw)
                if rr_ratio <= 0:
                    rr_ratio = 3.0
            except:
                rr_ratio = 3.0

            record_from = str(settings.get("record_orders_from", "all")).strip().lower()
            orders_per_sym_str = str(settings.get("orders_per_symbol", "all")).strip().lower()
            max_orders_per_sym = None if orders_per_sym_str == "all" else int(orders_per_sym_str) if orders_per_sym_str.isdigit() else None

            # Load orders
            try:
                with open(temp_path, 'r', encoding='utf-8') as f:
                    content = json.load(f)
                orders_section = content.get("orders", {})
            except:
                continue

            results_by_risk = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
            energies_found = False
            symbol_orders = defaultdict(list)

            for market_key, market_data in orders_section.items():
                if market_key in ["total_orders", "total_markets"] or market_data.get("category", "").lower() != "energies":
                    continue

                energies_found = True
                norm_market = " ".join(market_key.replace("_", " ").split()).upper()

                # Determine volume/risk
                if limited:
                    rule = symbol_rules.get(norm_market)
                    if not rule:
                        continue
                    volume_to_use = rule["volume"]
                    risk_usd_to_use = rule["risk_usd"]
                else:
                    volume_to_use = 0.01   # default for energies (e.g., oil, gas)
                    risk_usd_to_use = 4.0

                tick_info = SYMBOLSTICK_DATA.get(norm_market, {})
                if tick_info.get("broker") != broker_clean:
                    tick_info = {}
                tick_size = tick_info.get("tick_size", 0.01)
                tick_value = tick_info.get("tick_value", 1.0)  # typical for energies
                pip_size = 10 * tick_size
                pip_value_usd = tick_value * volume_to_use * (pip_size / tick_size)
                if pip_value_usd <= 0:
                    continue

                sl_pips = risk_usd_to_use / pip_value_usd
                tp_pips = sl_pips * rr_ratio

                for tf, order_list in market_data.items():
                    if not isinstance(order_list, list):
                        continue
                    for order in order_list:
                        order_type = order.get("order_type") or order.get("limit_order")
                        if order_type not in ["buy_limit", "sell_limit"] or "entry_price" not in order:
                            continue
                        try:
                            entry_price = float(order["entry_price"])
                            exit_price = float(order.get("exit_price")) if order.get("exit_price") is not None else None
                            profit_price = float(order.get("profit_price")) if order.get("profit_price") is not None else None
                        except:
                            continue

                        order_info = {
                            "tf": tf,
                            "order_type": order_type,
                            "entry_price": entry_price,
                            "exit_price": exit_price,
                            "profit_price": profit_price,
                            "market_key": market_key,
                            "volume_to_use": volume_to_use,
                            "risk_usd_to_use": risk_usd_to_use,
                            "sl_pips": sl_pips,
                            "tp_pips": tp_pips,
                            "tick_size": tick_size,
                            "pip_size": pip_size
                        }
                        symbol_orders[norm_market].append(order_info)

            # Filtering per symbol
            filtered_symbol_orders = {}
            for norm_market, orders in symbol_orders.items():
                filtered = orders

                if record_from != "all":
                    buys = [o for o in orders if o["order_type"] == "buy_limit"]
                    sells = [o for o in orders if o["order_type"] == "sell_limit"]
                    if record_from == "buys_only":
                        filtered = buys
                    elif record_from == "sells_only":
                        filtered = sells

                if max_orders_per_sym is not None and len(filtered) > max_orders_per_sym:
                    buys_f = [o for o in filtered if o["order_type"] == "buy_limit"]
                    sells_f = [o for o in filtered if o["order_type"] == "sell_limit"]
                    selected = []
                    if buys_f:
                        selected.extend(sorted(buys_f, key=lambda x: x["entry_price"])[:max_orders_per_sym])
                    if sells_f and len(selected) < max_orders_per_sym:
                        remaining = max_orders_per_sym - len(selected)
                        selected.extend(sorted(sells_f, key=lambda x: x["entry_price"], reverse=True)[:remaining])
                    filtered = selected

                filtered_symbol_orders[norm_market] = filtered

            # Coordination mode TP
            coordinated_tp = {}
            if calculation_type == "coordination":
                for norm_market, orders in filtered_symbol_orders.items():
                    if len(orders) < 2:
                        continue
                    buys = [o for o in orders if o["order_type"] == "buy_limit"]
                    sells = [o for o in orders if o["order_type"] == "sell_limit"]
                    buy_prices = sorted([o["entry_price"] for o in buys])
                    sell_prices = sorted([o["entry_price"] for o in sells])
                    rr_distance_pips = orders[0]["sl_pips"] * rr_ratio

                    for sell_order in sells:
                        candidates = [p for p in buy_prices if p < sell_order["entry_price"]]
                        if candidates:
                            closest_buy = max(candidates)
                            distance = sell_order["entry_price"] - closest_buy
                            if distance >= rr_distance_pips * orders[0]["pip_size"]:
                                coordinated_tp[(sell_order["tf"], sell_order["entry_price"], "sell_limit")] = closest_buy - orders[0]["tick_size"]

                    for buy_order in buys:
                        candidates = [p for p in sell_prices if p > buy_order["entry_price"]]
                        if candidates:
                            closest_sell = min(candidates)
                            distance = closest_sell - buy_order["entry_price"]
                            if distance >= rr_distance_pips * orders[0]["pip_size"]:
                                coordinated_tp[(buy_order["tf"], buy_order["entry_price"], "buy_limit")] = closest_sell + orders[0]["tick_size"]

            # Build final orders
            for norm_market, orders in filtered_symbol_orders.items():
                if not orders:
                    continue
                sample = orders[0]
                digits = len(str(sample["tick_size"]).split('.')[-1]) if '.' in str(sample["tick_size"]) else 0
                digits = max(digits, 2)

                for ord_info in orders:
                    entry_price = ord_info["entry_price"]
                    order_type = ord_info["order_type"]
                    tf = ord_info["tf"]
                    manual_sl = ord_info["exit_price"]
                    manual_tp = ord_info["profit_price"]
                    market_key = ord_info["market_key"]

                    sl_price = None
                    tp_price = None

                    if calc_sl:
                        sl_price = entry_price - (ord_info["sl_pips"] * ord_info["pip_size"]) if order_type == "buy_limit" else entry_price + (ord_info["sl_pips"] * ord_info["pip_size"])
                    elif manual_sl is not None:
                        sl_price = manual_sl

                    key = (tf, entry_price, order_type)
                    if calculation_type == "coordination" and key in coordinated_tp:
                        tp_price = coordinated_tp[key]
                    elif calc_tp:
                        tp_price = entry_price + (ord_info["tp_pips"] * ord_info["pip_size"]) if order_type == "buy_limit" else entry_price - (ord_info["tp_pips"] * ord_info["pip_size"])
                    elif manual_tp is not None:
                        tp_price = manual_tp
                    else:
                        tp_price = entry_price + (ord_info["tp_pips"] * ord_info["pip_size"]) if order_type == "buy_limit" else entry_price - (ord_info["tp_pips"] * ord_info["pip_size"])

                    if sl_price is not None:
                        sl_price = round(sl_price, digits)
                    tp_price = round(tp_price, digits)

                    calc_order = {
                        "market_name": market_key,
                        "timeframe": tf,
                        "limit_order": order_type,
                        "entry_price": entry_price,
                        "sl_price": sl_price,
                        "tp_price": tp_price,
                        "volume": ord_info["volume_to_use"],
                        "riskusd_amount": ord_info["risk_usd_to_use"],
                        "rr_ratio": rr_ratio,
                        "sl_pips": round(ord_info["sl_pips"], 2),
                        "tp_pips": round(ord_info["tp_pips"], 2),
                        "calculated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "broker": user_brokerid
                    }

                    risk_usd = ord_info["risk_usd_to_use"]
                    results_by_risk[risk_usd][market_key][tf].append(calc_order)

            if not energies_found or not results_by_risk:
                continue

            # Save to risk folders
            for risk_usd, markets_dict in results_by_risk.items():
                risk_folder = calc_risk_dir / f"risk_{str(risk_usd).replace('.', '_')}_usd"
                risk_folder.mkdir(parents=True, exist_ok=True)

                orders_structure = {
                    "orders": {
                        "total_orders": sum(len(orders) for market in markets_dict.values() for orders in market.values()),
                        "total_markets": len(markets_dict)
                    }
                }
                orders_dict = orders_structure["orders"]

                for market_key, tf_dict in markets_dict.items():
                    norm_market = " ".join(market_key.replace("_", " ").split()).upper()
                    tick_info = SYMBOLSTICK_DATA.get(norm_market, {})
                    if tick_info.get("broker") != broker_clean:
                        tick_info = {}
                    orders_dict[market_key] = {
                        "category": "energies",
                        "broker": user_brokerid,
                        "tick_size": tick_info.get("tick_size", 0.01),
                        "tick_value": tick_info.get("tick_value", 1.0),
                        **tf_dict
                    }

                out_file = risk_folder / f"{original_stem}_energies.json"
                try:
                    with open(out_file, 'w', encoding='utf-8') as f:
                        json.dump(orders_structure, f, indent=2, ensure_ascii=False)
                    files_saved_this_broker += 1
                except:
                    pass

        if files_saved_this_broker > 0:
            print(f" → Generated {files_saved_this_broker} energies JSON file(s)\n")
        else:
            print(" → No energies output files generated\n")

    return True

def calculate_indices_sl_tp_market():
    from datetime import datetime
    from pathlib import Path
    from collections import defaultdict
    import json
    import os

    BROKERS_JSON_PATH = r"C:\xampp\htdocs\chronedge\synarex\users.json"
    brokers_dir = os.path.dirname(BROKERS_JSON_PATH)
    developers_functions_path = os.path.join(brokers_dir, "developers_functions.json")
    
    if not os.path.exists(BROKERS_JSON_PATH) or not os.path.exists(developers_functions_path):
        print("[ERROR] Required JSON files missing.")
        return False

    try:
        with open(BROKERS_JSON_PATH, 'r', encoding='utf-8') as f:
            usersdictionary = json.load(f)
        with open(developers_functions_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to load configuration: {e}")
        return False

    global SYMBOLSTICK_DATA

    for user_brokerid, items in data.items():
        if not isinstance(items, list):
            continue
        filename = next((item[len("filename:"):].strip() for item in items if str(item).strip().startswith("filename:")), None)
        if not filename or not filename.endswith(".py"):
            continue
        if user_brokerid not in usersdictionary:
            continue

        cfg = usersdictionary[user_brokerid]
        base_folder = cfg.get("BASE_FOLDER")
        if not base_folder:
            continue

        developer_folder = os.path.join(base_folder, "..", "developers", user_brokerid)
        developer_path = Path(developer_folder).resolve()
        if not developer_path.is_dir():
            continue

        temp_files = list(developer_path.rglob("*_temp.json"))
        if not temp_files:
            continue

        print(f"[BROKER] {user_brokerid}")

        broker_clean = ''.join(c for c in user_brokerid if not c.isdigit()).lower()
        files_saved_this_broker = 0

        for temp_path in temp_files:
            temp_name = temp_path.name.lower()
            original_stem = temp_name[:-10] if temp_name.endswith('_temp.json') else temp_path.stem
            temp_dir = temp_path.parent
            calc_risk_dir = temp_dir / "calculatedrisk"
            calc_risk_dir.mkdir(exist_ok=True)

            for risk_folder in calc_risk_dir.iterdir():
                if risk_folder.is_dir() and risk_folder.name.startswith("risk_") and risk_folder.name.endswith("_usd"):
                    for idx_file in risk_folder.glob("*_indices.json"):
                        try:
                            idx_file.unlink()
                        except:
                            pass

            prefixed_asv = temp_dir / f"{original_stem}_allowedsymbolsandvolumes.json"
            if not prefixed_asv.is_file():
                continue
            try:
                with open(prefixed_asv, 'r', encoding='utf-8') as f:
                    asv_data = json.load(f)
            except:
                continue

            indices_config = asv_data.get("indices", {})

            limited = indices_config.get("limited", False)
            allowed_list = indices_config.get("allowed", [])

            if limited and not allowed_list:
                continue

            symbol_rules = {}
            if limited:
                for item in allowed_list:
                    sym = " ".join(item.get("symbol", "").replace("_", " ").split()).upper()
                    vol = float(item.get("volume", 0))
                    risk = float(item.get("risk", 0))
                    if vol > 0 and risk > 0:
                        symbol_rules[sym] = {"volume": vol, "risk_usd": risk}

            prefixed_am = temp_dir / f"{original_stem}_accountmanagement.json"
            if not prefixed_am.is_file():
                continue
            try:
                with open(prefixed_am, 'r', encoding='utf-8') as f:
                    am_data = json.load(f)
            except:
                continue

            settings = am_data.get("settings", {})
            calc_sl = str(settings.get("calculate_stoploss", "")).strip().lower() == "yes"
            calc_tp = str(settings.get("calculate_takeprofit", "")).strip().lower() == "yes"
            calculation_type = str(settings.get("calculation_type", "default")).strip().lower()

            try:
                rr_ratio_raw = settings.get("risk_reward_ratio", 3.0)
                rr_ratio = float(rr_ratio_raw)
                if rr_ratio <= 0:
                    rr_ratio = 3.0
            except:
                rr_ratio = 3.0

            record_from = str(settings.get("record_orders_from", "all")).strip().lower()
            orders_per_sym_str = str(settings.get("orders_per_symbol", "all")).strip().lower()
            max_orders_per_sym = None if orders_per_sym_str == "all" else int(orders_per_sym_str) if orders_per_sym_str.isdigit() else None

            try:
                with open(temp_path, 'r', encoding='utf-8') as f:
                    content = json.load(f)
                orders_section = content.get("orders", {})
            except:
                continue

            results_by_risk = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
            indices_found = False
            symbol_orders = defaultdict(list)

            for market_key, market_data in orders_section.items():
                if market_key in ["total_orders", "total_markets"] or market_data.get("category", "").lower() != "indices":
                    continue

                indices_found = True
                norm_market = " ".join(market_key.replace("_", " ").split()).upper()

                if limited:
                    rule = symbol_rules.get(norm_market)
                    if not rule:
                        continue
                    volume_to_use = rule["volume"]
                    risk_usd_to_use = rule["risk_usd"]
                else:
                    volume_to_use = 0.1   # typical for indices
                    risk_usd_to_use = 16.0

                tick_info = SYMBOLSTICK_DATA.get(norm_market, {})
                if tick_info.get("broker") != broker_clean:
                    tick_info = {}
                tick_size = tick_info.get("tick_size", 0.01)
                tick_value = tick_info.get("tick_value", 1.0)
                pip_size = 10 * tick_size
                pip_value_usd = tick_value * volume_to_use * (pip_size / tick_size)
                if pip_value_usd <= 0:
                    continue

                sl_pips = risk_usd_to_use / pip_value_usd
                tp_pips = sl_pips * rr_ratio

                for tf, order_list in market_data.items():
                    if not isinstance(order_list, list):
                        continue
                    for order in order_list:
                        order_type = order.get("order_type") or order.get("limit_order")
                        if order_type not in ["buy_limit", "sell_limit"] or "entry_price" not in order:
                            continue
                        try:
                            entry_price = float(order["entry_price"])
                            exit_price = float(order.get("exit_price")) if order.get("exit_price") is not None else None
                            profit_price = float(order.get("profit_price")) if order.get("profit_price") is not None else None
                        except:
                            continue

                        order_info = {
                            "tf": tf,
                            "order_type": order_type,
                            "entry_price": entry_price,
                            "exit_price": exit_price,
                            "profit_price": profit_price,
                            "market_key": market_key,
                            "volume_to_use": volume_to_use,
                            "risk_usd_to_use": risk_usd_to_use,
                            "sl_pips": sl_pips,
                            "tp_pips": tp_pips,
                            "tick_size": tick_size,
                            "pip_size": pip_size
                        }
                        symbol_orders[norm_market].append(order_info)

            # [Same filtering, coordination, building, and saving logic as previous versions...]
            # (Omitted for brevity — identical to energies version)

            filtered_symbol_orders = {}
            for norm_market, orders in symbol_orders.items():
                filtered = orders
                if record_from != "all":
                    buys = [o for o in orders if o["order_type"] == "buy_limit"]
                    sells = [o for o in orders if o["order_type"] == "sell_limit"]
                    if record_from == "buys_only": filtered = buys
                    elif record_from == "sells_only": filtered = sells
                if max_orders_per_sym is not None and len(filtered) > max_orders_per_sym:
                    buys_f = [o for o in filtered if o["order_type"] == "buy_limit"]
                    sells_f = [o for o in filtered if o["order_type"] == "sell_limit"]
                    selected = []
                    if buys_f:
                        selected.extend(sorted(buys_f, key=lambda x: x["entry_price"])[:max_orders_per_sym])
                    if sells_f and len(selected) < max_orders_per_sym:
                        remaining = max_orders_per_sym - len(selected)
                        selected.extend(sorted(sells_f, key=lambda x: x["entry_price"], reverse=True)[:remaining])
                    filtered = selected
                filtered_symbol_orders[norm_market] = filtered

            coordinated_tp = {}
            if calculation_type == "coordination":
                for norm_market, orders in filtered_symbol_orders.items():
                    if len(orders) < 2: continue
                    buys = [o for o in orders if o["order_type"] == "buy_limit"]
                    sells = [o for o in orders if o["order_type"] == "sell_limit"]
                    buy_prices = sorted([o["entry_price"] for o in buys])
                    sell_prices = sorted([o["entry_price"] for o in sells])
                    rr_distance_pips = orders[0]["sl_pips"] * rr_ratio
                    for sell_order in sells:
                        candidates = [p for p in buy_prices if p < sell_order["entry_price"]]
                        if candidates:
                            closest_buy = max(candidates)
                            distance = sell_order["entry_price"] - closest_buy
                            if distance >= rr_distance_pips * orders[0]["pip_size"]:
                                coordinated_tp[(sell_order["tf"], sell_order["entry_price"], "sell_limit")] = closest_buy - orders[0]["tick_size"]
                    for buy_order in buys:
                        candidates = [p for p in sell_prices if p > buy_order["entry_price"]]
                        if candidates:
                            closest_sell = min(candidates)
                            distance = closest_sell - buy_order["entry_price"]
                            if distance >= rr_distance_pips * orders[0]["pip_size"]:
                                coordinated_tp[(buy_order["tf"], buy_order["entry_price"], "buy_limit")] = closest_sell + orders[0]["tick_size"]

            for norm_market, orders in filtered_symbol_orders.items():
                if not orders: continue
                sample = orders[0]
                digits = len(str(sample["tick_size"]).split('.')[-1]) if '.' in str(sample["tick_size"]) else 0
                digits = max(digits, 2)

                for ord_info in orders:
                    entry_price = ord_info["entry_price"]
                    order_type = ord_info["order_type"]
                    tf = ord_info["tf"]
                    manual_sl = ord_info["exit_price"]
                    manual_tp = ord_info["profit_price"]
                    market_key = ord_info["market_key"]

                    sl_price = None
                    tp_price = None

                    if calc_sl:
                        sl_price = entry_price - (ord_info["sl_pips"] * ord_info["pip_size"]) if order_type == "buy_limit" else entry_price + (ord_info["sl_pips"] * ord_info["pip_size"])
                    elif manual_sl is not None:
                        sl_price = manual_sl

                    key = (tf, entry_price, order_type)
                    if calculation_type == "coordination" and key in coordinated_tp:
                        tp_price = coordinated_tp[key]
                    elif calc_tp:
                        tp_price = entry_price + (ord_info["tp_pips"] * ord_info["pip_size"]) if order_type == "buy_limit" else entry_price - (ord_info["tp_pips"] * ord_info["pip_size"])
                    elif manual_tp is not None:
                        tp_price = manual_tp
                    else:
                        tp_price = entry_price + (ord_info["tp_pips"] * ord_info["pip_size"]) if order_type == "buy_limit" else entry_price - (ord_info["tp_pips"] * ord_info["pip_size"])

                    if sl_price is not None:
                        sl_price = round(sl_price, digits)
                    tp_price = round(tp_price, digits)

                    calc_order = {
                        "market_name": market_key,
                        "timeframe": tf,
                        "limit_order": order_type,
                        "entry_price": entry_price,
                        "sl_price": sl_price,
                        "tp_price": tp_price,
                        "volume": ord_info["volume_to_use"],
                        "riskusd_amount": ord_info["risk_usd_to_use"],
                        "rr_ratio": rr_ratio,
                        "sl_pips": round(ord_info["sl_pips"], 2),
                        "tp_pips": round(ord_info["tp_pips"], 2),
                        "calculated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "broker": user_brokerid
                    }

                    risk_usd = ord_info["risk_usd_to_use"]
                    results_by_risk[risk_usd][market_key][tf].append(calc_order)

            if not indices_found or not results_by_risk:
                continue

            for risk_usd, markets_dict in results_by_risk.items():
                risk_folder = calc_risk_dir / f"risk_{str(risk_usd).replace('.', '_')}_usd"
                risk_folder.mkdir(parents=True, exist_ok=True)

                orders_structure = {
                    "orders": {
                        "total_orders": sum(len(orders) for market in markets_dict.values() for orders in market.values()),
                        "total_markets": len(markets_dict)
                    }
                }
                orders_dict = orders_structure["orders"]

                for market_key, tf_dict in markets_dict.items():
                    norm_market = " ".join(market_key.replace("_", " ").split()).upper()
                    tick_info = SYMBOLSTICK_DATA.get(norm_market, {})
                    if tick_info.get("broker") != broker_clean:
                        tick_info = {}
                    orders_dict[market_key] = {
                        "category": "indices",
                        "broker": user_brokerid,
                        "tick_size": tick_info.get("tick_size", 0.01),
                        "tick_value": tick_info.get("tick_value", 1.0),
                        **tf_dict
                    }

                out_file = risk_folder / f"{original_stem}_indices.json"
                try:
                    with open(out_file, 'w', encoding='utf-8') as f:
                        json.dump(orders_structure, f, indent=2, ensure_ascii=False)
                    files_saved_this_broker += 1
                except:
                    pass

        if files_saved_this_broker > 0:
            print(f" → Generated {files_saved_this_broker} indices JSON file(s)\n")
        else:
            print(" → No indices output files generated\n")

    return True

def calculate_metals_sl_tp_market():
    from datetime import datetime
    from pathlib import Path
    from collections import defaultdict
    import json
    import os

    BROKERS_JSON_PATH = r"C:\xampp\htdocs\chronedge\synarex\users.json"
    brokers_dir = os.path.dirname(BROKERS_JSON_PATH)
    developers_functions_path = os.path.join(brokers_dir, "developers_functions.json")
    
    if not os.path.exists(BROKERS_JSON_PATH) or not os.path.exists(developers_functions_path):
        print("[ERROR] Required JSON files missing.")
        return False

    try:
        with open(BROKERS_JSON_PATH, 'r', encoding='utf-8') as f:
            usersdictionary = json.load(f)
        with open(developers_functions_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to load configuration: {e}")
        return False

    global SYMBOLSTICK_DATA

    for user_brokerid, items in data.items():
        if not isinstance(items, list):
            continue
        filename = next((item[len("filename:"):].strip() for item in items if str(item).strip().startswith("filename:")), None)
        if not filename or not filename.endswith(".py"):
            continue
        if user_brokerid not in usersdictionary:
            continue

        cfg = usersdictionary[user_brokerid]
        base_folder = cfg.get("BASE_FOLDER")
        if not base_folder:
            continue

        developer_folder = os.path.join(base_folder, "..", "developers", user_brokerid)
        developer_path = Path(developer_folder).resolve()
        if not developer_path.is_dir():
            continue

        temp_files = list(developer_path.rglob("*_temp.json"))
        if not temp_files:
            continue

        print(f"[BROKER] {user_brokerid}")

        broker_clean = ''.join(c for c in user_brokerid if not c.isdigit()).lower()
        files_saved_this_broker = 0

        for temp_path in temp_files:
            temp_name = temp_path.name.lower()
            original_stem = temp_name[:-10] if temp_name.endswith('_temp.json') else temp_path.stem
            temp_dir = temp_path.parent
            calc_risk_dir = temp_dir / "calculatedrisk"
            calc_risk_dir.mkdir(exist_ok=True)

            # Clean existing metals output files
            for risk_folder in calc_risk_dir.iterdir():
                if risk_folder.is_dir() and risk_folder.name.startswith("risk_") and risk_folder.name.endswith("_usd"):
                    for metals_file in risk_folder.glob("*_metals.json"):
                        try:
                            metals_file.unlink()
                        except:
                            pass

            # Load *_allowedsymbolsandvolumes.json
            prefixed_asv = temp_dir / f"{original_stem}_allowedsymbolsandvolumes.json"
            if not prefixed_asv.is_file():
                continue
            try:
                with open(prefixed_asv, 'r', encoding='utf-8') as f:
                    asv_data = json.load(f)
            except:
                continue

            metals_config = asv_data.get("metals", {})

            # NEW LOGIC: same as forex/synthetics/basket/energies
            limited = metals_config.get("limited", False)
            allowed_list = metals_config.get("allowed", [])

            if limited and not allowed_list:
                continue  # limited=True but empty → skip

            symbol_rules = {}
            if limited:
                for item in allowed_list:
                    sym = " ".join(item.get("symbol", "").replace("_", " ").split()).upper()
                    vol = float(item.get("volume", 0))
                    risk = float(item.get("risk", 0))
                    if vol > 0 and risk > 0:
                        symbol_rules[sym] = {"volume": vol, "risk_usd": risk}

            # Load account management
            prefixed_am = temp_dir / f"{original_stem}_accountmanagement.json"
            if not prefixed_am.is_file():
                continue
            try:
                with open(prefixed_am, 'r', encoding='utf-8') as f:
                    am_data = json.load(f)
            except:
                continue

            settings = am_data.get("settings", {})
            calc_sl = str(settings.get("calculate_stoploss", "")).strip().lower() == "yes"
            calc_tp = str(settings.get("calculate_takeprofit", "")).strip().lower() == "yes"
            calculation_type = str(settings.get("calculation_type", "default")).strip().lower()

            try:
                rr_ratio_raw = settings.get("risk_reward_ratio", 3.0)
                rr_ratio = float(rr_ratio_raw)
                if rr_ratio <= 0:
                    rr_ratio = 3.0
            except:
                rr_ratio = 3.0

            record_from = str(settings.get("record_orders_from", "all")).strip().lower()
            orders_per_sym_str = str(settings.get("orders_per_symbol", "all")).strip().lower()
            max_orders_per_sym = None if orders_per_sym_str == "all" else int(orders_per_sym_str) if orders_per_sym_str.isdigit() else None

            # Load orders
            try:
                with open(temp_path, 'r', encoding='utf-8') as f:
                    content = json.load(f)
                orders_section = content.get("orders", {})
            except:
                continue

            results_by_risk = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
            metals_found = False
            symbol_orders = defaultdict(list)

            for market_key, market_data in orders_section.items():
                if market_key in ["total_orders", "total_markets"] or market_data.get("category", "").lower() != "metals":
                    continue

                metals_found = True
                norm_market = " ".join(market_key.replace("_", " ").split()).upper()

                if limited:
                    rule = symbol_rules.get(norm_market)
                    if not rule:
                        continue
                    volume_to_use = rule["volume"]
                    risk_usd_to_use = rule["risk_usd"]
                else:
                    volume_to_use = 0.01   # typical for XAUUSD/XAGUSD
                    risk_usd_to_use = 16.0  # common default risk for gold

                tick_info = SYMBOLSTICK_DATA.get(norm_market, {})
                if tick_info.get("broker") != broker_clean:
                    tick_info = {}
                tick_size = tick_info.get("tick_size", 0.01)
                tick_value = tick_info.get("tick_value", 1.0)  # usually 1 for metals
                pip_size = 10 * tick_size
                pip_value_usd = tick_value * volume_to_use * (pip_size / tick_size)
                if pip_value_usd <= 0:
                    continue

                sl_pips = risk_usd_to_use / pip_value_usd
                tp_pips = sl_pips * rr_ratio

                for tf, order_list in market_data.items():
                    if not isinstance(order_list, list):
                        continue
                    for order in order_list:
                        order_type = order.get("order_type") or order.get("limit_order")
                        if order_type not in ["buy_limit", "sell_limit"] or "entry_price" not in order:
                            continue
                        try:
                            entry_price = float(order["entry_price"])
                            exit_price = float(order.get("exit_price")) if order.get("exit_price") is not None else None
                            profit_price = float(order.get("profit_price")) if order.get("profit_price") is not None else None
                        except:
                            continue

                        order_info = {
                            "tf": tf,
                            "order_type": order_type,
                            "entry_price": entry_price,
                            "exit_price": exit_price,
                            "profit_price": profit_price,
                            "market_key": market_key,
                            "volume_to_use": volume_to_use,
                            "risk_usd_to_use": risk_usd_to_use,
                            "sl_pips": sl_pips,
                            "tp_pips": tp_pips,
                            "tick_size": tick_size,
                            "pip_size": pip_size
                        }
                        symbol_orders[norm_market].append(order_info)

            # Filtering per symbol
            filtered_symbol_orders = {}
            for norm_market, orders in symbol_orders.items():
                filtered = orders

                if record_from != "all":
                    buys = [o for o in orders if o["order_type"] == "buy_limit"]
                    sells = [o for o in orders if o["order_type"] == "sell_limit"]
                    if record_from == "buys_only":
                        filtered = buys
                    elif record_from == "sells_only":
                        filtered = sells

                if max_orders_per_sym is not None and len(filtered) > max_orders_per_sym:
                    buys_f = [o for o in filtered if o["order_type"] == "buy_limit"]
                    sells_f = [o for o in filtered if o["order_type"] == "sell_limit"]
                    selected = []
                    if buys_f:
                        selected.extend(sorted(buys_f, key=lambda x: x["entry_price"])[:max_orders_per_sym])
                    if sells_f and len(selected) < max_orders_per_sym:
                        remaining = max_orders_per_sym - len(selected)
                        selected.extend(sorted(sells_f, key=lambda x: x["entry_price"], reverse=True)[:remaining])
                    filtered = selected

                filtered_symbol_orders[norm_market] = filtered

            # Coordination mode TP
            coordinated_tp = {}
            if calculation_type == "coordination":
                for norm_market, orders in filtered_symbol_orders.items():
                    if len(orders) < 2:
                        continue
                    buys = [o for o in orders if o["order_type"] == "buy_limit"]
                    sells = [o for o in orders if o["order_type"] == "sell_limit"]
                    buy_prices = sorted([o["entry_price"] for o in buys])
                    sell_prices = sorted([o["entry_price"] for o in sells])
                    rr_distance_pips = orders[0]["sl_pips"] * rr_ratio

                    for sell_order in sells:
                        candidates = [p for p in buy_prices if p < sell_order["entry_price"]]
                        if candidates:
                            closest_buy = max(candidates)
                            distance = sell_order["entry_price"] - closest_buy
                            if distance >= rr_distance_pips * orders[0]["pip_size"]:
                                coordinated_tp[(sell_order["tf"], sell_order["entry_price"], "sell_limit")] = closest_buy - orders[0]["tick_size"]

                    for buy_order in buys:
                        candidates = [p for p in sell_prices if p > buy_order["entry_price"]]
                        if candidates:
                            closest_sell = min(candidates)
                            distance = closest_sell - buy_order["entry_price"]
                            if distance >= rr_distance_pips * orders[0]["pip_size"]:
                                coordinated_tp[(buy_order["tf"], buy_order["entry_price"], "buy_limit")] = closest_sell + orders[0]["tick_size"]

            # Build final orders
            for norm_market, orders in filtered_symbol_orders.items():
                if not orders:
                    continue
                sample = orders[0]
                digits = len(str(sample["tick_size"]).split('.')[-1]) if '.' in str(sample["tick_size"]) else 0
                digits = max(digits, 2)

                for ord_info in orders:
                    entry_price = ord_info["entry_price"]
                    order_type = ord_info["order_type"]
                    tf = ord_info["tf"]
                    manual_sl = ord_info["exit_price"]
                    manual_tp = ord_info["profit_price"]
                    market_key = ord_info["market_key"]

                    sl_price = None
                    tp_price = None

                    if calc_sl:
                        sl_price = entry_price - (ord_info["sl_pips"] * ord_info["pip_size"]) if order_type == "buy_limit" else entry_price + (ord_info["sl_pips"] * ord_info["pip_size"])
                    elif manual_sl is not None:
                        sl_price = manual_sl

                    key = (tf, entry_price, order_type)
                    if calculation_type == "coordination" and key in coordinated_tp:
                        tp_price = coordinated_tp[key]
                    elif calc_tp:
                        tp_price = entry_price + (ord_info["tp_pips"] * ord_info["pip_size"]) if order_type == "buy_limit" else entry_price - (ord_info["tp_pips"] * ord_info["pip_size"])
                    elif manual_tp is not None:
                        tp_price = manual_tp
                    else:
                        tp_price = entry_price + (ord_info["tp_pips"] * ord_info["pip_size"]) if order_type == "buy_limit" else entry_price - (ord_info["tp_pips"] * ord_info["pip_size"])

                    if sl_price is not None:
                        sl_price = round(sl_price, digits)
                    tp_price = round(tp_price, digits)

                    calc_order = {
                        "market_name": market_key,
                        "timeframe": tf,
                        "limit_order": order_type,
                        "entry_price": entry_price,
                        "sl_price": sl_price,
                        "tp_price": tp_price,
                        "volume": ord_info["volume_to_use"],
                        "riskusd_amount": ord_info["risk_usd_to_use"],
                        "rr_ratio": rr_ratio,
                        "sl_pips": round(ord_info["sl_pips"], 2),
                        "tp_pips": round(ord_info["tp_pips"], 2),
                        "calculated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "broker": user_brokerid
                    }

                    risk_usd = ord_info["risk_usd_to_use"]
                    results_by_risk[risk_usd][market_key][tf].append(calc_order)

            if not metals_found or not results_by_risk:
                continue

            # Save to risk folders
            for risk_usd, markets_dict in results_by_risk.items():
                risk_folder = calc_risk_dir / f"risk_{str(risk_usd).replace('.', '_')}_usd"
                risk_folder.mkdir(parents=True, exist_ok=True)

                orders_structure = {
                    "orders": {
                        "total_orders": sum(len(orders) for market in markets_dict.values() for orders in market.values()),
                        "total_markets": len(markets_dict)
                    }
                }
                orders_dict = orders_structure["orders"]

                for market_key, tf_dict in markets_dict.items():
                    norm_market = " ".join(market_key.replace("_", " ").split()).upper()
                    tick_info = SYMBOLSTICK_DATA.get(norm_market, {})
                    if tick_info.get("broker") != broker_clean:
                        tick_info = {}
                    orders_dict[market_key] = {
                        "category": "metals",
                        "broker": user_brokerid,
                        "tick_size": tick_info.get("tick_size", 0.01),
                        "tick_value": tick_info.get("tick_value", 1.0),
                        **tf_dict
                    }

                out_file = risk_folder / f"{original_stem}_metals.json"
                try:
                    with open(out_file, 'w', encoding='utf-8') as f:
                        json.dump(orders_structure, f, indent=2, ensure_ascii=False)
                    files_saved_this_broker += 1
                except:
                    pass

        if files_saved_this_broker > 0:
            print(f" → Generated {files_saved_this_broker} metals JSON file(s)\n")
        else:
            print(" → No metals output files generated\n")

    return True

def calculate_crypto_sl_tp_market():
    from datetime import datetime
    from pathlib import Path
    from collections import defaultdict
    import json
    import os

    BROKERS_JSON_PATH = r"C:\xampp\htdocs\chronedge\synarex\users.json"
    brokers_dir = os.path.dirname(BROKERS_JSON_PATH)
    developers_functions_path = os.path.join(brokers_dir, "developers_functions.json")
    
    if not os.path.exists(BROKERS_JSON_PATH) or not os.path.exists(developers_functions_path):
        print("[ERROR] Required JSON files missing.")
        return False

    try:
        with open(BROKERS_JSON_PATH, 'r', encoding='utf-8') as f:
            usersdictionary = json.load(f)
        with open(developers_functions_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to load configuration: {e}")
        return False

    global SYMBOLSTICK_DATA

    for user_brokerid, items in data.items():
        if not isinstance(items, list):
            continue
        filename = next((item[len("filename:"):].strip() for item in items if str(item).strip().startswith("filename:")), None)
        if not filename or not filename.endswith(".py"):
            continue
        if user_brokerid not in usersdictionary:
            continue

        cfg = usersdictionary[user_brokerid]
        base_folder = cfg.get("BASE_FOLDER")
        if not base_folder:
            continue

        developer_folder = os.path.join(base_folder, "..", "developers", user_brokerid)
        developer_path = Path(developer_folder).resolve()
        if not developer_path.is_dir():
            continue

        temp_files = list(developer_path.rglob("*_temp.json"))
        if not temp_files:
            continue

        print(f"[BROKER] {user_brokerid}")

        broker_clean = ''.join(c for c in user_brokerid if not c.isdigit()).lower()
        files_saved_this_broker = 0

        for temp_path in temp_files:
            temp_name = temp_path.name.lower()
            original_stem = temp_name[:-10] if temp_name.endswith('_temp.json') else temp_path.stem
            temp_dir = temp_path.parent
            calc_risk_dir = temp_dir / "calculatedrisk"
            calc_risk_dir.mkdir(exist_ok=True)

            # Clean existing crypto output files
            for risk_folder in calc_risk_dir.iterdir():
                if risk_folder.is_dir() and risk_folder.name.startswith("risk_") and risk_folder.name.endswith("_usd"):
                    for crypto_file in risk_folder.glob("*_crypto.json"):
                        try:
                            crypto_file.unlink()
                        except:
                            pass

            # Load *_allowedsymbolsandvolumes.json
            prefixed_asv = temp_dir / f"{original_stem}_allowedsymbolsandvolumes.json"
            if not prefixed_asv.is_file():
                continue
            try:
                with open(prefixed_asv, 'r', encoding='utf-8') as f:
                    asv_data = json.load(f)
            except:
                continue

            crypto_config = asv_data.get("crypto", {})

            # NEW LOGIC: consistent with all other categories
            limited = crypto_config.get("limited", False)
            allowed_list = crypto_config.get("allowed", [])

            if limited and not allowed_list:
                continue  # limited=True but empty allowed → no crypto processing

            symbol_rules = {}
            if limited:
                for item in allowed_list:
                    sym = " ".join(item.get("symbol", "").replace("_", " ").split()).upper()
                    vol = float(item.get("volume", 0))
                    risk = float(item.get("risk", 0))
                    if vol > 0 and risk > 0:
                        symbol_rules[sym] = {"volume": vol, "risk_usd": risk}

            # Load account management settings
            prefixed_am = temp_dir / f"{original_stem}_accountmanagement.json"
            if not prefixed_am.is_file():
                continue
            try:
                with open(prefixed_am, 'r', encoding='utf-8') as f:
                    am_data = json.load(f)
            except:
                continue

            settings = am_data.get("settings", {})
            calc_sl = str(settings.get("calculate_stoploss", "")).strip().lower() == "yes"
            calc_tp = str(settings.get("calculate_takeprofit", "")).strip().lower() == "yes"
            calculation_type = str(settings.get("calculation_type", "default")).strip().lower()

            try:
                rr_ratio_raw = settings.get("risk_reward_ratio", 3.0)
                rr_ratio = float(rr_ratio_raw)
                if rr_ratio <= 0:
                    rr_ratio = 3.0
            except:
                rr_ratio = 3.0

            record_from = str(settings.get("record_orders_from", "all")).strip().lower()
            orders_per_sym_str = str(settings.get("orders_per_symbol", "all")).strip().lower()
            max_orders_per_sym = None if orders_per_sym_str == "all" else int(orders_per_sym_str) if orders_per_sym_str.isdigit() else None

            # Load orders from *_temp.json
            try:
                with open(temp_path, 'r', encoding='utf-8') as f:
                    content = json.load(f)
                orders_section = content.get("orders", {})
            except:
                continue

            results_by_risk = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
            crypto_found = False
            symbol_orders = defaultdict(list)

            for market_key, market_data in orders_section.items():
                if market_key in ["total_orders", "total_markets"] or market_data.get("category", "").lower() != "crypto":
                    continue

                crypto_found = True
                norm_market = " ".join(market_key.replace("_", " ").split()).upper()

                # Determine volume and risk
                if limited:
                    rule = symbol_rules.get(norm_market)
                    if not rule:
                        continue
                    volume_to_use = rule["volume"]
                    risk_usd_to_use = rule["risk_usd"]
                else:
                    # Default values when allowing all crypto symbols
                    volume_to_use = 0.01   # common for BTC/ETH
                    risk_usd_to_use = 4.0

                tick_info = SYMBOLSTICK_DATA.get(norm_market, {})
                if tick_info.get("broker") != broker_clean:
                    tick_info = {}
                tick_size = tick_info.get("tick_size", 0.01)   # typical for crypto pairs
                tick_value = tick_info.get("tick_value", 1.0)
                pip_size = 10 * tick_size
                pip_value_usd = tick_value * volume_to_use * (pip_size / tick_size)
                if pip_value_usd <= 0:
                    continue

                sl_pips = risk_usd_to_use / pip_value_usd
                tp_pips = sl_pips * rr_ratio

                for tf, order_list in market_data.items():
                    if not isinstance(order_list, list):
                        continue
                    for order in order_list:
                        order_type = order.get("order_type") or order.get("limit_order")
                        if order_type not in ["buy_limit", "sell_limit"] or "entry_price" not in order:
                            continue
                        try:
                            entry_price = float(order["entry_price"])
                            exit_price = float(order.get("exit_price")) if order.get("exit_price") is not None else None
                            profit_price = float(order.get("profit_price")) if order.get("profit_price") is not None else None
                        except:
                            continue

                        order_info = {
                            "tf": tf,
                            "order_type": order_type,
                            "entry_price": entry_price,
                            "exit_price": exit_price,
                            "profit_price": profit_price,
                            "market_key": market_key,
                            "volume_to_use": volume_to_use,
                            "risk_usd_to_use": risk_usd_to_use,
                            "sl_pips": sl_pips,
                            "tp_pips": tp_pips,
                            "tick_size": tick_size,
                            "pip_size": pip_size
                        }
                        symbol_orders[norm_market].append(order_info)

            # Filtering per symbol
            filtered_symbol_orders = {}
            for norm_market, orders in symbol_orders.items():
                filtered = orders

                if record_from != "all":
                    buys = [o for o in orders if o["order_type"] == "buy_limit"]
                    sells = [o for o in orders if o["order_type"] == "sell_limit"]
                    if record_from == "buys_only":
                        filtered = buys
                    elif record_from == "sells_only":
                        filtered = sells

                if max_orders_per_sym is not None and len(filtered) > max_orders_per_sym:
                    buys_f = [o for o in filtered if o["order_type"] == "buy_limit"]
                    sells_f = [o for o in filtered if o["order_type"] == "sell_limit"]
                    selected = []
                    if buys_f:
                        selected.extend(sorted(buys_f, key=lambda x: x["entry_price"])[:max_orders_per_sym])
                    if sells_f and len(selected) < max_orders_per_sym:
                        remaining = max_orders_per_sym - len(selected)
                        selected.extend(sorted(sells_f, key=lambda x: x["entry_price"], reverse=True)[:remaining])
                    filtered = selected

                filtered_symbol_orders[norm_market] = filtered

            # Coordination mode TP
            coordinated_tp = {}
            if calculation_type == "coordination":
                for norm_market, orders in filtered_symbol_orders.items():
                    if len(orders) < 2:
                        continue
                    buys = [o for o in orders if o["order_type"] == "buy_limit"]
                    sells = [o for o in orders if o["order_type"] == "sell_limit"]
                    buy_prices = sorted([o["entry_price"] for o in buys])
                    sell_prices = sorted([o["entry_price"] for o in sells])
                    rr_distance_pips = orders[0]["sl_pips"] * rr_ratio

                    for sell_order in sells:
                        candidates = [p for p in buy_prices if p < sell_order["entry_price"]]
                        if candidates:
                            closest_buy = max(candidates)
                            distance = sell_order["entry_price"] - closest_buy
                            if distance >= rr_distance_pips * orders[0]["pip_size"]:
                                coordinated_tp[(sell_order["tf"], sell_order["entry_price"], "sell_limit")] = closest_buy - orders[0]["tick_size"]

                    for buy_order in buys:
                        candidates = [p for p in sell_prices if p > buy_order["entry_price"]]
                        if candidates:
                            closest_sell = min(candidates)
                            distance = closest_sell - buy_order["entry_price"]
                            if distance >= rr_distance_pips * orders[0]["pip_size"]:
                                coordinated_tp[(buy_order["tf"], buy_order["entry_price"], "buy_limit")] = closest_sell + orders[0]["tick_size"]

            # Build final calculated orders
            for norm_market, orders in filtered_symbol_orders.items():
                if not orders:
                    continue
                sample = orders[0]
                digits = len(str(sample["tick_size"]).split('.')[-1]) if '.' in str(sample["tick_size"]) else 0
                digits = max(digits, 2)

                for ord_info in orders:
                    entry_price = ord_info["entry_price"]
                    order_type = ord_info["order_type"]
                    tf = ord_info["tf"]
                    manual_sl = ord_info["exit_price"]
                    manual_tp = ord_info["profit_price"]
                    market_key = ord_info["market_key"]

                    sl_price = None
                    tp_price = None

                    if calc_sl:
                        sl_price = entry_price - (ord_info["sl_pips"] * ord_info["pip_size"]) if order_type == "buy_limit" else entry_price + (ord_info["sl_pips"] * ord_info["pip_size"])
                    elif manual_sl is not None:
                        sl_price = manual_sl

                    key = (tf, entry_price, order_type)
                    if calculation_type == "coordination" and key in coordinated_tp:
                        tp_price = coordinated_tp[key]
                    elif calc_tp:
                        tp_price = entry_price + (ord_info["tp_pips"] * ord_info["pip_size"]) if order_type == "buy_limit" else entry_price - (ord_info["tp_pips"] * ord_info["pip_size"])
                    elif manual_tp is not None:
                        tp_price = manual_tp
                    else:
                        tp_price = entry_price + (ord_info["tp_pips"] * ord_info["pip_size"]) if order_type == "buy_limit" else entry_price - (ord_info["tp_pips"] * ord_info["pip_size"])

                    if sl_price is not None:
                        sl_price = round(sl_price, digits)
                    tp_price = round(tp_price, digits)

                    calc_order = {
                        "market_name": market_key,
                        "timeframe": tf,
                        "limit_order": order_type,
                        "entry_price": entry_price,
                        "sl_price": sl_price,
                        "tp_price": tp_price,
                        "volume": ord_info["volume_to_use"],
                        "riskusd_amount": ord_info["risk_usd_to_use"],
                        "rr_ratio": rr_ratio,
                        "sl_pips": round(ord_info["sl_pips"], 2),
                        "tp_pips": round(ord_info["tp_pips"], 2),
                        "calculated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "broker": user_brokerid
                    }

                    risk_usd = ord_info["risk_usd_to_use"]
                    results_by_risk[risk_usd][market_key][tf].append(calc_order)

            if not crypto_found or not results_by_risk:
                continue

            # Save to risk folders
            for risk_usd, markets_dict in results_by_risk.items():
                risk_folder = calc_risk_dir / f"risk_{str(risk_usd).replace('.', '_')}_usd"
                risk_folder.mkdir(parents=True, exist_ok=True)

                orders_structure = {
                    "orders": {
                        "total_orders": sum(len(orders) for market in markets_dict.values() for orders in market.values()),
                        "total_markets": len(markets_dict)
                    }
                }
                orders_dict = orders_structure["orders"]

                for market_key, tf_dict in markets_dict.items():
                    norm_market = " ".join(market_key.replace("_", " ").split()).upper()
                    tick_info = SYMBOLSTICK_DATA.get(norm_market, {})
                    if tick_info.get("broker") != broker_clean:
                        tick_info = {}
                    orders_dict[market_key] = {
                        "category": "crypto",
                        "broker": user_brokerid,
                        "tick_size": tick_info.get("tick_size", 0.01),
                        "tick_value": tick_info.get("tick_value", 1.0),
                        **tf_dict
                    }

                out_file = risk_folder / f"{original_stem}_crypto.json"
                try:
                    with open(out_file, 'w', encoding='utf-8') as f:
                        json.dump(orders_structure, f, indent=2, ensure_ascii=False)
                    files_saved_this_broker += 1
                except:
                    pass

        if files_saved_this_broker > 0:
            print(f" → Generated {files_saved_this_broker} crypto JSON file(s)\n")
        else:
            print(" → No crypto output files generated\n")

    return True

def calculate_equities_sl_tp_market():
    from datetime import datetime
    from pathlib import Path
    from collections import defaultdict
    import json
    import os

    BROKERS_JSON_PATH = r"C:\xampp\htdocs\chronedge\synarex\users.json"
    brokers_dir = os.path.dirname(BROKERS_JSON_PATH)
    developers_functions_path = os.path.join(brokers_dir, "developers_functions.json")
    
    if not os.path.exists(BROKERS_JSON_PATH) or not os.path.exists(developers_functions_path):
        print("[ERROR] Required JSON files missing.")
        return False

    try:
        with open(BROKERS_JSON_PATH, 'r', encoding='utf-8') as f:
            usersdictionary = json.load(f)
        with open(developers_functions_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to load configuration: {e}")
        return False

    global SYMBOLSTICK_DATA

    for user_brokerid, items in data.items():
        if not isinstance(items, list):
            continue
        filename = next((item[len("filename:"):].strip() for item in items if str(item).strip().startswith("filename:")), None)
        if not filename or not filename.endswith(".py"):
            continue
        if user_brokerid not in usersdictionary:
            continue

        cfg = usersdictionary[user_brokerid]
        base_folder = cfg.get("BASE_FOLDER")
        if not base_folder:
            continue

        developer_folder = os.path.join(base_folder, "..", "developers", user_brokerid)
        developer_path = Path(developer_folder).resolve()
        if not developer_path.is_dir():
            continue

        temp_files = list(developer_path.rglob("*_temp.json"))
        if not temp_files:
            continue

        print(f"[BROKER] {user_brokerid}")

        broker_clean = ''.join(c for c in user_brokerid if not c.isdigit()).lower()
        files_saved_this_broker = 0

        for temp_path in temp_files:
            temp_name = temp_path.name.lower()
            original_stem = temp_name[:-10] if temp_name.endswith('_temp.json') else temp_path.stem
            temp_dir = temp_path.parent
            calc_risk_dir = temp_dir / "calculatedrisk"
            calc_risk_dir.mkdir(exist_ok=True)

            # Clean existing equities output files
            for risk_folder in calc_risk_dir.iterdir():
                if risk_folder.is_dir() and risk_folder.name.startswith("risk_") and risk_folder.name.endswith("_usd"):
                    for equities_file in risk_folder.glob("*_equities.json"):
                        try:
                            equities_file.unlink()
                        except:
                            pass

            # Load *_allowedsymbolsandvolumes.json
            prefixed_asv = temp_dir / f"{original_stem}_allowedsymbolsandvolumes.json"
            if not prefixed_asv.is_file():
                continue
            try:
                with open(prefixed_asv, 'r', encoding='utf-8') as f:
                    asv_data = json.load(f)
            except:
                continue

            equities_config = asv_data.get("equities", {})

            # NEW LOGIC: consistent with all other categories
            limited = equities_config.get("limited", False)
            allowed_list = equities_config.get("allowed", [])

            if limited and not allowed_list:
                continue  # limited=True but empty allowed → no equities processing

            symbol_rules = {}
            if limited:
                for item in allowed_list:
                    sym = " ".join(item.get("symbol", "").replace("_", " ").split()).upper()
                    vol = float(item.get("volume", 0))
                    risk = float(item.get("risk", 0))
                    if vol > 0 and risk > 0:
                        symbol_rules[sym] = {"volume": vol, "risk_usd": risk}

            # Load account management settings
            prefixed_am = temp_dir / f"{original_stem}_accountmanagement.json"
            if not prefixed_am.is_file():
                continue
            try:
                with open(prefixed_am, 'r', encoding='utf-8') as f:
                    am_data = json.load(f)
            except:
                continue

            settings = am_data.get("settings", {})
            calc_sl = str(settings.get("calculate_stoploss", "")).strip().lower() == "yes"
            calc_tp = str(settings.get("calculate_takeprofit", "")).strip().lower() == "yes"
            calculation_type = str(settings.get("calculation_type", "default")).strip().lower()

            try:
                rr_ratio_raw = settings.get("risk_reward_ratio", 3.0)
                rr_ratio = float(rr_ratio_raw)
                if rr_ratio <= 0:
                    rr_ratio = 3.0
            except:
                rr_ratio = 3.0

            record_from = str(settings.get("record_orders_from", "all")).strip().lower()
            orders_per_sym_str = str(settings.get("orders_per_symbol", "all")).strip().lower()
            max_orders_per_sym = None if orders_per_sym_str == "all" else int(orders_per_sym_str) if orders_per_sym_str.isdigit() else None

            # Load orders from *_temp.json
            try:
                with open(temp_path, 'r', encoding='utf-8') as f:
                    content = json.load(f)
                orders_section = content.get("orders", {})
            except:
                continue

            results_by_risk = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
            equities_found = False
            symbol_orders = defaultdict(list)

            for market_key, market_data in orders_section.items():
                if market_key in ["total_orders", "total_markets"] or market_data.get("category", "").lower() != "equities":
                    continue

                equities_found = True
                norm_market = " ".join(market_key.replace("_", " ").split()).upper()

                # Determine volume and risk
                if limited:
                    rule = symbol_rules.get(norm_market)
                    if not rule:
                        continue
                    volume_to_use = rule["volume"]
                    risk_usd_to_use = rule["risk_usd"]
                else:
                    # Default values when allowing all equities symbols
                    volume_to_use = 1.0    # equities usually traded in whole shares/lots
                    risk_usd_to_use = 16.0  # common higher risk for stocks

                tick_info = SYMBOLSTICK_DATA.get(norm_market, {})
                if tick_info.get("broker") != broker_clean:
                    tick_info = {}
                tick_size = tick_info.get("tick_size", 0.01)
                tick_value = tick_info.get("tick_value", 1.0)  # typically 1 USD per share
                pip_size = 10 * tick_size
                pip_value_usd = tick_value * volume_to_use * (pip_size / tick_size)
                if pip_value_usd <= 0:
                    continue

                sl_pips = risk_usd_to_use / pip_value_usd
                tp_pips = sl_pips * rr_ratio

                for tf, order_list in market_data.items():
                    if not isinstance(order_list, list):
                        continue
                    for order in order_list:
                        order_type = order.get("order_type") or order.get("limit_order")
                        if order_type not in ["buy_limit", "sell_limit"] or "entry_price" not in order:
                            continue
                        try:
                            entry_price = float(order["entry_price"])
                            exit_price = float(order.get("exit_price")) if order.get("exit_price") is not None else None
                            profit_price = float(order.get("profit_price")) if order.get("profit_price") is not None else None
                        except:
                            continue

                        order_info = {
                            "tf": tf,
                            "order_type": order_type,
                            "entry_price": entry_price,
                            "exit_price": exit_price,
                            "profit_price": profit_price,
                            "market_key": market_key,
                            "volume_to_use": volume_to_use,
                            "risk_usd_to_use": risk_usd_to_use,
                            "sl_pips": sl_pips,
                            "tp_pips": tp_pips,
                            "tick_size": tick_size,
                            "pip_size": pip_size
                        }
                        symbol_orders[norm_market].append(order_info)

            # Filtering per symbol
            filtered_symbol_orders = {}
            for norm_market, orders in symbol_orders.items():
                filtered = orders

                if record_from != "all":
                    buys = [o for o in orders if o["order_type"] == "buy_limit"]
                    sells = [o for o in orders if o["order_type"] == "sell_limit"]
                    if record_from == "buys_only":
                        filtered = buys
                    elif record_from == "sells_only":
                        filtered = sells

                if max_orders_per_sym is not None and len(filtered) > max_orders_per_sym:
                    buys_f = [o for o in filtered if o["order_type"] == "buy_limit"]
                    sells_f = [o for o in filtered if o["order_type"] == "sell_limit"]
                    selected = []
                    if buys_f:
                        selected.extend(sorted(buys_f, key=lambda x: x["entry_price"])[:max_orders_per_sym])
                    if sells_f and len(selected) < max_orders_per_sym:
                        remaining = max_orders_per_sym - len(selected)
                        selected.extend(sorted(sells_f, key=lambda x: x["entry_price"], reverse=True)[:remaining])
                    filtered = selected

                filtered_symbol_orders[norm_market] = filtered

            # Coordination mode TP
            coordinated_tp = {}
            if calculation_type == "coordination":
                for norm_market, orders in filtered_symbol_orders.items():
                    if len(orders) < 2:
                        continue
                    buys = [o for o in orders if o["order_type"] == "buy_limit"]
                    sells = [o for o in orders if o["order_type"] == "sell_limit"]
                    buy_prices = sorted([o["entry_price"] for o in buys])
                    sell_prices = sorted([o["entry_price"] for o in sells])
                    rr_distance_pips = orders[0]["sl_pips"] * rr_ratio

                    for sell_order in sells:
                        candidates = [p for p in buy_prices if p < sell_order["entry_price"]]
                        if candidates:
                            closest_buy = max(candidates)
                            distance = sell_order["entry_price"] - closest_buy
                            if distance >= rr_distance_pips * orders[0]["pip_size"]:
                                coordinated_tp[(sell_order["tf"], sell_order["entry_price"], "sell_limit")] = closest_buy - orders[0]["tick_size"]

                    for buy_order in buys:
                        candidates = [p for p in sell_prices if p > buy_order["entry_price"]]
                        if candidates:
                            closest_sell = min(candidates)
                            distance = closest_sell - buy_order["entry_price"]
                            if distance >= rr_distance_pips * orders[0]["pip_size"]:
                                coordinated_tp[(buy_order["tf"], buy_order["entry_price"], "buy_limit")] = closest_sell + orders[0]["tick_size"]

            # Build final calculated orders
            for norm_market, orders in filtered_symbol_orders.items():
                if not orders:
                    continue
                sample = orders[0]
                digits = len(str(sample["tick_size"]).split('.')[-1]) if '.' in str(sample["tick_size"]) else 0
                digits = max(digits, 2)

                for ord_info in orders:
                    entry_price = ord_info["entry_price"]
                    order_type = ord_info["order_type"]
                    tf = ord_info["tf"]
                    manual_sl = ord_info["exit_price"]
                    manual_tp = ord_info["profit_price"]
                    market_key = ord_info["market_key"]

                    sl_price = None
                    tp_price = None

                    if calc_sl:
                        sl_price = entry_price - (ord_info["sl_pips"] * ord_info["pip_size"]) if order_type == "buy_limit" else entry_price + (ord_info["sl_pips"] * ord_info["pip_size"])
                    elif manual_sl is not None:
                        sl_price = manual_sl

                    key = (tf, entry_price, order_type)
                    if calculation_type == "coordination" and key in coordinated_tp:
                        tp_price = coordinated_tp[key]
                    elif calc_tp:
                        tp_price = entry_price + (ord_info["tp_pips"] * ord_info["pip_size"]) if order_type == "buy_limit" else entry_price - (ord_info["tp_pips"] * ord_info["pip_size"])
                    elif manual_tp is not None:
                        tp_price = manual_tp
                    else:
                        tp_price = entry_price + (ord_info["tp_pips"] * ord_info["pip_size"]) if order_type == "buy_limit" else entry_price - (ord_info["tp_pips"] * ord_info["pip_size"])

                    if sl_price is not None:
                        sl_price = round(sl_price, digits)
                    tp_price = round(tp_price, digits)

                    calc_order = {
                        "market_name": market_key,
                        "timeframe": tf,
                        "limit_order": order_type,
                        "entry_price": entry_price,
                        "sl_price": sl_price,
                        "tp_price": tp_price,
                        "volume": ord_info["volume_to_use"],
                        "riskusd_amount": ord_info["risk_usd_to_use"],
                        "rr_ratio": rr_ratio,
                        "sl_pips": round(ord_info["sl_pips"], 2),
                        "tp_pips": round(ord_info["tp_pips"], 2),
                        "calculated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "broker": user_brokerid
                    }

                    risk_usd = ord_info["risk_usd_to_use"]
                    results_by_risk[risk_usd][market_key][tf].append(calc_order)

            if not equities_found or not results_by_risk:
                continue

            # Save to risk folders
            for risk_usd, markets_dict in results_by_risk.items():
                risk_folder = calc_risk_dir / f"risk_{str(risk_usd).replace('.', '_')}_usd"
                risk_folder.mkdir(parents=True, exist_ok=True)

                orders_structure = {
                    "orders": {
                        "total_orders": sum(len(orders) for market in markets_dict.values() for orders in market.values()),
                        "total_markets": len(markets_dict)
                    }
                }
                orders_dict = orders_structure["orders"]

                for market_key, tf_dict in markets_dict.items():
                    norm_market = " ".join(market_key.replace("_", " ").split()).upper()
                    tick_info = SYMBOLSTICK_DATA.get(norm_market, {})
                    if tick_info.get("broker") != broker_clean:
                        tick_info = {}
                    orders_dict[market_key] = {
                        "category": "equities",
                        "broker": user_brokerid,
                        "tick_size": tick_info.get("tick_size", 0.01),
                        "tick_value": tick_info.get("tick_value", 1.0),
                        **tf_dict
                    }

                out_file = risk_folder / f"{original_stem}_equities.json"
                try:
                    with open(out_file, 'w', encoding='utf-8') as f:
                        json.dump(orders_structure, f, indent=2, ensure_ascii=False)
                    files_saved_this_broker += 1
                except:
                    pass

        if files_saved_this_broker > 0:
            print(f" → Generated {files_saved_this_broker} equities JSON file(s)\n")
        else:
            print(" → No equities output files generated\n")

    return True



def calculate_forex_orders_new():
    try:
        # 1. Load User IDs
        if not os.path.exists(USERS_PATH) or os.path.getsize(USERS_PATH) == 0:
            print(f"Users file not found or empty: {USERS_PATH}")
            return False
            
        with open(USERS_PATH, 'r', encoding='utf-8') as f:
            users_data = json.load(f)
            
        # 2. Load Global Forex symbols
        if not os.path.exists(SYMBOL_CATEGORY_PATH) or os.path.getsize(SYMBOL_CATEGORY_PATH) == 0:
            print(f"Symbol category file not found or empty: {SYMBOL_CATEGORY_PATH}")
            return False

        with open(SYMBOL_CATEGORY_PATH, 'r', encoding='utf-8') as f:
            categories = json.load(f)
            forex_symbols = set(categories.get("forex", []))

        # 3. Iterate through each User
        for user_broker_id in users_data.keys():
            user_folder = os.path.join(DEV_PATH, user_broker_id)
            acc_mgmt_path = os.path.join(user_folder, "accountmanagement.json")
            primary_volumes_path = os.path.join(user_folder, "allowedsymbolsandvolumes.json")

            if not os.path.exists(acc_mgmt_path):
                continue

            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                acc_mgmt_data = json.load(f)
            
            rr_ratios = acc_mgmt_data.get("risk_reward_ratios", [1.0])
            poi_conditions = acc_mgmt_data.get("chart", {}).get("define_candles", {}).get("entries_poi_condition", {})
            
            # --- Handle Secondary Config Directories ---
            secondary_paths = []
            for apprehend_val in poi_conditions.values():
                if isinstance(apprehend_val, dict):
                    for entry_val in apprehend_val.values():
                        if isinstance(entry_val, dict) and entry_val.get("new_filename"):
                            target_dir = os.path.join(user_folder, entry_val["new_filename"])
                            secondary_file = os.path.join(target_dir, "allowedsymbolsandvolumes.json")
                            if not os.path.exists(secondary_file) and os.path.exists(primary_volumes_path):
                                os.makedirs(target_dir, exist_ok=True)
                                shutil.copy2(primary_volumes_path, secondary_file)
                            secondary_paths.append(secondary_file)

            all_config_files = [primary_volumes_path] + secondary_paths

            for volumes_path in all_config_files:
                if not os.path.exists(volumes_path): continue

                with open(volumes_path, 'r', encoding='utf-8') as f:
                    v_data = json.load(f)
                    user_config = {item['symbol'].upper(): item for item in v_data.get("forex", [])}

                config_folder = os.path.dirname(volumes_path)
                limit_order_files = glob.glob(os.path.join(config_folder, "**", "limit_orders.json"), recursive=True)

                for limit_path in limit_order_files:
                    if "risk_reward_" in limit_path: continue
                    with open(limit_path, 'r', encoding='utf-8') as f:
                        original_orders = json.load(f)

                    base_dir = os.path.dirname(limit_path)

                    for current_rr in rr_ratios:
                        orders_copy = copy.deepcopy(original_orders)
                        updated = False

                        for order in orders_copy:
                            symbol = order.get('symbol', '').upper()
                            if symbol not in forex_symbols: continue

                            try:
                                # Data Extraction
                                entry = float(order.get('entry', 0))
                                rr_ratio = float(current_rr)
                                order_type = order.get('order_type', '').upper()
                                tick_size = float(order.get('tick_size', 0.00001))
                                tick_value = float(order.get('tick_value', 0))
                                tf = order.get('timeframe', '1h')
                                
                                # Rounding: 5 decimals for most, 3 for JPY/others
                                digits = 5 if tick_size <= 1e-5 else 3
                                
                                # Fetch specifications from the user config
                                tf_specs = user_config.get(symbol, {}).get(f"{tf}_specs", {})
                                volume = float(tf_specs.get('volume', 0.01))
                                
                                # --- LOGIC BRANCH A: USD RISK BASED ---
                                if order.get("usd_based_risk_only") is True:
                                    # Use order's usd_risk, fallback to config
                                    risk_val = float(order.get("usd_risk", tf_specs.get("usd_risk", 0)))
                                    
                                    if risk_val > 0 and tick_value > 0:
                                        pip_size = 10 * tick_size
                                        pip_value_usd = tick_value * volume * (pip_size / tick_size)
                                        
                                        sl_pips = risk_val / pip_value_usd
                                        tp_pips = sl_pips * rr_ratio

                                        if "BUY" in order_type:
                                            order["exit"] = round(entry - (sl_pips * pip_size), digits)
                                            order["target"] = round(entry + (tp_pips * pip_size), digits)
                                        else:
                                            order["exit"] = round(entry + (sl_pips * pip_size), digits)
                                            order["target"] = round(entry - (tp_pips * pip_size), digits)

                                # --- LOGIC BRANCH B: DISTANCE BASED ---
                                else:
                                    sl_price = float(order.get('exit', 0))
                                    tp_price = float(order.get('target', 0))

                                    # Case: Target provided, calculate Exit (SL)
                                    if sl_price == 0 and tp_price > 0 and rr_ratio > 0:
                                        tp_dist = abs(tp_price - entry)
                                        risk_dist = tp_dist / rr_ratio
                                        order['exit'] = round(entry - risk_dist if "BUY" in order_type else entry + risk_dist, digits)
                                    
                                    # Case: Exit provided, calculate Target (TP)
                                    elif sl_price > 0 and rr_ratio > 0:
                                        risk_dist = abs(entry - sl_price)
                                        order['target'] = round(entry + (risk_dist * rr_ratio) if "BUY" in order_type else entry - (risk_dist * rr_ratio), digits)

                                # --- FINAL CLEANUP AND METADATA ---
                                # Remove usd_risk from output as requested
                                if "usd_risk" in order:
                                    del order["usd_risk"]
                                    
                                order[f"{tf}_volume"] = volume
                                order['risk_reward'] = rr_ratio
                                order['status'] = "Calculated"
                                order['calculated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                updated = True

                            except Exception as e:
                                print(f"Error calculating order for {symbol}: {e}")
                                continue

                        if updated:
                            rr_folder = f"risk_reward_{current_rr}"
                            target_out_dir = os.path.join(base_dir, rr_folder)
                            os.makedirs(target_out_dir, exist_ok=True)
                            with open(os.path.join(target_out_dir, "limit_orders.json"), 'w', encoding='utf-8') as f:
                                json.dump(orders_copy, f, indent=4)
            
            print(f"Successfully processed user: {user_broker_id}")

        return True
    except Exception as e:
        print(f"Critical Error in calculate_forex_orders: {e}")
        return False
