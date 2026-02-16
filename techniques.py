import os
import json
import re
import cv2
import numpy as np
import pytz
from multiprocessing import Pool, cpu_count
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import mplfinance as mpf
import pandas as pd
import glob
import os
import json
from datetime import datetime
import pytz
import shutil
from collections import defaultdict


DEV_PATH = r'C:\xampp\htdocs\chronedge\synarex\usersdata\developers'
DEV_USERS = r'C:\xampp\htdocs\chronedge\synarex\usersdata\developers\developers.json'

def load_developers_dictionary():
    # Corrected os.path.exists logic
    if not os.path.exists(DEV_USERS):
        print(f"Error: File not found at {DEV_USERS}")
        return {}
    try:
        with open(DEV_USERS, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: {DEV_USERS} contains invalid JSON: {e}")
        return {}
    except Exception as e:
        print(f"Error loading developers dictionary: {e}")
        return {}

def get_account_management(broker_name):
    path = os.path.join(r"C:\xampp\htdocs\chronedge\synarex\usersdata\developers", broker_name, "accountmanagement.json")
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None

def get_analysis_paths(
    base_folder,
    broker_name,
    sym,
    tf,
    direction,
    bars,
    output_filename_base,
    receiver_tf=None,
    target=None
):
    # Root of developer outputs
    dev_output_base = os.path.abspath(os.path.join(base_folder, "..", "developers", broker_name))

    # Existing Source files
    source_json = os.path.join(base_folder, sym, tf, "candlesdetails", f"{direction}_{bars}.json")
    source_chart = os.path.join(base_folder, sym, tf, f"chart_{bars}.png")
    full_bars_source = os.path.join(base_folder, sym, tf, "candlesdetails", "newest_oldest.json")

    # --- NEW: Ticks Paths ---
    # Source: base_folder\AUDNZD\AUDNZD_ticks.json
    source_ticks = os.path.join(base_folder, sym, f"{sym}_ticks.json")
    # Destination: developers\broker\AUDNZD\AUDNZD_ticks.json
    dest_ticks_dir = os.path.join(dev_output_base, sym)
    dest_ticks = os.path.join(dest_ticks_dir, f"{sym}_ticks.json")

    # Output directory (tf specific)
    output_dir = os.path.join(dev_output_base, sym, tf)
    output_json = os.path.join(output_dir, output_filename_base)
    output_chart = os.path.join(output_dir, output_filename_base.replace(".json", ".png"))
    config_json = os.path.join(output_dir, "config.json")

    comm_paths = {}
    if receiver_tf and target:
        base_name = output_filename_base.replace(".json", "")
        comm_filename_base = f"{receiver_tf}_{base_name}_{target}_{tf}"
        comm_paths = {
            "json": os.path.join(output_dir, f"{comm_filename_base}.json"),
            "png": os.path.join(output_dir, f"{comm_filename_base}.png"),
            "base_name": comm_filename_base
        }

    return {
        "dev_output_base": dev_output_base,
        "source_json": source_json,
        "source_chart": source_chart,
        "source_ticks": source_ticks,      # Added
        "dest_ticks": dest_ticks,          # Added
        "full_bars_source": full_bars_source,
        "output_dir": output_dir,
        "output_json": output_json,
        "output_chart": output_chart,
        "config_json": config_json,
        "comm_paths": comm_paths
    }  

def sync_ticks_data(broker_name):
    lagos_tz = pytz.timezone('Africa/Lagos')
    ts = datetime.now(lagos_tz).strftime('%Y-%m-%d %H:%M:%S')
    
    dev_dict = load_developers_dictionary()
    cfg = dev_dict.get(broker_name)
    if not cfg: return f"[{broker_name}] Ticks Error: Broker not found."

    base_folder = cfg.get("BASE_FOLDER")
    symbols = [d for d in os.listdir(base_folder) if os.path.isdir(os.path.join(base_folder, d))]
    
    copy_count = 0
    for sym in symbols:
        # We use dummy values for tf/bars just to get the pathing logic from the helper
        paths = get_analysis_paths(base_folder, broker_name, sym, "1m", "new_old", 100, "temp.json")
        
        src = paths["source_ticks"]
        dst = paths["dest_ticks"]
        
        if os.path.exists(src):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copy2(src, dst)
            copy_count += 1
            
    msg = f"[{ts}] [TICKS] {broker_name}: Copied {copy_count} tick files."
    print(msg)
    return msg

def copy_full_candle_data(broker_name):
    """
    Iterates through all symbols and timeframes, copies newest_oldest.json 
    to the developer output directory, and renames it to full_candles_data.json.
    """
    lagos_tz = pytz.timezone('Africa/Lagos')

    def log(msg, level="INFO"):
        ts = datetime.now(lagos_tz).strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{ts}] [{level}] {msg}")

    # 1. Load Configurations
    dev_dict = load_developers_dictionary() # Assuming this is available in your scope
    cfg = dev_dict.get(broker_name)
    if not cfg:
        return f"[{broker_name}] Error: Broker not in dictionary."
    
    base_folder = cfg.get("BASE_FOLDER")
    
    # Define destination base (consistent with get_analysis_paths)
    dev_output_base = os.path.abspath(os.path.join(base_folder, "..", "developers", broker_name))
    
    log(f"--- STARTING FULL CANDLE DATA: {broker_name} ---")
    
    processed_count = 0
    error_count = 0

    # 2. Iterate through Symbols
    if not os.path.exists(base_folder):
        return f"Error: Base folder {base_folder} does not exist."

    for sym in sorted(os.listdir(base_folder)):
        sym_p = os.path.join(base_folder, sym)
        if not os.path.isdir(sym_p):
            continue
            
        # 3. Iterate through Timeframes
        for tf in sorted(os.listdir(sym_p)):
            tf_p = os.path.join(sym_p, tf)
            if not os.path.isdir(tf_p):
                continue
            
            # Source: base_folder/SYM/TF/candlesdetails/newest_oldest.json
            source_path = os.path.join(tf_p, "candlesdetails", "newest_oldest.json")
            
            # Destination: developers/broker/SYM/TF/full_candles_data.json
            dest_dir = os.path.join(dev_output_base, sym, tf)
            dest_path = os.path.join(dest_dir, "full_candles_data.json")

            try:
                if os.path.exists(source_path):
                    # Ensure destination directory exists (where config.json lives)
                    os.makedirs(dest_dir, exist_ok=True)
                    
                    # Copy and rename
                    shutil.copy2(source_path, dest_path)
                    processed_count += 1
                else:
                    # Log missing source files as info/debug
                    pass 

            except Exception as e:
                log(f"Error copying {sym}/{tf}: {e}", "ERROR")
                error_count += 1

    return f"Copy Done. Files: {processed_count}"

def label_objects_and_text(
    img,
    cx,
    y_rect,
    h_rect,
    fvg_swing_type=None,                 
    custom_text=None,           
    object_type="arrow",        
    is_bullish_arrow=True,      
    is_marked=False,
    double_arrow=False,
    arrow_color=(0, 255, 0),
    object_color=(0, 255, 0),
    font_scale=0.55,
    text_thickness=2,
    label_position="auto",
    end_x=None,
    box_w=None,          # External function provides this
    box_h=None,          # External function provides this
    box_alpha=0.3        # Transparency threshold
):
    color = object_color if object_color != (0, 255, 0) else arrow_color

    # Dimensions for markers
    shaft_length = 26
    head_size = 9
    thickness = 2
    wing_size = 7 if double_arrow else 6

    # 1. Determine Vertical Placement (Anchor Point)
    if label_position == "auto":
        place_at_high = not is_bullish_arrow  # HH/LH → top, ll/LL → bottom
    else:
        place_at_high = (label_position.lower() == "high")

    # tip_y is the exact pixel of the wick tip (top or bottom)
    tip_y = y_rect if place_at_high else (y_rect + h_rect)

    # 2. Draw Objects (Arrows/Shapes/Lines) ONLY if is_marked is True
    if is_marked:
        if object_type in ["arrow", "reverse_arrow"]:
            def draw_single_arrow_logic(center_x: int, is_reverse=False):
                if not is_reverse:
                    if place_at_high: # Tip at top, shaft goes UP
                        shaft_start_y = tip_y - head_size
                        cv2.line(img, (center_x, shaft_start_y), (center_x, shaft_start_y - shaft_length), arrow_color, thickness)
                        pts = np.array([[center_x, tip_y - 2], [center_x - wing_size, tip_y - head_size], [center_x + wing_size, tip_y - head_size]], np.int32)
                    else: # Tip at bottom, shaft goes DOWN
                        shaft_start_y = tip_y + head_size
                        cv2.line(img, (center_x, shaft_start_y), (center_x, shaft_start_y + shaft_length), arrow_color, thickness)
                        pts = np.array([[center_x, tip_y + 2], [center_x - wing_size, tip_y + head_size], [center_x + wing_size, tip_y + head_size]], np.int32)
                else:
                    base_y = tip_y - 5 if place_at_high else tip_y + 5
                    end_y = base_y - shaft_length if place_at_high else base_y + shaft_length
                    cv2.line(img, (center_x, base_y), (center_x, end_y), arrow_color, thickness)
                    tip_offset = -head_size if place_at_high else head_size
                    pts = np.array([[center_x, end_y + tip_offset], [center_x - wing_size, end_y], [center_x + wing_size, end_y]], np.int32)
                cv2.fillPoly(img, [pts], arrow_color)

            is_rev = (object_type == "reverse_arrow")
            if double_arrow:
                draw_single_arrow_logic(cx - 5, is_reverse=is_rev)
                draw_single_arrow_logic(cx + 5, is_reverse=is_rev)
            else:
                draw_single_arrow_logic(cx, is_reverse=is_rev)

        elif object_type == "rightarrow":
            base_x, tip_x = cx - 30, cx - 10
            cv2.line(img, (base_x, tip_y), (tip_x, tip_y), color, thickness)
            pts = np.array([[tip_x, tip_y], [tip_x - head_size, tip_y - wing_size], [tip_x - head_size, tip_y + wing_size]], np.int32)
            cv2.fillPoly(img, [pts], color)

        elif object_type == "leftarrow":
            base_x, tip_x = cx + 30, cx + 10
            cv2.line(img, (base_x, tip_y), (tip_x, tip_y), color, thickness)
            pts = np.array([[tip_x, tip_y], [tip_x + head_size, tip_y - wing_size], [tip_x + head_size, tip_y + wing_size]], np.int32)
            cv2.fillPoly(img, [pts], color)

        elif object_type == "lline":
            # Thin 1px horizontal line
            stop_x = end_x if end_x is not None else img.shape[1]
            cv2.line(img, (cx, tip_y), (int(stop_x), tip_y), color, 1)

        elif object_type == "box_transparent":
            if box_w is not None and box_h is not None:
                # Calculate coordinates based on passed width/height
                x1, y1 = cx - (box_w // 2), tip_y - (box_h // 2)
                x2, y2 = x1 + box_w, y1 + box_h
                
                # Overlay for transparency
                overlay = img.copy()
                cv2.rectangle(overlay, (x1, y1), (x2, y2), color, -1)
                cv2.addWeighted(overlay, box_alpha, img, 1 - box_alpha, 0, img)
                
                # 1px Border
                cv2.rectangle(img, (x1, y1), (x2, y2), color, 1)

        else:
            shape_y = tip_y - 12 if place_at_high else tip_y + 12
            if object_type == "circle":
                cv2.circle(img, (cx, shape_y), 6, color, thickness=thickness)
            elif object_type == "dot":
                cv2.circle(img, (cx, shape_y), 6, color, thickness=-1)
            elif object_type == "pentagon":
                radius = 8
                pts = np.array([[cx, shape_y - radius], [cx + int(radius * 0.95), shape_y - int(radius * 0.31)], [cx + int(radius * 0.58), shape_y + int(radius * 0.81)], [cx - int(radius * 0.58), shape_y + int(radius * 0.81)], [cx - int(radius * 0.95), shape_y - int(radius * 0.31)]], np.int32)
                cv2.fillPoly(img, [pts], color)
            elif object_type == "star":
                outer_rad, inner_rad = 11, 5
                pts = []
                for i in range(10):
                    angle = i * (np.pi / 5) - (np.pi / 2)
                    r = outer_rad if i % 2 == 0 else inner_rad
                    pts.append([cx + int(np.cos(angle) * r), shape_y + int(np.sin(angle) * r)])
                cv2.fillPoly(img, [np.array(pts, np.int32)], color)

    # 3. Text Placement Logic
    if not (custom_text or fvg_swing_type is not None):
        return

    if is_marked:
        is_vertical_obj = object_type in ["arrow", "reverse_arrow"]
        if is_vertical_obj:
            reach = (shaft_length + head_size + 4)
        elif object_type == "box_transparent" and box_h is not None:
            reach = (box_h // 2) + 4
        else:
            reach = 14
    else:
        reach = 4

    if place_at_high:
        base_text_y = tip_y - reach
    else:
        base_text_y = tip_y + reach + 10

    if custom_text:
        (tw, th), _ = cv2.getTextSize(custom_text, cv2.FONT_HERSHEY_SIMPLEX, font_scale, text_thickness)
        cv2.putText(img, custom_text, (cx - tw // 2, int(base_text_y)),
                    cv2.FONT_HERSHEY_SIMPLEX, font_scale, arrow_color, text_thickness)
        fvg_swing_type_y = (base_text_y - 15) if place_at_high else (base_text_y + 15)
    else:
        fvg_swing_type_y = base_text_y

    if fvg_swing_type is not None:
        cv2.putText(img, str(fvg_swing_type), (cx - 8, int(fvg_swing_type_y)), 
                    cv2.FONT_HERSHEY_SIMPLEX, 0.35, (0, 0, 0), 2)
        cv2.putText(img, str(fvg_swing_type), (cx - 8, int(fvg_swing_type_y)), 
                    cv2.FONT_HERSHEY_SIMPLEX, 0.35, (255, 255, 255), 1)                   

def label_objects(
    img,
    cx,
    y_rect,
    h_rect,
    fvg_swing_type=None,                  
    custom_text=None,           
    object_type="arrow",       
    is_bullish_arrow=True,     
    is_marked=False,
    double_arrow=False,
    arrow_color=(0, 255, 0),
    object_color=(0, 255, 0),
    font_scale=0.55,
    text_thickness=2,
    label_position="auto",
    end_x=None,
    box_w=None,           # External function provides this
    box_h=None,           # External function provides this
    box_alpha=0.3,        # Transparency threshold
    start_x=None,         # NEW: For horizontal line start point
    stop_x=None,          # NEW: For horizontal line end point
    box_color=(0, 0, 0),  # NEW: Box color parameter
    no_border=True        # NEW: Box border control
):
    color = object_color if object_color != (0, 255, 0) else arrow_color

    # Dimensions for markers
    shaft_length = 26
    head_size = 9
    thickness = 2
    wing_size = 7 if double_arrow else 6

    # 1. Determine Vertical Placement (Anchor Point)
    if label_position == "auto":
        place_at_high = not is_bullish_arrow  # HH/LH → top, ll/LL → bottom
    else:
        place_at_high = (label_position.lower() == "high")

    # tip_y is the exact pixel of the wick tip (top or bottom)
    tip_y = y_rect if place_at_high else (y_rect + h_rect)

    # 2. Draw Objects (Arrows/Shapes/Lines) ONLY if is_marked is True
    if is_marked:
        if object_type in ["arrow", "reverse_arrow"]:
            def draw_single_arrow_logic(center_x: int, is_reverse=False):
                if not is_reverse:
                    if place_at_high: # Tip at top, shaft goes UP
                        shaft_start_y = tip_y - head_size
                        cv2.line(img, (center_x, shaft_start_y), (center_x, shaft_start_y - shaft_length), arrow_color, thickness)
                        pts = np.array([[center_x, tip_y - 2], [center_x - wing_size, tip_y - head_size], [center_x + wing_size, tip_y - head_size]], np.int32)
                    else: # Tip at bottom, shaft goes DOWN
                        shaft_start_y = tip_y + head_size
                        cv2.line(img, (center_x, shaft_start_y), (center_x, shaft_start_y + shaft_length), arrow_color, thickness)
                        pts = np.array([[center_x, tip_y + 2], [center_x - wing_size, tip_y + head_size], [center_x + wing_size, tip_y + head_size]], np.int32)
                else:
                    base_y = tip_y - 5 if place_at_high else tip_y + 5
                    end_y = base_y - shaft_length if place_at_high else base_y + shaft_length
                    cv2.line(img, (center_x, base_y), (center_x, end_y), arrow_color, thickness)
                    tip_offset = -head_size if place_at_high else head_size
                    pts = np.array([[center_x, end_y + tip_offset], [center_x - wing_size, end_y], [center_x + wing_size, end_y]], np.int32)
                cv2.fillPoly(img, [pts], arrow_color)

            is_rev = (object_type == "reverse_arrow")
            if double_arrow:
                draw_single_arrow_logic(cx - 5, is_reverse=is_rev)
                draw_single_arrow_logic(cx + 5, is_reverse=is_rev)
            else:
                draw_single_arrow_logic(cx, is_reverse=is_rev)

        elif object_type == "rightarrow":
            base_x, tip_x = cx - 30, cx - 10
            cv2.line(img, (base_x, tip_y), (tip_x, tip_y), color, thickness)
            pts = np.array([[tip_x, tip_y], [tip_x - head_size, tip_y - wing_size], [tip_x - head_size, tip_y + wing_size]], np.int32)
            cv2.fillPoly(img, [pts], color)

        elif object_type == "leftarrow":
            base_x, tip_x = cx + 30, cx + 10
            cv2.line(img, (base_x, tip_y), (tip_x, tip_y), color, thickness)
            pts = np.array([[tip_x, tip_y], [tip_x + head_size, tip_y - wing_size], [tip_x + head_size, tip_y + wing_size]], np.int32)
            cv2.fillPoly(img, [pts], color)

        elif object_type == "lline":
            # Thin 1px horizontal line with start and stop points
            start_x_final = start_x if start_x is not None else cx
            stop_x_final = stop_x if stop_x is not None else (end_x if end_x is not None else img.shape[1])
            cv2.line(img, (int(start_x_final), tip_y), (int(stop_x_final), tip_y), color, 1)

        elif object_type == "box_transparent":
            if box_w is not None and box_h is not None:
                # Calculate coordinates based on passed width/height
                x1, y1 = cx - (box_w // 2), tip_y - (box_h // 2)
                x2, y2 = x1 + box_w, y1 + box_h
                
                # Overlay for transparency
                overlay = img.copy()
                cv2.rectangle(overlay, (x1, y1), (x2, y2), box_color, -1)
                cv2.addWeighted(overlay, box_alpha, img, 1 - box_alpha, 0, img)
                
                # Border only if not specified as no border
                if not no_border:
                    cv2.rectangle(img, (x1, y1), (x2, y2), color, 1)

        else:
            shape_y = tip_y - 12 if place_at_high else tip_y + 12
            if object_type == "circle":
                cv2.circle(img, (cx, shape_y), 6, color, thickness=thickness)
            elif object_type == "dot":
                cv2.circle(img, (cx, shape_y), 6, color, thickness=-1)
            elif object_type == "pentagon":
                radius = 8
                pts = np.array([[cx, shape_y - radius], [cx + int(radius * 0.95), shape_y - int(radius * 0.31)], [cx + int(radius * 0.58), shape_y + int(radius * 0.81)], [cx - int(radius * 0.58), shape_y + int(radius * 0.81)], [cx - int(radius * 0.95), shape_y - int(radius * 0.31)]], np.int32)
                cv2.fillPoly(img, [pts], color)
            elif object_type == "star":
                outer_rad, inner_rad = 11, 5
                pts = []
                for i in range(10):
                    angle = i * (np.pi / 5) - (np.pi / 2)
                    r = outer_rad if i % 2 == 0 else inner_rad
                    pts.append([cx + int(np.cos(angle) * r), shape_y + int(np.sin(angle) * r)])
                cv2.fillPoly(img, [np.array(pts, np.int32)], color)

    # 3. Text Placement Logic
    if not (custom_text or fvg_swing_type is not None):
        return

    if is_marked:
        is_vertical_obj = object_type in ["arrow", "reverse_arrow"]
        if is_vertical_obj:
            reach = (shaft_length + head_size + 4)
        elif object_type == "box_transparent" and box_h is not None:
            reach = (box_h // 2) + 4
        else:
            reach = 14
    else:
        reach = 4

    if place_at_high:
        base_text_y = tip_y - reach
    else:
        base_text_y = tip_y + reach + 10

    if custom_text:
        (tw, th), _ = cv2.getTextSize(custom_text, cv2.FONT_HERSHEY_SIMPLEX, font_scale, text_thickness)
        cv2.putText(img, custom_text, (cx - tw // 2, int(base_text_y)),
                    cv2.FONT_HERSHEY_SIMPLEX, font_scale, arrow_color, text_thickness)
        fvg_swing_type_y = (base_text_y - 15) if place_at_high else (base_text_y + 15)
    else:
        fvg_swing_type_y = base_text_y

    if fvg_swing_type is not None:
        cv2.putText(img, str(fvg_swing_type), (cx - 8, int(fvg_swing_type_y)), 
                    cv2.FONT_HERSHEY_SIMPLEX, 0.35, (0, 0, 0), 2)
        cv2.putText(img, str(fvg_swing_type), (cx - 8, int(fvg_swing_type_y)), 
                    cv2.FONT_HERSHEY_SIMPLEX, 0.35, (255, 255, 255), 1)
        
def lower_highs_higher_lows(broker_name):
    lagos_tz = pytz.timezone('Africa/Lagos')
    def log(msg, level="INFO"):
        ts = datetime.now(lagos_tz).strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{ts}] [{level}] {msg}")

    dev_dict = load_developers_dictionary()
    cfg = dev_dict.get(broker_name)
    if not cfg: 
        return f"[{broker_name}] Error: Broker not in dictionary."
    
    base_folder = cfg.get("BASE_FOLDER")
    am_data = get_account_management(broker_name)
    if not am_data: 
        return f"[{broker_name}] Error: accountmanagement.json missing."
    
    define_candles = am_data.get("chart", {}).get("define_candles", {})
    
    # --- CONFIG LOGIC UPDATE ---
    # We look for the Parent (HH/LL) config specifically to steal its BARS setting
    parent_keyword = "higherhighsandlowerlows"
    parent_cfg_list = [v for k, v in define_candles.items() if parent_keyword in k.lower()]
    parent_bars = parent_cfg_list[0].get("BARS", 101) if parent_cfg_list else 101
    
    keyword = "lowerhighsandhigherlows"
    matching_configs = [(k, v) for k, v in define_candles.items() if keyword in k.lower()]

    if not matching_configs:
        return f"[{broker_name}] Error: No configuration found for '{keyword}'."

    log(f"--- STARTING IDENTIFICATION (CHILD): {broker_name} ---")
    log(f"Using Parent BARS: {parent_bars}")

    total_marked_all, processed_charts_all = 0, 0

    def resolve_marker(raw):
        if not raw: return None, False
        raw = str(raw).lower().strip()
        if raw in ["arrow", "arrows", "singlearrow"]: return "arrow", False
        if raw in ["doublearrow", "doublearrows"]: return "arrow", True
        if raw in ["reverse_arrow", "reversearrow"]: return "reverse_arrow", False
        if raw in ["reverse_doublearrow", "reverse_doublearrows"]: return "reverse_arrow", True
        if raw in ["rightarrow", "right_arrow"]: return "rightarrow", False
        if raw in ["leftarrow", "left_arrow"]: return "leftarrow", False
        if "dot" in raw: return "dot", False
        return raw, False

    for config_key, llhl_cfg in matching_configs:
        log(f"Processing Config Key: [{config_key}]")
        # Overriding local BARS with parent_bars as requested
        bars = parent_bars 
        output_filename_base = llhl_cfg.get("filename", "lowers.json")
        direction = llhl_cfg.get("read_candles_from", "new_old")
        
        neighbor_left = llhl_cfg.get("NEIGHBOR_LEFT", 5)
        neighbor_right = llhl_cfg.get("NEIGHBOR_RIGHT", 5)

        label_cfg = llhl_cfg.get("label", {})
        lh_text = label_cfg.get("lowerhighs_text", "LH")
        ll_text = label_cfg.get("higherlows_text", "HL")
        cm_text = label_cfg.get("contourmaker_text", "m")

        label_at = label_cfg.get("label_at", {})
        lh_pos = label_at.get("lower_highs", "high").lower()
        ll_pos = label_at.get("higher_lows", "low").lower()

        color_map = {"green": (0, 255, 0), "red": (255, 0, 0), "blue": (0, 0, 255)}
        lh_col = color_map.get(label_at.get("lower_highs_color", "red").lower(), (255, 0, 0))
        ll_col = color_map.get(label_at.get("higher_lows_color", "green").lower(), (0, 255, 0))

        lh_obj, lh_dbl = resolve_marker(label_at.get("lower_highs_marker", "arrow"))
        ll_obj, ll_dbl = resolve_marker(label_at.get("higher_lows_marker", "arrow"))
        lh_cm_obj, lh_cm_dbl = resolve_marker(label_at.get("lower_highs_contourmaker_marker", ""))
        ll_cm_obj, ll_cm_dbl = resolve_marker(label_at.get("higher_lows_contourmaker_marker", ""))

        symbols = sorted([d for d in os.listdir(base_folder) if os.path.isdir(os.path.join(base_folder, d))])
        
        for sym in symbols:
            sym_p = os.path.join(base_folder, sym)
            timeframes = sorted(os.listdir(sym_p))

            for tf in timeframes:
                paths = get_analysis_paths(base_folder, broker_name, sym, tf, direction, bars, output_filename_base)
                config_path = os.path.join(paths["output_dir"], "config.json")

                if not os.path.exists(paths["source_json"]) or not os.path.exists(paths["source_chart"]):
                    continue

                try:
                    # 1. Load existing config to check for Parent (HH/LL) claims
                    parent_claimed_candles = set()
                    if os.path.exists(config_path):
                        with open(config_path, 'r', encoding='utf-8') as f:
                            existing_cfg_data = json.load(f)
                            # Look for any key containing "higherhighsandlowerlows"
                            for k, v in existing_cfg_data.items():
                                if parent_keyword in k.lower() and isinstance(v, list):
                                    for candle in v:
                                        if candle.get("is_swing"):
                                            parent_claimed_candles.add(candle.get("candle_number"))

                    # 2. Load candle data
                    with open(paths["source_json"], 'r', encoding='utf-8') as f:
                        data = sorted(json.load(f), key=lambda x: x.get('candle_number', 0))
                    
                    img = cv2.imread(paths["source_chart"])
                    if img is None: continue

                    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
                    mask = cv2.inRange(hsv, (35, 50, 50), (85, 255, 255)) | cv2.inRange(hsv, (0, 50, 50), (10, 255, 255))
                    contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
                    
                    if len(contours) == 0: continue

                    contours = sorted(contours, key=lambda c: cv2.boundingRect(c)[0])
                    if len(data) != len(contours):
                        min_len = min(len(data), len(contours))
                        data = data[:min_len]
                        contours = contours[:min_len]

                    # Map coordinates
                    for idx, contour in enumerate(contours):
                        x, y, w, h = cv2.boundingRect(contour)
                        data[idx].update({
                            "candle_x": x + (w // 2), "candle_y": y,
                            "candle_width": w, "candle_height": h,
                            "candle_left": x, "candle_right": x + w,
                            "candle_top": y, "candle_bottom": y + h
                        })

                    n = len(data)
                    swing_count_in_chart = 0
                    for i in range(neighbor_left, n - neighbor_right):
                        fvg_swing_type = data[i].get('candle_number')
                        
                        # --- EXCLUSION CHECK ---
                        # If the Parent already claimed this candle, we skip it
                        if fvg_swing_type in parent_claimed_candles:
                            continue

                        curr_h, curr_l = data[i]['high'], data[i]['low']
                        l_h = [d['high'] for d in data[i-neighbor_left:i]]
                        l_l = [d['low'] for d in data[i-neighbor_left:i]]
                        r_h = [d['high'] for d in data[i+1:i+neighbor_right+1]]
                        r_l = [d['low'] for d in data[i+1:i+neighbor_right+1]]

                        is_peak = curr_h > max(l_h) and curr_h > max(r_h)
                        is_valley = curr_l < min(l_l) and curr_l < min(r_l)

                        if is_peak or is_valley:
                            swing_count_in_chart += 1
                            is_bull = is_valley
                            active_color = ll_col if is_bull else lh_col
                            custom_text = ll_text if is_bull else lh_text
                            obj_type = ll_obj if is_bull else lh_obj
                            dbl_arrow = ll_dbl if is_bull else lh_dbl
                            position = ll_pos if is_bull else lh_pos

                            label_objects_and_text(
                                img, data[i]["candle_x"], data[i]["candle_y"], data[i]["candle_height"], 
                                fvg_swing_type=fvg_swing_type,
                                custom_text=custom_text, object_type=obj_type,
                                is_bullish_arrow=is_bull, is_marked=True,
                                double_arrow=dbl_arrow, arrow_color=active_color,
                                label_position=position
                            )

                            # Handle Contour Maker
                            m_idx = i + neighbor_right
                            contour_maker_entry = None
                            if m_idx < n:
                                cm_obj = ll_cm_obj if is_bull else lh_cm_obj
                                cm_dbl = ll_cm_dbl if is_bull else lh_cm_dbl
                                
                                label_objects_and_text(
                                    img, data[m_idx]["candle_x"], data[m_idx]["candle_y"], data[m_idx]["candle_height"], 
                                    custom_text=cm_text, object_type=cm_obj,
                                    is_bullish_arrow=is_bull, is_marked=True,
                                    double_arrow=cm_dbl, arrow_color=active_color,
                                    label_position=position
                                )
                                contour_maker_entry = data[m_idx].copy()
                                contour_maker_entry.update({"is_contour_maker": True})

                            data[i].update({
                                "swing_type": "higher_low" if is_bull else "lower_high",
                                "is_swing": True, "active_color": active_color,
                                "draw_x": data[i]["candle_x"], "draw_y": data[i]["candle_y"],
                                "draw_w": data[i]["candle_width"], "draw_h": data[i]["candle_height"],
                                "contour_maker": contour_maker_entry,
                                "m_idx": m_idx if m_idx < n else None
                            })

                    # Save visual chart and Update config.json
                    os.makedirs(paths["output_dir"], exist_ok=True)
                    cv2.imwrite(paths["output_chart"], img)

                    config_content = {}
                    if os.path.exists(config_path):
                        with open(config_path, 'r', encoding='utf-8') as f:
                            try: config_content = json.load(f)
                            except: config_content = {}

                    config_content[config_key] = data
                    config_content[f"{config_key}_candle_list"] = data

                    with open(config_path, 'w', encoding='utf-8') as f:
                        json.dump(config_content, f, indent=4)
                    
                    processed_charts_all += 1
                    total_marked_all += swing_count_in_chart

                except Exception as e:
                    log(f"Error in {sym}/{tf}: {e}", "ERROR")

    summary = f"COMPLETED. Broker: {broker_name} | Total Swings: {total_marked_all} | Total Charts: {processed_charts_all}"
    log(summary)
    return summary

def higher_highs_lower_lows(broker_name):

    lagos_tz = pytz.timezone('Africa/Lagos')
    def log(msg, level="INFO"):
        ts = datetime.now(lagos_tz).strftime('%Y-%m-%d %H:%M:%S')

    dev_dict = load_developers_dictionary()
    cfg = dev_dict.get(broker_name)
    if not cfg:
        return f"[{broker_name}] Error: Broker not in dictionary."
    
    base_folder = cfg.get("BASE_FOLDER")
    am_data = get_account_management(broker_name)
    if not am_data:
        return f"[{broker_name}] Error: accountmanagement.json missing."
    
    define_candles = am_data.get("chart", {}).get("define_candles", {})
    keyword = "higherhighsandlowerlows"
    matching_configs = [(k, v) for k, v in define_candles.items() if keyword in k.lower()]
    if not matching_configs:
        return f"[{broker_name}] Error: No configuration found for '{keyword}'."
    
    total_marked_all, processed_charts_all = 0, 0

    def resolve_marker(raw):
        if not raw: return None, False
        raw = str(raw).lower().strip()
        if raw in ["arrow", "arrows", "singlearrow"]: return "arrow", False
        if raw in ["doublearrow", "doublearrows"]: return "arrow", True
        if raw in ["reverse_arrow", "reversearrow"]: return "reverse_arrow", False
        if raw in ["reverse_doublearrow", "reverse_doublearrows"]: return "reverse_arrow", True
        if raw in ["rightarrow", "right_arrow"]: return "rightarrow", False
        if raw in ["leftarrow", "left_arrow"]: return "leftarrow", False
        if "dot" in raw: return "dot", False
        return raw, False

    log(f"--- STARTING HH/ll ANALYSIS: {broker_name} ---")

    for config_key, hlll_cfg in matching_configs:
        bars = hlll_cfg.get("BARS", 101)
        output_filename_base = hlll_cfg.get("filename", "highers.json")
        direction = hlll_cfg.get("read_candles_from", "new_old")
        
        neighbor_left = hlll_cfg.get("NEIGHBOR_LEFT", 5)
        neighbor_right = hlll_cfg.get("NEIGHBOR_RIGHT", 5)
        label_cfg = hlll_cfg.get("label", {})
        hh_text = label_cfg.get("higherhighs_text", "HH")
        ll_text = label_cfg.get("lowerlows_text", "ll")
        cm_text = label_cfg.get("contourmaker_text", "m")
        label_at = label_cfg.get("label_at", {})
        hh_pos = label_at.get("higher_highs", "high").lower()
        ll_pos = label_at.get("lower_lows", "low").lower()
        
        color_map = {"green": (0, 255, 0), "red": (255, 0, 0), "blue": (0, 0, 255)}
        hh_col = color_map.get(label_at.get("higher_highs_color", "red").lower(), (255, 0, 0))
        ll_col = color_map.get(label_at.get("lower_lows_color", "green").lower(), (0, 255, 0))
        
        hh_obj, hh_dbl = resolve_marker(label_at.get("higher_highs_marker", "arrow"))
        ll_obj, ll_dbl = resolve_marker(label_at.get("lower_lows_marker", "arrow"))
        hh_cm_obj, hh_cm_dbl = resolve_marker(label_at.get("higher_highs_contourmaker_marker", ""))
        ll_cm_obj, ll_cm_dbl = resolve_marker(label_at.get("lower_lows_contourmaker_marker", ""))

        for sym in sorted(os.listdir(base_folder)):
            sym_p = os.path.join(base_folder, sym)
            if not os.path.isdir(sym_p): continue
            
            for tf in sorted(os.listdir(sym_p)):
                paths = get_analysis_paths(base_folder, broker_name, sym, tf, direction, bars, output_filename_base)
                config_path = os.path.join(paths["output_dir"], "config.json")
                
                if not os.path.exists(paths["source_json"]) or not os.path.exists(paths["source_chart"]):
                    continue
                
                try:
                    # Logging specific pair and timeframe
                    with open(paths["source_json"], 'r', encoding='utf-8') as f:
                        data = sorted(json.load(f), key=lambda x: x.get('candle_number', 0))
                    
                    img = cv2.imread(paths["source_chart"])
                    if img is None: 
                        log(f"   Skipping: Could not load image {paths['source_chart']}", "WARNING")
                        continue
                    
                    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
                    mask = cv2.inRange(hsv, (35, 50, 50), (85, 255, 255)) | cv2.inRange(hsv, (0, 50, 50), (10, 255, 255))
                    contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
                    
                    if len(contours) == 0: 
                        log(f"   No contours found for {sym} {tf}")
                        continue

                    contours = sorted(contours, key=lambda c: cv2.boundingRect(c)[0])
                    
                    if len(data) != len(contours):
                        min_len = min(len(data), len(contours))
                        data = data[:min_len]
                        contours = contours[:min_len]

                    for idx, contour in enumerate(contours):
                        x, y, w, h = cv2.boundingRect(contour)
                        data[idx].update({
                            "candle_x": x + (w // 2),
                            "candle_y": y,
                            "candle_width": w,
                            "candle_height": h,
                            "candle_left": x,
                            "candle_right": x + w,
                            "candle_top": y,
                            "candle_bottom": y + h
                        })

                    n = len(data)
                    swing_count_in_chart = 0
                    start_idx = neighbor_left
                    end_idx = n - neighbor_right

                    for i in range(start_idx, end_idx):
                        curr_h, curr_l = data[i]['high'], data[i]['low']
                        
                        l_h = [d['high'] for d in data[i - neighbor_left:i]]
                        l_l = [d['low'] for d in data[i - neighbor_left:i]]
                        r_h = [d['high'] for d in data[i + 1:i + 1 + neighbor_right]]
                        r_l = [d['low'] for d in data[i + 1:i + 1 + neighbor_right]]
                        
                        is_hh = curr_h > max(l_h) and curr_h > max(r_h)
                        is_ll = curr_l < min(l_l) and curr_l < min(r_l)
                        
                        if not (is_hh or is_ll):
                            continue
                        
                        swing_count_in_chart += 1
                        is_bull = is_ll
                        active_color = ll_col if is_bull else hh_col
                        custom_text = ll_text if is_bull else hh_text
                        obj_type = ll_obj if is_bull else hh_obj
                        dbl_arrow = ll_dbl if is_bull else hh_dbl
                        position = ll_pos if is_bull else hh_pos

                        label_objects_and_text(
                            img, data[i]["candle_x"], data[i]["candle_y"], data[i]["candle_height"],
                            fvg_swing_type=data[i]['candle_number'],
                            custom_text=custom_text,
                            object_type=obj_type,
                            is_bullish_arrow=is_bull,
                            is_marked=True,
                            double_arrow=dbl_arrow,
                            arrow_color=active_color,
                            label_position=position
                        )

                        m_idx = i + neighbor_right
                        contour_maker_entry = None
                        if m_idx < n:
                            cm_obj = ll_cm_obj if is_bull else hh_cm_obj
                            cm_dbl = ll_cm_dbl if is_bull else hh_cm_dbl
                            
                            label_objects_and_text(
                                img, data[m_idx]["candle_x"], data[m_idx]["candle_y"], data[m_idx]["candle_height"],
                                custom_text=cm_text,
                                object_type=cm_obj,
                                is_bullish_arrow=is_bull,
                                is_marked=True,
                                double_arrow=cm_dbl,
                                arrow_color=active_color,
                                label_position=position
                            )

                            data[m_idx]["is_contour_maker"] = True
                            contour_maker_entry = data[m_idx].copy()
                            contour_maker_entry.update({
                                "draw_x": data[m_idx]["candle_x"], "draw_y": data[m_idx]["candle_y"],
                                "draw_w": data[m_idx]["candle_width"], "draw_h": data[m_idx]["candle_height"],
                                "draw_left": data[m_idx]["candle_left"], "draw_right": data[m_idx]["candle_right"],
                                "draw_top": data[m_idx]["candle_top"], "draw_bottom": data[m_idx]["candle_bottom"],
                                "is_contour_maker": True
                            })

                        data[i].update({
                            "swing_type": "lower_low" if is_bull else "higher_high",
                            "is_swing": True,
                            "active_color": active_color,
                            "draw_x": data[i]["candle_x"], "draw_y": data[i]["candle_y"],
                            "draw_w": data[i]["candle_width"], "draw_h": data[i]["candle_height"],
                            "draw_left": data[i]["candle_left"], "draw_right": data[i]["candle_right"],
                            "draw_top": data[i]["candle_top"], "draw_bottom": data[i]["candle_bottom"],
                            "contour_maker": contour_maker_entry,
                            "m_idx": m_idx if m_idx < n else None
                        })

                    # Finalize outputs for this specific TF
                    os.makedirs(paths["output_dir"], exist_ok=True)
                    cv2.imwrite(paths["output_chart"], img)

                    config_json = {}
                    if os.path.exists(config_path):
                        try:
                            with open(config_path, 'r', encoding='utf-8') as f:
                                config_json = json.load(f)
                        except:
                            config_json = {}
                    
                    config_json[config_key] = data
                    config_json[f"{config_key}_candle_list"] = data 

                    with open(config_path, 'w', encoding='utf-8') as f:
                        json.dump(config_json, f, indent=4)
                    
                    log(f"{sym} | {tf} | Key: {config_key} Swings found: {swing_count_in_chart}")
                    
                    processed_charts_all += 1
                    total_marked_all += swing_count_in_chart

                except Exception as e:
                    log(f"   [ERROR] Failed processing {sym}/{tf}: {e}", "ERROR")

    log(f"--- HH/ll COMPLETE --- Total Swings: {total_marked_all} | Total Charts: {processed_charts_all}")
    return f"Identify Done. Swings: {total_marked_all} | Charts: {processed_charts_all}"  

def directional_bias(broker_name):
    lagos_tz = pytz.timezone('Africa/Lagos')
    
    def log(msg, level="INFO"):
        ts = datetime.now(lagos_tz).strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{ts}] [{level}] {msg}")

    def get_base_type(bias_direction):
        if not bias_direction: return None
        return "support" if bias_direction == "upward" else "resistance"

    def resolve_marker(raw):
        raw = str(raw or "").lower().strip()
        if not raw: return None, False
        if "double" in raw: return "arrow", True
        if "arrow"  in raw: return "arrow", False
        if "dot" in raw or "circle" in raw: return "dot", False
        if "pentagon" in raw: return "pentagon", False
        return raw, False

    dev_dict = load_developers_dictionary()
    cfg = dev_dict.get(broker_name)
    if not cfg:
        return f"[{broker_name}] Error: Broker not in dictionary."
    
    base_folder = cfg.get("BASE_FOLDER")
    am_data = get_account_management(broker_name)
    if not am_data:
        return f"[{broker_name}] Error: accountmanagement.json missing."
    
    chart_cfg = am_data.get("chart", {})
    define_candles = chart_cfg.get("define_candles", {})
    db_section = define_candles.get("directional_bias_candles", {})
    
    if not db_section:
        return f"[{broker_name}] Error: 'directional_bias_candles' section missing."

    total_db_marked = 0
    total_liq_marked = 0

    self_apprehend_cfg = db_section.get("apprehend_directional_bias_candles", {})
    self_label_cfg = self_apprehend_cfg.get("label", {}) if self_apprehend_cfg else {}
    self_db_text = self_label_cfg.get("directional_bias_candles_text", "DB2")
    self_label_at = self_label_cfg.get("label_at", {})
    
    self_up_obj, self_up_dbl = resolve_marker(self_label_at.get("upward_directional_bias_marker"))
    self_dn_obj, self_dn_dbl = resolve_marker(self_label_at.get("downward_directional_bias_marker"))
    self_up_pos = self_label_at.get("upward_directional_bias", "high").lower()
    self_dn_pos = self_label_at.get("downward_directional_bias", "high").lower()
    has_self_apprehend = bool(self_apprehend_cfg)

    for apprehend_key, apprehend_cfg in db_section.items():
        if not isinstance(apprehend_cfg, dict) or apprehend_key == "apprehend_directional_bias_candles":
            continue 

        log(f"Processing directional bias apprehend: '{apprehend_key}'")

        target_type = apprehend_cfg.get("target", "").lower()
        label_cfg = apprehend_cfg.get("label", {})
        db_text   = label_cfg.get("directional_bias_candles_text", "DB")
        label_at  = label_cfg.get("label_at", {})
        up_obj, up_dbl = resolve_marker(label_at.get("upward_directional_bias_marker"))
        dn_obj, dn_dbl = resolve_marker(label_at.get("downward_directional_bias_marker"))
        up_pos = label_at.get("upward_directional_bias", "high").lower()
        dn_pos = label_at.get("downward_directional_bias", "high").lower()

        # source_config_name is the key in config.json (e.g., "value")
        source_config_name = apprehend_key.replace("apprehend_", "")
        source_config = define_candles.get(source_config_name)
        if not source_config: continue

        bars = source_config.get("BARS", 101)
        filename = source_config.get("filename", "output.json")
        
        is_hlll = "higherhighsandlowerlows" in source_config_name.lower()
        is_llhl = "lowerhighsandhigherlows"   in source_config_name.lower()

        for sym in sorted(os.listdir(base_folder)):
            sym_p = os.path.join(base_folder, sym)
            if not os.path.isdir(sym_p): continue

            for tf in sorted(os.listdir(sym_p)):
                dev_output_dir = os.path.join(os.path.abspath(os.path.join(base_folder, "..", "developers", broker_name)), sym, tf)
                config_json_path = os.path.join(dev_output_dir, "config.json")
                
                # Check if the config file exists
                if not os.path.exists(config_json_path):
                    continue

                paths = get_analysis_paths(base_folder, broker_name, sym, tf, "new_old", bars, filename)
                
                # We still need the chart and source_json (candle prices) to process logic
                if not os.path.exists(paths.get("source_json")) or not os.path.exists(paths.get("source_chart")):
                    continue

                try:
                    # 1. Load Price Data (for calculations)
                    with open(paths["source_json"], 'r', encoding='utf-8') as f:
                        full_data = sorted(json.load(f), key=lambda x: x.get('candle_number', 0))
                    
                    # 2. Load the developer config.json (The actual Target)
                    with open(config_json_path, 'r', encoding='utf-8') as f:
                        local_config = json.load(f)

                    # Get the list data (e.g., local_config["value"])
                    input_structures = local_config.get(source_config_name, [])
                    if not input_structures:
                        continue

                    # 3. Setup CV2 Images
                    clean_img  = cv2.imread(paths["source_chart"])
                    marked_img = cv2.imread(paths["output_chart"]) if os.path.exists(paths["output_chart"]) else clean_img.copy()
                    if clean_img is None: continue

                    hsv = cv2.cvtColor(clean_img, cv2.COLOR_BGR2HSV)
                    mask = cv2.inRange(hsv, (35,50,50), (85,255,255)) | cv2.inRange(hsv, (0,50,50),(10,255,255))
                    raw_contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
                    contours = sorted(raw_contours, key=lambda c: cv2.boundingRect(c)[0])
                    n_candles = len(full_data)

                    final_flat_list = []
                    for structure in input_structures:
                        # Extract the base Marker
                        marker_candle = {k: v for k, v in structure.items() if k not in ["contour_maker", "directional_bias"]}
                        final_flat_list.append(marker_candle)

                        reference_idx = None
                        reference_high = None
                        reference_low = None
                        active_color = tuple(structure.get("active_color", [0,255,0]))

                        # Process Contour Maker
                        if (is_hlll or is_llhl) and target_type == "contourmaker":
                            cm_data = structure.get("contour_maker")
                            if cm_data:
                                cm_only = {k: v for k, v in cm_data.items() if k != "contour_maker_liquidity_candle"}
                                cm_only["is_contour_maker"] = True
                                final_flat_list.append(cm_only)
                                reference_idx = structure.get("m_idx")
                                reference_high, reference_low = cm_data["high"], cm_data["low"]

                        if reference_idx is None or reference_idx >= n_candles:
                            continue

                        # Process Level 1 Directional Bias
                        first_db_info = None
                        for k in range(reference_idx + 1, n_candles):
                            candle = full_data[k]
                            if candle['high'] < reference_low:
                                first_db_info = {**candle, "idx": k, "type": "downward", "level": 1, "is_directional_bias": True}
                                break
                            if candle['low'] > reference_high:
                                first_db_info = {**candle, "idx": k, "type": "upward", "level": 1, "is_directional_bias": True}
                                break

                        if first_db_info:
                            db_idx = first_db_info["idx"]
                            base_type = get_base_type(first_db_info["type"])
                            first_db_info["base_type"] = base_type
                            final_flat_list.append(first_db_info)

                            # Process Contour Maker Liquidity Sweep
                            for l_idx in range(db_idx + 1, n_candles):
                                l_candle = full_data[l_idx]
                                if (base_type == "support" and l_candle['low'] < reference_low) or \
                                   (base_type == "resistance" and l_candle['high'] > reference_high):
                                    liq_obj = {**l_candle, "idx": l_idx, "is_contour_maker_liquidity": True}
                                    final_flat_list.append(liq_obj)
                                    total_liq_marked += 1
                                    break

                            # Visual Marking
                            x, y, w, h = cv2.boundingRect(contours[db_idx])
                            is_up = first_db_info["type"] == "upward"
                            label_objects_and_text(
                                img=marked_img, cx=x + w // 2, y_rect=y, h_rect=h,
                                custom_text=db_text, object_type=up_obj if is_up else dn_obj,
                                is_bullish_arrow=is_up, is_marked=True,
                                double_arrow=up_dbl if is_up else dn_dbl,
                                arrow_color=active_color, label_position=up_pos if is_up else dn_pos
                            )
                            total_db_marked += 1

                            # Process Level 2 Bias (Self Apprehend)
                            if has_self_apprehend and db_idx + 1 < n_candles:
                                s_ref_h, s_ref_l = first_db_info["high"], first_db_info["low"]
                                second_db_info = None
                                for m in range(db_idx + 1, n_candles):
                                    c2 = full_data[m]
                                    if c2['high'] < s_ref_l:
                                        second_db_info = {**c2, "idx": m, "type": "downward", "level": 2, "is_next_bias_candle": True}
                                        break
                                    if c2['low'] > s_ref_h:
                                        second_db_info = {**c2, "idx": m, "type": "upward", "level": 2, "is_next_bias_candle": True}
                                        break
                                
                                if second_db_info:
                                    s_idx = second_db_info["idx"]
                                    next_base_type = get_base_type(second_db_info["type"])
                                    final_flat_list.append(second_db_info)

                                    for dl_idx in range(s_idx + 1, n_candles):
                                        dl_candle = full_data[dl_idx]
                                        if (next_base_type == "support" and dl_candle['low'] < s_ref_l) or \
                                           (next_base_type == "resistance" and dl_candle['high'] > s_ref_h):
                                            final_flat_list.append({**dl_candle, "idx": dl_idx, "directional_bias_liquidity_candle": True})
                                            total_liq_marked += 1
                                            break

                                    sx, sy, sw, sh = cv2.boundingRect(contours[s_idx])
                                    s_is_up = second_db_info["type"] == "upward"
                                    label_objects_and_text(
                                        img=marked_img, cx=sx + sw // 2, y_rect=sy, h_rect=sh,
                                        custom_text=self_db_text, object_type=self_up_obj if s_is_up else self_dn_obj,
                                        is_bullish_arrow=s_is_up, is_marked=True,
                                        double_arrow=self_up_dbl if s_is_up else self_dn_dbl,
                                        arrow_color=active_color, label_position=self_up_pos if s_is_up else self_dn_pos
                                    )
                                    total_db_marked += 1

                    # Save visual chart
                    cv2.imwrite(paths["output_chart"], marked_img)
                    
                    # Update ONLY the specific key in local_config
                    local_config[source_config_name] = final_flat_list
                    # Optional: keep price data for other functions to use
                    local_config[f"{source_config_name}_candle_list"] = full_data
                    
                    with open(config_json_path, 'w', encoding='utf-8') as f:
                        json.dump(local_config, f, indent=4)
                    
                    log(f"Successfully finalized {sym} {tf}")

                except Exception as e:
                    log(f"Error processing {sym}/{tf}: {e}", "ERROR")

    return f"Directional Bias Done. DB Markers: {total_db_marked}, Liq Sweeps: {total_liq_marked}"

def fair_value_gaps(broker_name):
    lagos_tz = pytz.timezone('Africa/Lagos')
    
    def log(msg, level="INFO"):
        ts = datetime.now(lagos_tz).strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{ts}] [{level}] {msg}")
    
    dev_dict = load_developers_dictionary()
    cfg = dev_dict.get(broker_name)
    if not cfg:
        return f"[{broker_name}] Error: Broker not in dictionary."
    
    base_folder = cfg.get("BASE_FOLDER")
    am_data = get_account_management(broker_name)
    if not am_data:
        return f"[{broker_name}] Error: accountmanagement.json missing."
    
    # === DYNAMIC SEARCH BY KEYWORD "fvg" IN SECTION NAME ===
    define_candles = am_data.get("chart", {}).get("define_candles", {})
    if not define_candles:
        return f"[{broker_name}] Error: 'define_candles' section missing in accountmanagement.json."

    keyword = "fvg"
    matching_configs = []

    for key, section in define_candles.items():
        if isinstance(section, dict) and keyword in key.lower():
            matching_configs.append((key, section))

    if not matching_configs:
        return f"[{broker_name}] Error: No section key containing '{keyword}' found in 'define_candles'."

    log(f"Found {len(matching_configs)} FVG configuration(s) matching keyword '{keyword}': {[k for k, _ in matching_configs]}")

    total_marked_all = 0
    processed_charts_all = 0

    for config_key, fvg_cfg in matching_configs:
        log(f"Processing FVG configuration: '{config_key}'")

        # Config extraction
        bars = fvg_cfg.get("BARS", 101)
        output_filename_base = fvg_cfg.get("filename", "fvg.json")
        direction = fvg_cfg.get("read_candles_from", "new_old")
        number_mode = fvg_cfg.get("number_candles", "all").lower()
        validate_filter = fvg_cfg.get("validate_my_condition", False)
        
        # Condition extraction
        conditions = fvg_cfg.get("condition", {})
        do_c1_check = (conditions.get("strong_c1") == "greater")
        do_strength_check = conditions.get("strong_fvg") in ["taller_body", "tallest_body"]
        do_c3_check = (conditions.get("strong_c3") == "greater")
        
        check_c1_type = (conditions.get("c1_candle_type") == "same_with_fvg")
        check_c3_type = (conditions.get("c3_candle_type") == "same_with_fvg")
        c3_closing_cfg = conditions.get("c3_closing")
        fvg_body_size_cfg = str(conditions.get("fvg_body_size", "")).strip().lower()
        
        # Body > Wick requirement toggle
        body_vs_wick_mode = str(conditions.get("c1_c3_higher_body_than_wicks", "")).strip().lower()
        apply_body_vs_wick_rule = (body_vs_wick_mode == "apply")
        
        # Lookback Logic
        raw_lookback = conditions.get("c1_lookback")
        if raw_lookback is None or str(raw_lookback).strip().lower() in ["", "0", "null", "none"]:
            c1_lookback = 0
        else:
            try:
                c1_lookback = min(int(raw_lookback), 5)
            except (ValueError, TypeError):
                c1_lookback = 0
        
        # Label & Color Logic
        label_cfg = fvg_cfg.get("label", {})
        bull_text = label_cfg.get("bullish_text", "+fvg")
        bear_text = label_cfg.get("bearish_text", "-fvg")
        label_at = label_cfg.get("label_at", {})
        
        color_map = {"green": (0, 255, 0), "red": (255, 0, 0)}  # BGR
        bullish_color = color_map.get(label_at.get("bullish_color", "green").lower(), (0, 255, 0))
        bearish_color = color_map.get(label_at.get("bearish_color", "red").lower(), (255, 0, 0))
        
        def resolve_marker(raw):
            raw = str(raw).lower().strip()
            if raw in ["arrow", "arrows", "singlearrow"]: return "arrow", False
            if raw in ["doublearrow", "doublearrows"]: return "arrow", True
            if raw in ["reverse_arrow", "reversearrow"]: return "reverse_arrow", False
            if raw in ["reverse_doublearrow", "reverse_doublearrows"]: return "reverse_arrow", True
            return raw, False
        
        bull_obj, bull_double = resolve_marker(label_at.get("bullish_marker", "arrow"))
        bear_obj, bear_double = resolve_marker(label_at.get("bearish_marker", "arrow"))
        
        number_all = (number_mode == "all")
        number_only_marked = number_mode in ["define_candles", "define_candle", "definecandle"]
        
        total_marked = 0
        processed_charts = 0

        log(f"Starting FVG Analysis with config '{config_key}' | Mode: Left-to-Right Only")

        for sym in sorted(os.listdir(base_folder)):
            sym_p = os.path.join(base_folder, sym)
            if not os.path.isdir(sym_p): continue
            
            for tf in sorted(os.listdir(sym_p)):
                tf_path = os.path.join(sym_p, tf)
                if not os.path.isdir(tf_path): continue

                paths = get_analysis_paths(base_folder, broker_name, sym, tf, direction, bars, output_filename_base)

                if not os.path.exists(paths["source_json"]) or not os.path.exists(paths["source_chart"]):
                    continue
                    
                try:
                    with open(paths["source_json"], 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    min_required = 3 + c1_lookback
                    if len(data) < min_required: continue
                    
                    data = sorted(data, key=lambda x: x.get('candle_number', 0))
                    
                    img = cv2.imread(paths["source_chart"])
                    if img is None: continue
                    
                    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
                    mask = cv2.inRange(hsv, (35, 50, 50), (85, 255, 255)) | cv2.inRange(hsv, (0, 50, 50), (10, 255, 255))
                    contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
                    contours = sorted(contours, key=lambda c: cv2.boundingRect(c)[0], reverse=False)
                    
                    marked_count = 0
                    # Storage for the FVG attributes to apply to candles 1, 2, and 3
                    potential_fvgs_map = {} 
                    c1_tags = {} # candle_num -> {fvg_c1: True, c1_for_fvg_number: X}
                    c3_tags = {} # candle_num -> {fvg_c3: True, c3_for_fvg_number: X}
                    
                    # --- Identify Potential FVGs ---
                    for i in range(1 + c1_lookback, len(data) - 1):
                        if i >= len(contours): break
                        
                        c_idx_1 = i - 1 - c1_lookback
                        c1, c2, c3 = data[c_idx_1], data[i], data[i+1]
                        
                        c1_u_wick = round(c1['high'] - max(c1['open'], c1['close']), 5)
                        c1_l_wick = round(min(c1['open'], c1['close']) - c1['low'], 5)
                        c1_total_wick = round(c1_u_wick + c1_l_wick, 5)
                        
                        c3_u_wick = round(c3['high'] - max(c3['open'], c3['close']), 5)
                        c3_l_wick = round(min(c3['open'], c3['close']) - c3['low'], 5)
                        c3_total_wick = round(c3_u_wick + c3_l_wick, 5)
                        
                        body1 = round(abs(c1['close'] - c1['open']), 5)
                        body2 = round(abs(c2['close'] - c2['open']), 5)
                        body3 = round(abs(c3['close'] - c3['open']), 5)
                        
                        height1 = round(c1['high'] - c1['low'], 5)
                        height2 = round(c2['high'] - c2['low'], 5)
                        height3 = round(c3['high'] - c3['low'], 5)
                        
                        fvg_type = None
                        gap_top, gap_bottom = 0, 0
                        
                        if c1['low'] > c3['high']:
                            fvg_type = "bearish"
                            gap_top, gap_bottom = c1['low'], c3['high']
                        elif c1['high'] < c3['low']:
                            fvg_type = "bullish"
                            gap_top, gap_bottom = c3['low'], c1['high']
                        
                        if fvg_type:
                            gap_size = round(abs(gap_top - gap_bottom), 5)
                            is_bullish_fvg_c = c2['close'] > c2['open']
                            
                            c1_type_match = ((fvg_type == "bullish" and c1['close'] > c1['open']) or 
                                            (fvg_type == "bearish" and c1['close'] < c1['open'])) if check_c1_type else True
                            c3_type_match = ((fvg_type == "bullish" and c3['close'] > c3['open']) or 
                                            (fvg_type == "bearish" and c3['close'] < c3['open'])) if check_c3_type else True
                            
                            c3_beyond_wick = (fvg_type == "bearish" and c3['close'] < c2['low']) or \
                                             (fvg_type == "bullish" and c3['close'] > c2['high'])
                            c3_beyond_close = (fvg_type == "bearish" and c3['close'] < c2['close']) or \
                                              (fvg_type == "bullish" and c3['close'] > c2['close'])
                            
                            c3_closing_match = True
                            if c3_closing_cfg == "beyond_wick":    c3_closing_match = c3_beyond_wick
                            elif c3_closing_cfg == "beyond_close": c3_closing_match = c3_beyond_close
                            
                            is_strong_c1 = ((fvg_type == "bearish" and c1['high'] > c2['high']) or 
                                           (fvg_type == "bullish" and c1['low'] < c2['low'])) if do_c1_check else True
                            is_strong_c3 = ((fvg_type == "bearish" and c3['low'] < c2['low']) or 
                                           (fvg_type == "bullish" and c3['high'] > c2['high'])) if do_c3_check else True
                            
                            excess_c1 = round(body2 - body1, 5)
                            excess_c3 = round(body2 - body3, 5)
                            is_strong_fvg = (excess_c1 > 0 and excess_c3 > 0) if do_strength_check else True
                            
                            fvg_durability = True
                            if fvg_body_size_cfg == "sum_c1_c3_body":
                                fvg_durability = (body1 + body3) <= body2
                            elif fvg_body_size_cfg == "sum_c1_c3_height":
                                fvg_durability = (height1 + height3) <= height2
                            elif fvg_body_size_cfg == "multiply_c1_body_by_2":
                                fvg_durability = (body1 * 2) <= body2
                            elif fvg_body_size_cfg == "multiply_c3_body_by_2":
                                fvg_durability = (body3 * 2) <= body2
                            elif fvg_body_size_cfg == "multiply_c1_height_by_2":
                                fvg_durability = (height1 * 2) <= body2
                            elif fvg_body_size_cfg == "multiply_c3_height_by_2":
                                fvg_durability = (height3 * 2) <= body2
                            
                            meets_all = all([
                                c1_type_match, c3_type_match, c3_closing_match,
                                is_strong_c1, is_strong_c3, is_strong_fvg, fvg_durability
                            ])
                            
                            if not validate_filter or (validate_filter and meets_all):
                                # Candle 2 (The FVG) Data
                                enriched_c2 = c2.copy()
                                enriched_c2.update({
                                    "fvg_type": fvg_type,
                                    "fvg_gap_size": gap_size,
                                    "fvg_gap_top": round(gap_top, 5),
                                    "fvg_gap_bottom": round(gap_bottom, 5),
                                    "c1_lookback_used": c1_lookback,
                                    "c1_body_size": body1,
                                    "c2_body_size": body2,
                                    "c3_body_size": body3,
                                    "c1_height": height1,
                                    "c2_height": height2,
                                    "c3_height": height3,
                                    "c1_upper_and_lower_wick": c1_total_wick,
                                    "c1_upper_and_lower_wick_against_body": round(body1 - c1_total_wick, 5),
                                    "c1_upper_and_lower_wick_higher": c1_total_wick > body1,
                                    "c1_body_higher": body1 >= c1_total_wick,
                                    "c3_upper_and_lower_wick": c3_total_wick,
                                    "c3_upper_and_lower_wick_against_body": round(body3 - c3_total_wick, 5),
                                    "c3_upper_and_lower_wick_higher": c3_total_wick > body3,
                                    "c3_body_higher": body3 >= c3_total_wick,
                                    "fvg_body_size_durability": fvg_durability,
                                    "c1_candletype_withfvg": c1_type_match,
                                    "c3_candletype_withfvg": c3_type_match,
                                    "c3_beyond_wick": c3_beyond_wick,
                                    "is_strong_c1": is_strong_c1,
                                    "is_strong_c3": is_strong_c3,
                                    "is_strong_fvg": is_strong_fvg,
                                    "c2_body_excess_against_c1": excess_c1,
                                    "c2_body_excess_against_c3": excess_c3,
                                    "meets_all_conditions": meets_all,
                                    "is_fvg": True,
                                    "_contour_idx": i,
                                    "_is_bull_c2": is_bullish_fvg_c
                                })
                                potential_fvgs_map[c2.get('candle_number')] = enriched_c2
                                
                                # Tag Candle 1 and Candle 3 for later list construction
                                c1_tags[c1.get('candle_number')] = {"fvg_c1": True, "c1_for_fvg_number": c2.get('candle_number')}
                                c3_tags[c3.get('candle_number')] = {"fvg_c3": True, "c3_for_fvg_number": c2.get('candle_number')}

                    # --- Build Final JSON (Flat List Structure) ---
                    fvg_results = []
                    max_gap_found = max([p["fvg_gap_size"] for p in potential_fvgs_map.values()], default=0)
                    
                    for idx, candle in enumerate(data):
                        fvg_swing_type = candle.get('candle_number')
                        
                        # Apply contour/coordinate data to every candle
                        if idx < len(contours):
                            x_c, y_c, w_c, h_c = cv2.boundingRect(contours[idx])
                            candle.update({
                                "candle_x": x_c + w_c // 2, "candle_y": y_c, "candle_width": w_c, "candle_height": h_c,
                                "candle_left": x_c, "candle_right": x_c + w_c, "candle_top": y_c, "candle_bottom": y_c + h_c,
                                "draw_x": x_c + w_c // 2, "draw_y": y_c, "draw_w": w_c, "draw_h": h_c,
                                "draw_left": x_c, "draw_right": x_c + w_c, "draw_top": y_c, "draw_bottom": y_c + h_c
                            })

                        # If this candle is C2 (The FVG)
                        if fvg_swing_type in potential_fvgs_map:
                            entry = potential_fvgs_map[fvg_swing_type]
                            # Sync coordinates from the base candle to the enriched one
                            coord_keys = ["candle_x", "candle_y", "candle_width", "candle_height", "candle_left", "candle_right", "candle_top", "candle_bottom", "draw_x", "draw_y", "draw_w", "draw_h", "draw_left", "draw_right", "draw_top", "draw_bottom"]
                            entry.update({k: candle[k] for k in coord_keys if k in candle})

                            is_tallest = (entry["fvg_gap_size"] == max_gap_found)
                            wick_compromised = (entry["c1_upper_and_lower_wick_higher"] or entry["c3_upper_and_lower_wick_higher"])
                            entry["constestant_fvg_chosed"] = is_tallest and (wick_compromised if apply_body_vs_wick_rule else True)
                            
                            body_condition_ok = ((entry["c1_body_higher"] and entry["c3_body_higher"]) or entry["constestant_fvg_chosed"]) if apply_body_vs_wick_rule else True
                            
                            if body_condition_ok:
                                c_idx = entry["_contour_idx"]
                                x_rect, y_rect, w_rect, h_rect = cv2.boundingRect(contours[c_idx])
                                label_objects_and_text(
                                    img=img, cx=x_rect + w_rect // 2, y_rect=y_rect, h_rect=h_rect,
                                    fvg_swing_type=fvg_swing_type if (number_all or number_only_marked) else None,
                                    custom_text=bull_text if entry["fvg_type"] == "bullish" else bear_text,
                                    object_type=bull_obj if entry["fvg_type"] == "bullish" else bear_obj,
                                    is_bullish_arrow=entry["_is_bull_c2"], is_marked=True,
                                    double_arrow=bull_double if entry["fvg_type"] == "bullish" else bear_double,
                                    arrow_color=bullish_color if entry["_is_bull_c2"] else bearish_color,
                                    label_position="low" if entry["_is_bull_c2"] else "high"
                                )
                                final_entry = {k: v for k, v in entry.items() if not k.startswith("_") and k != "fvg_gap_size"}
                                fvg_results.append(final_entry)
                                marked_count += 1
                                continue 
                        
                        # If this candle is C1 or C3 for an FVG, apply the tags
                        if fvg_swing_type in c1_tags:
                            candle.update(c1_tags[fvg_swing_type])
                        if fvg_swing_type in c3_tags:
                            candle.update(c3_tags[fvg_swing_type])

                        fvg_results.append(candle)
                    
                    if marked_count > 0 or (number_all and len(data) > 0):
                        os.makedirs(paths["output_dir"], exist_ok=True)
                        cv2.imwrite(paths["output_chart"], img)
                        config_path = os.path.join(paths["output_dir"], "config.json")
                        try:
                            config_content = {}
                            if os.path.exists(config_path):
                                with open(config_path, 'r', encoding='utf-8') as f:
                                    try:
                                        config_content = json.load(f)
                                        if not isinstance(config_content, dict): config_content = {}
                                    except: config_content = {}
                            config_content[config_key] = fvg_results
                            with open(config_path, 'w', encoding='utf-8') as f:
                                json.dump(config_content, f, indent=4)
                        except Exception as e:
                            log(f"Config sync failed for {sym}/{tf}: {e}", "WARN")

                        processed_charts += 1
                        total_marked += marked_count
                        
                except Exception as e:
                    log(f"Error processing {sym}/{tf} with config '{config_key}': {e}", "ERROR")

        log(f"Completed config '{config_key}': FVGs marked: {total_marked} | Charts: {processed_charts}")
        total_marked_all += total_marked
        processed_charts_all += processed_charts

    return f"Done (all FVG configs). Total FVGs: {total_marked_all} | Total Charts: {processed_charts_all}"

def fvg_higherhighsandlowerlows(broker_name):
    lagos_tz = pytz.timezone('Africa/Lagos')
    
    def log(msg, level="INFO"):
        ts = datetime.now(lagos_tz).strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{ts}] [{level}] {msg}")

    dev_dict = load_developers_dictionary()
    cfg = dev_dict.get(broker_name)
    if not cfg:
        return f"[{broker_name}] Error: Broker not in dictionary."

    base_folder = cfg.get("BASE_FOLDER")
    am_data = get_account_management(broker_name)
    if not am_data:
        return f"[{broker_name}] Error: accountmanagement.json missing."

    # Find FVG configuration section(s)
    define_candles = am_data.get("chart", {}).get("define_candles", {})
    fvg_configs = [(k, v) for k, v in define_candles.items() if "fvg" in k.lower()]
    
    if not fvg_configs:
        return f"[{broker_name}] Warning: No FVG config found → cannot determine parameters."

    config_key, fvg_cfg = fvg_configs[0]
    log(f"Using FVG config section: {config_key} for swing detection parameters")

    direction = fvg_cfg.get("read_candles_from", "new_old")
    bars = fvg_cfg.get("BARS", 301)
    output_filename_base = fvg_cfg.get("filename", "fvg.json")

    NEIGHBOR_LEFT = fvg_cfg.get("NEIGHBOR_LEFT", 5)
    NEIGHBOR_RIGHT = fvg_cfg.get("NEIGHBOR_RIGHT", 5)

    HH_TEXT = "fvg-HH"
    ll_TEXT = "fvg-ll"
    CM_TEXT = "m"  
    color_map = {"green": (0, 255, 0), "red": (255, 0, 0)}
    HH_COLOR = color_map["red"]
    ll_COLOR = color_map["green"]

    total_swings_added = 0
    processed_charts = 0

    for sym in sorted(os.listdir(base_folder)):
        sym_p = os.path.join(base_folder, sym)
        if not os.path.isdir(sym_p): continue

        for tf in sorted(os.listdir(sym_p)):
            tf_path = os.path.join(sym_p, tf)
            if not os.path.isdir(tf_path): continue

            paths = get_analysis_paths(base_folder, broker_name, sym, tf, direction, bars, output_filename_base)
            config_path = os.path.join(paths["output_dir"], "config.json")
            chart_path = paths["output_chart"]

            if not os.path.exists(config_path) or not os.path.exists(chart_path):
                continue

            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config_content = json.load(f)

                data = config_content.get(config_key)
                if not data or not isinstance(data, list):
                    continue

                data = sorted(data, key=lambda x: x.get('candle_number', 0))
                img = cv2.imread(chart_path)
                if img is None: continue

                n = len(data)
                min_req = NEIGHBOR_LEFT + NEIGHBOR_RIGHT + 1
                if n < min_req: continue

                swing_count = 0
                swings_found = []

                # --- STEP 1: DETECT SWINGS & MARK CONTOUR MAKERS ---
                for i in range(NEIGHBOR_LEFT, n - NEIGHBOR_RIGHT):
                    curr_h, curr_l = data[i]['high'], data[i]['low']
                    left_highs = [d['high'] for d in data[i - NEIGHBOR_LEFT:i]]
                    right_highs = [d['high'] for d in data[i + 1:i + 1 + NEIGHBOR_RIGHT]]
                    left_lows  = [d['low']  for d in data[i - NEIGHBOR_LEFT:i]]
                    right_lows  = [d['low']  for d in data[i + 1:i + 1 + NEIGHBOR_RIGHT]]

                    is_hh = curr_h > max(left_highs) and curr_h > max(right_highs)
                    is_ll = curr_l < min(left_lows)  and curr_l < min(right_lows)

                    if not (is_hh or is_ll): continue

                    swing_count += 1
                    is_bullish_swing = is_ll
                    swing_color = ll_COLOR if is_bullish_swing else HH_COLOR
                    
                    label_objects_and_text(
                        img=img, cx=data[i].get("candle_x"), y_rect=data[i].get("candle_y"), h_rect=data[i].get("candle_height"),
                        fvg_swing_type=data[i].get('candle_number'),
                        custom_text=ll_TEXT if is_bullish_swing else HH_TEXT, object_type="arrow",
                        is_bullish_arrow=is_bullish_swing, is_marked=True,
                        double_arrow=False, arrow_color=swing_color,
                        label_position="low" if is_bullish_swing else "high"
                    )

                    m_idx = i + NEIGHBOR_RIGHT
                    contour_maker_data = None
                    if m_idx < n:
                        label_objects_and_text(
                            img=img, cx=data[m_idx].get("candle_x"), y_rect=data[m_idx].get("candle_y"), h_rect=data[m_idx].get("candle_height"),
                            custom_text=CM_TEXT, object_type="dot",
                            is_bullish_arrow=is_bullish_swing, is_marked=True,
                            double_arrow=False, arrow_color=swing_color,
                            label_position="low" if is_bullish_swing else "high"
                        )
                        
                        data[m_idx].update({"is_contour_maker": True, "contour_maker_for_swing_candle": data[i]['candle_number']})
                        contour_maker_data = {
                            "candle_number": data[m_idx]['candle_number'],
                            "candle_x": data[m_idx].get("candle_x"),
                            "candle_y": data[m_idx].get("candle_y"),
                            "data_index": m_idx
                        }

                    swing_entry = {
                        "candle_number": data[i].get('candle_number'),
                        "swing_type": "higher_low" if is_bullish_swing else "higher_high",
                        "high": curr_h,
                        "low": curr_l
                    }
                    data[i].update({
                        "swing_type": swing_entry["swing_type"],
                        "is_swing": True,
                        "swing_color_bgr": [int(c) for c in swing_color],
                        "m_idx": m_idx if m_idx < n else None,
                        "contour_maker": contour_maker_data
                    })
                    swings_found.append(swing_entry)

                # --- STEP 2: ASSIGN BASE TYPE (SUPPORT/RESISTANCE) ---
                for candle in data:
                    ref_price = candle.get("low")
                    new_flag = None
                    for s in swings_found:
                        if s["swing_type"] == "higher_low" and s["low"] > ref_price:
                            new_flag = "support"
                            break
                        elif s["swing_type"] == "higher_high" and s["high"] < ref_price:
                            new_flag = "resistance"
                            break
                    
                    if new_flag:
                        if candle.get("fvg_c1") is True: candle["fvg_c1_base"] = new_flag
                        if candle.get("is_fvg") is True: candle["fvg_base"] = new_flag
                        if candle.get("fvg_c3") is True: candle["fvg_c3_base"] = new_flag

                # --- STEP 3: DEDUPLICATE ---
                unique_data_dict = {}
                for entry in data:
                    c_num = entry.get("candle_number")
                    if c_num not in unique_data_dict: unique_data_dict[c_num] = entry
                    else:
                        for key, val in entry.items():
                            if key not in unique_data_dict[c_num] or unique_data_dict[c_num][key] is None:
                                unique_data_dict[c_num][key] = val
                data = sorted(unique_data_dict.values(), key=lambda x: x.get('candle_number', 0))

                # --- STEP 4: ASSOCIATE SWINGS AND FIND LIQUIDITY ---
                for i in range(2, len(data)):
                    if data[i].get("fvg_c3") is True:
                        c1, c2, c3 = data[i-2], data[i-1], data[i]
                        family_str = f"{c1['candle_number']}, {c2['candle_number']}, {c3['candle_number']}"
                        
                        target_swing_idx = -1
                        cm_idx = -1
                        for j in range(i + 1, len(data)):
                            if data[j].get("is_swing"):
                                data[j]["swing_type_for_fvgf_number"] = family_str
                                target_swing_idx = j
                                cm_idx = data[j].get("m_idx", -1)
                                break
                        
                        if target_swing_idx != -1 and cm_idx != -1:
                            target_candles = [("c1", c1), ("c2", c2), ("c3", c3)]
                            
                            for label, triad_candle in target_candles:
                                triad_h = triad_candle['high']
                                triad_l = triad_candle['low']
                                found_h_liq = False
                                found_l_liq = False
                                
                                for k in range(cm_idx + 1, len(data)):
                                    liq_cand = data[k]
                                    l_open = liq_cand['open']
                                    l_high = liq_cand['high']
                                    l_low  = liq_cand['low']
                                    l_num  = liq_cand['candle_number']
                                    
                                    # --- High Liquidation Check ---
                                    if not found_h_liq:
                                        if l_open > triad_h:
                                            if l_low < triad_h: found_h_liq = True
                                        else:
                                            if l_high > triad_h: found_h_liq = True
                                        
                                        if found_h_liq:
                                            triad_candle[f"fvg_{label}_high_liquidated_by_candle_number"] = l_num
                                            liq_cand[f"liquidates_fvg_{label}_high"] = True

                                    # --- Low Liquidation Check ---
                                    if not found_l_liq:
                                        if l_open < triad_l:
                                            if l_high > triad_l: found_l_liq = True
                                        else:
                                            if l_low < triad_l: found_l_liq = True
                                        
                                        if found_l_liq:
                                            triad_candle[f"fvg_{label}_low_liquidated_by_candle_number"] = l_num
                                            liq_cand[f"liquidates_fvg_{label}_low"] = True

                                    if found_h_liq and found_l_liq: 
                                        break

                if swing_count > 0:
                    cv2.imwrite(chart_path, img)
                    config_content[config_key] = data
                    config_content[f"{config_key}_candle_list"] = data 
                    with open(config_path, 'w', encoding='utf-8') as f:
                        json.dump(config_content, f, indent=4)
                    processed_charts += 1
                    total_swings_added += swing_count

            except Exception as e:
                log(f"Error in {sym}/{tf}: {e}", "ERROR")

    return f"Finished. Total Swings: {total_swings_added}, Charts: {processed_charts}"

def timeframes_communication(broker_name):
    lagos_tz = pytz.timezone('Africa/Lagos')

    def log(msg, level="INFO"):
        ts = datetime.now(lagos_tz).strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{ts}] [{level}] {msg}")

    dev_dict = load_developers_dictionary()
    cfg = dev_dict.get(broker_name)
    if not cfg:
        return f"[{broker_name}] Error: Broker not in dictionary."

    base_folder = cfg.get("BASE_FOLDER")
    am_data = get_account_management(broker_name)
    if not am_data:
        return f"[{broker_name}] Error: accountmanagement.json missing."

    define_candles = am_data.get("chart", {}).get("define_candles", {})
    tf_comm_section = define_candles.get("timeframes_communication", {})

    if not tf_comm_section:
        log("No 'timeframes_communication' section found.", "WARN")
        return f"[{broker_name}] No timeframes_communication section."

    total_marked_all = 0
    tf_normalize = {
        "1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m", "1h": "1h", "4h": "4h",
        "m1": "1m", "m5": "5m", "m15": "15m", "m30": "30m", "h1": "1h", "h4": "4h"
    }

    custom_style = mpf.make_mpf_style(
        base_mpl_style="default",
        marketcolors=mpf.make_marketcolors(
            up="green", down="red", edge="inherit",
            wick={"up": "green", "down": "red"}
        ),
        gridstyle="",
        gridcolor="none",
        rc={'axes.grid': False, 'figure.facecolor': 'white', 'axes.facecolor': 'white'}
    )

    for apprehend_key, comm_cfg in tf_comm_section.items():
        if not isinstance(comm_cfg, dict) or not apprehend_key.startswith("apprehend_"):
            continue

        log(f"Processing communication strategy: '{apprehend_key}'")

        source_config_name = apprehend_key.replace("apprehend_", "")
        source_config = define_candles.get(source_config_name)
        if not source_config:
            log(f"Source config '{source_config_name}' not found.", "ERROR")
            continue

        sender_raw = comm_cfg.get("timeframe_sender", "").strip()
        receiver_raw = comm_cfg.get("timeframe_receiver", "").strip()
        sender_tfs = [tf_normalize.get(s.strip().lower(), s.strip().lower()) for s in sender_raw.split(",") if s.strip()]
        receiver_tfs = [tf_normalize.get(r.strip().lower(), r.strip().lower()) for r in receiver_raw.split(",") if r.strip()]

        raw_targets = comm_cfg.get("target", "")
        targets = [t.strip().lower() for t in raw_targets.split(",") if t.strip()]
        
        source_filename = source_config.get("filename", "output.json")
        base_output_name = source_filename.replace(".json", "")
        bars = source_config.get("BARS", 101)

        for sym in sorted(os.listdir(base_folder)):
            sym_path = os.path.join(base_folder, sym)
            if not os.path.isdir(sym_path): continue

            for sender_tf, receiver_tf in zip(sender_tfs, receiver_tfs):
                log(f"{sym}: Processing {sender_tf} → {receiver_tf}")

                sender_tf_path = os.path.join(sym_path, sender_tf)
                receiver_tf_path = os.path.join(sym_path, receiver_tf)

                if not os.path.isdir(sender_tf_path) or not os.path.isdir(receiver_tf_path):
                    continue

                dev_output_dir = os.path.join(
                    os.path.abspath(os.path.join(base_folder, "..", "developers", broker_name)),
                    sym, sender_tf
                )
                config_json_path = os.path.join(dev_output_dir, "config.json")

                if not os.path.exists(config_json_path): continue

                try:
                    # 1. LOAD CONFIG
                    with open(config_json_path, 'r', encoding='utf-8') as f:
                        local_config = json.load(f)

                    structures = local_config.get(source_config_name, [])
                    if not structures: continue

                    # 2. LOAD RECEIVER CANDLES
                    receiver_full_json = os.path.join(receiver_tf_path, "candlesdetails", "newest_oldest.json")
                    with open(receiver_full_json, 'r', encoding='utf-8') as f:
                        all_receiver_candles = json.load(f)

                    df_full = pd.DataFrame(all_receiver_candles)
                    df_full["time"] = pd.to_datetime(df_full["time"])
                    df_full = df_full.set_index("time").sort_index()
                    candle_index_map = {ts: idx for idx, ts in enumerate(df_full.index)}

                    config_updated = False

                    for target in targets:
                        matched_times = []
                        for candle_obj in structures:
                            is_match = False
                            if target == "contourmaker" and candle_obj.get("is_contour_maker") is True:
                                is_match = True
                            elif target == "directional_bias" and candle_obj.get("is_directional_bias") is True:
                                is_match = True
                            elif target == "next_bias" and candle_obj.get("is_next_bias_candle") is True:
                                is_match = True
                            elif target == candle_obj.get("target_label"):
                                is_match = True

                            if is_match and "time" in candle_obj:
                                ref_time_dt = pd.to_datetime(candle_obj["time"])
                                if ref_time_dt in df_full.index:
                                    matched_times.append(ref_time_dt)

                        matched_times_sorted = sorted(set(matched_times))
                        used_suffixes = {}

                        for signal_time in matched_times_sorted:
                            c_idx_start = candle_index_map[signal_time]
                            df_chart = df_full.loc[signal_time:].iloc[:bars]

                            if len(df_chart) < 5: continue

                            # 3. CONSTRUCT UNIQUE KEY NAME
                            base_name = f"{receiver_tf}_{base_output_name}_{target}_{sender_tf}_{c_idx_start}"
                            suffix = ""
                            if base_name in used_suffixes:
                                used_suffixes[base_name] += 1
                                suffix = f"_{chr(96 + used_suffixes[base_name])}"
                            else:
                                used_suffixes[base_name] = 0
                            
                            final_key_name = base_name + suffix

                            # 4. PLOT IMAGE (PNG)
                            scatter_data = pd.Series([float('nan')] * len(df_chart), index=df_chart.index)
                            scatter_data.iloc[0] = df_chart.iloc[0]["high"] * 1.001
                            addplots = [mpf.make_addplot(scatter_data, type='scatter', markersize=300, marker='o', color='yellow', alpha=0.9)]

                            fig, _ = mpf.plot(df_chart, type='candle', style=custom_style, addplot=addplots, 
                                              returnfig=True, figsize=(28, 10), tight_layout=False)
                            fig.suptitle(f"{sym} ({receiver_tf}) | Key: {final_key_name}", fontsize=16, fontweight='bold', y=0.95)
                            
                            os.makedirs(dev_output_dir, exist_ok=True)
                            fig.savefig(os.path.join(dev_output_dir, f"{final_key_name}.png"), bbox_inches="tight", dpi=120)
                            plt.close(fig)

                            # 5. BUILD THE LIST OF CANDLES (this will be the direct value)
                            forward_candles = []

                            for _, r in df_chart.iterrows():
                                current_idx = candle_index_map[r.name]
                                
                                candle_obj = {
                                    "time": r.name.strftime('%Y-%m-%d %H:%M:%S'), 
                                    "open": float(r["open"]), 
                                    "high": float(r["high"]), 
                                    "low": float(r["low"]), 
                                    "close": float(r["close"]), 
                                    "timeframe": receiver_tf,
                                    "candle_number": current_idx
                                }
                                forward_candles.append(candle_obj)

                            # 6. SAVE THE LIST DIRECTLY UNDER THE KEY
                            local_config[final_key_name] = forward_candles

                            config_updated = True
                            total_marked_all += 1
                            log(f"tf Communication {final_key_name} Processed")

                    # 7. SAVE UPDATED CONFIG.JSON (once per symbol/timeframe pair)
                    if config_updated:
                        with open(config_json_path, 'w', encoding='utf-8') as f:
                            json.dump(local_config, f, indent=4)
                        log(f"Successfully updated config.json for {sym}/{sender_tf}")

                except Exception as e:
                    log(f"FATAL ERROR {sym} ({sender_tf}→{receiver_tf}): {str(e)}", "ERROR")

    return f"Done. Total entries added to config files: {total_marked_all}"

def receiver_comm_higher_highs_lower_lows(broker_name):
    lagos_tz = pytz.timezone('Africa/Lagos')
    
    def log(msg, level="INFO"):
        ts = datetime.now(lagos_tz).strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{ts}] [{level}] {msg}")

    dev_dict = load_developers_dictionary()
    cfg = dev_dict.get(broker_name)
    if not cfg:
        return f"[{broker_name}] Error: Broker not in dictionary."
    
    base_folder = cfg.get("BASE_FOLDER")
    am_data = get_account_management(broker_name)
    if not am_data:
        return f"[{broker_name}] Error: accountmanagement.json missing."
    
    define_candles = am_data.get("chart", {}).get("define_candles", {})
    tf_comm_section = define_candles.get("timeframes_communication", {})
    
    total_marked_all = 0
    processed_keys_all = 0

    def resolve_marker(raw):
        if not raw: return None, False
        raw = str(raw).lower().strip()
        if raw in ["arrow", "arrows", "singlearrow"]: return "arrow", False
        if raw in ["doublearrow", "doublearrows"]: return "arrow", True
        if raw in ["reverse_arrow", "reversearrow"]: return "reverse_arrow", False
        if raw in ["reverse_doublearrow", "reverse_doublearrows"]: return "reverse_arrow", True
        if raw in ["rightarrow", "right_arrow"]: return "rightarrow", False
        if raw in ["leftarrow", "left_arrow"]: return "leftarrow", False
        if "dot" in raw: return "dot", False
        return raw, False

    for apprehend_key, comm_cfg in tf_comm_section.items():
        if not apprehend_key.startswith("apprehend_"): 
            continue
        
        source_config_name = apprehend_key.replace("apprehend_", "")
        hlll_cfg = define_candles.get(source_config_name, {})
        if not hlll_cfg: 
            continue

        neighbor_left = hlll_cfg.get("NEIGHBOR_LEFT", 5)
        neighbor_right = hlll_cfg.get("NEIGHBOR_RIGHT", 5)
        source_filename = hlll_cfg.get("filename", "highers.json")
        bars = hlll_cfg.get("BARS", 101)
        direction = hlll_cfg.get("read_candles_from", "new_old")

        label_cfg = hlll_cfg.get("label", {})
        hh_text = label_cfg.get("higherhighs_text", "HH")
        ll_text = label_cfg.get("lowerlows_text", "ll")
        cm_text = label_cfg.get("contourmaker_text", "m")

        label_at = label_cfg.get("label_at", {})
        hh_pos = label_at.get("higher_highs", "high").lower()
        ll_pos = label_at.get("lower_lows", "low").lower()

        color_map = {"green": (0, 255, 0), "red": (255, 0, 0), "blue": (0, 0, 255)}
        hh_col = color_map.get(label_at.get("higher_highs_color", "red").lower(), (255, 0, 0))
        ll_col = color_map.get(label_at.get("lower_lows_color", "green").lower(), (0, 255, 0))

        hh_obj, hh_dbl = resolve_marker(label_at.get("higher_highs_marker", "arrow"))
        ll_obj, ll_dbl = resolve_marker(label_at.get("lower_lows_marker", "arrow"))
        hh_cm_obj, hh_cm_dbl = resolve_marker(label_at.get("higher_highs_contourmaker_marker", ""))
        ll_cm_obj, ll_cm_dbl = resolve_marker(label_at.get("lower_lows_contourmaker_marker", ""))

        sender_tfs_raw = comm_cfg.get("timeframe_sender", "")
        receiver_tfs_raw = comm_cfg.get("timeframe_receiver", "")
        
        sender_tfs = [s.strip().lower() for s in sender_tfs_raw.split(",") if s.strip()]
        receiver_tfs = [r.strip().lower() for r in receiver_tfs_raw.split(",") if r.strip()]

        for sym in sorted(os.listdir(base_folder)):
            sym_p = os.path.join(base_folder, sym)
            if not os.path.isdir(sym_p): continue

            for s_tf, r_tf in zip(sender_tfs, receiver_tfs):
                # 1. FIND THE CONFIG.JSON
                dev_output_dir = os.path.join(
                    os.path.abspath(os.path.join(base_folder, "..", "developers", broker_name)),
                    sym, s_tf
                )
                config_path = os.path.join(dev_output_dir, "config.json")
                
                if not os.path.exists(config_path):
                    continue

                try:
                    with open(config_path, 'r', encoding='utf-8') as f:
                        config_data = json.load(f)
                    
                    config_updated = False
                    # Pattern example: 5m_output_contourmaker_15m_45
                    search_pattern = f"{r_tf}_{source_filename.replace('.json','')}"

                    for key in list(config_data.keys()):
                        if not key.startswith(search_pattern):
                            continue
                        
                        # Now the value is directly a list of candles
                        raw_candles = config_data.get(key)
                        if not isinstance(raw_candles, list):
                            continue

                        # Sort ascending by candle_number (oldest → newest)
                        data = sorted(raw_candles, key=lambda x: x.get('candle_number', 0))
                        
                        png_path = os.path.join(dev_output_dir, f"{key}.png")
                        if not os.path.exists(png_path):
                            continue

                        img = cv2.imread(png_path)
                        if img is None: 
                            continue

                        # 2. IMAGE PROCESSING (Contour detection)
                        hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
                        mask = cv2.inRange(hsv, (35, 50, 50), (85, 255, 255)) | \
                               cv2.inRange(hsv, (0, 50, 50), (10, 255, 255))
                        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
                        
                        if not contours: 
                            continue
                        contours = sorted(contours, key=lambda c: cv2.boundingRect(c)[0])

                        min_len = min(len(data), len(contours))
                        data = data[:min_len]
                        contours = contours[:min_len]

                        # 3. RECORD COORDINATES
                        for i in range(min_len):
                            x, y, w, h = cv2.boundingRect(contours[i])
                            data[i].update({
                                "candle_x": int(x + w // 2),
                                "candle_y": int(y),
                                "candle_width": int(w),
                                "candle_height": int(h),
                                "candle_left": int(x),
                                "candle_right": int(x + w),
                                "candle_top": int(y),
                                "candle_bottom": int(y + h)
                            })

                        # 4. SWING DETECTION
                        modified = False
                        n = len(data)
                        for i in range(neighbor_left, n - neighbor_right):
                            curr_h, curr_l = data[i]['high'], data[i]['low']
                            
                            l_h = [d['high'] for d in data[i-neighbor_left : i]     if 'high' in d]
                            r_h = [d['high'] for d in data[i+1 : i+1+neighbor_right] if 'high' in d]
                            l_l = [d['low']  for d in data[i-neighbor_left : i]     if 'low'  in d]
                            r_l = [d['low']  for d in data[i+1 : i+1+neighbor_right] if 'low'  in d]

                            is_hh = curr_h > max(l_h) and curr_h > max(r_h) if l_h and r_h else False
                            is_ll = curr_l < min(l_l) and curr_l < min(r_l) if l_l and r_l else False

                            if not (is_hh or is_ll): 
                                continue

                            is_bull = is_ll
                            active_color = ll_col if is_bull else hh_col
                            label_text = ll_text if is_bull else hh_text
                            obj_type = ll_obj if is_bull else hh_obj
                            dbl_arrow = ll_dbl if is_bull else hh_dbl
                            pos = ll_pos if is_bull else hh_pos

                            # Draw on image
                            label_objects_and_text(
                                img, 
                                data[i]["candle_x"], 
                                data[i]["candle_y"], 
                                data[i]["candle_height"],
                                fvg_swing_type=data[i]['candle_number'],
                                custom_text=label_text, 
                                object_type=obj_type,
                                is_bullish_arrow=is_bull, 
                                is_marked=True,
                                double_arrow=dbl_arrow, 
                                arrow_color=active_color,
                                label_position=pos
                            )

                            data[i].update({
                                "swing_type": "lower_low" if is_bull else "higher_high",
                                "active_color": [int(c) for c in active_color],  # make serializable
                                "is_swing": True
                            })
                            
                            # Contour Maker logic
                            m_idx = i + neighbor_right
                            if m_idx < n:
                                data[m_idx]["is_contour_maker"] = True
                                label_objects_and_text(
                                    img, 
                                    data[m_idx]["candle_x"], 
                                    data[m_idx]["candle_y"], 
                                    data[m_idx]["candle_height"],
                                    custom_text=cm_text, 
                                    object_type=(ll_cm_obj if is_bull else hh_cm_obj),
                                    is_bullish_arrow=is_bull, 
                                    is_marked=True,
                                    double_arrow=(ll_cm_dbl if is_bull else hh_cm_dbl),
                                    arrow_color=active_color, 
                                    label_position=pos
                                )

                            modified = True
                            total_marked_all += 1

                        if modified:
                            cv2.imwrite(png_path, img)
                            # Write the list directly back under the key
                            config_data[key] = data
                            config_updated = True
                            processed_keys_all += 1
                            log(f"receiver {key} swing processed")

                    # 5. SAVE UPDATED CONFIG.JSON
                    if config_updated:
                        with open(config_path, 'w', encoding='utf-8') as f:
                            json.dump(config_data, f, indent=4)

                except Exception as e:
                    log(f"Error in {sym} / {s_tf}: {e}", "ERROR")

    return f"Done. Updated {processed_keys_all} keys in config files with {total_marked_all} swings."

def liquidity_candles(broker_name):
    lagos_tz = pytz.timezone('Africa/Lagos')
    
    def log(msg, level="INFO"):
        ts = datetime.now(lagos_tz).strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{ts}] [{level}] {msg}")

    def resolve_marker(raw):
        if not raw:
            return None, False
        raw = str(raw).lower().strip()
        if raw in ["arrow", "arrows", "singlearrow"]: return "arrow", False
        if raw in ["doublearrow", "doublearrows"]: return "arrow", True
        if raw in ["rightarrow", "right_arrow"]: return "rightarrow", False
        if raw in ["leftarrow", "left_arrow"]: return "leftarrow", False
        if "dot" in raw: return "dot", False
        return raw, False

    log(f"--- STARTING SPACE-BASED LIQUIDITY ANALYSIS: {broker_name} ---")

    dev_dict = load_developers_dictionary() 
    cfg = dev_dict.get(broker_name)
    if not cfg:
        return f"Error: Broker {broker_name} not in dictionary."
    
    base_folder = cfg.get("BASE_FOLDER")
    am_data = get_account_management(broker_name)
    if not am_data:
        return "Error: accountmanagement.json missing."

    define_candles = am_data.get("chart", {}).get("define_candles", {})
    liq_root = define_candles.get("liquidity_candle", {})
    
    total_liq_found = 0

    for apprehend_key, liq_cfg in liq_root.items():
        if not apprehend_key.startswith("apprehend_"): 
            continue
        
        source_def_name = apprehend_key.replace("apprehend_", "")
        source_def = define_candles.get(source_def_name, {})
        if not source_def: 
            continue

        raw_filename = source_def.get("filename", "")
        target_file_filter = raw_filename.replace(".json", "").lower()
        primary_png_name = raw_filename.replace(".json", ".png")

        apprentice_section = liq_cfg.get("liquidity_apprentice_candle", {})
        apprentice_cfg = apprentice_section.get("swing_types", {})
        
        is_bullish = any("higher" in k for k in apprentice_cfg.keys())
        swing_prefix = "higher" if is_bullish else "lower"

        sweeper_section = liq_cfg.get("liquidity_sweeper_candle", {})
        liq_label_at = sweeper_section.get("label_at", {})

        markers = {
            "liq_hh": resolve_marker(liq_label_at.get(f"{swing_prefix}_high_liquidity_candle_marker")),
            "liq_ll": resolve_marker(liq_label_at.get(f"{swing_prefix}_low_liquidity_candle_marker")),
            "liq_hh_txt": liq_label_at.get(f"{swing_prefix}_high_liquidity_candle_text", ""),
            "liq_ll_txt": liq_label_at.get(f"{swing_prefix}_low_liquidity_candle_text", ""),
            "app_hh": resolve_marker(apprentice_cfg.get("label_at", {}).get(f"swing_type_{swing_prefix}_high_marker")),
            "app_ll": resolve_marker(apprentice_cfg.get("label_at", {}).get(f"swing_type_{swing_prefix}_low_marker"))
        }

        for sym in sorted(os.listdir(base_folder)):
            sym_p = os.path.join(base_folder, sym)
            if not os.path.isdir(sym_p): 
                continue

            for tf in os.listdir(sym_p):
                tf_p = os.path.join(sym_p, tf)
                if not os.path.isdir(tf_p): 
                    continue
                
                dev_output_dir = os.path.join(
                    os.path.abspath(os.path.join(base_folder, "..", "developers", broker_name)), 
                    sym, 
                    tf
                )
                config_path = os.path.join(dev_output_dir, "config.json")

                if not os.path.exists(config_path): 
                    continue

                try:
                    with open(config_path, 'r', encoding='utf-8') as f:
                        config_data = json.load(f)

                    config_modified = False
                    
                    for file_key in list(config_data.keys()):
                        if not (file_key.lower() == source_def_name.lower() or 
                                target_file_filter in file_key.lower()):
                            continue

                        entry = config_data[file_key]

                        # ────────────────────────────────────────────────
                        # NEW: Only accept direct list of candles
                        # ────────────────────────────────────────────────
                        if not isinstance(entry, list):
                            log(f"Skipping {file_key} — value is not a list (expected direct array of candles)", "WARN")
                            continue

                        candles = entry

                        if len(candles) < 2:
                            log(f"Skipping {file_key} — fewer than 2 candles", "WARN")
                            continue

                        # Optional: basic validation that items look like candles
                        if not all(isinstance(c, dict) for c in candles):
                            log(f"Skipping {file_key} — not all items are dictionaries", "WARN")
                            continue
                        # ────────────────────────────────────────────────

                        png_path = os.path.join(dev_output_dir, f"{file_key}.png")
                        if not os.path.exists(png_path):
                            png_path = os.path.join(dev_output_dir, primary_png_name)

                        img = cv2.imread(png_path)
                        key_modified = False

                        for i in range(len(candles) - 1):
                            base_c = candles[i]
                            next_c = candles[i + 1]
                            
                            b_h = base_c.get("high")
                            b_l = base_c.get("low")
                            n_h = next_c.get("high")
                            n_l = next_c.get("low")
                            
                            if None in [b_h, b_l, n_h, n_l]: 
                                continue

                            target_side = None
                            ref_price = None

                            if n_h >= b_h and n_l <= b_l:
                                continue 
                            
                            elif n_h > b_h and n_l > b_l:
                                target_side = "low"
                                ref_price = b_l
                                
                            elif n_l < b_l and n_h < b_h:
                                target_side = "high"
                                ref_price = b_h
                            
                            else:
                                continue

                            for j in range(i + 2, len(candles)):
                                sweeper_c = candles[j]
                                swept = False

                                if target_side == "low" and sweeper_c.get("low", 999999) <= ref_price:
                                    swept = True
                                    pos = "low"
                                    m_key = "liq_ll"
                                    a_key = "app_ll"
                                elif target_side == "high" and sweeper_c.get("high", 0) >= ref_price:
                                    swept = True
                                    pos = "high"
                                    m_key = "liq_hh"
                                    a_key = "app_hh"

                                if swept:
                                    obj, dbl = markers[m_key]
                                    app_obj, app_dbl = markers[a_key]
                                    txt = markers.get(f"{m_key}_txt", "")

                                    sweeper_c.update({
                                        "is_liquidity_sweep": True, 
                                        "liquidity_price": ref_price
                                    })
                                    base_c.update({
                                        "swept_by_liquidity": True, 
                                        "swept_by_candle_number": sweeper_c.get("candle_number")
                                    })

                                    if img is not None:
                                        label_objects_and_text(img, 
                                            int(sweeper_c.get("candle_x", 0)), 
                                            int(sweeper_c.get("candle_y", 0)), 
                                            int(sweeper_c.get("candle_height", 0)),
                                            custom_text=txt, 
                                            object_type=obj, 
                                            is_bullish_arrow=(target_side == "low"),
                                            is_marked=True, 
                                            double_arrow=dbl, 
                                            arrow_color=(0, 255, 255), 
                                            label_position=pos)
                                        
                                        label_objects_and_text(img, 
                                            int(base_c.get("candle_x", 0)), 
                                            int(base_c.get("candle_y", 0)), 
                                            int(base_c.get("candle_height", 0)),
                                            custom_text="", 
                                            object_type=app_obj, 
                                            is_bullish_arrow=(target_side == "low"),
                                            is_marked=True, 
                                            double_arrow=app_dbl, 
                                            arrow_color=(255, 165, 0), 
                                            label_position=pos)

                                    key_modified = True
                                    config_modified = True
                                    total_liq_found += 1
                                    break 

                        if key_modified and img is not None:
                            cv2.imwrite(png_path, img)

                    if config_modified:
                        with open(config_path, 'w', encoding='utf-8') as f:
                            json.dump(config_data, f, indent=4)

                except Exception as e:
                    log(f"Error processing {sym} ({tf}): {e}", "ERROR")

    log(f"--- LIQUIDITY COMPLETE --- Total Space Sweeps: {total_liq_found}")
    return f"Completed: {total_liq_found} sweeps."

def entry_point_of_interest(broker_name):
    lagos_tz = pytz.timezone('Africa/Lagos') 
    
    
    def log(msg, level="INFO"):
        ts = datetime.now(lagos_tz).strftime('%H:%M:%S')
        print(f"[{ts}] {msg}")

    def get_max_candle_count(dev_base_path, timeframe):
        """Helper to find candle count based on maximum_holding_days config."""
        config_path = os.path.join(dev_base_path, "accountmanagement.json")
        max_days = 2  # Default fallback
        
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    max_days = config.get("chart", {}).get("maximum_holding_days", 2)
        except Exception:
            pass

        # Conversion map
        tf_map = {
            "1m": 1, "5m": 5, "10m": 10, "15m": 15, "30m": 30,
            "1h": 60, "4h": 240, "1d": 1440
        }
        
        mins_per_candle = tf_map.get(timeframe.lower(), 1)
        total_minutes_in_period = max_days * 24 * 60
        return total_minutes_in_period // mins_per_candle

    def mark_paused_symbols_in_full_candles(dev_base_path, new_folder_name):
        paused_folder = os.path.join(dev_base_path, new_folder_name, "paused_symbols_folder")
        paused_file = os.path.join(paused_folder, "paused_symbols.json")
        
        if not os.path.exists(paused_file):
            return

        try:
            with open(paused_file, 'r', encoding='utf-8') as f:
                paused_records = json.load(f)
        except Exception as e:
            log(f"Error reading paused symbols: {e}")
            return

        updated_paused_records = []
        records_removed = False
        markers_map = {}

        for record in paused_records:
            sym, tf = record.get("symbol"), record.get("timeframe")
            if sym and tf:
                markers_map.setdefault((sym, tf), []).append(record)

        for (sym, tf), records in markers_map.items():
            # Fetch the dynamic threshold for this timeframe
            max_allowed_count = get_max_candle_count(dev_base_path, tf)
            
            full_candle_path = os.path.join(dev_base_path, new_folder_name, sym, f"{tf}_full_candles_data.json")
            if not os.path.exists(full_candle_path):
                updated_paused_records.extend(records)
                continue

            try:
                with open(full_candle_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                if not isinstance(data, list) or not data:
                    updated_paused_records.extend(records)
                    continue

                candles = data[1:] if (len(data) > 0 and "summary" in data[0]) else data
                total_candles = len(candles)
                
                summary = {}
                current_tf_records_to_keep = []

                for rec in records:
                    from_time = rec.get("time")
                    after_data = rec.get("after", {})
                    after_time = after_data.get("time")
                    entry_val = rec.get("entry")
                    order_type = rec.get("order_type")

                    clean_from = from_time.replace(':', '-').replace(' ', '_')
                    clean_after = after_time.replace(':', '-').replace(' ', '_') if after_time else "N/A"
                    
                    should_remove_this_record = False
                    # Tracking variables for the summary
                    final_count_ahead = 0
                    final_remaining = 0

                    for idx, candle in enumerate(candles):
                        c_time = candle.get("time")

                        if c_time == from_time:
                            candle[f"from_{clean_from}"] = True
                            candle["entry"] = entry_val
                            candle["order_type"] = order_type

                        if after_time and c_time == after_time:
                            count_ahead = total_candles - (idx + 1)
                            remaining = max_allowed_count - count_ahead
                            
                            final_count_ahead = count_ahead
                            final_remaining = remaining

                            if count_ahead >= max_allowed_count:
                                should_remove_this_record = True
                                records_removed = True
                            
                            candle[f"after_{clean_after}"] = True
                            candle[f"connected_with_{clean_from}"] = True
                            candle["candles_count_ahead_after_candle"] = count_ahead
                            candle["remaining_candles_to_threshold"] = remaining

                    if not should_remove_this_record:
                        current_tf_records_to_keep.append(rec)
                        conn_idx = len(current_tf_records_to_keep)
                        summary[f"connection_{conn_idx}"] = {
                            f"from_{clean_from}": entry_val,
                            "order_type": order_type,
                            "after_time": after_time,
                            "candles_count_ahead_after_candle": final_count_ahead,
                            "remaining_candles_to_threshold": final_remaining
                        }

                # Save the updated candles and summary
                final_output = [{"summary": summary}] + candles
                with open(full_candle_path, 'w', encoding='utf-8') as f:
                    json.dump(final_output, f, indent=4)
                
                updated_paused_records.extend(current_tf_records_to_keep)

            except Exception as e:
                log(f"Error processing {sym} {tf}: {e}")
                updated_paused_records.extend(records)

        if records_removed:
            with open(paused_file, 'w', encoding='utf-8') as f:
                json.dump(updated_paused_records, f, indent=4)
    
    def cleanup_non_paused_symbols(dev_base_path, new_folder_name):
        """
        Deletes all symbol folders in the new_folder_name directory that are 
        NOT present in the paused_symbols.json file.
        """
        target_dir = os.path.join(dev_base_path, new_folder_name)
        paused_file = os.path.join(target_dir, "paused_symbols_folder", "paused_symbols.json")
        
        if not os.path.exists(target_dir):
            return

        # 1. Identify which symbols are paused
        paused_symbols = set()
        if os.path.exists(paused_file):
            try:
                with open(paused_file, 'r', encoding='utf-8') as f:
                    paused_records = json.load(f)
                    paused_symbols = {rec.get("symbol") for rec in paused_records if rec.get("symbol")}
            except Exception as e:
                log(f"Error reading paused symbols during cleanup: {e}")
                return

        # 2. Iterate through folders and delete if not in the paused list
        # We skip 'paused_symbols_folder' itself and any files (like logs)
        for item in os.listdir(target_dir):
            item_path = os.path.join(target_dir, item)
            
            # We only care about directories that represent symbols
            if os.path.isdir(item_path) and item != "paused_symbols_folder":
                if item not in paused_symbols:
                    try:
                        shutil.rmtree(item_path)
                        # log(f"Cleaned up non-paused symbol folder: {item}")
                    except Exception as e:
                        log(f"Failed to delete folder {item}: {e}")

    def identify_paused_symbols_poi(dev_base_path, new_folder_name):
        """
        Analyzes full_candles_data.json to find price violations (hitler candles)
        for paused records. Removes symbols from paused list when violation is found.
        """
        paused_folder = os.path.join(dev_base_path, new_folder_name, "paused_symbols_folder")
        paused_file = os.path.join(paused_folder, "paused_symbols.json")
        
        if not os.path.exists(paused_file):
            return

        try:
            with open(paused_file, 'r', encoding='utf-8') as f:
                paused_records = json.load(f)
        except Exception as e:
            log(f"Error reading paused symbols for POI: {e}")
            return

        # Group by symbol/tf to minimize file I/O
        markers_map = {}
        for record in paused_records:
            sym, tf = record.get("symbol"), record.get("timeframe")
            if sym and tf:
                markers_map.setdefault((sym, tf), []).append(record)

        updated_paused_records = []  # Will hold records that should remain paused
        records_removed = False

        for (sym, tf), records in markers_map.items():
            full_candle_path = os.path.join(dev_base_path, new_folder_name, sym, f"{tf}_full_candles_data.json")
            
            if not os.path.exists(full_candle_path):
                updated_paused_records.extend(records)
                continue

            try:
                with open(full_candle_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                if not isinstance(data, list) or len(data) < 2:
                    updated_paused_records.extend(records)
                    continue

                # Separate summary and candles
                summary_obj = data[0].get("summary", {})
                candles = data[1:]
                
                modified_summary = False
                records_to_keep_for_this_symbol = []  # Records that didn't trigger hitler

                # Process each connection defined in the summary
                for conn_key, conn_val in summary_obj.items():
                    # Extract 'after_time' and 'order_type' to determine logic
                    after_time = conn_val.get("after_time")
                    order_type = conn_val.get("order_type")
                    
                    # Find the 'from' key to get the entry price
                    from_key = next((k for k in conn_val.keys() if k.startswith("from_")), None)
                    if not from_key or not after_time:
                        records_to_keep_for_this_symbol.append(conn_val)  # Keep if incomplete data
                        continue
                    
                    entry_price = conn_val[from_key]
                    hitler_found = False

                    # Search for price violation after the 'after' candle
                    search_active = False
                    for candle in candles:
                        c_time = candle.get("time")
                        
                        if not search_active:
                            if c_time == after_time:
                                search_active = True
                            continue
                        
                        c_num = candle.get("candle_number", "unknown")
                        
                        if "buy" in order_type.lower():
                            low_val = candle.get("low")
                            if low_val is not None and low_val < entry_price:
                                label = f"hitlercandle{c_num}_ahead_after_candle_breaches_from_low_price_{entry_price}"
                                conn_val[label] = True
                                hitler_found = True
                                records_removed = True  # Mark for removal from paused list
                                break
                        
                        elif "sell" in order_type.lower():
                            high_val = candle.get("high")
                            if high_val is not None and high_val > entry_price:
                                label = f"hitlercandle{c_num}_ahead_after_candle_breaches_from_high_price_{entry_price}"
                                conn_val[label] = True
                                hitler_found = True
                                records_removed = True  # Mark for removal from paused list
                                break

                    if not hitler_found:
                        conn_val["no_hitler"] = True
                        # Find and keep the original paused record that matches this connection
                        matching_record = next(
                            (r for r in records if r.get("after", {}).get("time") == after_time),
                            None
                        )
                        if matching_record:
                            records_to_keep_for_this_symbol.append(matching_record)
                    
                    modified_summary = True

                # Add records that should remain paused to the global list
                updated_paused_records.extend(records_to_keep_for_this_symbol)

                # Save the updated candles with hitler markers
                if modified_summary:
                    data[0]["summary"] = summary_obj
                    with open(full_candle_path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=4)

            except Exception as e:
                log(f"Error in identify_paused_symbols_poi for {sym} {tf}: {e}")
                updated_paused_records.extend(records)  # Keep records on error

        # Update paused file if any records were removed
        if records_removed:
            with open(paused_file, 'w', encoding='utf-8') as f:
                json.dump(updated_paused_records, f, indent=4)
            log(f"Removed {len(paused_records) - len(updated_paused_records)} symbols from paused list due to price violations")

    def process_entry_newfilename(entry_settings, source_def_name, raw_filename_base, base_folder, dev_base_path):
        new_folder_name = entry_settings.get("new_filename")
        if not new_folder_name:
            return 0
        mark_paused_symbols_in_full_candles(dev_base_path, new_folder_name)

        identify_paused_symbols_poi(dev_base_path, new_folder_name)

        cleanup_non_paused_symbols(dev_base_path, new_folder_name)

        # 1. Clear previous limit orders before starting a new run
        limit_orders_old_record_cleanup(dev_base_path, new_folder_name)

        # 2. Identify which symbols should be skipped (Paused Symbols)
        paused_symbols_file = os.path.join(dev_base_path, new_folder_name, "paused_symbols_folder", "paused_symbols.json")
        paused_names = set()
        
        if os.path.exists(paused_symbols_file):
            try:
                with open(paused_symbols_file, 'r', encoding='utf-8') as f:
                    paused_list = json.load(f)
                    paused_names = {item.get("symbol") for item in paused_list if "symbol" in item}
            except Exception as e:
                log(f"Error loading paused symbols: {e}")

        process_receiver = str(entry_settings.get("process_receiver_files", "no")).lower()
        identify_config = entry_settings.get("identify_definitions", {})
        sync_count = 0

        # 3. Iterate through symbols in the base folder
        for sym in sorted(os.listdir(base_folder)):
            sym_p = os.path.join(base_folder, sym)
            
            if not os.path.isdir(sym_p) or sym in paused_names:
                continue

            target_sym_dir = os.path.join(dev_base_path, new_folder_name, sym)
            os.makedirs(target_sym_dir, exist_ok=True)
            
            target_config_path = os.path.join(target_sym_dir, "config.json")
            target_data = {}
            if os.path.exists(target_config_path):
                try:
                    with open(target_config_path, 'r', encoding='utf-8') as f:
                        target_data = json.load(f)
                except Exception:
                    target_data = {}

            modified = False
            pending_images = {}
            # List to store full candle data tasks to be written if the timeframe is kept
            pending_full_candle_data = {}

            # --- STEP 1: Process all timeframes for this symbol ---
            for tf in os.listdir(sym_p):
                tf_p = os.path.join(sym_p, tf)
                if not os.path.isdir(tf_p): 
                    continue
                
                source_dev_dir = os.path.join(dev_base_path, sym, tf)
                
                # --- NEW LOGIC: Handle full_candles_data.json ---
                source_full_candle_path = os.path.join(source_dev_dir, "full_candles_data.json")
                if os.path.exists(source_full_candle_path):
                    try:
                        with open(source_full_candle_path, 'r', encoding='utf-8') as f:
                            full_data_content = json.load(f)
                            # Queue it with the prefixed name
                            pending_full_candle_data[f"{tf}_full_candles_data.json"] = (tf, full_data_content)
                    except Exception as e:
                        log(f"Error reading full_candles_data for {sym} {tf}: {e}")

                source_config_path = os.path.join(source_dev_dir, "config.json")
                if not os.path.exists(source_config_path): 
                    continue

                with open(source_config_path, 'r', encoding='utf-8') as f:
                    src_data = json.load(f)

                if tf not in target_data:
                    target_data[tf] = {}

                for file_key, candles in src_data.items():
                    clean_key = file_key.lower()
                    if "candle_list" in clean_key or "candlelist" in clean_key: 
                        continue

                    is_primary = (clean_key == source_def_name.lower() or clean_key == raw_filename_base)
                    is_receiver = (not is_primary and raw_filename_base in clean_key)

                    if (is_receiver and process_receiver != "yes") or (not is_primary and not is_receiver):
                        continue

                    new_key = f"{new_folder_name}_{file_key}"
                    processed_candles = {}

                    if identify_config:
                        processed_candles = identify_definitions({file_key: candles}, identify_config, source_def_name, raw_filename_base)
                        if file_key in processed_candles:
                            updated_candles, extracted_patterns = apply_definition_conditions(processed_candles[file_key], identify_config, new_folder_name, file_key)
                            target_data[tf][new_key] = updated_candles
                            if extracted_patterns:
                                target_data[tf][f"{new_key}_patterns"] = extracted_patterns
                            modified = True

                        processed_candles = intruder_and_outlaw_check(processed_candles)
                        poi_config = entry_settings.get("point_of_interest")
                        if poi_config and file_key in processed_candles:
                            identify_poi(target_data[tf], new_key, candles, poi_config)
                            identify_poi_mitigation(target_data[tf], new_key, poi_config)
                            identify_swing_mitigation_between_definitions(target_data[tf], new_key, candles, poi_config)
                            identify_selected(target_data[tf], new_key, poi_config)

                    target_data[tf][file_key] = candles
                    if identify_config and file_key in processed_candles:
                        target_data[tf][new_key] = processed_candles[file_key]
                    modified = True

                    # Image Handling
                    src_png = os.path.join(source_dev_dir, f"{file_key}.png")
                    if not os.path.exists(src_png):
                        src_png = os.path.join(source_dev_dir, f"{raw_filename_base}.png")

                    if os.path.exists(src_png):
                        img = cv2.imread(src_png)
                        if img is not None:
                            if poi_config:
                                img = draw_poi_tools(img, target_data[tf], new_key, poi_config)
                                record_config = entry_settings.get("record_prices")
                                if record_config:
                                    identify_prices(target_data[tf], new_key, record_config, dev_base_path, new_folder_name)
                            
                            img_filename = f"{tf}_{file_key}.png"
                            pending_images[img_filename] = (tf, img)

            # --- STEP 2: Process Ticks JSON ---
            source_ticks_path = os.path.join(dev_base_path, sym, f"{sym}_ticks.json")
            if os.path.exists(source_ticks_path):
                target_ticks_path = os.path.join(target_sym_dir, f"{sym}_ticks.json")
                try:
                    with open(source_ticks_path, 'r', encoding='utf-8') as f:
                        ticks_data = json.load(f)
                    with open(target_ticks_path, 'w', encoding='utf-8') as f:
                        json.dump(ticks_data, f, indent=4)
                except Exception as e:
                    log(f"Error processing ticks for {sym}: {e}")
            
            enrich_limit_orders(dev_base_path, new_folder_name)

            # --- STEP 3: Sanitize and identify orders ---
            should_delete_folder, tfs_to_keep = sanitize_symbols_or_files(target_sym_dir, target_data)

            if should_delete_folder:
                if os.path.exists(target_sym_dir):
                    shutil.rmtree(target_sym_dir)
                continue 

            #

            identify_paused_symbols(target_data, dev_base_path, new_folder_name)

            populate_limit_orders_with_paused_orders(dev_base_path, new_folder_name)


            # --- STEP 4: Final Write (Images and Full Candle Data) ---
            # Write Images
            for img_name, (tf, img_data) in pending_images.items():
                if tf in tfs_to_keep:
                    cv2.imwrite(os.path.join(target_sym_dir, img_name), img_data)
                else:
                    full_path = os.path.join(target_sym_dir, img_name)
                    if os.path.exists(full_path): os.remove(full_path)

            # Write tf_full_candles_data.json files
            for json_name, (tf, content) in pending_full_candle_data.items():
                if tf in tfs_to_keep:
                    with open(os.path.join(target_sym_dir, json_name), 'w', encoding='utf-8') as f:
                        json.dump(content, f, indent=4)

            if modified:
                with open(target_config_path, 'w', encoding='utf-8') as f:
                    json.dump(target_data, f, indent=4)
                sync_count += 1

        return sync_count

    def identify_definitions(candle_data, identify_config, source_def_name, raw_filename_base):
        if not identify_config:
            return candle_data
            
        processed_data = candle_data.copy()
        
        # Ordinal mapping for naming convention
        ordinals = ["zero", "first", "second", "third", "fourth", "fifth", "sixth", 
                    "seventh", "eighth", "ninth", "tenth", "eleventh", "twelfth", "thirteenth", "fourteenth", "fifteenth", "sixteenth", "seventeenth", "eighteenth", "nineteenth", "twenteenth"]

        # Sort definitions to ensure sequential processing (define_1, define_2...)
        definitions = sorted([(k, v) for k, v in identify_config.items() 
                            if k.startswith("define_")], 
                            key=lambda x: int(x[0].split('_')[1]))
        
        if not definitions:
            return processed_data

        def get_target_swing(current_type, logic_type):
            logic_type = logic_type.lower()
            if "opposite" in logic_type:
                return "lower_low" if current_type == "higher_high" else "higher_high"
            if "identical" in logic_type:
                return current_type
            return None

        for file_key, candles in processed_data.items():
            if not isinstance(candles, list): continue
            
            # --- GLOBAL LOOP: Every swing candle gets a turn to be the 'Anchor' (define_1) ---
            for i, anchor_candle in enumerate(candles):
                if not (isinstance(anchor_candle, dict) and "swing_type" in anchor_candle):
                    continue
                    
                s_type = anchor_candle.get("swing_type", "").lower()
                if s_type not in ["higher_high", "lower_low"]:
                    continue

                # Step 1: Initialize the chain with define_1
                def1_name = definitions[0][0]
                anchor_candle[def1_name] = True
                anchor_candle[f"{def1_name}_swing_type"] = s_type
                anchor_idx = anchor_candle.get("candle_number", i)
                
                # chain_history tracks the 'firstfound' of each step to determine the NEXT step's start point
                # Format: { def_name: (index_in_list, candle_object) }
                chain_history = {def1_name: (i, anchor_candle)}

                # Step 2: Process subsequent definitions (The Chain)
                for def_idx in range(1, len(definitions)):
                    curr_def_name, curr_def_config = definitions[def_idx]
                    prev_def_name, _ = definitions[def_idx - 1]
                    
                    # Logic dictates we start searching AFTER the 'firstfound' of the previous definition
                    if prev_def_name not in chain_history:
                        break
                    
                    prev_idx_in_list, prev_candle_obj = chain_history[prev_def_name]
                    search_start_idx = prev_idx_in_list + 1
                    
                    prev_swing_type = prev_candle_obj.get(f"{prev_def_name}_swing_type", "")
                    target_swing = get_target_swing(prev_swing_type, curr_def_config.get("type", ""))
                    
                    if not target_swing:
                        continue

                    found_count_for_this_step = 0
                    
                    # Search forward for ALL matches
                    for j in range(search_start_idx, len(candles)):
                        target_candle = candles[j]
                        if not (isinstance(target_candle, dict) and target_candle.get("swing_type")):
                            continue
                        
                        if target_candle["swing_type"].lower() == target_swing:
                            found_count_for_this_step += 1
                            curr_candle_num = target_candle.get("candle_number", j)
                            ref_candle_num = prev_candle_obj.get("candle_number", prev_idx_in_list)
                            
                            # Mark Base Flags
                            target_candle[curr_def_name] = True
                            target_candle[f"{curr_def_name}_swing_type"] = target_swing
                            
                            # Determine Ordinal (firstfound, secondfound, etc.)
                            if found_count_for_this_step < len(ordinals):
                                ord_str = f"{ordinals[found_count_for_this_step]}found"
                            else:
                                ord_str = f"{found_count_for_this_step}thfound"
                            
                            # Construct Dynamic Key
                            # e.g., define_2_firstfound_4_in_connection_with_define_1_1
                            conn_key = f"{curr_def_name}_{ord_str}_{curr_candle_num}_in_connection_with_{prev_def_name}_{ref_candle_num}"
                            
                            logic_label = "opposite" if "opposite" in curr_def_config.get("type", "").lower() else "identical"
                            target_candle[conn_key] = logic_label
                            
                            # If this is the FIRST one found for this step, 
                            # it becomes the anchor for the NEXT definition (define_N+1)
                            if found_count_for_this_step == 1:
                                chain_history[curr_def_name] = (j, target_candle)

                    # If no matches were found for this step, the chain for this anchor is broken
                    if found_count_for_this_step == 0:
                        break

        return processed_data

    def apply_definition_conditions(candles, identify_config, new_filename_value, file_key):
        if not identify_config or not isinstance(candles, list):
            return candles, {}

        # --- SECTION 1: DYNAMIC VALIDATION (The "Logic Check") ---
        for target_candle in candles:
            if not isinstance(target_candle, dict): continue
            conn_keys = [k for k in target_candle.keys() if "_in_connection_with_" in k]
            
            for conn_key in conn_keys:
                parts = conn_key.split('_')
                curr_def_base = f"{parts[0]}_{parts[1]}" 
                
                def_cfg = identify_config.get(curr_def_base, {})
                condition_cfg = def_cfg.get("condition", "").lower()
                if not condition_cfg: 
                    target_candle[f"{conn_key}_met"] = True
                    continue

                mode = "behind" if "behind" in condition_cfg else "beyond"
                target_match = re.search(r'define_(\d+)', condition_cfg)
                if not target_match: continue
                target_def_index = int(target_match.group(1))

                # Trace back to find the specific define_n ref candle
                ref_candle = None
                search_key = conn_key
                while True:
                    t_parts = search_key.split('_')
                    p_def_lvl = int(t_parts[8])
                    p_num = int(t_parts[9])
                    
                    if p_def_lvl == target_def_index:
                        ref_candle = next((c for c in candles if c.get("candle_number") == p_num), None)
                        break
                    
                    parent_candle = next((c for c in candles if c.get("candle_number") == p_num), None)
                    if not parent_candle: break
                    search_key = next((k for k in parent_candle.keys() if k.startswith(f"define_{p_def_lvl}_") and "_in_connection_" in k), None)
                    if not search_key: break

                if ref_candle:
                    ref_h, ref_l = ref_candle.get("high"), ref_candle.get("low")
                    r_type = ref_candle.get("swing_type", "").lower()
                    
                    # Helper function for the core price logic
                    def check_logic(c_type, c_h, c_l, r_type, r_h, r_l, mode):
                        if mode == "behind":
                            if c_type == "higher_high" and r_type == "higher_high": return c_h < r_h
                            if c_type == "lower_low" and r_type == "lower_low": return c_l > r_l
                            if c_type == "higher_high" and r_type == "lower_low": return c_l > r_h
                            if c_type == "lower_low" and r_type == "higher_high": return c_h < r_l
                        elif mode == "beyond":
                            if c_type == "higher_high" and r_type == "higher_high": return c_h > r_h
                            if c_type == "lower_low" and r_type == "lower_low": return c_l < r_l
                            if c_type == "higher_high" and r_type == "lower_low": return c_h > r_h
                            if c_type == "lower_low" and r_type == "higher_high": return c_l < r_l
                        return False

                    # 1. Check the target candle itself
                    curr_h, curr_l = target_candle.get("high"), target_candle.get("low")
                    c_type = target_candle.get("swing_type", "").lower()
                    
                    logic_met = check_logic(c_type, curr_h, curr_l, r_type, ref_h, ref_l, mode)

                    # 2. Check Collective Beyond Requirement
                    min_collective = def_cfg.get("minimum_collectivebeyondcandles")
                    if logic_met and mode == "beyond" and isinstance(min_collective, int) and min_collective > 0:
                        # Find index of current candle to look behind in the list
                        try:
                            curr_idx = candles.index(target_candle)
                            # Check the 'n' candles before this one
                            for i in range(1, min_collective + 1):
                                prev_idx = curr_idx - i
                                if prev_idx < 0:
                                    logic_met = False # Not enough history
                                    break
                                
                                prev_c = candles[prev_idx]
                                p_h, p_l = prev_c.get("high"), prev_c.get("low")
                                # We use the target's swing type for the collective check as they are "with" the target
                                if not check_logic(c_type, p_h, p_l, r_type, ref_h, ref_l, mode):
                                    logic_met = False
                                    break
                        except ValueError:
                            pass

                    if logic_met:
                        target_candle[f"{conn_key}_met"] = True

        # --- SECTION 2: EXTRACTION (The "Grouping") ---
        # (Rest of the function remains the same)
        def_nums = [int(k.split('_')[1]) for k in identify_config.keys() if k.startswith("define_")]
        max_def = max(def_nums) if def_nums else 0
        patterns_dict = {}
        pattern_idx = 1

        for candle in candles:
            if not isinstance(candle, dict): continue
            last_def_keys = [k for k in candle.keys() if k.startswith(f"define_{max_def}_") and k.endswith("_met")]
            for m_key in last_def_keys:
                current_family = [candle]
                is_valid_family = True
                current_trace_key = m_key
                for d in range(max_def, 1, -1):
                    p_parts = current_trace_key.split('_')
                    parent_num = int(p_parts[9])
                    parent_def_lvl = int(p_parts[8])
                    parent_candle = next((c for c in candles if c.get("candle_number") == parent_num), None)
                    if not parent_candle:
                        is_valid_family = False
                        break
                    if parent_def_lvl > 1:
                        parent_met_key = next((k for k in parent_candle.keys() if k.startswith(f"define_{parent_def_lvl}_") and k.endswith("_met")), None)
                        if not parent_met_key:
                            is_valid_family = False
                            break
                        current_trace_key = parent_met_key
                    current_family.insert(0, parent_candle)

                if is_valid_family:
                    unique_family = []
                    seen_nums = set()
                    for c in current_family:
                        if c['candle_number'] not in seen_nums:
                            unique_family.append(c)
                            seen_nums.add(c['candle_number'])
                    patterns_dict[f"pattern_{pattern_idx}"] = unique_family
                    pattern_idx += 1

        patterns_dict = sanitize_pattern_definitions(patterns_dict)
        return candles, patterns_dict

    def sanitize_pattern_definitions(patterns_dict):
        """
        Sanitizes each pattern in the dictionary.
        Ensures that the N-th candle in a pattern family only contains 'define_N' metadata.
        """
        if not patterns_dict:
            return {}

        sanitized_patterns = {}

        for p_name, family in patterns_dict.items():
            new_family = []
            
            # The family is ordered [define_1, define_2, ..., define_max]
            for idx, candle in enumerate(family):
                if not isinstance(candle, dict):
                    new_family.append(candle)
                    continue
                
                # Create a shallow copy to avoid modifying the original list in-place
                clean_candle = candle.copy()
                current_rank = idx + 1  # 1-based indexing for define_n
                
                # Identify keys to keep:
                # 1. Standard OHLCV and technical data
                # 2. 'define_N' keys specific to this candle's position in the pattern
                keys_to_delete = []
                
                for key in clean_candle.keys():
                    # If the key is a 'define_X' key
                    if key.startswith("define_"):
                        # Extract the number from 'define_N...'
                        try:
                            parts = key.split('_')
                            def_num = int(parts[1])
                            
                            # Logic: If this is the 2nd candle in the list, 
                            # it should ONLY have define_2 related keys.
                            if def_num != current_rank:
                                keys_to_delete.append(key)
                        except (ValueError, IndexError):
                            continue
                
                # Remove the non-relevant define keys
                for k in keys_to_delete:
                    del clean_candle[k]
                    
                new_family.append(clean_candle)
                
            sanitized_patterns[p_name] = new_family
            
        return sanitized_patterns

    def intruder_and_outlaw_check(processed_data):
        for file_key, candles in processed_data.items():
            if not isinstance(candles, list):
                continue

            for i, candle in enumerate(candles):
                if not isinstance(candle, dict):
                    continue

                sender_num = candle.get("candle_number", i)
                sender_swing = candle.get("swing_type", "").lower()

                # 1. Identify connection keys from identify_definitions
                # Format: define_2_firstfound_129_in_connection_with_define_1_68
                connection_keys = [k for k in candle.keys() if "_in_connection_with_" in k]
                
                for conn_key in connection_keys:
                    try:
                        # Parse the logic label (identical/opposite) stored as the value in identify_definitions
                        logic_label = candle[conn_key] 
                        
                        # Split key to find the messenger number (last part of the string)
                        parts = conn_key.split('_')
                        messenger_num = int(parts[-1])
                        
                        # 2. INTRUDER CHECK (Liquidity Sweep)
                        messenger_candle = next((c for c in candles if isinstance(c, dict) and c.get("candle_number") == messenger_num), None)
                        
                        if messenger_candle and messenger_candle.get("swept_by_liquidity") is True:
                            intruder_num = messenger_candle.get("swept_by_candle_number")
                            if intruder_num is not None and messenger_num < intruder_num < sender_num:
                                # Construct dynamic key for Intruder
                                intruder_key = f"{conn_key}_{logic_label}_condition_beyond_firstchecked_intruder_number_{intruder_num}"
                                candle[intruder_key] = True

                        # 3. OUTLAW CHECK (Opposite Swing in Range)
                        outlaw_found = None
                        for mid_candle in candles:
                            if not isinstance(mid_candle, dict): continue
                            mid_num = mid_candle.get("candle_number")
                            
                            # Only check candles between the 'firstchecked' (messenger) and current 'sender'
                            if mid_num is not None and messenger_num < mid_num < sender_num:
                                mid_swing = mid_candle.get("swing_type", "").lower()
                                
                                is_outlaw = False
                                if sender_swing == "lower_low" and mid_swing == "higher_high":
                                    is_outlaw = True
                                elif sender_swing == "higher_high" and mid_swing == "lower_low":
                                    is_outlaw = True
                                
                                if is_outlaw:
                                    # Capture the first occurrence
                                    if outlaw_found is None or mid_num < outlaw_found:
                                        outlaw_found = mid_num

                        if outlaw_found is not None:
                            # Construct dynamic key for Outlaw
                            # Example: define_2_firstfound_129_in_connection_with_define_1_68_opposite_condition_beyond_firstchecked_identity_outlaw_number_130
                            outlaw_key = f"{conn_key}_{logic_label}_condition_beyond_firstchecked_identity_outlaw_number_{outlaw_found}"
                            candle[outlaw_key] = True

                    except (ValueError, IndexError):
                        continue

        return processed_data

    def identify_poi(target_data_tf, new_key, original_candles, poi_config):
        """
        Identifies Point of Interest (Breaker) based strictly on price violation.
        Updated to specifically target lower_low and higher_high violations.
        Tags anchor candles with 'from': True and 'after': True.
        """
        if not poi_config or not isinstance(target_data_tf, dict):
            return

        pattern_key = f"{new_key}_patterns"
        patterns = target_data_tf.get(pattern_key, {})
        
        from_sub = poi_config.get("from_subject")  
        after_sub = poi_config.get("after_subject") 

        candle_map = {
            c.get("candle_number"): c 
            for c in original_candles 
            if isinstance(c, dict) and "candle_number" in c
        }

        for p_name, family in patterns.items():
            # 1. Locate the anchor candles
            from_candle = next((c for c in family if c.get(from_sub) is True), None)
            after_candle = next((c for c in family if c.get(after_sub) is True), None)
            
            if not from_candle or not after_candle:
                continue

            # --- NEW FLAGS ADDED HERE ---
            from_candle["from"] = True
            after_candle["after"] = True
            # ----------------------------
                    
            after_num = after_candle.get("candle_number")
            swing_type = from_candle.get("swing_type", "").lower()
            
            # Determine target price level based on swing type
            if "high" in swing_type:
                price_key = poi_config.get("subject_is_higherhigh_or_lowerhigh", "low")
            else:
                price_key = poi_config.get("subject_is_lowerlow_or_higherlow", "high")

            clean_key = price_key.replace("_price", "") 
            target_price = from_candle.get(clean_key)
            
            if target_price is None:
                continue

            hitler_record = None

            # Search for the violator candle after 'after_subject'
            for oc in original_candles:
                if not isinstance(oc, dict) or oc.get("candle_number") <= after_num:
                    continue
                    
                # Logic for lower_low and higher_high violations
                if swing_type == "lower_low":
                    violator_low = oc.get("low")
                    if violator_low is not None and violator_low < target_price:
                        hitler_record = oc.copy()
                        break
                
                elif swing_type == "higher_high":
                    violator_high = oc.get("high")
                    if violator_high is not None and violator_high > target_price:
                        hitler_record = oc.copy()
                        break
                
                else:
                    continue

            if hitler_record:
                h_num = hitler_record.get("candle_number")
                direction_label = "below" if swing_type == "lower_low" else "above"
                
                label = f"after_subject_{after_sub}_violator_{h_num}_breaks_{direction_label}_{from_sub}_{clean_key}_price_{target_price:.5f}"
                
                from_candle[label] = True
                hitler_record["is_hitler_breaker"] = True
                
                # Enrich coordinates for visualization
                coordinate_keys = [
                    "candle_x", "candle_y", "candle_width", "candle_height",
                    "candle_left", "candle_right", "candle_top", "candle_bottom"
                ]
                full_record = candle_map.get(h_num)
                if full_record:
                    for k in coordinate_keys:
                        hitler_record[k] = full_record.get(k)
                
                family.append(hitler_record)

        return target_data_tf

    def identify_poi_mitigation(target_data_tf, new_key, poi_config):
        """
        Removes patterns where specific candles (restrict_definitions_mitigation) 
        violate the target price based on swing type.
        """
        if not poi_config or not isinstance(target_data_tf, dict):
            return

        pattern_key = f"{new_key}_patterns"
        patterns = target_data_tf.get(pattern_key, {})
        from_sub = poi_config.get("from_subject")
        restrict_raw = poi_config.get("restrict_definitions_mitigation")
        
        if not restrict_raw:
            return

        restrict_subs = [s.strip() for s in restrict_raw.split(",")]
        patterns_to_remove = []

        for p_name, family in patterns.items():
            from_candle = next((c for c in family if c.get(from_sub) is True), None)
            if not from_candle:
                continue

            target_swingtype = from_candle.get("swing_type", "").lower()
            
            # Determine the target price from configuration
            if "high" in target_swingtype:
                price_key = poi_config.get("subject_is_higherhigh_or_lowerhigh", "low")
            else:
                price_key = poi_config.get("subject_is_lowerlow_or_higherlow", "high")

            clean_key = price_key.replace("_price", "") 
            target_price = from_candle.get(clean_key)
            
            if target_price is None:
                continue

            is_mitigated = False
            
            # Check if any restricted candle violates the price level
            for sub_key in restrict_subs:
                restrict_candle = next((c for c in family if c.get(sub_key) is True), None)
                
                if restrict_candle:
                    # Apply the specific logic requested
                    if target_swingtype == "lower_low":
                        violator_low = restrict_candle.get("low")
                        # If violator_low is < target_price, it's a mitigation
                        if violator_low is not None and violator_low < target_price:
                            is_mitigated = True
                            break
                    
                    elif target_swingtype == "higher_high":
                        violator_high = restrict_candle.get("high")
                        # If violator_high is > target_price, it's a mitigation
                        if violator_high is not None and violator_high > target_price:
                            is_mitigated = True
                            break
                    
                    else:
                        # ("no violator")
                        continue

            if is_mitigated:
                patterns_to_remove.append(p_name)

        # Clean up patterns that hit the mitigation criteria
        for p_name in patterns_to_remove:
            del patterns[p_name]

        return target_data_tf
    
    def identify_swing_mitigation_between_definitions(target_data_tf, new_key, original_candles, poi_config):
        """
        Checks for swing violations between multiple pairs of definitions (sender and receiver).
        The config expects a comma-separated string: "define_1_define_3, define_4_define_10"
        If any candle between a pair matches the receiver's swing_type and violates the price, 
        the pattern is removed.
        """
        if not poi_config or not isinstance(target_data_tf, dict):
            return

        pattern_key = f"{new_key}_patterns"
        patterns = target_data_tf.get(pattern_key, {})
        from_sub = poi_config.get("from_subject")
        
        # Get the raw string, e.g., "define_1_define_3, define_2_define_4"
        restrict_raw = poi_config.get("restrict_swing_mitigation_between_definitions")
        if not restrict_raw:
            return

        # Split by comma to handle multiple pairs
        restrict_pairs = [p.strip() for p in restrict_raw.split(",") if p.strip()]
        patterns_to_remove = []

        for p_name, family in patterns.items():
            from_candle = next((c for c in family if c.get(from_sub) is True), None)
            if not from_candle:
                continue

            # Determine target price level once per pattern based on from_subject
            target_swingtype = from_candle.get("swing_type", "").lower()
            if "high" in target_swingtype:
                price_key = poi_config.get("subject_is_higherhigh_or_lowerhigh", "low")
            else:
                price_key = poi_config.get("subject_is_lowerlow_or_higherlow", "high")

            clean_key = price_key.replace("_price", "")
            target_price = from_candle.get(clean_key)
            
            if target_price is None:
                continue

            is_mitigated = False

            # Evaluate each pair defined in the config
            for pair_str in restrict_pairs:
                parts = pair_str.split("_")
                # Expecting format: define, N, define, M -> 4 parts
                if len(parts) < 4:
                    continue
                
                sender_key = f"{parts[0]}_{parts[1]}"
                receiver_key = f"{parts[2]}_{parts[3]}"

                sender_candle = next((c for c in family if c.get(sender_key) is True), None)
                receiver_candle = next((c for c in family if c.get(receiver_key) is True), None)

                if not sender_candle or not receiver_candle:
                    continue

                s_num = sender_candle.get("candle_number")
                r_num = receiver_candle.get("candle_number")
                receiver_swing_type = receiver_candle.get("swing_type", "").lower()
                
                # Define search range (exclusive)
                start_range = min(s_num, r_num) + 1
                end_range = max(s_num, r_num) - 1

                # Scan range for violations
                for oc in original_candles:
                    if not isinstance(oc, dict):
                        continue
                    
                    c_num = oc.get("candle_number")
                    if start_range <= c_num <= end_range:
                        current_swing = oc.get("swing_type", "").lower()
                        
                        # Match the swing type of the receiver
                        if current_swing == receiver_swing_type:
                            if receiver_swing_type == "lower_low":
                                v_low = oc.get("low")
                                if v_low is not None and v_low < target_price:
                                    is_mitigated = True
                                    break
                            elif receiver_swing_type == "higher_high":
                                v_high = oc.get("high")
                                if v_high is not None and v_high > target_price:
                                    is_mitigated = True
                                    break
                
                if is_mitigated:
                    break # No need to check other pairs for this pattern if one triggered

            if is_mitigated:
                patterns_to_remove.append(p_name)

        # Clean up patterns
        for p_name in patterns_to_remove:
            del patterns[p_name]

        return target_data_tf

    def identify_selected(target_data_tf, new_key, poi_config):
        """
        Filters pattern records based on extreme or non-extreme values of a specific define_n.
        Config format: "multiple_selection": "define_3_extreme" or "define_3_non_extreme"
        """
        if not poi_config or not isinstance(target_data_tf, dict):
            return target_data_tf

        pattern_key = f"{new_key}_patterns"
        patterns = target_data_tf.get(pattern_key, {})
        if not patterns:
            return target_data_tf

        selection_raw = poi_config.get("multiple_selection")
        if not selection_raw:
            return target_data_tf

        # Parse config: e.g., "define_3_extreme" -> target_key="define_3", mode="extreme"
        parts = selection_raw.split("_")
        if len(parts) < 3:
            return target_data_tf

        target_define_key = f"{parts[0]}_{parts[1]}" # e.g., "define_3"
        mode = parts[2].lower() # "extreme" or "non"
        if mode == "non":
            mode = "non_extreme"

        # 1. Collect all patterns containing the target definition and their prices
        eligible_patterns = []
        
        for p_name, family in patterns.items():
            # Find the candle in this pattern that has target_define_key: True
            target_candle = next((c for c in family if c.get(target_define_key) is True), None)
            
            if target_candle:
                swing_type = target_candle.get("swing_type", "").lower()
                # Determine which price to look at based on swing type
                if "high" in swing_type:
                    price = target_candle.get("high")
                else:
                    price = target_candle.get("low")
                
                if price is not None:
                    eligible_patterns.append({
                        "name": p_name,
                        "price": price,
                        "swing_type": swing_type
                    })

        if not eligible_patterns:
            return target_data_tf

        # 2. Determine the winner based on the criteria
        # We assume all patterns for a specific define_n share the same swing_type category 
        # (all highs or all lows) for a meaningful comparison.
        first_swing = eligible_patterns[0]["swing_type"]
        is_high_type = "high" in first_swing
        
        selected_pattern_name = None
        
        if is_high_type:
            # For Higher Highs: 
            # Extreme = Highest High | Non-Extreme = Lowest High
            if mode == "extreme":
                winner = max(eligible_patterns, key=lambda x: x["price"])
            else: # non_extreme
                winner = min(eligible_patterns, key=lambda x: x["price"])
        else:
            # For Lower Lows: 
            # Extreme = Lowest Low | Non-Extreme = Highest Low
            if mode == "extreme":
                winner = min(eligible_patterns, key=lambda x: x["price"])
            else: # non_extreme
                winner = max(eligible_patterns, key=lambda x: x["price"])

        selected_pattern_name = winner["name"]

        # 3. Remove all patterns that were part of this comparison but didn't win
        # Note: Patterns NOT containing the define_n are left untouched.
        patterns_to_remove = [p["name"] for p in eligible_patterns if p["name"] != selected_pattern_name]
        
        for p_name in patterns_to_remove:
            if p_name in patterns:
                del patterns[p_name]

        return target_data_tf

    def draw_poi_tools(img, target_data_tf, new_key, poi_config):
        """
        Draws visual markers on the image. 
        Boxes feature a single-edge border on the 'sensitive' price level.
        Updates 'from_candle' with flags regarding its extension or break status.
        """
        if not poi_config or img is None:
            return img

        pattern_key = f"{new_key}_patterns"
        patterns = target_data_tf.get(pattern_key, {})
        
        drawing_tool = poi_config.get("drawing_tool", "horizontal_line")
        from_sub = poi_config.get("from_subject")
        
        # Config mapping for sensitive edge
        hh_lh_edge = poi_config.get("subject_is_higherhigh_or_lowerhigh") # e.g., "low_price"
        ll_hl_edge = poi_config.get("subject_is_lowerlow_or_higherlow")   # e.g., "high_price"
        
        img_height, img_width = img.shape[:2]

        for p_name, family in patterns.items():
            # 1. Identify the origin (from_candle)
            from_candle = next((c for c in family if c.get(from_sub) is True), None)
            if not from_candle:
                continue

            # Locate the breaker candle using the boolean flags
            breaker_candle = next((c for c in family if c.get("is_hitler_breaker") or c.get("is_invalid_hitler")), None)
            
            # 2. Determine X boundaries and Update Flags
            start_x = int(from_candle.get("draw_right", from_candle.get("candle_right", 0)))
            
            if breaker_candle:
                end_x = int(breaker_candle.get("draw_left", breaker_candle.get("candle_left", img_width)))
                color = (0, 0, 255)  # Red for broken
                
                # FIX: Get the candle_number from the breaker record to avoid "unknown"
                hitler_num = breaker_candle.get("candle_number", "unknown")
                
                # Update the status flags on the origin candle
                from_candle[f"drawn_and_stopped_on_hitler{hitler_num}"] = True
                from_candle["pending_entry_level"] = False
            else:
                end_x = img_width
                color = (0, 255, 0)  # Green for active
                
                # Set the "no breaker" flag
                from_candle["pending_entry_level"] = True

            # 3. Handle Drawing Tools
            
            # --- TOOL: BOX ---
            if "box" in drawing_tool:
                y_high = int(from_candle.get("draw_top", 0))
                y_low = int(from_candle.get("draw_bottom", 0))
                
                # Draw Transparent Fill
                black_color = (0, 0, 0) 
                overlay = img.copy()
                cv2.rectangle(overlay, (start_x, y_high), (end_x, y_low), black_color, -1)
                cv2.addWeighted(overlay, 0.15, img, 0.85, 0, img)

                # Determine Sensitive Border logic
                swing_type = from_candle.get("swing_type", "").lower()
                border_y = None

                if "higher_high" in swing_type or "lower_high" in swing_type:
                    border_y = y_low if hh_lh_edge == "low_price" else y_high
                elif "lower_low" in swing_type or "higher_low" in swing_type:
                    border_y = y_high if ll_hl_edge == "high_price" else y_low

                # Draw the single sensitive border line
                if border_y is not None:
                    cv2.line(img, (start_x, border_y), (end_x, border_y), black_color, 1)

            # --- TOOL: DASHED HORIZONTAL LINE ---
            elif "dashed_horizontal_line" in drawing_tool:
                swing_type = from_candle.get("swing_type", "").lower()
                if "high" in swing_type:
                    target_y = int(from_candle.get("draw_top", 0))
                else:
                    target_y = int(from_candle.get("draw_bottom", 0))

                dash_length, gap_length = 10, 5
                curr_x = start_x
                while curr_x < end_x:
                    next_x = min(curr_x + dash_length, end_x)
                    cv2.line(img, (curr_x, target_y), (next_x, target_y), color, 2)
                    curr_x += dash_length + gap_length

            # --- TOOL: STANDARD HORIZONTAL LINE ---
            elif "horizontal_line" in drawing_tool:
                swing_type = from_candle.get("swing_type", "").lower()
                target_y = int(from_candle.get("draw_top", 0)) if "high" in swing_type else int(from_candle.get("draw_bottom", 0))
                cv2.line(img, (start_x, target_y), (end_x, target_y), color, 2)

        return img

    def identify_prices(target_data_tf, new_key, record_config, dev_base_path, new_folder_name):
        """
        Saves limit orders into a pending_orders folder INSIDE the specific new_filename folder.
        Path: dev_base_path/new_folder_name/pending_orders/limit_orders.json
        """
        if not record_config:
            return

        pattern_key = f"{new_key}_patterns"
        patterns = target_data_tf.get(pattern_key, {})
        
        pending_list = []
        price_map = {
            "low_price": "low",
            "high_price": "high",
            "open_price": "open",
            "close_price": "close"
        }
        
        # Updated Path Logic: Inside the new_folder_name directory
        orders_dir = os.path.join(dev_base_path, new_folder_name, "pending_orders")
        os.makedirs(orders_dir, exist_ok=True)
        orders_file = os.path.join(orders_dir, "limit_orders.json")

        for p_name, family in patterns.items():
            origin_candle = next((c for c in family if c.get("pending_entry_level") is True), None)
            
            if origin_candle:
                order_data = {
                    "symbol": origin_candle.get("symbol", "unknown"),
                    "timeframe": origin_candle.get("timeframe", "unknown"),
                    "risk_reward": record_config.get("risk_reward", 0),
                    "order_type": "unknown",
                    "entry": 0,
                    "exit": 0,
                    "target": 0
                }

                for role in ["entry", "exit", "target"]:
                    role_cfg = record_config.get(role, {})
                    subject_key = role_cfg.get("subject")
                    
                    if subject_key:
                        target_candle = next((c for c in family if c.get(subject_key) is True), None)
                        if target_candle:
                            swing_type = target_candle.get("swing_type", "").lower()
                            price_attr_raw = ""
                            if "high" in swing_type:
                                price_attr_raw = role_cfg.get("subject_is_higherhigh_or_lowerhigh")
                            elif "low" in swing_type:
                                price_attr_raw = role_cfg.get("subject_is_lowerlow_or_higherlow")

                            actual_key = price_map.get(price_attr_raw, price_attr_raw)
                            if actual_key:
                                order_data[role] = target_candle.get(actual_key, 0)
                
                entry_subject = record_config.get("entry", {}).get("subject")
                entry_candle = next((c for c in family if entry_subject and c.get(entry_subject) is True), origin_candle)
                
                e_swing = entry_candle.get("swing_type", "").lower()
                type_cfg = record_config.get("order_type", {})
                
                if "high" in e_swing:
                    order_data["order_type"] = type_cfg.get("subject_is_higherhigh_or_lowerhigh", "sell_limit")
                else:
                    order_data["order_type"] = type_cfg.get("subject_is_lowerlow_or_higherlow", "buy_limit")

                pending_list.append(order_data)

        if pending_list:
            existing_orders = []
            if os.path.exists(orders_file):
                try:
                    with open(orders_file, 'r', encoding='utf-8') as f:
                        existing_orders = json.load(f)
                except: 
                    existing_orders = []

            existing_orders.extend(pending_list)
            with open(orders_file, 'w', encoding='utf-8') as f:
                json.dump(existing_orders, f, indent=4)

    def enrich_limit_orders(dev_base_path, new_folder_name):
        """
        Reads the limit_orders.json, looks up the symbol-specific ticks.json,
        and enriches each order with tick_size and tick_value.
        """
        orders_dir = os.path.join(dev_base_path, new_folder_name, "pending_orders")
        orders_file = os.path.join(orders_dir, "limit_orders.json")

        if not os.path.exists(orders_file):
            return

        try:
            with open(orders_file, 'r', encoding='utf-8') as f:
                orders = json.load(f)
            
            if not orders:
                return

            # Cache for tick data to avoid re-reading the same file for every timeframe
            tick_cache = {}
            modified = False

            for order in orders:
                symbol = order.get("symbol")
                if not symbol:
                    continue

                # If not in cache, try to load the symbol_ticks.json
                if symbol not in tick_cache:
                    # Path: dev_base_path/new_folder_name/symbol/symbol_ticks.json
                    ticks_path = os.path.join(dev_base_path, new_folder_name, symbol, f"{symbol}_ticks.json")
                    
                    if os.path.exists(ticks_path):
                        try:
                            with open(ticks_path, 'r', encoding='utf-8') as f:
                                tick_cache[symbol] = json.load(f)
                        except Exception as e:
                            log(f"Error loading ticks for enrichment of {symbol}: {e}")
                            tick_cache[symbol] = None
                    else:
                        tick_cache[symbol] = None

                # Enrich the order if tick data exists
                symbol_data = tick_cache.get(symbol)
                if symbol_data:
                    order["tick_size"] = symbol_data.get("tick_size")
                    order["tick_value"] = symbol_data.get("tick_value")
                    modified = True

            if modified:
                with open(orders_file, 'w', encoding='utf-8') as f:
                    json.dump(orders, f, indent=4)

        except Exception as e:
            log(f"Failed to enrich limit orders: {e}")

    def limit_orders_old_record_cleanup(dev_base_path, new_folder_name):
        """
        Deletes the limit_orders.json file inside the specific new_filename folder 
        to ensure a fresh start for that entry's synchronization.
        """
        orders_file = os.path.join(dev_base_path, new_folder_name, "pending_orders", "limit_orders.json")
        if os.path.exists(orders_file):
            try:
                os.remove(orders_file)
            except Exception as e:
                log(f"Could not clear limit orders for {new_folder_name}: {e}")

    def sanitize_symbols_or_files(target_sym_dir, target_data):
        """
        Returns (should_delete_whole_folder, list_of_timeframes_to_keep)
        """
        tfs_to_keep = []
        tfs_to_remove = []

        for tf, tf_content in list(target_data.items()):
            has_patterns = any(key.endswith("_patterns") and value for key, value in tf_content.items())
            
            if has_patterns:
                tfs_to_keep.append(tf)
            else:
                tfs_to_remove.append(tf)

        # If no timeframes have patterns, the whole symbol is invalid
        if not tfs_to_keep:
            return True, []

        # Clean the target_data dictionary so empty TFs aren't saved to JSON
        for tf in tfs_to_remove:
            del target_data[tf]

        return False, tfs_to_keep
    
    def identify_paused_symbols(target_data, dev_base_path, new_folder_name):
        """
        Synchronizes all limit orders with their pattern anchors (from/after) 
        and saves them to paused_symbols.json without overwriting previous symbols.
        """
        orders_file = os.path.join(dev_base_path, new_folder_name, "pending_orders", "limit_orders.json")
        paused_folder = os.path.join(dev_base_path, new_folder_name, "paused_symbols_folder")
        paused_file = os.path.join(paused_folder, "paused_symbols.json")

        if not os.path.exists(orders_file):
            return

        try:
            with open(orders_file, 'r', encoding='utf-8') as f:
                active_orders = json.load(f)
        except Exception as e:
            log(f"Error reading limit orders: {e}")
            return

        # Load existing paused records to append to them, or start fresh if it's the first symbol
        all_paused_records = []
        if os.path.exists(paused_file):
            try:
                with open(paused_file, 'r', encoding='utf-8') as f:
                    all_paused_records = json.load(f)
            except:
                all_paused_records = []

        # Create a lookup set of (symbol, timeframe, time) to avoid duplicate entries in paused_symbols
        existing_keys = {(r.get("symbol"), r.get("timeframe"), r.get("time")) for r in all_paused_records}

        new_records_found = False

        for order in active_orders:
            order_sym = order.get("symbol")
            order_tf = order.get("timeframe")
            order_entry = order.get("entry")
            
            # Access the specific timeframe data
            tf_data = target_data.get(order_tf, {})
            
            for key, value in tf_data.items():
                if key.endswith("_patterns"):
                    for p_name, family in value.items():
                        from_c = next((c for c in family if c.get("from") is True), None)
                        after_c = next((c for c in family if c.get("after") is True), None)

                        # MATCHING LOGIC: 
                        # 1. Symbol matches
                        # 2. This specific "from" candle hasn't been added yet
                        if from_c and after_c and from_c.get("symbol") == order_sym:
                            # We use time as a unique identifier for the pattern start
                            pattern_time = from_c.get("time")
                            
                            if (order_sym, order_tf, pattern_time) not in existing_keys:
                                # Create record with full order details
                                record = {
                                    "from": True,
                                    "symbol": order_sym,
                                    "timeframe": order_tf,
                                    "entry": order_entry,
                                    "order_type": order.get("order_type"),
                                    "time": pattern_time,
                                    "exit": order.get("exit", 0),
                                    "target": order.get("target"),
                                    "tick_size": order.get("tick_size"),
                                    "tick_value": order.get("tick_value"),
                                    "after": {
                                        "after": True,
                                        "time": after_c.get("time")
                                    }
                                }
                                all_paused_records.append(record)
                                existing_keys.add((order_sym, order_tf, pattern_time))
                                new_records_found = True

        # Save the cumulative list back to the file
        if new_records_found:
            os.makedirs(paused_folder, exist_ok=True)
            with open(paused_file, 'w', encoding='utf-8') as f:
                json.dump(all_paused_records, f, indent=4)
    
    def populate_limit_orders_with_paused_orders(dev_base_path, new_folder_name):
        """
        Checks the limit orders file and adds any orders from paused_symbols.json 
        that are missing in the active limit orders.
        
        Args:
            dev_base_path: Base development path
            new_folder_name: Current run folder name
        """
        orders_file = os.path.join(dev_base_path, new_folder_name, "pending_orders", "limit_orders.json")
        paused_folder = os.path.join(dev_base_path, new_folder_name, "paused_symbols_folder")
        paused_file = os.path.join(paused_folder, "paused_symbols.json")
        
        # If no paused symbols file exists, nothing to do
        if not os.path.exists(paused_file):
            log("No paused symbols file found, skipping limit orders population")
            return 0
        
        # Load paused symbols/orders
        try:
            with open(paused_file, 'r', encoding='utf-8') as f:
                paused_orders = json.load(f)
        except Exception as e:
            log(f"Error reading paused symbols file: {e}")
            return 0
        
        if not paused_orders:
            return 0
        
        # Load existing active limit orders, or create empty list if file doesn't exist
        active_orders = []
        if os.path.exists(orders_file):
            try:
                with open(orders_file, 'r', encoding='utf-8') as f:
                    active_orders = json.load(f)
            except Exception as e:
                log(f"Error reading limit orders file: {e}")
                active_orders = []
        
        # Create lookup set of existing active orders to identify missing ones
        # Using (symbol, timeframe, entry, time) as unique identifier
        existing_order_keys = set()
        for order in active_orders:
            key = (
                order.get("symbol"),
                order.get("timeframe"),
                order.get("entry"),
                order.get("time")  # pattern time
            )
            existing_order_keys.add(key)
        
        # Track orders to add
        orders_added = 0
        orders_to_add = []
        
        # Check each paused order and add if missing from active orders
        for paused_order in paused_orders:
            # Extract the after time from the nested structure
            after_time = None
            if "after" in paused_order and isinstance(paused_order["after"], dict):
                after_time = paused_order["after"].get("time")
            
            order_key = (
                paused_order.get("symbol"),
                paused_order.get("timeframe"),
                paused_order.get("entry"),
                paused_order.get("time")  # pattern time
            )
            
            # If this order is not in active orders, add it
            if order_key not in existing_order_keys:
                # Create a clean order object from the paused record
                new_order = {
                    "symbol": paused_order.get("symbol"),
                    "timeframe": paused_order.get("timeframe"),
                    "entry": paused_order.get("entry"),
                    "exit": paused_order.get("exit", 0),
                    "order_type": paused_order.get("order_type", "LIMIT"),
                    "target": paused_order.get("target"),
                    "tick_size": paused_order.get("tick_size"),
                    "tick_value": paused_order.get("tick_value"),
                    "time": paused_order.get("time"),  # pattern time
                    "from_paused": True,  # Flag to indicate this was restored from paused
                    "status": "active"
                }
                
                # Add after time if it exists
                if after_time:
                    new_order["after_time"] = after_time
                
                orders_to_add.append(new_order)
                existing_order_keys.add(order_key)  # Prevent duplicates in this run
                orders_added += 1
        
        # If we found missing orders, append them to the active orders and save
        if orders_added > 0:
            # Combine existing orders with new ones
            updated_orders = active_orders + orders_to_add
            
            # Ensure the pending_orders directory exists
            orders_dir = os.path.join(dev_base_path, new_folder_name, "pending_orders")
            os.makedirs(orders_dir, exist_ok=True)
            
            # Save the updated orders file
            try:
                with open(orders_file, 'w', encoding='utf-8') as f:
                    json.dump(updated_orders, f, indent=4)
            except Exception as e:
                log(f"Error writing updated limit orders: {e}")
                return 0
        
        return orders_added           

    def main_logic():
        """Main logic for processing entry points of interest."""
        log(f"Starting: {broker_name}")

        dev_dict = load_developers_dictionary() 
        cfg = dev_dict.get(broker_name)
        if not cfg:
            log(f"Broker {broker_name} not found")
            return f"Error: Broker {broker_name} not in dictionary."
        
        base_folder = cfg.get("BASE_FOLDER")
        dev_base_path = os.path.abspath(os.path.join(base_folder, "..", "developers", broker_name))
        
        am_data = get_account_management(broker_name)
        if not am_data:
            log("accountmanagement.json missing")
            return "Error: accountmanagement.json missing."

        define_candles = am_data.get("chart", {}).get("define_candles", {})
        entries_root = define_candles.get("entries_poi_condition", {})
        
        total_syncs = 0
        entry_count = 0

        for apprehend_key, source_configs in entries_root.items():
            if not apprehend_key.startswith("apprehend_"):
                continue
                
            source_def_name = apprehend_key.replace("apprehend_", "")
            source_def = define_candles.get(source_def_name, {})
            if not source_def:
                continue

            raw_filename_base = source_def.get("filename", "").replace(".json", "").lower()

            for entry_key, entry_settings in source_configs.items():
                if not entry_key.startswith("entry_"):
                    continue

                new_folder_name = entry_settings.get('new_filename')
                if new_folder_name:
                    log(f"Processing: {new_folder_name}")
                    
                    # Check if identify_definitions exist
                    identify_config = entry_settings.get("identify_definitions")
                    if identify_config:
                        log(f"  With identify_definitions: {list(identify_config.keys())}")
                    
                    entry_count += 1
                    
                    # Call the inner function for file synchronization
                    syncs = process_entry_newfilename(
                        entry_settings, 
                        source_def_name, 
                        raw_filename_base, 
                        base_folder, 
                        dev_base_path
                    )
                    
                    total_syncs += syncs
        
        if entry_count > 0:
            return f"Completed: {entry_count} entry points processed"
        else:
            return f"No entry points found for processing."
        
    # ---- Execute Main Logic ---- 3
    return main_logic()

def clear_unathorized_entries_folders(broker_name):
    """
    1. Identifies protected filenames from accountmanagement.json in DEV_PATH.
    2. Deletes any folder in the developer directory NOT listed in the JSON's protected filenames.
    """
    dev_dict = load_developers_dictionary()
    cfg = dev_dict.get(broker_name)
    
    if not cfg:
        print(f"[{broker_name}] Error: Broker not in dictionary.")
        return False

    # Path for Developer Output and JSON
    # Assumes DEV_PATH is defined globally in your script
    dev_output_base = os.path.join(DEV_PATH, broker_name)
    json_path = os.path.join(dev_output_base, "accountmanagement.json")

    # --- PART 1: Identify Protected Filenames from JSON ---
    protected_filenames = set()
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r') as f:
                data = json.load(f)
            
            # Navigate the specific JSON structure
            poi_conditions = (data.get("chart", {})
                                  .get("define_candles", {})
                                  .get("entries_poi_condition", {}))
            
            for key, apprehend_box in poi_conditions.items():
                if key.startswith("apprehend") and isinstance(apprehend_box, dict):
                    for entry_key, entry_val in apprehend_box.items():
                        if entry_key.startswith("entry_") and isinstance(entry_val, dict):
                            filename = entry_val.get("new_filename")
                            if filename:
                                protected_filenames.add(filename)
        except Exception as e:
            print(f"[{broker_name}] Error reading JSON: {e}")
            return False
    else:
        print(f"[{broker_name}] JSON not found at: {json_path}. Aborting cleanup to prevent accidental wipe.")
        return False

    # --- PART 2: Cleanup ---
    if not os.path.exists(dev_output_base):
        return True

    deleted_count = 0
    try:
        for item in os.listdir(dev_output_base):
            item_path = os.path.join(dev_output_base, item)
            
            # Target ONLY folders; ignore files like accountmanagement.json
            if os.path.isdir(item_path):
                if item not in protected_filenames:
                    shutil.rmtree(item_path)
                    deleted_count += 1
                    #print(f"[{broker_name}] cleaned up unauthorized {item} folder")
                    
    except Exception as e:
        print(f"[{broker_name}] Cleanup Error: {e}")
        return False
    return 

def single():  
    dev_dict = load_developers_dictionary()
    if not dev_dict:
        print("No developers to process.")
        return


    broker_names = sorted(dev_dict.keys()) 
    cores = cpu_count()
    print(f"--- STARTING MULTIPROCESSING (Cores: {cores}) ---")

    with Pool(processes=cores) as pool:

        # STEP 2: Higher Highs & lower lows
        hh_ll_results = pool.map(entry_point_of_interest, broker_names)
        for r in hh_ll_results: print(r)

def main():
    dev_dict = load_developers_dictionary()
    if not dev_dict:
        print("No developers to process.")
        return

    broker_names = sorted(dev_dict.keys())
    cores = cpu_count()
    print(f"--- STARTING MULTIPROCESSING (Cores: {cores}) ---")

    with Pool(processes=cores) as pool:
        print("\n[STEP 1] Syncing Symbol Ticks Data...")
        tick_results = pool.map(sync_ticks_data, broker_names)
        for r in tick_results: print(r)

        tick_results = pool.map(copy_full_candle_data, broker_names)
        for r in tick_results: print(r)

        print("\n[STEP 2] Running Higher Highs & lower lows Analysis...")
        hh_ll_results = pool.map(higher_highs_lower_lows, broker_names)
        for r in hh_ll_results: print(r)

        print("\n[STEP 6] Running liquidity sweeps...")
        hh_ll_results = pool.map(liquidity_candles, broker_names)
        for r in hh_ll_results: print(r)

        hh_ll_results = pool.map(entry_point_of_interest, broker_names)
        for r in hh_ll_results: print(r)

        hh_ll_results = pool.map(clear_unathorized_entries_folders, broker_names)
        for r in hh_ll_results: print(r)






        

    print("\n[SUCCESS] All tasks completed.")

if __name__ == "__main__":
   main()


