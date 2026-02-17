import ohlc
import techniques
import calculateprices
import placeorders
import demo_placeorders
import time
import os
import json


INV_PATH = r"C:\xampp\htdocs\synapse\synarex\usersdata\investors"

def fetch_ohlc():
    try:
        ohlc.main()
    except Exception as e:
        print(f"Error in ohlc: {e}")

def technical_analysis():
    try:
        techniques.main()
    except Exception as e:
        print(f"Error in techniques: {e}")

def calculate_prices():
    try:
        calculateprices.calculate_orders()
    except Exception as e:
        print(f"Error in calculateprices: {e}")

def switch_demo_to_real():
    """
    Overwrites investors.json with the contents of demoinvestors.json.
    Target: INV_PATH
    """
    demo_path = os.path.join(INV_PATH, "demoinvestors.json")
    real_path = os.path.join(INV_PATH, "investors.json")

    print(f"\n{'='*10} 🔄 SWITCHING DEMO TO REAL {'='*10}")

    # 1. Check if source (demo) exists
    if not os.path.exists(demo_path):
        print(f" [!] Error: Source file {demo_path} not found.")
        return False

    try:
        # 2. Open demo and load data
        with open(demo_path, 'r', encoding='utf-8') as f_demo:
            demo_data = json.load(f_demo)

        # 3. Overwrite investors.json with demo data
        with open(real_path, 'w', encoding='utf-8') as f_real:
            json.dump(demo_data, f_real, indent=4)
            
        print(f" └─ ✅ Success: investors.json has been overwritten with demo records.")
        print(f"{'='*35}\n")
        return True

    except Exception as e:
        print(f" └─ ❌ Critical Error during switch: {e}")
        return False

def switch_to_real_investors():
    """
    Overwrites real_investors.json with the contents of investors.json.
    Target: INV_PATH
    """
    real_path = os.path.join(INV_PATH, "investors.json")
    real_backup_path = os.path.join(INV_PATH, "real_investors.json")

    print(f"\n{'='*10} 🔄 SWITCHING REAL TO DEMO {'='*10}")

    # 1. Check if source (real) exists
    if not os.path.exists(real_path):
        print(f" [!] Error: Source file {real_path} not found.")
        return False

    try:
        # 2. Open real and load data
        with open(real_backup_path, 'r', encoding='utf-8') as f_real:
            real_backup_path = json.load(f_real)

        # 3. Overwrite real_investors.json with real data
        with open(real_path, 'w', encoding='utf-8') as f_real_backup:
            json.dump(real_backup_path, f_real_backup, indent=4)
            
        print(f" └─ ✅ Success: real_investors.json has been updated with real records.")
        print(f"{'='*35}\n")
        return True

    except Exception as e:
        print(f" └─ ❌ Critical Error during switch: {e}")
        return False
  
def place_orders():
    try:
        placeorders.place_orders()
    except Exception as e:
        print(f"Error in placeorders: {e}")

def place_demo_orders():
    try:
        demo_placeorders.place_orders()
    except Exception as e:
        print(f"Error in placedemoorders: {e}")


  
def run_trade():
    fetch_ohlc()
    technical_analysis()
    calculate_prices()
    #place_orders()
    time.sleep(1800)
    run_trade()

if __name__ == "__main__":
   run_trade()


