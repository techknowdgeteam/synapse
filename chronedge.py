import ohlc
import techniques
import calculateprices
import placeorders
import demo_placeorders
import time

def fetch_ohlc():
    try:
        ohlc.main()
        print("ohlc completed.")
    except Exception as e:
        print(f"Error in ohlc: {e}")

def technical_analysis():
    try:
        techniques.main()
        print("technical analysis completed.")
    except Exception as e:
        print(f"Error in techniques: {e}")

def calculate_prices():
    try:
        calculateprices.calculate_orders()
    except Exception as e:
        print(f"Error in calculateprices: {e}")

def place_orders():
    try:
        placeorders.place_orders()
        print("Placing real account orders completed.")
    except Exception as e:
        print(f"Error in placeorders: {e}")

def place_demo_orders():
    try:
        demo_placeorders.place_orders()
        print("Placing demo account orders completed.")
    except Exception as e:
        print(f"Error in placedemoorders: {e}")

def run_trade():
    fetch_ohlc()
    technical_analysis()
    calculate_prices()
    place_orders()
    time.sleep(3200)
    run_trade()
    
if __name__ == "__main__":
   run_trade()


