import timeorders


def updating_database_record():
    try:
        timeorders.current_time()
        print("updated")
    except Exception as e:
        print(f"Error updating {e}")
