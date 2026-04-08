import connectwithinfinitydb as db
import json
import os
from datetime import datetime
import time
from webdriver_manager.chrome import ChromeDriverManager

DEFAULT_PATH = r"C:\xampp\htdocs\synapse\synarex"
FETCHED_INVESTORS = r"C:\xampp\htdocs\synapse\synarex\fetched_investors.json"
UPDATED_INVESTORS = r"C:\xampp\htdocs\synapse\synarex\updated_investors.json"

def update_insiders_from_json_old():
    """
    Updates the insiders database table with data from updated_investors.json.
    Properly handles JSON fields including trades and unauthorized_actions.
    Handles NULL values in numeric fields.
    """
    try:
        print(f"[ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ] Starting update process...")

        if not os.path.exists(UPDATED_INVESTORS):
            print(f"Error: File not found at {UPDATED_INVESTORS}")
            return

        with open(UPDATED_INVESTORS, 'r', encoding='utf-8') as f:
            updated_data = json.load(f)

        print(f"Loaded {len(updated_data)} investor records from {UPDATED_INVESTORS}")
        print("-" * 80)

        success_count = 0
        error_count = 0
        skip_count = 0

        for record_key, data in updated_data.items():
            target_id = record_key 
            
            print(f"\n📋 Processing ID: {target_id}")
            
            # --- STEP 1: VERIFY IF ID EXISTS ---
            check_query = f"SELECT id FROM insiders WHERE id = '{target_id}'"
            check_result = db.execute_query(check_query)
            
            if not check_result.get('results'):
                print(f"   ❌ SKIP: ID '{target_id}' not found in database. Moving to next...")
                skip_count += 1
                continue

            print(f"   ✅ ID verified in database")
            
            # --- STEP 2: DATA PREPARATION ---
            # Helper function to properly format JSON for SQL
            def prepare_json_field(val):
                """Convert Python object to JSON string for SQL storage"""
                if val is None:
                    return '{}'
                if isinstance(val, str):
                    # If it's already a string, try to parse and re-stringify to ensure valid JSON
                    try:
                        parsed = json.loads(val)
                        return json.dumps(parsed, separators=(',', ':'))
                    except:
                        return val
                # Convert Python dict/list to JSON string
                return json.dumps(val, separators=(',', ':'))
            
            def escape_sql_string(val):
                """Escape single quotes for SQL string insertion"""
                if val is None:
                    return ''
                return str(val).replace("'", "''")
            
            def format_numeric_field(val):
                """Format numeric field, handling None and NULL values"""
                if val is None or val == '':
                    return 'NULL'
                try:
                    return str(float(val))
                except (ValueError, TypeError):
                    return 'NULL'
            
            # Extract and prepare data
            server = escape_sql_string(data.get('server', ''))
            login = escape_sql_string(data.get('login', ''))
            password = escape_sql_string(data.get('password', ''))
            application_status = escape_sql_string(data.get('application_status', 'pending'))
            
            # Numeric fields - handle NULL properly
            broker_balance = format_numeric_field(data.get('broker_balance', 0))
            profitandloss = format_numeric_field(data.get('profitandloss', 0))
            contract_days_left = format_numeric_field(data.get('contract_days_left', 30))
            current_balance = format_numeric_field(data.get('current_balance', data.get('broker_balance', 0)))
            authorized_tickets_count = format_numeric_field(data.get('authorized_tickets_count', 0))
            magic_number = format_numeric_field(data.get('magic_number', 0))
            
            # JSON fields - preserve exact structure
            trades_data = data.get('trades', {})
            trades_json = prepare_json_field(trades_data)
            
            unauthorized_actions = data.get('unauthorized_actions', {})
            unauthorized_json = prepare_json_field(unauthorized_actions)
            
            # Additional fields that might be in the JSON
            execution_start_date = escape_sql_string(data.get('execution_start_date', ''))
            last_audit_timestamp = escape_sql_string(data.get('last_audit_timestamp', ''))
            
            # Check for bypass note
            bypass_note = escape_sql_string(data.get('bypass_note', ''))
            message = escape_sql_string(data.get('message', ''))
            
            print(f"   📊 Data prepared:")
            print(f"      • Server: {server}")
            print(f"      • Login: {login}")
            print(f"      • Status: {application_status}")
            print(f"      • Balance: {broker_balance if broker_balance != 'NULL' else 'NULL'}")
            print(f"      • P&L: {profitandloss if profitandloss != 'NULL' else 'NULL'}")
            print(f"      • Contract Days: {contract_days_left if contract_days_left != 'NULL' else 'NULL'}")
            print(f"      • Authorized Tickets: {authorized_tickets_count if authorized_tickets_count != 'NULL' else 'NULL'}")
            print(f"      • Magic Number: {magic_number if magic_number != 'NULL' else 'NULL'}")
            
            # --- STEP 3: CONSTRUCT UPDATE QUERY ---
            # Build the UPDATE query with all fields
            update_query = f"""
                UPDATE insiders 
                SET 
                    server = '{server}',
                    login = '{login}',
                    password = '{password}',
                    application_status = '{application_status}',
                    broker_balance = {broker_balance},
                    profitandloss = {profitandloss},
                    contract_days_left = {contract_days_left},
                    trades = '{trades_json}',
                    unauthorized_actions = '{unauthorized_json}',
                    execution_start_date = '{execution_start_date}',
                    last_audit_timestamp = '{last_audit_timestamp}',
                    current_balance = {current_balance},
                    authorized_tickets_count = {authorized_tickets_count},
                    magic_number = {magic_number},
                    bypass_note = '{bypass_note}',
                    message = '{message}',
                    last_updated = NOW()
                WHERE id = '{target_id}'
            """.strip()
            
            # Debug: Print first 500 chars of query
            print(f"   🔍 Executing UPDATE for ID {target_id}...")
            if len(update_query) > 500:
                print(f"      Query preview: {update_query[:500]}...")
            else:
                print(f"      Query: {update_query}")
            
            # Execute the update
            result = db.execute_query(update_query)
            
            if result.get('status') == 'success':
                print(f"   ✅ SUCCESS: Updated ID {target_id}")
                success_count += 1
                
                # Optional: Verify the update - handle different return formats and NULL values
                verify_query = f"SELECT id, application_status, broker_balance, profitandloss FROM insiders WHERE id = '{target_id}'"
                verify_result = db.execute_query(verify_query)
                
                if verify_result.get('results'):
                    verified_row = verify_result['results'][0]
                    
                    # Helper function to safely convert to float for display
                    def safe_float_display(value):
                        """Safely convert database value to float for display, handling NULL"""
                        if value is None or value == 'NULL' or str(value).upper() == 'NULL':
                            return 'NULL'
                        try:
                            return f"${float(value):.2f}"
                        except (ValueError, TypeError):
                            return str(value)
                    
                    # Handle different return types (tuple, list, or dict)
                    if isinstance(verified_row, dict):
                        # Dictionary format
                        verified_id = verified_row.get('id')
                        verified_status = verified_row.get('application_status')
                        verified_balance = safe_float_display(verified_row.get('broker_balance'))
                        verified_pnl = safe_float_display(verified_row.get('profitandloss'))
                        print(f"      Verified: ID={verified_id}, Status={verified_status}, Balance={verified_balance}, P&L={verified_pnl}")
                    elif isinstance(verified_row, (list, tuple)):
                        # List or tuple format
                        verified_id = verified_row[0] if len(verified_row) > 0 else 'N/A'
                        verified_status = verified_row[1] if len(verified_row) > 1 else 'N/A'
                        verified_balance = safe_float_display(verified_row[2] if len(verified_row) > 2 else None)
                        verified_pnl = safe_float_display(verified_row[3] if len(verified_row) > 3 else None)
                        print(f"      Verified: ID={verified_id}, Status={verified_status}, Balance={verified_balance}, P&L={verified_pnl}")
                    else:
                        print(f"      Verified: Update confirmed (unable to parse return format)")
            else:
                print(f"   ❌ ERROR: Failed to update ID {target_id}: {result.get('message')}")
                error_count += 1
            
            # Small pause to prevent rate-limiting
            time.sleep(0.5)
        
        # Print final summary
        print("\n" + "="*80)
        print("📊 UPDATE SUMMARY")
        print("="*80)
        print(f"   • Total records processed: {len(updated_data)}")
        print(f"   • Successfully updated: {success_count}")
        print(f"   • Errors: {error_count}")
        print(f"   • Skipped (ID not found): {skip_count}")
        print("="*80)
        
        return {
            'success': success_count,
            'errors': error_count,
            'skipped': skip_count,
            'total': len(updated_data)
        }

    except Exception as e:
        print(f"❌ Critical Error: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        db.shutdown()
        print(f"\n[ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ] Database connection closed.")

def fetch_insiders_rows():
    try:
        print(f"[ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ] Starting fetch...")
        
        # Ensure correct table name
        query = "SELECT * FROM insiders" 
        result = db.execute_query(query)
        
        if result.get('status') != 'success':
            print(f"QUERY ERROR: {result.get('message')}")
            return
            
        rows = result.get('results', [])

        if not rows:
            print("WARNING: Database returned 'success' but the results list is empty.")
            print("Check if the table 'insiders' actually has rows in the PHP interface.")
            return
            
        print(f"SUCCESS: Fetched {len(rows)} records from 'insiders'")
        
        investors_data = {}
        for row in rows:
            # Safely identify the unique ID for the JSON key
            record_id = str(row.get('id') or row.get('ID') or "")
            if record_id:
                investors_data[record_id] = row
            else:
                # Fallback if no ID is found (uses timestamp as key)
                temp_key = f"unknown_{datetime.now().timestamp()}"
                investors_data[temp_key] = row
        
        # Save to file
        os.makedirs(os.path.dirname(FETCHED_INVESTORS), exist_ok=True)
        with open(FETCHED_INVESTORS, 'w', encoding='utf-8') as f:
            json.dump(investors_data, f, indent=4, default=str)
            
        print(f"DONE: Data saved to {FETCHED_INVESTORS}")
        
    except Exception as e:
        print(f"CRITICAL ERROR in fetch process: {e}")
    finally:
        db.shutdown()

def update_insiders_from_json():
    """
    Updates the insiders database table with data from updated_investors.json.
    Properly handles JSON fields including trades and unauthorized_actions.
    Handles NULL values in numeric fields.
    """
    try:
        print(f"[ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ] Starting update process...")

        if not os.path.exists(UPDATED_INVESTORS):
            print(f"Error: File not found at {UPDATED_INVESTORS}")
            return

        with open(UPDATED_INVESTORS, 'r', encoding='utf-8') as f:
            updated_data = json.load(f)

        print(f"Loaded {len(updated_data)} investor records from {UPDATED_INVESTORS}")
        print("-" * 80)

        success_count = 0
        error_count = 0
        skip_count = 0

        for record_key, data in updated_data.items():
            target_id = record_key 
            
            print(f"\n📋 Processing ID: {target_id}")
            
            # --- STEP 1: VERIFY IF ID EXISTS ---
            check_query = f"SELECT id FROM insiders WHERE id = '{target_id}'"
            check_result = db.execute_query(check_query)
            
            if not check_result.get('results'):
                print(f"   ❌ SKIP: ID '{target_id}' not found in database. Moving to next...")
                skip_count += 1
                continue

            print(f"   ✅ ID verified in database")
            
            # --- STEP 2: DATA PREPARATION FUNCTIONS ---
            
            def escape_sql_string(val):
                """Escape single quotes for SQL string insertion and wrap in quotes"""
                if val is None or val == '':
                    return "''"
                escaped = str(val).replace("'", "''")
                return f"'{escaped}'"
            
            def format_numeric_field(val):
                """Format numeric field for SQL - returns unquoted number or NULL"""
                if val is None or val == '':
                    return 'NULL'
                try:
                    num_val = float(val)
                    return f"{num_val:.2f}"
                except (ValueError, TypeError):
                    return 'NULL'
            
            def format_integer_field(val):
                """Format integer field for SQL - returns unquoted integer or NULL"""
                if val is None or val == '':
                    return 'NULL'
                try:
                    int_val = int(float(val))
                    return str(int_val)
                except (ValueError, TypeError):
                    return 'NULL'
            
            def prepare_json_field(val):
                """Convert Python object to JSON string for SQL storage"""
                if val is None:
                    return "'{}'"
                if isinstance(val, str):
                    try:
                        parsed = json.loads(val)
                        json_str = json.dumps(parsed, separators=(',', ':'))
                        return f"'{json_str}'"
                    except:
                        json_str = json.dumps(str(val))
                        return f"'{json_str}'"
                json_str = json.dumps(val, separators=(',', ':'))
                return f"'{json_str}'"
            
            def format_date_field(val):
                """Format date field for SQL"""
                if val is None or val == '':
                    return 'NULL'
                return f"'{str(val)}'"
            
            def format_datetime_field(val):
                """Format datetime field for SQL"""
                if val is None or val == '':
                    return 'NULL'
                return f"'{str(val)}'"
            
            # --- STEP 3: EXTRACT AND PREPARE DATA BY COLUMN TYPE ---
            
            # VARCHAR/TEXT fields (need quotes)
            server = escape_sql_string(data.get('server', ''))
            login = escape_sql_string(data.get('login', ''))
            password = escape_sql_string(data.get('password', ''))
            application_status = escape_sql_string(data.get('application_status', 'pending'))
            broker = escape_sql_string(data.get('broker', ''))
            email = escape_sql_string(data.get('email', ''))
            fullname = escape_sql_string(data.get('fullname', ''))
            passkey = escape_sql_string(data.get('passkey', ''))
            balance_display = escape_sql_string(data.get('balance_display', ''))
            loyalties = escape_sql_string(data.get('loyalties', ''))
            paymentdetails = escape_sql_string(data.get('paymentdetails', ''))
            message = escape_sql_string(data.get('message', ''))
            bypass_note = escape_sql_string(data.get('bypass_note', ''))
            
            # DECIMAL fields (no quotes)
            broker_balance = format_numeric_field(data.get('broker_balance', 0))
            profitandloss = format_numeric_field(data.get('profitandloss', 0))
            current_balance = format_numeric_field(data.get('current_balance', data.get('broker_balance', 0)))
            
            # INT fields (no quotes)
            contract_days_left = format_integer_field(data.get('contract_days_left', 30))
            authorized_tickets_count = format_integer_field(data.get('authorized_tickets_count', 0))
            magic_number = format_integer_field(data.get('magic_number', 0))
            
            # JSON fields
            trades_data = data.get('trades', {})
            trades_json = prepare_json_field(trades_data)
            
            unauthorized_actions = data.get('unauthorized_actions', {})
            unauthorized_json = prepare_json_field(unauthorized_actions)
            
            # Date/DateTime fields
            execution_start_date = format_date_field(data.get('execution_start_date', ''))
            submitted_at = format_datetime_field(data.get('submitted_at', ''))
            last_audit_timestamp = format_datetime_field(data.get('last_audit_timestamp', ''))
            
            # History fields
            broker_balance_history = escape_sql_string(data.get('broker_balance_history', ''))
            execution_dates_history = escape_sql_string(data.get('execution_dates_history', ''))
            profitandlosshistory = escape_sql_string(data.get('profitandlosshistory', ''))
            tradeshistory = escape_sql_string(data.get('tradeshistory', ''))
            
            print(f"   📊 Data prepared:")
            print(f"      • Balance (DECIMAL): {broker_balance}")
            print(f"      • P&L (DECIMAL): {profitandloss}")
            print(f"      • Contract Days (INT): {contract_days_left}")
            
            # --- STEP 4: TRY MULTIPLE APPROACHES ---
            
            # APPROACH 1: Standard UPDATE
            update_query = f"""
                UPDATE insiders 
                SET 
                    broker = {broker},
                    email = {email},
                    fullname = {fullname},
                    server = {server},
                    login = {login},
                    password = {password},
                    application_status = {application_status},
                    broker_balance = {broker_balance},
                    profitandloss = {profitandloss},
                    passkey = {passkey},
                    submitted_at = {submitted_at},
                    balance_display = {balance_display},
                    execution_start_date = {execution_start_date},
                    broker_balance_history = {broker_balance_history},
                    execution_dates_history = {execution_dates_history},
                    profitandlosshistory = {profitandlosshistory},
                    trades = {trades_json},
                    tradeshistory = {tradeshistory},
                    loyalties = {loyalties},
                    contract_days_left = {contract_days_left},
                    paymentdetails = {paymentdetails},
                    message = {message},
                    unauthorized_actions = {unauthorized_json},
                    last_updated = NOW()
                WHERE id = '{target_id}'
            """.strip()
            
            print(f"   🔍 Executing UPDATE for ID {target_id}...")
            
            # Save the exact query to a file for debugging
            debug_file = os.path.join(os.path.dirname(UPDATED_INVESTORS), 'debug_query.sql')
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(update_query)
            print(f"   📝 Full query saved to: {debug_file}")
            
            # Execute the update
            result = db.execute_query(update_query)
            
            if result.get('status') == 'success':
                print(f"   ✅ UPDATE query returned success")
                
                # APPROACH 2: Try a direct numeric update using CAST
                print(f"   🔧 Attempting direct numeric update with CAST...")
                cast_query = f"""
                    UPDATE insiders 
                    SET 
                        broker_balance = CAST({broker_balance} AS DECIMAL(15,2)),
                        profitandloss = CAST({profitandloss} AS DECIMAL(15,2)),
                        contract_days_left = CAST({contract_days_left} AS SIGNED),
                        last_updated = NOW()
                    WHERE id = '{target_id}'
                """.strip()
                
                cast_result = db.execute_query(cast_query)
                if cast_result.get('status') == 'success':
                    print(f"   ✅ CAST update successful")
                
                # Verify after both updates
                verify_query = f"SELECT broker_balance, profitandloss, contract_days_left FROM insiders WHERE id = '{target_id}'"
                verify_result = db.execute_query(verify_query)
                
                if verify_result and verify_result.get('results'):
                    verified_row = verify_result['results'][0]
                    if isinstance(verified_row, dict):
                        print(f"      📊 Final verification:")
                        print(f"         • Balance in DB: {verified_row.get('broker_balance')}")
                        print(f"         • P&L in DB: {verified_row.get('profitandloss')}")
                        print(f"         • Contract Days: {verified_row.get('contract_days_left')}")
                        
                        # Check if values are still NULL
                        if verified_row.get('broker_balance') is None:
                            print(f"      ⚠️ CRITICAL: Balance still NULL after update!")
                            print(f"      💡 This suggests the db.execute_query() function is modifying the query")
                            
                            # APPROACH 3: Try using parameterized query if available
                            if hasattr(db, 'execute_param_query'):
                                print(f"   🔧 Attempting parameterized query...")
                                param_query = """
                                    UPDATE insiders 
                                    SET broker_balance = %s, profitandloss = %s, contract_days_left = %s
                                    WHERE id = %s
                                """
                                params = (float(data.get('broker_balance', 0)), 
                                         float(data.get('profitandloss', 0)),
                                         int(data.get('contract_days_left', 30)) if data.get('contract_days_left') else None,
                                         int(target_id))
                                param_result = db.execute_param_query(param_query, params)
                                if param_result.get('status') == 'success':
                                    print(f"   ✅ Parameterized query successful")
                                    # Verify again
                                    final_verify = db.execute_query(verify_query)
                                    if final_verify and final_verify.get('results'):
                                        final_row = final_verify['results'][0]
                                        print(f"      📊 After parameterized query:")
                                        print(f"         • Balance: {final_row.get('broker_balance')}")
                                        print(f"         • P&L: {final_row.get('profitandloss')}")
                else:
                    print(f"      ⚠️ Verification failed")
                
                success_count += 1
            else:
                print(f"   ❌ ERROR: Failed to update ID {target_id}: {result.get('message')}")
                error_count += 1
            
            time.sleep(0.5)
        
        # Print final summary
        print("\n" + "="*80)
        print("📊 UPDATE SUMMARY")
        print("="*80)
        print(f"   • Total records processed: {len(updated_data)}")
        print(f"   • Successfully updated: {success_count}")
        print(f"   • Errors: {error_count}")
        print(f"   • Skipped (ID not found): {skip_count}")
        print("="*80)
        
        return {
            'success': success_count,
            'errors': error_count,
            'skipped': skip_count,
            'total': len(updated_data)
        }

    except Exception as e:
        print(f"❌ Critical Error: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        db.shutdown()
        print(f"\n[ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ] Database connection closed.")

if __name__ == "__main__":
    fetch_insiders_rows()
    