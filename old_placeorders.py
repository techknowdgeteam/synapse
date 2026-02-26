def _0_50_4_orders():
    def _0_50cent_usd_live_sl_tp_amounts():
        
        """
        READS: hightolow.json
        CALCULATES: Live $3 risk & profit
        PRINTS: 3-line block for every market
        SAVES:
            - live_risk_profit_all.json → only valid ≤ $0.60
            - OVERWRITES hightolow.json → REMOVES bad orders PERMANENTLY
        FILTER: Delete any order with live_risk_usd > 0.60 from BOTH files
        """

        BASE_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
        INPUT_FILE = "hightolow.json"
        OUTPUT_FILE = "live_risk_profit_all.json"

        for user_brokerid, cfg in usersdictionary.items():
            TERMINAL_PATH = cfg["TERMINAL_PATH"]
            LOGIN_ID = cfg["LOGIN_ID"]
            PASSWORD = cfg["PASSWORD"]
            SERVER = cfg["SERVER"]

            log_and_print(f"\n{'='*60}", "INFO")
            log_and_print(f"PROCESSING BROKER: {user_brokerid.upper()}", "INFO")
            log_and_print(f"{'='*60}", "INFO")

            # ------------------- CONNECT TO MT5 -------------------
            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=60000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue
            if not mt5.login(int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"Login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account = mt5.account_info()
            if not account:
                log_and_print("No account info", "ERROR")
                mt5.shutdown()
                continue

            balance = account.balance
            if not (0.50 <= balance < 3.99):
                log_and_print(f"Balance ${balance:.2f} not in $12–$20 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue

            currency = account.currency
            log_and_print(f"Connected → Balance: ${balance:.2f} {currency}", "INFO")

            # ------------------- LOAD JSON -------------------
            json_path = Path(BASE_DIR) / user_brokerid / "risk_0_50cent_usd" / INPUT_FILE
            if not json_path.exists():
                log_and_print(f"JSON not found: {json_path}", "ERROR")
                mt5.shutdown()
                continue

            try:
                with json_path.open("r", encoding="utf-8") as f:
                    original_data = json.load(f)
                entries = original_data.get("entries", [])
            except Exception as e:
                log_and_print(f"Failed to read JSON: {e}", "ERROR")
                mt5.shutdown()
                continue

            if not entries:
                log_and_print("No entries in JSON.", "INFO")
                mt5.shutdown()
                continue

            log_and_print(f"Loaded {len(entries)} entries → Calculating LIVE risk...", "INFO")

            # ------------------- PROCESS & FILTER -------------------
            valid_entries = []        # For overwriting hightolow.json
            results = []              # For live_risk_profit_all.json
            total = len(entries)
            kept = 0
            removed = 0

            for i, entry in enumerate(entries, 1):
                market = entry["market"]
                try:
                    price = float(entry["entry_price"])
                    sl = float(entry["sl_price"])
                    tp = float(entry["tp_price"])
                    volume = float(entry["volume"])
                    order_type = entry["limit_order"]
                    sl_pips = float(entry.get("sl_pips", 0))
                    tp_pips = float(entry.get("tp_pips", 0))

                    # --- LIVE DATA ---
                    info = mt5.symbol_info(market)
                    tick = mt5.symbol_info_tick(market)

                    if not info or not tick:
                        log_and_print(f"NO LIVE DATA for {market} → Using fallback", "WARNING")
                        pip_value = 0.1
                        risk_usd = volume * sl_pips * pip_value
                        profit_usd = volume * tp_pips * pip_value
                    else:
                        point = info.point
                        contract = info.trade_contract_size

                        risk_points = abs(price - sl) / point
                        profit_points = abs(tp - price) / point

                        point_val = contract * point
                        if "JPY" in market and currency == "USD":
                            point_val /= 100

                        risk_ac = risk_points * point_val * volume
                        profit_ac = profit_points * point_val * volume

                        risk_usd = risk_ac
                        profit_usd = profit_ac

                        if currency != "USD":
                            conv = f"USD{currency}"
                            rate_tick = mt5.symbol_info_tick(conv)
                            rate = rate_tick.bid if rate_tick else 1.0
                            risk_usd /= rate
                            profit_usd /= rate

                    risk_usd = round(risk_usd, 2)
                    profit_usd = round(profit_usd, 2)

                    # --- PRINT ALL ---
                    print(f"market: {market}")
                    print(f"risk: {risk_usd} USD")
                    print(f"profit: {profit_usd} USD")
                    print("---")

                    # --- FILTER: KEEP ONLY <= 0.60 ---
                    if risk_usd <= 0.60:
                        # Keep in BOTH files
                        valid_entries.append(entry)  # Original format
                        results.append({
                            "market": market,
                            "order_type": order_type,
                            "entry_price": round(price, 6),
                            "sl": round(sl, 6),
                            "tp": round(tp, 6),
                            "volume": round(volume, 5),
                            "live_risk_usd": risk_usd,
                            "live_profit_usd": profit_usd,
                            "sl_pips": round(sl_pips, 2),
                            "tp_pips": round(tp_pips, 2),
                            "has_live_tick": bool(info and tick),
                            "current_bid": round(tick.bid, 6) if tick else None,
                            "current_ask": round(tick.ask, 6) if tick else None,
                        })
                        kept += 1
                    else:
                        removed += 1
                        log_and_print(f"REMOVED {market}: live risk ${risk_usd} > $0.60 → DELETED FROM BOTH JSON FILES", "WARNING")

                except Exception as e:
                    log_and_print(f"ERROR on {market}: {e}", "ERROR")
                    removed += 1

                if i % 5 == 0 or i == total:
                    log_and_print(f"Processed {i}/{total} | Kept: {kept} | Removed: {removed}", "INFO")

            # ------------------- SAVE OUTPUT: live_risk_profit_all.json -------------------
            out_path = json_path.parent / OUTPUT_FILE
            report = {
                "broker": user_brokerid,
                "account_currency": currency,
                "generated_at": datetime.now(pytz.timezone("Africa/Lagos")).strftime("%Y-%m-%d %H:%M:%S.%f%z"),
                "source_file": str(json_path),
                "total_entries": total,
                "kept_risk_<=_0.60": kept,
                "removed_risk_>_0.60": removed,
                "filter_applied": "Delete from both input & output if live_risk_usd > 0.60",
                "orders": results
            }

            try:
                with out_path.open("w", encoding="utf-8") as f:
                    json.dump(report, f, indent=2)
                log_and_print(f"SAVED → {out_path} | Kept: {kept} | Removed: {removed}", "SUCCESS")
            except Exception as e:
                log_and_print(f"Save failed: {e}", "ERROR")

            # ------------------- OVERWRITE INPUT: hightolow.json -------------------
            cleaned_input = original_data.copy()
            cleaned_input["entries"] = valid_entries  # Only good ones

            try:
                with json_path.open("w", encoding="utf-8") as f:
                    json.dump(cleaned_input, f, indent=2)
                log_and_print(f"OVERWRITTEN → {json_path} | Now has {len(valid_entries)} entries (removed {removed})", "SUCCESS")
            except Exception as e:
                log_and_print(f"Failed to overwrite input JSON: {e}", "ERROR")

            mt5.shutdown()
            log_and_print(f"FINISHED {user_brokerid} → {kept}/{total} valid orders in BOTH files", "SUCCESS")

        log_and_print("\nALL DONE – BAD ORDERS (> $0.60) DELETED FROM INPUT & OUTPUT!", "SUCCESS")
        return True
    
    def place_0_50cent_usd_orders():
        

        BASE_INPUT_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
        RISK_FOLDER = "risk_0_50cent_usd"
        STRATEGY_FILE = "hightolow.json"
        REPORT_SUFFIX = "forex_order_report.json"
        ISSUES_FILE = "ordersissues.json"

        for user_brokerid, broker_cfg in usersdictionary.items():
            TERMINAL_PATH = broker_cfg["TERMINAL_PATH"]
            LOGIN_ID = broker_cfg["LOGIN_ID"]
            PASSWORD = broker_cfg["PASSWORD"]
            SERVER = broker_cfg["SERVER"]

            log_and_print(f"Processing broker: {user_brokerid} (Balance $12–$20 mode)", "INFO")

            # === MT5 Init ===
            if not os.path.exists(TERMINAL_PATH):
                log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
                continue

            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=30000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue

            if not mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"MT5 login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account_info = mt5.account_info()
            if not account_info:
                log_and_print(f"Failed to get account info: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            balance = account_info.balance
            if not (0.50 <= balance < 10000):
                log_and_print(f"Balance ${balance:.2f} not in $12–$20 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue


            account_info = mt5.account_info()
            if not account_info:
                log_and_print(f"Failed to get account info: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue
            balance = account_info.balance
            equity = account_info.equity
            log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f}", "INFO")
            if equity < 0.50 and balance >= 0.50:
                log_and_print(f"Equity ${equity:.2f} < $0.50 while Balance ${balance:.2f} ≥ $0.50 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if equity >= 0.50 and balance < 0.50:
                log_and_print(f"Equity ${equity:.2f} > $0.50 while Balance ${balance:.2f} < $0.50 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if not (0.50 <= balance < 10000):
                log_and_print(f"Balance ${balance:.2f} not in $0.50–$3.99 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue
            # === Only reaches here if: equity >= 8 AND balance in [8, 11.99) ===
            log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f} → Using {RISK_FOLDER} + {STRATEGY_FILE}", "INFO")



            log_and_print(f"Balance: ${balance:.2f} → Using {RISK_FOLDER} + {STRATEGY_FILE}", "INFO")

            # === Load hightolow.json ===
            file_path = Path(BASE_INPUT_DIR) / user_brokerid / RISK_FOLDER / STRATEGY_FILE
            if not file_path.exists():
                log_and_print(f"File not found: {file_path}", "WARNING")
                mt5.shutdown()
                continue

            try:
                with file_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                    entries = data.get("entries", [])
            except Exception as e:
                log_and_print(f"Failed to read {file_path}: {e}", "ERROR")
                mt5.shutdown()
                continue

            if not entries:
                log_and_print("No entries in hightolow.json", "INFO")
                mt5.shutdown()
                continue

            # === Load existing orders & positions ===
            existing_pending = {}  # (symbol, type) → ticket
            running_positions = set()  # symbols with open position

            for order in (mt5.orders_get() or []):
                if order.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT):
                    existing_pending[(order.symbol, order.type)] = order.ticket

            for pos in (mt5.positions_get() or []):
                running_positions.add(pos.symbol)

            # === Reporting ===
            report_file = file_path.parent / REPORT_SUFFIX
            existing_reports = json.load(report_file.open("r", encoding="utf-8")) if report_file.exists() else []
            issues_list = []
            now_str = datetime.now(pytz.timezone("Africa/Lagos")).strftime("%Y-%m-%d %H:%M:%S.%f+01:00")
            placed = failed = skipped = 0

            for entry in entries:
                try:
                    symbol = entry["market"]
                    price = float(entry["entry_price"])
                    sl = float(entry["sl_price"])
                    tp = float(entry["tp_price"])
                    volume = float(entry["volume"])
                    order_type_str = entry["limit_order"]
                    order_type = mt5.ORDER_TYPE_BUY_LIMIT if order_type_str == "buy_limit" else mt5.ORDER_TYPE_SELL_LIMIT

                    # === SKIP: Already running or pending ===
                    if symbol in running_positions:
                        skipped += 1
                        log_and_print(f"{symbol} has running position → SKIPPED", "INFO")
                        continue

                    key = (symbol, order_type)
                    if key in existing_pending:
                        skipped += 1
                        log_and_print(f"{symbol} {order_type_str} already pending → SKIPPED", "INFO")
                        continue

                    # === Symbol check ===
                    symbol_info = mt5.symbol_info(symbol)
                    if not symbol_info or not symbol_info.visible:
                        issues_list.append({"symbol": symbol, "reason": "Symbol not available"})
                        failed += 1
                        continue

                    # === Volume fix ===
                    vol_step = symbol_info.volume_step
                    volume = max(symbol_info.volume_min,
                                round(volume / vol_step) * vol_step)
                    volume = min(volume, symbol_info.volume_max)

                    # === Price distance check ===
                    tick = mt5.symbol_info_tick(symbol)
                    if not tick:
                        issues_list.append({"symbol": symbol, "reason": "No tick data"})
                        failed += 1
                        continue

                    point = symbol_info.point
                    if order_type == mt5.ORDER_TYPE_BUY_LIMIT:
                        if price >= tick.ask or (tick.ask - price) < 10 * point:
                            skipped += 1
                            continue
                    else:
                        if price <= tick.bid or (price - tick.bid) < 10 * point:
                            skipped += 1
                            continue

                    # === Build & send order ===
                    request = {
                        "action": mt5.TRADE_ACTION_PENDING,
                        "symbol": symbol,
                        "volume": volume,
                        "type": order_type,
                        "price": price,
                        "sl": sl,
                        "tp": tp,
                        "deviation": 10,
                        "magic": 123456,
                        "comment": "Risk3_Auto",
                        "type_time": mt5.ORDER_TIME_GTC,
                        "type_filling": mt5.ORDER_FILLING_IOC,
                    }

                    result = mt5.order_send(request)
                    if result is None:
                        result = type('obj', (), {'retcode': 10000, 'comment': 'order_send returned None'})()

                    success = result.retcode == mt5.TRADE_RETCODE_DONE
                    if success:
                        existing_pending[key] = result.order
                        placed += 1
                        log_and_print(f"{symbol} {order_type_str} @ {price} → PLACED (ticket {result.order})", "SUCCESS")
                    else:
                        failed += 1
                        issues_list.append({"symbol": symbol, "reason": result.comment})

                    # === Report ===
                    if "cent" in RISK_FOLDER:
                        risk_usd = 0.5
                    else:
                        risk_usd = float(RISK_FOLDER.split("_")[1].replace("usd", ""))

                    # === Report ===
                    report_entry = {
                        "symbol": symbol,
                        "order_type": order_type_str,
                        "price": price,
                        "volume": volume,
                        "sl": sl,
                        "tp": tp,
                        "risk_usd": risk_usd,   # ← Now correct: 0.5, 1.0, 2.0, 3.0, 4.0
                        "ticket": result.order if success else None,
                        "success": success,
                        "error_code": result.retcode if not success else None,
                        "error_msg": result.comment if not success else None,
                        "timestamp": now_str
                    }
                    existing_reports.append(report_entry)
                    try:
                        with report_file.open("w", encoding="utf-8") as f:
                            json.dump(existing_reports, f, indent=2)
                    except:
                        pass

                except Exception as e:
                    failed += 1
                    issues_list.append({"symbol": symbol, "reason": f"Exception: {e}"})
                    log_and_print(f"Error processing {symbol}: {e}", "ERROR")

            # === Save issues ===
            issues_path = file_path.parent / ISSUES_FILE
            try:
                existing_issues = json.load(issues_path.open("r", encoding="utf-8")) if issues_path.exists() else []
                with issues_path.open("w", encoding="utf-8") as f:
                    json.dump(existing_issues + issues_list, f, indent=2)
            except:
                pass

            mt5.shutdown()
            log_and_print(
                f"{user_brokerid} DONE → Placed: {placed}, Failed: {failed}, Skipped: {skipped}",
                "SUCCESS"
            )

        log_and_print("All $12–$20 accounts processed.", "SUCCESS")
        return True

    def _0_50cent_usd_history_and_deduplication():
        """
        HISTORY + PENDING + POSITION DUPLICATE DETECTOR + RISK SNIPER
        - Cancels risk > $0.60  (even if TP=0)
        - Cancels HISTORY DUPLICATES
        - Cancels PENDING LIMIT DUPLICATES
        - Cancels PENDING if POSITION already exists
        - Shows duplicate market name on its own line
        ONLY PROCESSES ACCOUNTS WITH BALANCE $12.00 – $19.99
        """
        BASE_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
        REPORT_NAME = "pending_risk_profit_per_order.json"
        MAX_RISK_USD = 0.60
        LOOKBACK_DAYS = 5
        PRICE_PRECISION = 5
        TZ = pytz.timezone("Africa/Lagos")

        five_days_ago = datetime.now(TZ) - timedelta(days=LOOKBACK_DAYS)

        for user_brokerid, cfg in usersdictionary.items():
            TERMINAL_PATH = cfg["TERMINAL_PATH"]
            LOGIN_ID     = cfg["LOGIN_ID"]
            PASSWORD     = cfg["PASSWORD"]
            SERVER       = cfg["SERVER"]

            log_and_print(f"\n{'='*80}", "INFO")
            log_and_print(f"BROKER: {user_brokerid.upper()} | FULL DUPLICATE + RISK GUARD", "INFO")
            log_and_print(f"{'='*80}", "INFO")

            # ---------- MT5 Init ----------
            if not os.path.exists(TERMINAL_PATH):
                log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
                continue
            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=30000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue
            if not mt5.login(int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"Login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account = mt5.account_info()
            if not account:
                log_and_print("No account info.", "ERROR")
                mt5.shutdown()
                continue

            balance = account.balance
            equity = account.equity
            log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f}", "INFO")
            if equity < 0.50 and balance >= 0.50:
                log_and_print(f"Equity ${equity:.2f} < $0.50 while Balance ${balance:.2f} ≥ $0.50 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if equity >= 0.50 and balance < 0.50:
                log_and_print(f"Equity ${equity:.2f} > $0.50 while Balance ${balance:.2f} < $0.50 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if not (0.50 <= balance < 3.99):
                log_and_print(f"Balance ${balance:.2f} not in $0.50–$3.99 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue

            currency = account.currency
            log_and_print(f"Account: {account.login} | Balance: ${balance:.2f} {currency} → Proceeding with risk_0_50cent_usd checks", "INFO")

            # ---------- Get Data ----------
            pending_orders = [o for o in (mt5.orders_get() or [])
                            if o.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT)]
            positions = mt5.positions_get()
            history_deals = mt5.history_deals_get(int(five_days_ago.timestamp()), int(datetime.now(TZ).timestamp()))

            if not pending_orders:
                log_and_print("No pending orders.", "INFO")
                mt5.shutdown()
                continue

            # ---------- BUILD DATABASES ----------
            log_and_print(f"Building duplicate databases...", "INFO")

            # 1. Historical Setups
            historical_keys = {}  # (symbol, entry, sl) → details
            if history_deals:
                for deal in history_deals:
                    if deal.entry != mt5.DEAL_ENTRY_IN: continue
                    if deal.type not in (mt5.DEAL_TYPE_BUY, mt5.DEAL_TYPE_SELL): continue

                    order = mt5.history_orders_get(ticket=deal.order)
                    if not order: continue
                    order = order[0]
                    if order.sl == 0: continue

                    symbol = deal.symbol
                    entry = round(deal.price, PRICE_PRECISION)
                    sl = round(order.sl, PRICE_PRECISION)

                    key = (symbol, entry, sl)
                    if key not in historical_keys:
                        profit = sum(d.profit for d in history_deals if d.order == deal.order and d.entry == mt5.DEAL_ENTRY_OUT)
                        historical_keys[key] = {
                            "time": datetime.fromtimestamp(deal.time, TZ).strftime("%Y-%m-%d %H:%M"),
                            "profit": round(profit, 2),
                            "symbol": symbol
                        }

            # 2. Open Positions (by symbol)
            open_symbols = {pos.symbol for pos in positions} if positions else set()

            # 3. Pending Orders Key Map
            pending_keys = {}  # (symbol, entry, sl) → [order_tickets]
            for order in pending_orders:
                key = (order.symbol, round(order.price_open, PRICE_PRECISION), round(order.sl, PRICE_PRECISION))
                pending_keys.setdefault(key, []).append(order.ticket)

            log_and_print(f"Loaded: {len(historical_keys)} history | {len(open_symbols)} open | {len(pending_keys)} unique pending setups", "INFO")

            # ---------- Process & Cancel ----------
            per_order_data = []
            kept = cancelled_risk = cancelled_hist = cancelled_pend_dup = cancelled_pos_dup = skipped = 0

            for order in pending_orders:
                symbol = order.symbol
                ticket = order.ticket
                volume = order.volume_current
                entry = round(order.price_open, PRICE_PRECISION)
                sl = round(order.sl, PRICE_PRECISION)
                tp = order.tp                     # may be 0

                # ---- NEW: ONLY REQUIRE SL, TP CAN BE 0 ----
                if sl == 0:
                    log_and_print(f"SKIP {ticket} | {symbol} | No SL", "WARNING")
                    skipped += 1
                    continue

                info = mt5.symbol_info(symbol)
                if not info or not mt5.symbol_info_tick(symbol):
                    log_and_print(f"SKIP {ticket} | {symbol} | No symbol data", "WARNING")
                    skipped += 1
                    continue

                point = info.point
                contract = info.trade_contract_size
                point_val = contract * point
                if "JPY" in symbol and currency == "USD":
                    point_val /= 100

                # ---- RISK CALCULATION (always possible with SL) ----
                risk_points = abs(entry - sl) / point
                risk_usd = risk_points * point_val * volume
                if currency != "USD":
                    rate = mt5.symbol_info_tick(f"USD{currency}")
                    if not rate:
                        log_and_print(f"SKIP {ticket} | No USD{currency} rate", "WARNING")
                        skipped += 1
                        continue
                    risk_usd /= rate.bid

                # ---- PROFIT CALCULATION (only if TP exists) ----
                profit_usd = None
                if tp != 0:
                    profit_usd = abs(tp - entry) / point * point_val * volume
                    if currency != "USD":
                        profit_usd /= rate.bid

                # ---- DUPLICATE KEYS ----
                key = (symbol, entry, sl)
                dup_hist = historical_keys.get(key)
                is_position_open = symbol in open_symbols
                is_pending_duplicate = len(pending_keys.get(key, [])) > 1

                print(f"\nmarket: {symbol}")
                print(f"risk: {risk_usd:.2f} USD | profit: {profit_usd if profit_usd is not None else 'N/A'} USD")

                cancel_reason = None
                cancel_type = None

                # === 1. RISK CANCEL (works even if TP=0) ===
                if risk_usd > MAX_RISK_USD:
                    cancel_reason = f"RISK > ${MAX_RISK_USD}"
                    cancel_type = "RISK"
                    print(f"{cancel_reason} → CANCELLED")

                # === 2. HISTORY DUPLICATE ===
                elif dup_hist:
                    cancel_reason = "HISTORY DUPLICATE"
                    cancel_type = "HIST_DUP"
                    print("HISTORY DUPLICATE ORDER FOUND!")
                    print(dup_hist["symbol"])
                    print(f"entry: {entry} | sl: {sl}")
                    print(f"used: {dup_hist['time']} | P/L: {dup_hist['profit']:+.2f} {currency}")
                    print("→ HISTORY DUPLICATE CANCELLED")
                    print("!" * 60)

                # === 3. PENDING DUPLICATE ===
                elif is_pending_duplicate:
                    cancel_reason = "PENDING DUPLICATE"
                    cancel_type = "PEND_DUP"
                    print("PENDING LIMIT DUPLICATE FOUND!")
                    print(symbol)
                    print(f"→ DUPLICATE PENDING ORDER CANCELLED")
                    print("-" * 60)

                # === 4. POSITION EXISTS (Cancel Pending) ===
                elif is_position_open:
                    cancel_reason = "POSITION ALREADY OPEN"
                    cancel_type = "POS_DUP"
                    print("POSITION ALREADY RUNNING!")
                    print(symbol)
                    print(f"→ PENDING ORDER CANCELLED (POSITION ACTIVE)")
                    print("^" * 60)

                # === NO ISSUE → KEEP ===
                else:
                    print("No duplicate. Order kept.")
                    kept += 1
                    per_order_data.append({
                        "ticket": ticket,
                        "symbol": symbol,
                        "entry": entry,
                        "sl": sl,
                        "tp": tp,
                        "risk_usd": round(risk_usd, 2),
                        "profit_usd": round(profit_usd, 2) if profit_usd is not None else None,
                        "status": "KEPT"
                    })
                    continue  # Skip cancel

                # === CANCEL ORDER ===
                req = {"action": mt5.TRADE_ACTION_REMOVE, "order": ticket}
                res = mt5.order_send(req)
                if res.retcode == mt5.TRADE_RETCODE_DONE:
                    log_and_print(f"{cancel_type} CANCELLED {ticket} | {symbol} | {cancel_reason}", "WARNING")
                    if cancel_type == "RISK": cancelled_risk += 1
                    elif cancel_type == "HIST_DUP": cancelled_hist += 1
                    elif cancel_type == "PEND_DUP": cancelled_pend_dup += 1
                    elif cancel_type == "POS_DUP": cancelled_pos_dup += 1
                else:
                    log_and_print(f"CANCEL FAILED {ticket} | {res.comment}", "ERROR")

                per_order_data.append({
                    "ticket": ticket,
                    "symbol": symbol,
                    "entry": entry,
                    "sl": sl,
                    "tp": tp,
                    "risk_usd": round(risk_usd, 2),
                    "profit_usd": round(profit_usd, 2) if profit_usd is not None else None,
                    "status": "CANCELLED",
                    "reason": cancel_reason,
                    "duplicate_time": dup_hist["time"] if dup_hist else None,
                    "duplicate_pl": dup_hist["profit"] if dup_hist else None
                })

            # === SUMMARY ===
            log_and_print(f"\nSUMMARY:", "SUCCESS")
            log_and_print(f"KEPT: {kept}", "INFO")
            log_and_print(f"CANCELLED → RISK: {cancelled_risk} | HIST DUP: {cancelled_hist} | "
                        f"PEND DUP: {cancelled_pend_dup} | POS DUP: {cancelled_pos_dup} | SKIPPED: {skipped}", "WARNING")

            # === SAVE REPORT ===
            out_dir = Path(BASE_DIR) / user_brokerid / "risk_0_50cent_usd"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / REPORT_NAME

            report = {
                "broker": user_brokerid,
                "checked_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
                "max_risk_usd": MAX_RISK_USD,
                "lookback_days": LOOKBACK_DAYS,
                "summary": {
                    "kept": kept,
                    "cancelled_risk": cancelled_risk,
                    "cancelled_history_duplicate": cancelled_hist,
                    "cancelled_pending_duplicate": cancelled_pend_dup,
                    "cancelled_position_duplicate": cancelled_pos_dup,
                    "skipped": skipped
                },
                "orders": per_order_data
            }

            try:
                with out_path.open("w", encoding="utf-8") as f:
                    json.dump(report, f, indent=2)
                log_and_print(f"Report saved: {out_path}", "SUCCESS")
            except Exception as e:
                log_and_print(f"Save error: {e}", "ERROR")

            mt5.shutdown()

        log_and_print("\nALL $12–$20 ACCOUNTS: DUPLICATE SCAN + RISK GUARD = DONE", "SUCCESS")
        return True

    def _0_50cent_usd_ratio_levels():
        """
        0_50cent_usd RATIO LEVELS + TP UPDATE (PENDING + RUNNING POSITIONS) – BROKER-SAFE
        - Balance $12–$19.99 only
        - Auto-supports riskreward: 1, 2, 3, 4... (any integer)
        - Case-insensitive config
        - consistency → Dynamic TP = RISKREWARD × Risk
        - martingale → TP = 1R (always), ignores RISKREWARD
        - Smart ratio ladder (shows 1R, 2R, 3R only when needed)
        """
        TZ = pytz.timezone("Africa/Lagos")

        log_and_print(f"\n{'='*80}", "INFO")
        log_and_print("0_50cent_usd RATIO LEVELS + TP UPDATE (PENDING + RUNNING) – CONSISTENCY: N×R | MARTINGALE: 1R", "INFO")
        log_and_print(f"{'='*80}", "INFO")

        for user_brokerid, cfg in usersdictionary.items():
            TERMINAL_PATH = cfg.get("TERMINAL_PATH") or cfg.get("terminal_path")
            LOGIN_ID      = cfg.get("LOGIN_ID")      or cfg.get("login_id")
            PASSWORD      = cfg.get("PASSWORD")      or cfg.get("password")
            SERVER        = cfg.get("SERVER")        or cfg.get("server")
            SCALE         = (cfg.get("SCALE")        or cfg.get("scale")        or "").strip().lower()
            STRATEGY      = (cfg.get("STRATEGY")    or cfg.get("strategy")    or "").strip().lower()

            # === Case-insensitive riskreward lookup ===
            riskreward_raw = None
            for key in cfg:
                if key.lower() == "riskreward":
                    riskreward_raw = cfg[key]
                    break

            if riskreward_raw is None:
                riskreward_raw = 2
                log_and_print(f"{user_brokerid}: 'riskreward' not found → using default 2R", "WARNING")

            log_and_print(
                f"\nProcessing broker: {user_brokerid} | Scale: {SCALE.upper()} | "
                f"Strategy: {STRATEGY.upper()} | riskreward: {riskreward_raw}R", "INFO"
            )

            # === Validate required fields ===
            missing = []
            for f in ("TERMINAL_PATH", "LOGIN_ID", "PASSWORD", "SERVER", "SCALE"):
                if not locals()[f]: missing.append(f)
            if missing:
                log_and_print(f"Missing config: {', '.join(missing)} → SKIPPED", "ERROR")
                continue

            # === MT5 Init ===
            if not os.path.exists(TERMINAL_PATH):
                log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
                continue

            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD,
                                server=SERVER, timeout=30000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue

            if not mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"MT5 login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account_info = mt5.account_info()
            if not account_info:
                log_and_print(f"Failed to get account info: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            balance = account_info.balance
            if not (0.50 <= balance < 3.99):
                log_and_print(f"Balance ${balance:.2f} not in $12–$20 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue

            log_and_print(f"Balance: ${balance:.2f} → Scanning positions & pending orders...", "INFO")

            # === Determine effective RR ===
            try:
                config_rr = int(float(riskreward_raw))
                if config_rr < 1: config_rr = 1
            except (ValueError, TypeError):
                config_rr = 2
                log_and_print(f"Invalid riskreward '{riskreward_raw}' → using 2R", "WARNING")

            effective_rr = 1 if SCALE == "martingale" else config_rr
            rr_source = "MARTINGALE (forced 1R)" if SCALE == "martingale" else f"CONFIG ({effective_rr}R)"
            log_and_print(f"Effective TP: {effective_rr}R [{rr_source}]", "INFO")

            # ------------------------------------------------------------------ #
            # 1. PENDING LIMIT ORDERS
            # ------------------------------------------------------------------ #
            pending_orders = [
                o for o in (mt5.orders_get() or [])
                if o.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT)
                and getattr(o, 'sl', 0) != 0 and getattr(o, 'tp', 0) != 0
            ]

            # ------------------------------------------------------------------ #
            # 2. RUNNING POSITIONS
            # ------------------------------------------------------------------ #
            running_positions = [
                p for p in (mt5.positions_get() or [])
                if p.type in (mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_SELL)
                and p.sl != 0 and p.tp != 0
            ]

            # Merge into a single iterable with a flag
            items_to_process = []
            for o in pending_orders:
                items_to_process.append(('PENDING', o))
            for p in running_positions:
                items_to_process.append(('RUNNING', p))

            if not items_to_process:
                log_and_print("No valid pending orders or running positions found.", "INFO")
                mt5.shutdown()
                continue

            log_and_print(f"Found {len(pending_orders)} pending + {len(running_positions)} running → total {len(items_to_process)}", "INFO")

            processed_symbols = set()
            updated_count = 0

            for kind, obj in items_to_process:
                symbol   = obj.symbol
                ticket   = getattr(obj, 'ticket', None) or getattr(obj, 'order', None)
                entry_price = getattr(obj, 'price_open', None) or getattr(obj, 'price_current', None)
                sl_price = obj.sl
                current_tp = obj.tp
                is_buy   = obj.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY)

                if symbol in processed_symbols:
                    continue

                risk_distance = abs(entry_price - sl_price)
                if risk_distance <= 0:
                    log_and_print(f"Zero risk distance on {symbol} ({kind}) → skipped", "WARNING")
                    continue

                symbol_info = mt5.symbol_info(symbol)
                if not symbol_info:
                    log_and_print(f"Symbol info missing: {symbol}", "WARNING")
                    continue

                digits = symbol_info.digits
                def r(p): return round(p, digits)

                entry_price = r(entry_price)
                sl_price    = r(sl_price)
                current_tp  = r(current_tp)
                direction   = 1 if is_buy else -1
                target_tp   = r(entry_price + direction * effective_rr * risk_distance)

                # ----- Ratio ladder (display only) -----
                ratio1 = r(entry_price + direction * 1 * risk_distance)
                ratio2 = r(entry_price + direction * 2 * risk_distance)
                ratio3 = r(entry_price + direction * 3 * risk_distance) if effective_rr >= 3 else None

                print(f"\n{symbol} | {kind} | Target: {effective_rr}R ({SCALE.upper()})")
                print(f"  Entry : {entry_price}")
                print(f"  1R    : {ratio1}")
                print(f"  2R    : {ratio2}")
                if ratio3:
                    print(f"  3R    : {ratio3}")
                print(f"  TP    : {current_tp} → ", end="")

                # ----- Modify TP -----
                tolerance = 10 ** -digits
                if abs(current_tp - target_tp) > tolerance:
                    if kind == "PENDING":
                        # modify pending order
                        request = {
                            "action": mt5.TRADE_ACTION_MODIFY,
                            "order": ticket,
                            "price": entry_price,
                            "sl": sl_price,
                            "tp": target_tp,
                            "type": obj.type,
                            "type_time": obj.type_time,
                            "type_filling": obj.type_filling,
                            "magic": getattr(obj, 'magic', 0),
                            "comment": getattr(obj, 'comment', "")
                        }
                        if hasattr(obj, 'expiration') and obj.expiration:
                            request["expiration"] = obj.expiration
                    else:  # RUNNING
                        # modify open position (SL/TP only)
                        request = {
                            "action": mt5.TRADE_ACTION_SLTP,
                            "position": ticket,
                            "sl": sl_price,
                            "tp": target_tp,
                            "symbol": symbol
                        }

                    result = mt5.order_send(request)
                    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                        print(f"{target_tp} [UPDATED]")
                        log_and_print(
                            f"TP → {effective_rr}R | {symbol} | {kind} | {current_tp} → {target_tp} [{SCALE.upper()}]",
                            "SUCCESS"
                        )
                        updated_count += 1
                    else:
                        err = result.comment if result else "Unknown"
                        print(f"{current_tp} [FAILED: {err}]")
                        log_and_print(f"TP UPDATE FAILED | {symbol} | {kind} | {err}", "ERROR")
                else:
                    print(f"{current_tp} [OK]")

                print(f"  SL    : {sl_price}")
                processed_symbols.add(symbol)

            mt5.shutdown()
            log_and_print(
                f"{user_brokerid} → {len(processed_symbols)} symbol(s) | "
                f"{updated_count} TP(s) set to {effective_rr}R [{SCALE.upper()}]",
                "SUCCESS"
            )

        log_and_print(
            "\nALL $12–$20 ACCOUNTS: R:R UPDATE (PENDING + RUNNING) – "
            "consistency=N×R, martingale=1R = DONE",
            "SUCCESS"
        )
        return True
    _0_50cent_usd_live_sl_tp_amounts()
    place_0_50cent_usd_orders()
    _0_50cent_usd_history_and_deduplication()
    _0_50cent_usd_ratio_levels()

def _4_8_orders():
    def _1usd_live_sl_tp_amounts():
        BASE_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
        INPUT_FILE = "hightolow.json"
        OUTPUT_FILE = "live_risk_profit_all.json"

        for user_brokerid, cfg in usersdictionary.items():
            TERMINAL_PATH = cfg["TERMINAL_PATH"]
            LOGIN_ID = cfg["LOGIN_ID"]
            PASSWORD = cfg["PASSWORD"]
            SERVER = cfg["SERVER"]

            log_and_print(f"\n{'='*60}", "INFO")
            log_and_print(f"PROCESSING BROKER: {user_brokerid.upper()}", "INFO")
            log_and_print(f"{'='*60}", "INFO")

            # ------------------- CONNECT TO MT5 -------------------
            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=60000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue
            if not mt5.login(int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"Login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account = mt5.account_info()
            if not account:
                log_and_print("No account info", "ERROR")
                mt5.shutdown()
                continue

            balance = account.balance
            if not (4.0 <= balance < 7.99):
                log_and_print(f"Balance ${balance:.2f} not in $12–$20 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue

            currency = account.currency
            log_and_print(f"Connected → Balance: ${balance:.2f} {currency}", "INFO")

            # ------------------- LOAD JSON -------------------
            json_path = Path(BASE_DIR) / user_brokerid / "risk_1_usd" / INPUT_FILE
            if not json_path.exists():
                log_and_print(f"JSON not found: {json_path}", "ERROR")
                mt5.shutdown()
                continue

            try:
                with json_path.open("r", encoding="utf-8") as f:
                    original_data = json.load(f)
                entries = original_data.get("entries", [])
            except Exception as e:
                log_and_print(f"Failed to read JSON: {e}", "ERROR")
                mt5.shutdown()
                continue

            if not entries:
                log_and_print("No entries in JSON.", "INFO")
                mt5.shutdown()
                continue

            log_and_print(f"Loaded {len(entries)} entries → Calculating LIVE risk...", "INFO")

            # ------------------- PROCESS & FILTER -------------------
            valid_entries = []        # For overwriting hightolow.json
            results = []              # For live_risk_profit_all.json
            total = len(entries)
            kept = 0
            removed = 0

            for i, entry in enumerate(entries, 1):
                market = entry["market"]
                try:
                    price = float(entry["entry_price"])
                    sl = float(entry["sl_price"])
                    tp = float(entry["tp_price"])
                    volume = float(entry["volume"])
                    order_type = entry["limit_order"]
                    sl_pips = float(entry.get("sl_pips", 0))
                    tp_pips = float(entry.get("tp_pips", 0))

                    # --- LIVE DATA ---
                    info = mt5.symbol_info(market)
                    tick = mt5.symbol_info_tick(market)

                    if not info or not tick:
                        log_and_print(f"NO LIVE DATA for {market} → Using fallback", "WARNING")
                        pip_value = 0.1
                        risk_usd = volume * sl_pips * pip_value
                        profit_usd = volume * tp_pips * pip_value
                    else:
                        point = info.point
                        contract = info.trade_contract_size

                        risk_points = abs(price - sl) / point
                        profit_points = abs(tp - price) / point

                        point_val = contract * point
                        if "JPY" in market and currency == "USD":
                            point_val /= 100

                        risk_ac = risk_points * point_val * volume
                        profit_ac = profit_points * point_val * volume

                        risk_usd = risk_ac
                        profit_usd = profit_ac

                        if currency != "USD":
                            conv = f"USD{currency}"
                            rate_tick = mt5.symbol_info_tick(conv)
                            rate = rate_tick.bid if rate_tick else 1.0
                            risk_usd /= rate
                            profit_usd /= rate

                    risk_usd = round(risk_usd, 2)
                    profit_usd = round(profit_usd, 2)

                    # --- PRINT ALL ---
                    print(f"market: {market}")
                    print(f"risk: {risk_usd} USD")
                    print(f"profit: {profit_usd} USD")
                    print("---")

                    # --- FILTER: KEEP ONLY <= 1.10 ---
                    if risk_usd <= 1.10:
                        # Keep in BOTH files
                        valid_entries.append(entry)  # Original format
                        results.append({
                            "market": market,
                            "order_type": order_type,
                            "entry_price": round(price, 6),
                            "sl": round(sl, 6),
                            "tp": round(tp, 6),
                            "volume": round(volume, 5),
                            "live_risk_usd": risk_usd,
                            "live_profit_usd": profit_usd,
                            "sl_pips": round(sl_pips, 2),
                            "tp_pips": round(tp_pips, 2),
                            "has_live_tick": bool(info and tick),
                            "current_bid": round(tick.bid, 6) if tick else None,
                            "current_ask": round(tick.ask, 6) if tick else None,
                        })
                        kept += 1
                    else:
                        removed += 1
                        log_and_print(f"REMOVED {market}: live risk ${risk_usd} > $1.10 → DELETED FROM BOTH JSON FILES", "WARNING")

                except Exception as e:
                    log_and_print(f"ERROR on {market}: {e}", "ERROR")
                    removed += 1

                if i % 5 == 0 or i == total:
                    log_and_print(f"Processed {i}/{total} | Kept: {kept} | Removed: {removed}", "INFO")

            # ------------------- SAVE OUTPUT: live_risk_profit_all.json -------------------
            out_path = json_path.parent / OUTPUT_FILE
            report = {
                "broker": user_brokerid,
                "account_currency": currency,
                "generated_at": datetime.now(pytz.timezone("Africa/Lagos")).strftime("%Y-%m-%d %H:%M:%S.%f%z"),
                "source_file": str(json_path),
                "total_entries": total,
                "kept_risk_<=_1.10": kept,
                "removed_risk_>_1.10": removed,
                "filter_applied": "Delete from both input & output if live_risk_usd > 1.10",
                "orders": results
            }

            try:
                with out_path.open("w", encoding="utf-8") as f:
                    json.dump(report, f, indent=2)
                log_and_print(f"SAVED → {out_path} | Kept: {kept} | Removed: {removed}", "SUCCESS")
            except Exception as e:
                log_and_print(f"Save failed: {e}", "ERROR")

            # ------------------- OVERWRITE INPUT: hightolow.json -------------------
            cleaned_input = original_data.copy()
            cleaned_input["entries"] = valid_entries  # Only good ones

            try:
                with json_path.open("w", encoding="utf-8") as f:
                    json.dump(cleaned_input, f, indent=2)
                log_and_print(f"OVERWRITTEN → {json_path} | Now has {len(valid_entries)} entries (removed {removed})", "SUCCESS")
            except Exception as e:
                log_and_print(f"Failed to overwrite input JSON: {e}", "ERROR")

            mt5.shutdown()
            log_and_print(f"FINISHED {user_brokerid} → {kept}/{total} valid orders in BOTH files", "SUCCESS")

        log_and_print("\nALL DONE – BAD ORDERS (> $1.10) DELETED FROM INPUT & OUTPUT!", "SUCCESS")
        return True
    
    def place_1usd_orders():
        

        BASE_INPUT_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
        RISK_FOLDER = "risk_1_usd"
        STRATEGY_FILE = "hightolow.json"
        REPORT_SUFFIX = "forex_order_report.json"
        ISSUES_FILE = "ordersissues.json"

        for user_brokerid, broker_cfg in usersdictionary.items():
            TERMINAL_PATH = broker_cfg["TERMINAL_PATH"]
            LOGIN_ID = broker_cfg["LOGIN_ID"]
            PASSWORD = broker_cfg["PASSWORD"]
            SERVER = broker_cfg["SERVER"]

            log_and_print(f"Processing broker: {user_brokerid} (Balance $12–$20 mode)", "INFO")

            # === MT5 Init ===
            if not os.path.exists(TERMINAL_PATH):
                log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
                continue

            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=30000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue

            if not mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"MT5 login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue


            account_info = mt5.account_info()
            if not account_info:
                log_and_print(f"Failed to get account info: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue
            balance = account_info.balance
            equity = account_info.equity
            log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f}", "INFO")
            if equity < 4.0 and balance >= 4.0:
                log_and_print(f"Equity ${equity:.2f} < $4.0 while Balance ${balance:.2f} ≥ $4.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if equity >= 4.0 and balance < 4.0:
                log_and_print(f"Equity ${equity:.2f} > $4.0 while Balance ${balance:.2f} < $4.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if not (4.0 <= balance < 7.99):
                log_and_print(f"Balance ${balance:.2f} not in $4–$7.99 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue
            # === Only reaches here if: equity >= 8 AND balance in [8, 11.99) ===
            log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f} → Using {RISK_FOLDER} + {STRATEGY_FILE}", "INFO")

            

            log_and_print(f"Balance: ${balance:.2f} → Using {RISK_FOLDER} + {STRATEGY_FILE}", "INFO")

            # === Load hightolow.json ===
            file_path = Path(BASE_INPUT_DIR) / user_brokerid / RISK_FOLDER / STRATEGY_FILE
            if not file_path.exists():
                log_and_print(f"File not found: {file_path}", "WARNING")
                mt5.shutdown()
                continue

            try:
                with file_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                    entries = data.get("entries", [])
            except Exception as e:
                log_and_print(f"Failed to read {file_path}: {e}", "ERROR")
                mt5.shutdown()
                continue

            if not entries:
                log_and_print("No entries in hightolow.json", "INFO")
                mt5.shutdown()
                continue

            # === Load existing orders & positions ===
            existing_pending = {}  # (symbol, type) → ticket
            running_positions = set()  # symbols with open position

            for order in (mt5.orders_get() or []):
                if order.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT):
                    existing_pending[(order.symbol, order.type)] = order.ticket

            for pos in (mt5.positions_get() or []):
                running_positions.add(pos.symbol)

            # === Reporting ===
            report_file = file_path.parent / REPORT_SUFFIX
            existing_reports = json.load(report_file.open("r", encoding="utf-8")) if report_file.exists() else []
            issues_list = []
            now_str = datetime.now(pytz.timezone("Africa/Lagos")).strftime("%Y-%m-%d %H:%M:%S.%f+01:00")
            placed = failed = skipped = 0

            for entry in entries:
                try:
                    symbol = entry["market"]
                    price = float(entry["entry_price"])
                    sl = float(entry["sl_price"])
                    tp = float(entry["tp_price"])
                    volume = float(entry["volume"])
                    order_type_str = entry["limit_order"]
                    order_type = mt5.ORDER_TYPE_BUY_LIMIT if order_type_str == "buy_limit" else mt5.ORDER_TYPE_SELL_LIMIT

                    # === SKIP: Already running or pending ===
                    if symbol in running_positions:
                        skipped += 1
                        log_and_print(f"{symbol} has running position → SKIPPED", "INFO")
                        continue

                    key = (symbol, order_type)
                    if key in existing_pending:
                        skipped += 1
                        log_and_print(f"{symbol} {order_type_str} already pending → SKIPPED", "INFO")
                        continue

                    # === Symbol check ===
                    symbol_info = mt5.symbol_info(symbol)
                    if not symbol_info or not symbol_info.visible:
                        issues_list.append({"symbol": symbol, "reason": "Symbol not available"})
                        failed += 1
                        continue

                    # === Volume fix ===
                    vol_step = symbol_info.volume_step
                    volume = max(symbol_info.volume_min,
                                round(volume / vol_step) * vol_step)
                    volume = min(volume, symbol_info.volume_max)

                    # === Price distance check ===
                    tick = mt5.symbol_info_tick(symbol)
                    if not tick:
                        issues_list.append({"symbol": symbol, "reason": "No tick data"})
                        failed += 1
                        continue

                    point = symbol_info.point
                    if order_type == mt5.ORDER_TYPE_BUY_LIMIT:
                        if price >= tick.ask or (tick.ask - price) < 10 * point:
                            skipped += 1
                            continue
                    else:
                        if price <= tick.bid or (price - tick.bid) < 10 * point:
                            skipped += 1
                            continue

                    # === Build & send order ===
                    request = {
                        "action": mt5.TRADE_ACTION_PENDING,
                        "symbol": symbol,
                        "volume": volume,
                        "type": order_type,
                        "price": price,
                        "sl": sl,
                        "tp": tp,
                        "deviation": 10,
                        "magic": 123456,
                        "comment": "Risk3_Auto",
                        "type_time": mt5.ORDER_TIME_GTC,
                        "type_filling": mt5.ORDER_FILLING_IOC,
                    }

                    result = mt5.order_send(request)
                    if result is None:
                        result = type('obj', (), {'retcode': 10000, 'comment': 'order_send returned None'})()

                    success = result.retcode == mt5.TRADE_RETCODE_DONE
                    if success:
                        existing_pending[key] = result.order
                        placed += 1
                        log_and_print(f"{symbol} {order_type_str} @ {price} → PLACED (ticket {result.order})", "SUCCESS")
                    else:
                        failed += 1
                        issues_list.append({"symbol": symbol, "reason": result.comment})

                    # === Report ===
                    if "cent" in RISK_FOLDER:
                        risk_usd = 0.5
                    else:
                        risk_usd = float(RISK_FOLDER.split("_")[1].replace("usd", ""))

                    # === Report ===
                    report_entry = {
                        "symbol": symbol,
                        "order_type": order_type_str,
                        "price": price,
                        "volume": volume,
                        "sl": sl,
                        "tp": tp,
                        "risk_usd": risk_usd,   # ← Now correct: 0.5, 1.0, 2.0, 3.0, 4.0
                        "ticket": result.order if success else None,
                        "success": success,
                        "error_code": result.retcode if not success else None,
                        "error_msg": result.comment if not success else None,
                        "timestamp": now_str
                    }
                    existing_reports.append(report_entry)
                    try:
                        with report_file.open("w", encoding="utf-8") as f:
                            json.dump(existing_reports, f, indent=2)
                    except:
                        pass

                except Exception as e:
                    failed += 1
                    issues_list.append({"symbol": symbol, "reason": f"Exception: {e}"})
                    log_and_print(f"Error processing {symbol}: {e}", "ERROR")

            # === Save issues ===
            issues_path = file_path.parent / ISSUES_FILE
            try:
                existing_issues = json.load(issues_path.open("r", encoding="utf-8")) if issues_path.exists() else []
                with issues_path.open("w", encoding="utf-8") as f:
                    json.dump(existing_issues + issues_list, f, indent=2)
            except:
                pass

            mt5.shutdown()
            log_and_print(
                f"{user_brokerid} DONE → Placed: {placed}, Failed: {failed}, Skipped: {skipped}",
                "SUCCESS"
            )

        log_and_print("All $12–$20 accounts processed.", "SUCCESS")
        return True

    def _1usd_history_and_deduplication():
        """
        HISTORY + PENDING + POSITION DUPLICATE DETECTOR + RISK SNIPER
        - Cancels risk > $1.10  (even if TP=0)
        - Cancels HISTORY DUPLICATES
        - Cancels PENDING LIMIT DUPLICATES
        - Cancels PENDING if POSITION already exists
        - Shows duplicate market name on its own line
        ONLY PROCESSES ACCOUNTS WITH BALANCE $12.00 – $19.99
        """
        BASE_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
        REPORT_NAME = "pending_risk_profit_per_order.json"
        MAX_RISK_USD = 1.10
        LOOKBACK_DAYS = 5
        PRICE_PRECISION = 5
        TZ = pytz.timezone("Africa/Lagos")

        five_days_ago = datetime.now(TZ) - timedelta(days=LOOKBACK_DAYS)

        for user_brokerid, cfg in usersdictionary.items():
            TERMINAL_PATH = cfg["TERMINAL_PATH"]
            LOGIN_ID     = cfg["LOGIN_ID"]
            PASSWORD     = cfg["PASSWORD"]
            SERVER       = cfg["SERVER"]

            log_and_print(f"\n{'='*80}", "INFO")
            log_and_print(f"BROKER: {user_brokerid.upper()} | FULL DUPLICATE + RISK GUARD", "INFO")
            log_and_print(f"{'='*80}", "INFO")

            # ---------- MT5 Init ----------
            if not os.path.exists(TERMINAL_PATH):
                log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
                continue
            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=30000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue
            if not mt5.login(int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"Login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account = mt5.account_info()
            if not account:
                log_and_print("No account info.", "ERROR")
                mt5.shutdown()
                continue

            balance = account.balance
            equity = account.equity
            log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f}", "INFO")
            if equity < 4.0 and balance >= 4.0:
                log_and_print(f"Equity ${equity:.2f} < $4.0 while Balance ${balance:.2f} ≥ $4.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if equity >= 4.0 and balance < 4.0:
                log_and_print(f"Equity ${equity:.2f} > $4.0 while Balance ${balance:.2f} < $4.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if not (4.0 <= balance < 7.99):
                log_and_print(f"Balance ${balance:.2f} not in $4–$7.99 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue

            currency = account.currency
            log_and_print(f"Account: {account.login} | Balance: ${balance:.2f} {currency} → Proceeding with risk_1_usd checks", "INFO")

            # ---------- Get Data ----------
            pending_orders = [o for o in (mt5.orders_get() or [])
                            if o.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT)]
            positions = mt5.positions_get()
            history_deals = mt5.history_deals_get(int(five_days_ago.timestamp()), int(datetime.now(TZ).timestamp()))

            if not pending_orders:
                log_and_print("No pending orders.", "INFO")
                mt5.shutdown()
                continue

            # ---------- BUILD DATABASES ----------
            log_and_print(f"Building duplicate databases...", "INFO")

            # 1. Historical Setups
            historical_keys = {}  # (symbol, entry, sl) → details
            if history_deals:
                for deal in history_deals:
                    if deal.entry != mt5.DEAL_ENTRY_IN: continue
                    if deal.type not in (mt5.DEAL_TYPE_BUY, mt5.DEAL_TYPE_SELL): continue

                    order = mt5.history_orders_get(ticket=deal.order)
                    if not order: continue
                    order = order[0]
                    if order.sl == 0: continue

                    symbol = deal.symbol
                    entry = round(deal.price, PRICE_PRECISION)
                    sl = round(order.sl, PRICE_PRECISION)

                    key = (symbol, entry, sl)
                    if key not in historical_keys:
                        profit = sum(d.profit for d in history_deals if d.order == deal.order and d.entry == mt5.DEAL_ENTRY_OUT)
                        historical_keys[key] = {
                            "time": datetime.fromtimestamp(deal.time, TZ).strftime("%Y-%m-%d %H:%M"),
                            "profit": round(profit, 2),
                            "symbol": symbol
                        }

            # 2. Open Positions (by symbol)
            open_symbols = {pos.symbol for pos in positions} if positions else set()

            # 3. Pending Orders Key Map
            pending_keys = {}  # (symbol, entry, sl) → [order_tickets]
            for order in pending_orders:
                key = (order.symbol, round(order.price_open, PRICE_PRECISION), round(order.sl, PRICE_PRECISION))
                pending_keys.setdefault(key, []).append(order.ticket)

            log_and_print(f"Loaded: {len(historical_keys)} history | {len(open_symbols)} open | {len(pending_keys)} unique pending setups", "INFO")

            # ---------- Process & Cancel ----------
            per_order_data = []
            kept = cancelled_risk = cancelled_hist = cancelled_pend_dup = cancelled_pos_dup = skipped = 0

            for order in pending_orders:
                symbol = order.symbol
                ticket = order.ticket
                volume = order.volume_current
                entry = round(order.price_open, PRICE_PRECISION)
                sl = round(order.sl, PRICE_PRECISION)
                tp = order.tp                     # may be 0

                # ---- NEW: ONLY REQUIRE SL, TP CAN BE 0 ----
                if sl == 0:
                    log_and_print(f"SKIP {ticket} | {symbol} | No SL", "WARNING")
                    skipped += 1
                    continue

                info = mt5.symbol_info(symbol)
                if not info or not mt5.symbol_info_tick(symbol):
                    log_and_print(f"SKIP {ticket} | {symbol} | No symbol data", "WARNING")
                    skipped += 1
                    continue

                point = info.point
                contract = info.trade_contract_size
                point_val = contract * point
                if "JPY" in symbol and currency == "USD":
                    point_val /= 100

                # ---- RISK CALCULATION (always possible with SL) ----
                risk_points = abs(entry - sl) / point
                risk_usd = risk_points * point_val * volume
                if currency != "USD":
                    rate = mt5.symbol_info_tick(f"USD{currency}")
                    if not rate:
                        log_and_print(f"SKIP {ticket} | No USD{currency} rate", "WARNING")
                        skipped += 1
                        continue
                    risk_usd /= rate.bid

                # ---- PROFIT CALCULATION (only if TP exists) ----
                profit_usd = None
                if tp != 0:
                    profit_usd = abs(tp - entry) / point * point_val * volume
                    if currency != "USD":
                        profit_usd /= rate.bid

                # ---- DUPLICATE KEYS ----
                key = (symbol, entry, sl)
                dup_hist = historical_keys.get(key)
                is_position_open = symbol in open_symbols
                is_pending_duplicate = len(pending_keys.get(key, [])) > 1

                print(f"\nmarket: {symbol}")
                print(f"risk: {risk_usd:.2f} USD | profit: {profit_usd if profit_usd is not None else 'N/A'} USD")

                cancel_reason = None
                cancel_type = None

                # === 1. RISK CANCEL (works even if TP=0) ===
                if risk_usd > MAX_RISK_USD:
                    cancel_reason = f"RISK > ${MAX_RISK_USD}"
                    cancel_type = "RISK"
                    print(f"{cancel_reason} → CANCELLED")

                # === 2. HISTORY DUPLICATE ===
                elif dup_hist:
                    cancel_reason = "HISTORY DUPLICATE"
                    cancel_type = "HIST_DUP"
                    print("HISTORY DUPLICATE ORDER FOUND!")
                    print(dup_hist["symbol"])
                    print(f"entry: {entry} | sl: {sl}")
                    print(f"used: {dup_hist['time']} | P/L: {dup_hist['profit']:+.2f} {currency}")
                    print("→ HISTORY DUPLICATE CANCELLED")
                    print("!" * 60)

                # === 3. PENDING DUPLICATE ===
                elif is_pending_duplicate:
                    cancel_reason = "PENDING DUPLICATE"
                    cancel_type = "PEND_DUP"
                    print("PENDING LIMIT DUPLICATE FOUND!")
                    print(symbol)
                    print(f"→ DUPLICATE PENDING ORDER CANCELLED")
                    print("-" * 60)

                # === 4. POSITION EXISTS (Cancel Pending) ===
                elif is_position_open:
                    cancel_reason = "POSITION ALREADY OPEN"
                    cancel_type = "POS_DUP"
                    print("POSITION ALREADY RUNNING!")
                    print(symbol)
                    print(f"→ PENDING ORDER CANCELLED (POSITION ACTIVE)")
                    print("^" * 60)

                # === NO ISSUE → KEEP ===
                else:
                    print("No duplicate. Order kept.")
                    kept += 1
                    per_order_data.append({
                        "ticket": ticket,
                        "symbol": symbol,
                        "entry": entry,
                        "sl": sl,
                        "tp": tp,
                        "risk_usd": round(risk_usd, 2),
                        "profit_usd": round(profit_usd, 2) if profit_usd is not None else None,
                        "status": "KEPT"
                    })
                    continue  # Skip cancel

                # === CANCEL ORDER ===
                req = {"action": mt5.TRADE_ACTION_REMOVE, "order": ticket}
                res = mt5.order_send(req)
                if res.retcode == mt5.TRADE_RETCODE_DONE:
                    log_and_print(f"{cancel_type} CANCELLED {ticket} | {symbol} | {cancel_reason}", "WARNING")
                    if cancel_type == "RISK": cancelled_risk += 1
                    elif cancel_type == "HIST_DUP": cancelled_hist += 1
                    elif cancel_type == "PEND_DUP": cancelled_pend_dup += 1
                    elif cancel_type == "POS_DUP": cancelled_pos_dup += 1
                else:
                    log_and_print(f"CANCEL FAILED {ticket} | {res.comment}", "ERROR")

                per_order_data.append({
                    "ticket": ticket,
                    "symbol": symbol,
                    "entry": entry,
                    "sl": sl,
                    "tp": tp,
                    "risk_usd": round(risk_usd, 2),
                    "profit_usd": round(profit_usd, 2) if profit_usd is not None else None,
                    "status": "CANCELLED",
                    "reason": cancel_reason,
                    "duplicate_time": dup_hist["time"] if dup_hist else None,
                    "duplicate_pl": dup_hist["profit"] if dup_hist else None
                })

            # === SUMMARY ===
            log_and_print(f"\nSUMMARY:", "SUCCESS")
            log_and_print(f"KEPT: {kept}", "INFO")
            log_and_print(f"CANCELLED → RISK: {cancelled_risk} | HIST DUP: {cancelled_hist} | "
                        f"PEND DUP: {cancelled_pend_dup} | POS DUP: {cancelled_pos_dup} | SKIPPED: {skipped}", "WARNING")

            # === SAVE REPORT ===
            out_dir = Path(BASE_DIR) / user_brokerid / "risk_1_usd"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / REPORT_NAME

            report = {
                "broker": user_brokerid,
                "checked_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
                "max_risk_usd": MAX_RISK_USD,
                "lookback_days": LOOKBACK_DAYS,
                "summary": {
                    "kept": kept,
                    "cancelled_risk": cancelled_risk,
                    "cancelled_history_duplicate": cancelled_hist,
                    "cancelled_pending_duplicate": cancelled_pend_dup,
                    "cancelled_position_duplicate": cancelled_pos_dup,
                    "skipped": skipped
                },
                "orders": per_order_data
            }

            try:
                with out_path.open("w", encoding="utf-8") as f:
                    json.dump(report, f, indent=2)
                log_and_print(f"Report saved: {out_path}", "SUCCESS")
            except Exception as e:
                log_and_print(f"Save error: {e}", "ERROR")

            mt5.shutdown()

        log_and_print("\nALL $12–$20 ACCOUNTS: DUPLICATE SCAN + RISK GUARD = DONE", "SUCCESS")
        return True

    def _1usd_ratio_levels():
        """
        1usd RATIO LEVELS + TP UPDATE (PENDING + RUNNING POSITIONS) – BROKER-SAFE
        - Balance $12–$19.99 only
        - Auto-supports riskreward: 1, 2, 3, 4... (any integer)
        - Case-insensitive config
        - consistency → Dynamic TP = RISKREWARD × Risk
        - martingale → TP = 1R (always), ignores RISKREWARD
        - Smart ratio ladder (shows 1R, 2R, 3R only when needed)
        """
        TZ = pytz.timezone("Africa/Lagos")

        log_and_print(f"\n{'='*80}", "INFO")
        log_and_print("1usd RATIO LEVELS + TP UPDATE (PENDING + RUNNING) – CONSISTENCY: N×R | MARTINGALE: 1R", "INFO")
        log_and_print(f"{'='*80}", "INFO")

        for user_brokerid, cfg in usersdictionary.items():
            TERMINAL_PATH = cfg.get("TERMINAL_PATH") or cfg.get("terminal_path")
            LOGIN_ID      = cfg.get("LOGIN_ID")      or cfg.get("login_id")
            PASSWORD      = cfg.get("PASSWORD")      or cfg.get("password")
            SERVER        = cfg.get("SERVER")        or cfg.get("server")
            SCALE         = (cfg.get("SCALE")        or cfg.get("scale")        or "").strip().lower()
            STRATEGY      = (cfg.get("STRATEGY")    or cfg.get("strategy")    or "").strip().lower()

            # === Case-insensitive riskreward lookup ===
            riskreward_raw = None
            for key in cfg:
                if key.lower() == "riskreward":
                    riskreward_raw = cfg[key]
                    break

            if riskreward_raw is None:
                riskreward_raw = 2
                log_and_print(f"{user_brokerid}: 'riskreward' not found → using default 2R", "WARNING")

            log_and_print(
                f"\nProcessing broker: {user_brokerid} | Scale: {SCALE.upper()} | "
                f"Strategy: {STRATEGY.upper()} | riskreward: {riskreward_raw}R", "INFO"
            )

            # === Validate required fields ===
            missing = []
            for f in ("TERMINAL_PATH", "LOGIN_ID", "PASSWORD", "SERVER", "SCALE"):
                if not locals()[f]: missing.append(f)
            if missing:
                log_and_print(f"Missing config: {', '.join(missing)} → SKIPPED", "ERROR")
                continue

            # === MT5 Init ===
            if not os.path.exists(TERMINAL_PATH):
                log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
                continue

            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD,
                                server=SERVER, timeout=30000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue

            if not mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"MT5 login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account_info = mt5.account_info()
            if not account_info:
                log_and_print(f"Failed to get account info: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            balance = account_info.balance
            if not (4.0 <= balance < 7.99):
                log_and_print(f"Balance ${balance:.2f} not in $12–$20 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue

            log_and_print(f"Balance: ${balance:.2f} → Scanning positions & pending orders...", "INFO")

            # === Determine effective RR ===
            try:
                config_rr = int(float(riskreward_raw))
                if config_rr < 1: config_rr = 1
            except (ValueError, TypeError):
                config_rr = 2
                log_and_print(f"Invalid riskreward '{riskreward_raw}' → using 2R", "WARNING")

            effective_rr = 1 if SCALE == "martingale" else config_rr
            rr_source = "MARTINGALE (forced 1R)" if SCALE == "martingale" else f"CONFIG ({effective_rr}R)"
            log_and_print(f"Effective TP: {effective_rr}R [{rr_source}]", "INFO")

            # ------------------------------------------------------------------ #
            # 1. PENDING LIMIT ORDERS
            # ------------------------------------------------------------------ #
            pending_orders = [
                o for o in (mt5.orders_get() or [])
                if o.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT)
                and getattr(o, 'sl', 0) != 0 and getattr(o, 'tp', 0) != 0
            ]

            # ------------------------------------------------------------------ #
            # 2. RUNNING POSITIONS
            # ------------------------------------------------------------------ #
            running_positions = [
                p for p in (mt5.positions_get() or [])
                if p.type in (mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_SELL)
                and p.sl != 0 and p.tp != 0
            ]

            # Merge into a single iterable with a flag
            items_to_process = []
            for o in pending_orders:
                items_to_process.append(('PENDING', o))
            for p in running_positions:
                items_to_process.append(('RUNNING', p))

            if not items_to_process:
                log_and_print("No valid pending orders or running positions found.", "INFO")
                mt5.shutdown()
                continue

            log_and_print(f"Found {len(pending_orders)} pending + {len(running_positions)} running → total {len(items_to_process)}", "INFO")

            processed_symbols = set()
            updated_count = 0

            for kind, obj in items_to_process:
                symbol   = obj.symbol
                ticket   = getattr(obj, 'ticket', None) or getattr(obj, 'order', None)
                entry_price = getattr(obj, 'price_open', None) or getattr(obj, 'price_current', None)
                sl_price = obj.sl
                current_tp = obj.tp
                is_buy   = obj.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY)

                if symbol in processed_symbols:
                    continue

                risk_distance = abs(entry_price - sl_price)
                if risk_distance <= 0:
                    log_and_print(f"Zero risk distance on {symbol} ({kind}) → skipped", "WARNING")
                    continue

                symbol_info = mt5.symbol_info(symbol)
                if not symbol_info:
                    log_and_print(f"Symbol info missing: {symbol}", "WARNING")
                    continue

                digits = symbol_info.digits
                def r(p): return round(p, digits)

                entry_price = r(entry_price)
                sl_price    = r(sl_price)
                current_tp  = r(current_tp)
                direction   = 1 if is_buy else -1
                target_tp   = r(entry_price + direction * effective_rr * risk_distance)

                # ----- Ratio ladder (display only) -----
                ratio1 = r(entry_price + direction * 1 * risk_distance)
                ratio2 = r(entry_price + direction * 2 * risk_distance)
                ratio3 = r(entry_price + direction * 3 * risk_distance) if effective_rr >= 3 else None

                print(f"\n{symbol} | {kind} | Target: {effective_rr}R ({SCALE.upper()})")
                print(f"  Entry : {entry_price}")
                print(f"  1R    : {ratio1}")
                print(f"  2R    : {ratio2}")
                if ratio3:
                    print(f"  3R    : {ratio3}")
                print(f"  TP    : {current_tp} → ", end="")

                # ----- Modify TP -----
                tolerance = 10 ** -digits
                if abs(current_tp - target_tp) > tolerance:
                    if kind == "PENDING":
                        # modify pending order
                        request = {
                            "action": mt5.TRADE_ACTION_MODIFY,
                            "order": ticket,
                            "price": entry_price,
                            "sl": sl_price,
                            "tp": target_tp,
                            "type": obj.type,
                            "type_time": obj.type_time,
                            "type_filling": obj.type_filling,
                            "magic": getattr(obj, 'magic', 0),
                            "comment": getattr(obj, 'comment', "")
                        }
                        if hasattr(obj, 'expiration') and obj.expiration:
                            request["expiration"] = obj.expiration
                    else:  # RUNNING
                        # modify open position (SL/TP only)
                        request = {
                            "action": mt5.TRADE_ACTION_SLTP,
                            "position": ticket,
                            "sl": sl_price,
                            "tp": target_tp,
                            "symbol": symbol
                        }

                    result = mt5.order_send(request)
                    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                        print(f"{target_tp} [UPDATED]")
                        log_and_print(
                            f"TP → {effective_rr}R | {symbol} | {kind} | {current_tp} → {target_tp} [{SCALE.upper()}]",
                            "SUCCESS"
                        )
                        updated_count += 1
                    else:
                        err = result.comment if result else "Unknown"
                        print(f"{current_tp} [FAILED: {err}]")
                        log_and_print(f"TP UPDATE FAILED | {symbol} | {kind} | {err}", "ERROR")
                else:
                    print(f"{current_tp} [OK]")

                print(f"  SL    : {sl_price}")
                processed_symbols.add(symbol)

            mt5.shutdown()
            log_and_print(
                f"{user_brokerid} → {len(processed_symbols)} symbol(s) | "
                f"{updated_count} TP(s) set to {effective_rr}R [{SCALE.upper()}]",
                "SUCCESS"
            )

        log_and_print(
            "\nALL $12–$20 ACCOUNTS: R:R UPDATE (PENDING + RUNNING) – "
            "consistency=N×R, martingale=1R = DONE",
            "SUCCESS"
        )
        return True
    _1usd_live_sl_tp_amounts()
    place_1usd_orders()
    _1usd_history_and_deduplication()
    _1usd_ratio_levels()

def _8_12_orders():
    def _2usd_live_sl_tp_amounts():
        
        """
        READS: hightolow.json
        CALCULATES: Live $3 risk & profit
        PRINTS: 3-line block for every market
        SAVES:
            - live_risk_profit_all.json → only valid ≤ $2.10
            - OVERWRITES hightolow.json → REMOVES bad orders PERMANENTLY
        FILTER: Delete any order with live_risk_usd > 2.10 from BOTH files
        """

        BASE_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
        INPUT_FILE = "hightolow.json"
        OUTPUT_FILE = "live_risk_profit_all.json"

        for user_brokerid, cfg in usersdictionary.items():
            TERMINAL_PATH = cfg["TERMINAL_PATH"]
            LOGIN_ID = cfg["LOGIN_ID"]
            PASSWORD = cfg["PASSWORD"]
            SERVER = cfg["SERVER"]

            log_and_print(f"\n{'='*60}", "INFO")
            log_and_print(f"PROCESSING BROKER: {user_brokerid.upper()}", "INFO")
            log_and_print(f"{'='*60}", "INFO")

            # ------------------- CONNECT TO MT5 -------------------
            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=60000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue
            if not mt5.login(int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"Login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account = mt5.account_info()
            if not account:
                log_and_print("No account info", "ERROR")
                mt5.shutdown()
                continue

            balance = account.balance
            if not (8.0 <= balance < 11.99):
                log_and_print(f"Balance ${balance:.2f} not in $12–$20 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue

            currency = account.currency
            log_and_print(f"Connected → Balance: ${balance:.2f} {currency}", "INFO")

            # ------------------- LOAD JSON -------------------
            json_path = Path(BASE_DIR) / user_brokerid / "risk_2_usd" / INPUT_FILE
            if not json_path.exists():
                log_and_print(f"JSON not found: {json_path}", "ERROR")
                mt5.shutdown()
                continue

            try:
                with json_path.open("r", encoding="utf-8") as f:
                    original_data = json.load(f)
                entries = original_data.get("entries", [])
            except Exception as e:
                log_and_print(f"Failed to read JSON: {e}", "ERROR")
                mt5.shutdown()
                continue

            if not entries:
                log_and_print("No entries in JSON.", "INFO")
                mt5.shutdown()
                continue

            log_and_print(f"Loaded {len(entries)} entries → Calculating LIVE risk...", "INFO")

            # ------------------- PROCESS & FILTER -------------------
            valid_entries = []        # For overwriting hightolow.json
            results = []              # For live_risk_profit_all.json
            total = len(entries)
            kept = 0
            removed = 0

            for i, entry in enumerate(entries, 1):
                market = entry["market"]
                try:
                    price = float(entry["entry_price"])
                    sl = float(entry["sl_price"])
                    tp = float(entry["tp_price"])
                    volume = float(entry["volume"])
                    order_type = entry["limit_order"]
                    sl_pips = float(entry.get("sl_pips", 0))
                    tp_pips = float(entry.get("tp_pips", 0))

                    # --- LIVE DATA ---
                    info = mt5.symbol_info(market)
                    tick = mt5.symbol_info_tick(market)

                    if not info or not tick:
                        log_and_print(f"NO LIVE DATA for {market} → Using fallback", "WARNING")
                        pip_value = 0.1
                        risk_usd = volume * sl_pips * pip_value
                        profit_usd = volume * tp_pips * pip_value
                    else:
                        point = info.point
                        contract = info.trade_contract_size

                        risk_points = abs(price - sl) / point
                        profit_points = abs(tp - price) / point

                        point_val = contract * point
                        if "JPY" in market and currency == "USD":
                            point_val /= 100

                        risk_ac = risk_points * point_val * volume
                        profit_ac = profit_points * point_val * volume

                        risk_usd = risk_ac
                        profit_usd = profit_ac

                        if currency != "USD":
                            conv = f"USD{currency}"
                            rate_tick = mt5.symbol_info_tick(conv)
                            rate = rate_tick.bid if rate_tick else 1.0
                            risk_usd /= rate
                            profit_usd /= rate

                    risk_usd = round(risk_usd, 2)
                    profit_usd = round(profit_usd, 2)

                    # --- PRINT ALL ---
                    print(f"market: {market}")
                    print(f"risk: {risk_usd} USD")
                    print(f"profit: {profit_usd} USD")
                    print("---")

                    # --- FILTER: KEEP ONLY <= 2.10 ---
                    if risk_usd <= 2.10:
                        # Keep in BOTH files
                        valid_entries.append(entry)  # Original format
                        results.append({
                            "market": market,
                            "order_type": order_type,
                            "entry_price": round(price, 6),
                            "sl": round(sl, 6),
                            "tp": round(tp, 6),
                            "volume": round(volume, 5),
                            "live_risk_usd": risk_usd,
                            "live_profit_usd": profit_usd,
                            "sl_pips": round(sl_pips, 2),
                            "tp_pips": round(tp_pips, 2),
                            "has_live_tick": bool(info and tick),
                            "current_bid": round(tick.bid, 6) if tick else None,
                            "current_ask": round(tick.ask, 6) if tick else None,
                        })
                        kept += 1
                    else:
                        removed += 1
                        log_and_print(f"REMOVED {market}: live risk ${risk_usd} > $2.10 → DELETED FROM BOTH JSON FILES", "WARNING")

                except Exception as e:
                    log_and_print(f"ERROR on {market}: {e}", "ERROR")
                    removed += 1

                if i % 5 == 0 or i == total:
                    log_and_print(f"Processed {i}/{total} | Kept: {kept} | Removed: {removed}", "INFO")

            # ------------------- SAVE OUTPUT: live_risk_profit_all.json -------------------
            out_path = json_path.parent / OUTPUT_FILE
            report = {
                "broker": user_brokerid,
                "account_currency": currency,
                "generated_at": datetime.now(pytz.timezone("Africa/Lagos")).strftime("%Y-%m-%d %H:%M:%S.%f%z"),
                "source_file": str(json_path),
                "total_entries": total,
                "kept_risk_<=_2.10": kept,
                "removed_risk_>_2.10": removed,
                "filter_applied": "Delete from both input & output if live_risk_usd > 2.10",
                "orders": results
            }

            try:
                with out_path.open("w", encoding="utf-8") as f:
                    json.dump(report, f, indent=2)
                log_and_print(f"SAVED → {out_path} | Kept: {kept} | Removed: {removed}", "SUCCESS")
            except Exception as e:
                log_and_print(f"Save failed: {e}", "ERROR")

            # ------------------- OVERWRITE INPUT: hightolow.json -------------------
            cleaned_input = original_data.copy()
            cleaned_input["entries"] = valid_entries  # Only good ones

            try:
                with json_path.open("w", encoding="utf-8") as f:
                    json.dump(cleaned_input, f, indent=2)
                log_and_print(f"OVERWRITTEN → {json_path} | Now has {len(valid_entries)} entries (removed {removed})", "SUCCESS")
            except Exception as e:
                log_and_print(f"Failed to overwrite input JSON: {e}", "ERROR")

            mt5.shutdown()
            log_and_print(f"FINISHED {user_brokerid} → {kept}/{total} valid orders in BOTH files", "SUCCESS")

        log_and_print("\nALL DONE – BAD ORDERS (> $2.10) DELETED FROM INPUT & OUTPUT!", "SUCCESS")
        return True
    
    def place_2usd_orders():
        

        BASE_INPUT_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
        RISK_FOLDER = "risk_2_usd"
        STRATEGY_FILE = "hightolow.json"
        REPORT_SUFFIX = "forex_order_report.json"
        ISSUES_FILE = "ordersissues.json"

        for user_brokerid, broker_cfg in usersdictionary.items():
            TERMINAL_PATH = broker_cfg["TERMINAL_PATH"]
            LOGIN_ID = broker_cfg["LOGIN_ID"]
            PASSWORD = broker_cfg["PASSWORD"]
            SERVER = broker_cfg["SERVER"]

            log_and_print(f"Processing broker: {user_brokerid} (Balance $12–$20 mode)", "INFO")

            # === MT5 Init ===
            if not os.path.exists(TERMINAL_PATH):
                log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
                continue

            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=30000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue

            if not mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"MT5 login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue


            account_info = mt5.account_info()
            if not account_info:
                log_and_print(f"Failed to get account info: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue
            balance = account_info.balance
            equity = account_info.equity
            log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f}", "INFO")
            if equity < 8.0 and balance >= 8.0:
                log_and_print(f"Equity ${equity:.2f} < $8.0 while Balance ${balance:.2f} ≥ $8.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if equity >= 8.0 and balance < 8.0:
                log_and_print(f"Equity ${equity:.2f} > $8.0 while Balance ${balance:.2f} < $8.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if not (8.0 <= balance < 11.99):
                log_and_print(f"Balance ${balance:.2f} not in $8–$11.99 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue
            # === Only reaches here if: equity >= 8 AND balance in [8, 11.99) ===
            log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f} → Using {RISK_FOLDER} + {STRATEGY_FILE}", "INFO")


            # === Load hightolow.json ===
            file_path = Path(BASE_INPUT_DIR) / user_brokerid / RISK_FOLDER / STRATEGY_FILE
            if not file_path.exists():
                log_and_print(f"File not found: {file_path}", "WARNING")
                mt5.shutdown()
                continue

            try:
                with file_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                    entries = data.get("entries", [])
            except Exception as e:
                log_and_print(f"Failed to read {file_path}: {e}", "ERROR")
                mt5.shutdown()
                continue

            if not entries:
                log_and_print("No entries in hightolow.json", "INFO")
                mt5.shutdown()
                continue

            # === Load existing orders & positions ===
            existing_pending = {}  # (symbol, type) → ticket
            running_positions = set()  # symbols with open position

            for order in (mt5.orders_get() or []):
                if order.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT):
                    existing_pending[(order.symbol, order.type)] = order.ticket

            for pos in (mt5.positions_get() or []):
                running_positions.add(pos.symbol)

            # === Reporting ===
            report_file = file_path.parent / REPORT_SUFFIX
            existing_reports = json.load(report_file.open("r", encoding="utf-8")) if report_file.exists() else []
            issues_list = []
            now_str = datetime.now(pytz.timezone("Africa/Lagos")).strftime("%Y-%m-%d %H:%M:%S.%f+01:00")
            placed = failed = skipped = 0

            for entry in entries:
                try:
                    symbol = entry["market"]
                    price = float(entry["entry_price"])
                    sl = float(entry["sl_price"])
                    tp = float(entry["tp_price"])
                    volume = float(entry["volume"])
                    order_type_str = entry["limit_order"]
                    order_type = mt5.ORDER_TYPE_BUY_LIMIT if order_type_str == "buy_limit" else mt5.ORDER_TYPE_SELL_LIMIT

                    # === SKIP: Already running or pending ===
                    if symbol in running_positions:
                        skipped += 1
                        log_and_print(f"{symbol} has running position → SKIPPED", "INFO")
                        continue

                    key = (symbol, order_type)
                    if key in existing_pending:
                        skipped += 1
                        log_and_print(f"{symbol} {order_type_str} already pending → SKIPPED", "INFO")
                        continue

                    # === Symbol check ===
                    symbol_info = mt5.symbol_info(symbol)
                    if not symbol_info or not symbol_info.visible:
                        issues_list.append({"symbol": symbol, "reason": "Symbol not available"})
                        failed += 1
                        continue

                    # === Volume fix ===
                    vol_step = symbol_info.volume_step
                    volume = max(symbol_info.volume_min,
                                round(volume / vol_step) * vol_step)
                    volume = min(volume, symbol_info.volume_max)

                    # === Price distance check ===
                    tick = mt5.symbol_info_tick(symbol)
                    if not tick:
                        issues_list.append({"symbol": symbol, "reason": "No tick data"})
                        failed += 1
                        continue

                    point = symbol_info.point
                    if order_type == mt5.ORDER_TYPE_BUY_LIMIT:
                        if price >= tick.ask or (tick.ask - price) < 10 * point:
                            skipped += 1
                            continue
                    else:
                        if price <= tick.bid or (price - tick.bid) < 10 * point:
                            skipped += 1
                            continue

                    # === Build & send order ===
                    request = {
                        "action": mt5.TRADE_ACTION_PENDING,
                        "symbol": symbol,
                        "volume": volume,
                        "type": order_type,
                        "price": price,
                        "sl": sl,
                        "tp": tp,
                        "deviation": 10,
                        "magic": 123456,
                        "comment": "Risk3_Auto",
                        "type_time": mt5.ORDER_TIME_GTC,
                        "type_filling": mt5.ORDER_FILLING_IOC,
                    }

                    result = mt5.order_send(request)
                    if result is None:
                        result = type('obj', (), {'retcode': 10000, 'comment': 'order_send returned None'})()

                    success = result.retcode == mt5.TRADE_RETCODE_DONE
                    if success:
                        existing_pending[key] = result.order
                        placed += 1
                        log_and_print(f"{symbol} {order_type_str} @ {price} → PLACED (ticket {result.order})", "SUCCESS")
                    else:
                        failed += 1
                        issues_list.append({"symbol": symbol, "reason": result.comment})

                    # === Report ===
                    if "cent" in RISK_FOLDER:
                        risk_usd = 0.5
                    else:
                        risk_usd = float(RISK_FOLDER.split("_")[1].replace("usd", ""))

                    # === Report ===
                    report_entry = {
                        "symbol": symbol,
                        "order_type": order_type_str,
                        "price": price,
                        "volume": volume,
                        "sl": sl,
                        "tp": tp,
                        "risk_usd": risk_usd,   # ← Now correct: 0.5, 1.0, 2.0, 3.0, 4.0
                        "ticket": result.order if success else None,
                        "success": success,
                        "error_code": result.retcode if not success else None,
                        "error_msg": result.comment if not success else None,
                        "timestamp": now_str
                    }
                    existing_reports.append(report_entry)
                    try:
                        with report_file.open("w", encoding="utf-8") as f:
                            json.dump(existing_reports, f, indent=2)
                    except:
                        pass

                except Exception as e:
                    failed += 1
                    issues_list.append({"symbol": symbol, "reason": f"Exception: {e}"})
                    log_and_print(f"Error processing {symbol}: {e}", "ERROR")

            # === Save issues ===
            issues_path = file_path.parent / ISSUES_FILE
            try:
                existing_issues = json.load(issues_path.open("r", encoding="utf-8")) if issues_path.exists() else []
                with issues_path.open("w", encoding="utf-8") as f:
                    json.dump(existing_issues + issues_list, f, indent=2)
            except:
                pass

            mt5.shutdown()
            log_and_print(
                f"{user_brokerid} DONE → Placed: {placed}, Failed: {failed}, Skipped: {skipped}",
                "SUCCESS"
            )

        log_and_print("All $12–$20 accounts processed.", "SUCCESS")
        return True

    def _2usd_history_and_deduplication():
        """
        HISTORY + PENDING + POSITION DUPLICATE DETECTOR + RISK SNIPER
        - Cancels risk > $2.10  (even if TP=0)
        - Cancels HISTORY DUPLICATES
        - Cancels PENDING LIMIT DUPLICATES
        - Cancels PENDING if POSITION already exists
        - Shows duplicate market name on its own line
        ONLY PROCESSES ACCOUNTS WITH BALANCE $12.00 – $19.99
        """
        BASE_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
        REPORT_NAME = "pending_risk_profit_per_order.json"
        MAX_RISK_USD = 2.10
        LOOKBACK_DAYS = 5
        PRICE_PRECISION = 5
        TZ = pytz.timezone("Africa/Lagos")

        five_days_ago = datetime.now(TZ) - timedelta(days=LOOKBACK_DAYS)

        for user_brokerid, cfg in usersdictionary.items():
            TERMINAL_PATH = cfg["TERMINAL_PATH"]
            LOGIN_ID     = cfg["LOGIN_ID"]
            PASSWORD     = cfg["PASSWORD"]
            SERVER       = cfg["SERVER"]

            log_and_print(f"\n{'='*80}", "INFO")
            log_and_print(f"BROKER: {user_brokerid.upper()} | FULL DUPLICATE + RISK GUARD", "INFO")
            log_and_print(f"{'='*80}", "INFO")

            # ---------- MT5 Init ----------
            if not os.path.exists(TERMINAL_PATH):
                log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
                continue
            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=30000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue
            if not mt5.login(int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"Login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account = mt5.account_info()
            if not account:
                log_and_print("No account info.", "ERROR")
                mt5.shutdown()
                continue

            balance = account.balance
            equity = account.equity
            log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f}", "INFO")
            if equity < 8.0 and balance >= 8.0:
                log_and_print(f"Equity ${equity:.2f} < $8.0 while Balance ${balance:.2f} ≥ $8.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if equity >= 8.0 and balance < 8.0:
                log_and_print(f"Equity ${equity:.2f} > $8.0 while Balance ${balance:.2f} < $8.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if not (8.0 <= balance < 11.99):
                log_and_print(f"Balance ${balance:.2f} not in $8–$11.99 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue

            currency = account.currency
            log_and_print(f"Account: {account.login} | Balance: ${balance:.2f} {currency} → Proceeding with risk_2_usd checks", "INFO")

            # ---------- Get Data ----------
            pending_orders = [o for o in (mt5.orders_get() or [])
                            if o.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT)]
            positions = mt5.positions_get()
            history_deals = mt5.history_deals_get(int(five_days_ago.timestamp()), int(datetime.now(TZ).timestamp()))

            if not pending_orders:
                log_and_print("No pending orders.", "INFO")
                mt5.shutdown()
                continue

            # ---------- BUILD DATABASES ----------
            log_and_print(f"Building duplicate databases...", "INFO")

            # 1. Historical Setups
            historical_keys = {}  # (symbol, entry, sl) → details
            if history_deals:
                for deal in history_deals:
                    if deal.entry != mt5.DEAL_ENTRY_IN: continue
                    if deal.type not in (mt5.DEAL_TYPE_BUY, mt5.DEAL_TYPE_SELL): continue

                    order = mt5.history_orders_get(ticket=deal.order)
                    if not order: continue
                    order = order[0]
                    if order.sl == 0: continue

                    symbol = deal.symbol
                    entry = round(deal.price, PRICE_PRECISION)
                    sl = round(order.sl, PRICE_PRECISION)

                    key = (symbol, entry, sl)
                    if key not in historical_keys:
                        profit = sum(d.profit for d in history_deals if d.order == deal.order and d.entry == mt5.DEAL_ENTRY_OUT)
                        historical_keys[key] = {
                            "time": datetime.fromtimestamp(deal.time, TZ).strftime("%Y-%m-%d %H:%M"),
                            "profit": round(profit, 2),
                            "symbol": symbol
                        }

            # 2. Open Positions (by symbol)
            open_symbols = {pos.symbol for pos in positions} if positions else set()

            # 3. Pending Orders Key Map
            pending_keys = {}  # (symbol, entry, sl) → [order_tickets]
            for order in pending_orders:
                key = (order.symbol, round(order.price_open, PRICE_PRECISION), round(order.sl, PRICE_PRECISION))
                pending_keys.setdefault(key, []).append(order.ticket)

            log_and_print(f"Loaded: {len(historical_keys)} history | {len(open_symbols)} open | {len(pending_keys)} unique pending setups", "INFO")

            # ---------- Process & Cancel ----------
            per_order_data = []
            kept = cancelled_risk = cancelled_hist = cancelled_pend_dup = cancelled_pos_dup = skipped = 0

            for order in pending_orders:
                symbol = order.symbol
                ticket = order.ticket
                volume = order.volume_current
                entry = round(order.price_open, PRICE_PRECISION)
                sl = round(order.sl, PRICE_PRECISION)
                tp = order.tp                     # may be 0

                # ---- NEW: ONLY REQUIRE SL, TP CAN BE 0 ----
                if sl == 0:
                    log_and_print(f"SKIP {ticket} | {symbol} | No SL", "WARNING")
                    skipped += 1
                    continue

                info = mt5.symbol_info(symbol)
                if not info or not mt5.symbol_info_tick(symbol):
                    log_and_print(f"SKIP {ticket} | {symbol} | No symbol data", "WARNING")
                    skipped += 1
                    continue

                point = info.point
                contract = info.trade_contract_size
                point_val = contract * point
                if "JPY" in symbol and currency == "USD":
                    point_val /= 100

                # ---- RISK CALCULATION (always possible with SL) ----
                risk_points = abs(entry - sl) / point
                risk_usd = risk_points * point_val * volume
                if currency != "USD":
                    rate = mt5.symbol_info_tick(f"USD{currency}")
                    if not rate:
                        log_and_print(f"SKIP {ticket} | No USD{currency} rate", "WARNING")
                        skipped += 1
                        continue
                    risk_usd /= rate.bid

                # ---- PROFIT CALCULATION (only if TP exists) ----
                profit_usd = None
                if tp != 0:
                    profit_usd = abs(tp - entry) / point * point_val * volume
                    if currency != "USD":
                        profit_usd /= rate.bid

                # ---- DUPLICATE KEYS ----
                key = (symbol, entry, sl)
                dup_hist = historical_keys.get(key)
                is_position_open = symbol in open_symbols
                is_pending_duplicate = len(pending_keys.get(key, [])) > 1

                print(f"\nmarket: {symbol}")
                print(f"risk: {risk_usd:.2f} USD | profit: {profit_usd if profit_usd is not None else 'N/A'} USD")

                cancel_reason = None
                cancel_type = None

                # === 1. RISK CANCEL (works even if TP=0) ===
                if risk_usd > MAX_RISK_USD:
                    cancel_reason = f"RISK > ${MAX_RISK_USD}"
                    cancel_type = "RISK"
                    print(f"{cancel_reason} → CANCELLED")

                # === 2. HISTORY DUPLICATE ===
                elif dup_hist:
                    cancel_reason = "HISTORY DUPLICATE"
                    cancel_type = "HIST_DUP"
                    print("HISTORY DUPLICATE ORDER FOUND!")
                    print(dup_hist["symbol"])
                    print(f"entry: {entry} | sl: {sl}")
                    print(f"used: {dup_hist['time']} | P/L: {dup_hist['profit']:+.2f} {currency}")
                    print("→ HISTORY DUPLICATE CANCELLED")
                    print("!" * 60)

                # === 3. PENDING DUPLICATE ===
                elif is_pending_duplicate:
                    cancel_reason = "PENDING DUPLICATE"
                    cancel_type = "PEND_DUP"
                    print("PENDING LIMIT DUPLICATE FOUND!")
                    print(symbol)
                    print(f"→ DUPLICATE PENDING ORDER CANCELLED")
                    print("-" * 60)

                # === 4. POSITION EXISTS (Cancel Pending) ===
                elif is_position_open:
                    cancel_reason = "POSITION ALREADY OPEN"
                    cancel_type = "POS_DUP"
                    print("POSITION ALREADY RUNNING!")
                    print(symbol)
                    print(f"→ PENDING ORDER CANCELLED (POSITION ACTIVE)")
                    print("^" * 60)

                # === NO ISSUE → KEEP ===
                else:
                    print("No duplicate. Order kept.")
                    kept += 1
                    per_order_data.append({
                        "ticket": ticket,
                        "symbol": symbol,
                        "entry": entry,
                        "sl": sl,
                        "tp": tp,
                        "risk_usd": round(risk_usd, 2),
                        "profit_usd": round(profit_usd, 2) if profit_usd is not None else None,
                        "status": "KEPT"
                    })
                    continue  # Skip cancel

                # === CANCEL ORDER ===
                req = {"action": mt5.TRADE_ACTION_REMOVE, "order": ticket}
                res = mt5.order_send(req)
                if res.retcode == mt5.TRADE_RETCODE_DONE:
                    log_and_print(f"{cancel_type} CANCELLED {ticket} | {symbol} | {cancel_reason}", "WARNING")
                    if cancel_type == "RISK": cancelled_risk += 1
                    elif cancel_type == "HIST_DUP": cancelled_hist += 1
                    elif cancel_type == "PEND_DUP": cancelled_pend_dup += 1
                    elif cancel_type == "POS_DUP": cancelled_pos_dup += 1
                else:
                    log_and_print(f"CANCEL FAILED {ticket} | {res.comment}", "ERROR")

                per_order_data.append({
                    "ticket": ticket,
                    "symbol": symbol,
                    "entry": entry,
                    "sl": sl,
                    "tp": tp,
                    "risk_usd": round(risk_usd, 2),
                    "profit_usd": round(profit_usd, 2) if profit_usd is not None else None,
                    "status": "CANCELLED",
                    "reason": cancel_reason,
                    "duplicate_time": dup_hist["time"] if dup_hist else None,
                    "duplicate_pl": dup_hist["profit"] if dup_hist else None
                })

            # === SUMMARY ===
            log_and_print(f"\nSUMMARY:", "SUCCESS")
            log_and_print(f"KEPT: {kept}", "INFO")
            log_and_print(f"CANCELLED → RISK: {cancelled_risk} | HIST DUP: {cancelled_hist} | "
                        f"PEND DUP: {cancelled_pend_dup} | POS DUP: {cancelled_pos_dup} | SKIPPED: {skipped}", "WARNING")

            # === SAVE REPORT ===
            out_dir = Path(BASE_DIR) / user_brokerid / "risk_2_usd"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / REPORT_NAME

            report = {
                "broker": user_brokerid,
                "checked_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
                "max_risk_usd": MAX_RISK_USD,
                "lookback_days": LOOKBACK_DAYS,
                "summary": {
                    "kept": kept,
                    "cancelled_risk": cancelled_risk,
                    "cancelled_history_duplicate": cancelled_hist,
                    "cancelled_pending_duplicate": cancelled_pend_dup,
                    "cancelled_position_duplicate": cancelled_pos_dup,
                    "skipped": skipped
                },
                "orders": per_order_data
            }

            try:
                with out_path.open("w", encoding="utf-8") as f:
                    json.dump(report, f, indent=2)
                log_and_print(f"Report saved: {out_path}", "SUCCESS")
            except Exception as e:
                log_and_print(f"Save error: {e}", "ERROR")

            mt5.shutdown()

        log_and_print("\nALL $12–$20 ACCOUNTS: DUPLICATE SCAN + RISK GUARD = DONE", "SUCCESS")
        return True

    def _2usd_ratio_levels():
        """
        2usd RATIO LEVELS + TP UPDATE (PENDING + RUNNING POSITIONS) – BROKER-SAFE
        - Balance $12–$19.99 only
        - Auto-supports riskreward: 1, 2, 3, 4... (any integer)
        - Case-insensitive config
        - consistency → Dynamic TP = RISKREWARD × Risk
        - martingale → TP = 1R (always), ignores RISKREWARD
        - Smart ratio ladder (shows 1R, 2R, 3R only when needed)
        """
        TZ = pytz.timezone("Africa/Lagos")

        log_and_print(f"\n{'='*80}", "INFO")
        log_and_print("2usd RATIO LEVELS + TP UPDATE (PENDING + RUNNING) – CONSISTENCY: N×R | MARTINGALE: 1R", "INFO")
        log_and_print(f"{'='*80}", "INFO")

        for user_brokerid, cfg in usersdictionary.items():
            TERMINAL_PATH = cfg.get("TERMINAL_PATH") or cfg.get("terminal_path")
            LOGIN_ID      = cfg.get("LOGIN_ID")      or cfg.get("login_id")
            PASSWORD      = cfg.get("PASSWORD")      or cfg.get("password")
            SERVER        = cfg.get("SERVER")        or cfg.get("server")
            SCALE         = (cfg.get("SCALE")        or cfg.get("scale")        or "").strip().lower()
            STRATEGY      = (cfg.get("STRATEGY")    or cfg.get("strategy")    or "").strip().lower()

            # === Case-insensitive riskreward lookup ===
            riskreward_raw = None
            for key in cfg:
                if key.lower() == "riskreward":
                    riskreward_raw = cfg[key]
                    break

            if riskreward_raw is None:
                riskreward_raw = 2
                log_and_print(f"{user_brokerid}: 'riskreward' not found → using default 2R", "WARNING")

            log_and_print(
                f"\nProcessing broker: {user_brokerid} | Scale: {SCALE.upper()} | "
                f"Strategy: {STRATEGY.upper()} | riskreward: {riskreward_raw}R", "INFO"
            )

            # === Validate required fields ===
            missing = []
            for f in ("TERMINAL_PATH", "LOGIN_ID", "PASSWORD", "SERVER", "SCALE"):
                if not locals()[f]: missing.append(f)
            if missing:
                log_and_print(f"Missing config: {', '.join(missing)} → SKIPPED", "ERROR")
                continue

            # === MT5 Init ===
            if not os.path.exists(TERMINAL_PATH):
                log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
                continue

            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD,
                                server=SERVER, timeout=30000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue

            if not mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"MT5 login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account_info = mt5.account_info()
            if not account_info:
                log_and_print(f"Failed to get account info: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            balance = account_info.balance
            if not (8.0 <= balance < 11.99):
                log_and_print(f"Balance ${balance:.2f} not in $12–$20 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue

            log_and_print(f"Balance: ${balance:.2f} → Scanning positions & pending orders...", "INFO")

            # === Determine effective RR ===
            try:
                config_rr = int(float(riskreward_raw))
                if config_rr < 1: config_rr = 1
            except (ValueError, TypeError):
                config_rr = 2
                log_and_print(f"Invalid riskreward '{riskreward_raw}' → using 2R", "WARNING")

            effective_rr = 1 if SCALE == "martingale" else config_rr
            rr_source = "MARTINGALE (forced 1R)" if SCALE == "martingale" else f"CONFIG ({effective_rr}R)"
            log_and_print(f"Effective TP: {effective_rr}R [{rr_source}]", "INFO")

            # ------------------------------------------------------------------ #
            # 1. PENDING LIMIT ORDERS
            # ------------------------------------------------------------------ #
            pending_orders = [
                o for o in (mt5.orders_get() or [])
                if o.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT)
                and getattr(o, 'sl', 0) != 0 and getattr(o, 'tp', 0) != 0
            ]

            # ------------------------------------------------------------------ #
            # 2. RUNNING POSITIONS
            # ------------------------------------------------------------------ #
            running_positions = [
                p for p in (mt5.positions_get() or [])
                if p.type in (mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_SELL)
                and p.sl != 0 and p.tp != 0
            ]

            # Merge into a single iterable with a flag
            items_to_process = []
            for o in pending_orders:
                items_to_process.append(('PENDING', o))
            for p in running_positions:
                items_to_process.append(('RUNNING', p))

            if not items_to_process:
                log_and_print("No valid pending orders or running positions found.", "INFO")
                mt5.shutdown()
                continue

            log_and_print(f"Found {len(pending_orders)} pending + {len(running_positions)} running → total {len(items_to_process)}", "INFO")

            processed_symbols = set()
            updated_count = 0

            for kind, obj in items_to_process:
                symbol   = obj.symbol
                ticket   = getattr(obj, 'ticket', None) or getattr(obj, 'order', None)
                entry_price = getattr(obj, 'price_open', None) or getattr(obj, 'price_current', None)
                sl_price = obj.sl
                current_tp = obj.tp
                is_buy   = obj.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY)

                if symbol in processed_symbols:
                    continue

                risk_distance = abs(entry_price - sl_price)
                if risk_distance <= 0:
                    log_and_print(f"Zero risk distance on {symbol} ({kind}) → skipped", "WARNING")
                    continue

                symbol_info = mt5.symbol_info(symbol)
                if not symbol_info:
                    log_and_print(f"Symbol info missing: {symbol}", "WARNING")
                    continue

                digits = symbol_info.digits
                def r(p): return round(p, digits)

                entry_price = r(entry_price)
                sl_price    = r(sl_price)
                current_tp  = r(current_tp)
                direction   = 1 if is_buy else -1
                target_tp   = r(entry_price + direction * effective_rr * risk_distance)

                # ----- Ratio ladder (display only) -----
                ratio1 = r(entry_price + direction * 1 * risk_distance)
                ratio2 = r(entry_price + direction * 2 * risk_distance)
                ratio3 = r(entry_price + direction * 3 * risk_distance) if effective_rr >= 3 else None

                print(f"\n{symbol} | {kind} | Target: {effective_rr}R ({SCALE.upper()})")
                print(f"  Entry : {entry_price}")
                print(f"  1R    : {ratio1}")
                print(f"  2R    : {ratio2}")
                if ratio3:
                    print(f"  3R    : {ratio3}")
                print(f"  TP    : {current_tp} → ", end="")

                # ----- Modify TP -----
                tolerance = 10 ** -digits
                if abs(current_tp - target_tp) > tolerance:
                    if kind == "PENDING":
                        # modify pending order
                        request = {
                            "action": mt5.TRADE_ACTION_MODIFY,
                            "order": ticket,
                            "price": entry_price,
                            "sl": sl_price,
                            "tp": target_tp,
                            "type": obj.type,
                            "type_time": obj.type_time,
                            "type_filling": obj.type_filling,
                            "magic": getattr(obj, 'magic', 0),
                            "comment": getattr(obj, 'comment', "")
                        }
                        if hasattr(obj, 'expiration') and obj.expiration:
                            request["expiration"] = obj.expiration
                    else:  # RUNNING
                        # modify open position (SL/TP only)
                        request = {
                            "action": mt5.TRADE_ACTION_SLTP,
                            "position": ticket,
                            "sl": sl_price,
                            "tp": target_tp,
                            "symbol": symbol
                        }

                    result = mt5.order_send(request)
                    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                        print(f"{target_tp} [UPDATED]")
                        log_and_print(
                            f"TP → {effective_rr}R | {symbol} | {kind} | {current_tp} → {target_tp} [{SCALE.upper()}]",
                            "SUCCESS"
                        )
                        updated_count += 1
                    else:
                        err = result.comment if result else "Unknown"
                        print(f"{current_tp} [FAILED: {err}]")
                        log_and_print(f"TP UPDATE FAILED | {symbol} | {kind} | {err}", "ERROR")
                else:
                    print(f"{current_tp} [OK]")

                print(f"  SL    : {sl_price}")
                processed_symbols.add(symbol)

            mt5.shutdown()
            log_and_print(
                f"{user_brokerid} → {len(processed_symbols)} symbol(s) | "
                f"{updated_count} TP(s) set to {effective_rr}R [{SCALE.upper()}]",
                "SUCCESS"
            )

        log_and_print(
            "\nALL $12–$20 ACCOUNTS: R:R UPDATE (PENDING + RUNNING) – "
            "consistency=N×R, martingale=1R = DONE",
            "SUCCESS"
        )
        return True
    #_2usd_live_sl_tp_amounts()
    place_2usd_orders()
    #_2usd_history_and_deduplication()
    #_2usd_ratio_levels()

def _12_20_orders():
    def _3usd_live_sl_tp_amounts():
        
        """
        READS: hightolow.json
        CALCULATES: Live $3 risk & profit
        PRINTS: 3-line block for every market
        SAVES:
            - live_risk_profit_all.json → only valid ≤ $3.10
            - OVERWRITES hightolow.json → REMOVES bad orders PERMANENTLY
        FILTER: Delete any order with live_risk_usd > 3.10 from BOTH files
        """

        BASE_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
        INPUT_FILE = "hightolow.json"
        OUTPUT_FILE = "live_risk_profit_all.json"

        for user_brokerid, cfg in usersdictionary.items():
            TERMINAL_PATH = cfg["TERMINAL_PATH"]
            LOGIN_ID = cfg["LOGIN_ID"]
            PASSWORD = cfg["PASSWORD"]
            SERVER = cfg["SERVER"]

            log_and_print(f"\n{'='*60}", "INFO")
            log_and_print(f"PROCESSING BROKER: {user_brokerid.upper()}", "INFO")
            log_and_print(f"{'='*60}", "INFO")

            # ------------------- CONNECT TO MT5 -------------------
            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=60000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue
            if not mt5.login(int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"Login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account = mt5.account_info()
            if not account:
                log_and_print("No account info", "ERROR")
                mt5.shutdown()
                continue

            balance = account.balance
            if not (12.0 <= balance < 19.99):
                log_and_print(f"Balance ${balance:.2f} not in $12–$20 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue

            currency = account.currency
            log_and_print(f"Connected → Balance: ${balance:.2f} {currency}", "INFO")

            # ------------------- LOAD JSON -------------------
            json_path = Path(BASE_DIR) / user_brokerid / "risk_3_usd" / INPUT_FILE
            if not json_path.exists():
                log_and_print(f"JSON not found: {json_path}", "ERROR")
                mt5.shutdown()
                continue

            try:
                with json_path.open("r", encoding="utf-8") as f:
                    original_data = json.load(f)
                entries = original_data.get("entries", [])
            except Exception as e:
                log_and_print(f"Failed to read JSON: {e}", "ERROR")
                mt5.shutdown()
                continue

            if not entries:
                log_and_print("No entries in JSON.", "INFO")
                mt5.shutdown()
                continue

            log_and_print(f"Loaded {len(entries)} entries → Calculating LIVE risk...", "INFO")

            # ------------------- PROCESS & FILTER -------------------
            valid_entries = []        # For overwriting hightolow.json
            results = []              # For live_risk_profit_all.json
            total = len(entries)
            kept = 0
            removed = 0

            for i, entry in enumerate(entries, 1):
                market = entry["market"]
                try:
                    price = float(entry["entry_price"])
                    sl = float(entry["sl_price"])
                    tp = float(entry["tp_price"])
                    volume = float(entry["volume"])
                    order_type = entry["limit_order"]
                    sl_pips = float(entry.get("sl_pips", 0))
                    tp_pips = float(entry.get("tp_pips", 0))

                    # --- LIVE DATA ---
                    info = mt5.symbol_info(market)
                    tick = mt5.symbol_info_tick(market)

                    if not info or not tick:
                        log_and_print(f"NO LIVE DATA for {market} → Using fallback", "WARNING")
                        pip_value = 0.1
                        risk_usd = volume * sl_pips * pip_value
                        profit_usd = volume * tp_pips * pip_value
                    else:
                        point = info.point
                        contract = info.trade_contract_size

                        risk_points = abs(price - sl) / point
                        profit_points = abs(tp - price) / point

                        point_val = contract * point
                        if "JPY" in market and currency == "USD":
                            point_val /= 100

                        risk_ac = risk_points * point_val * volume
                        profit_ac = profit_points * point_val * volume

                        risk_usd = risk_ac
                        profit_usd = profit_ac

                        if currency != "USD":
                            conv = f"USD{currency}"
                            rate_tick = mt5.symbol_info_tick(conv)
                            rate = rate_tick.bid if rate_tick else 1.0
                            risk_usd /= rate
                            profit_usd /= rate

                    risk_usd = round(risk_usd, 2)
                    profit_usd = round(profit_usd, 2)

                    # --- PRINT ALL ---
                    print(f"market: {market}")
                    print(f"risk: {risk_usd} USD")
                    print(f"profit: {profit_usd} USD")
                    print("---")

                    # --- FILTER: KEEP ONLY <= 3.10 ---
                    if risk_usd <= 3.10:
                        # Keep in BOTH files
                        valid_entries.append(entry)  # Original format
                        results.append({
                            "market": market,
                            "order_type": order_type,
                            "entry_price": round(price, 6),
                            "sl": round(sl, 6),
                            "tp": round(tp, 6),
                            "volume": round(volume, 5),
                            "live_risk_usd": risk_usd,
                            "live_profit_usd": profit_usd,
                            "sl_pips": round(sl_pips, 2),
                            "tp_pips": round(tp_pips, 2),
                            "has_live_tick": bool(info and tick),
                            "current_bid": round(tick.bid, 6) if tick else None,
                            "current_ask": round(tick.ask, 6) if tick else None,
                        })
                        kept += 1
                    else:
                        removed += 1
                        log_and_print(f"REMOVED {market}: live risk ${risk_usd} > $3.10 → DELETED FROM BOTH JSON FILES", "WARNING")

                except Exception as e:
                    log_and_print(f"ERROR on {market}: {e}", "ERROR")
                    removed += 1

                if i % 5 == 0 or i == total:
                    log_and_print(f"Processed {i}/{total} | Kept: {kept} | Removed: {removed}", "INFO")

            # ------------------- SAVE OUTPUT: live_risk_profit_all.json -------------------
            out_path = json_path.parent / OUTPUT_FILE
            report = {
                "broker": user_brokerid,
                "account_currency": currency,
                "generated_at": datetime.now(pytz.timezone("Africa/Lagos")).strftime("%Y-%m-%d %H:%M:%S.%f%z"),
                "source_file": str(json_path),
                "total_entries": total,
                "kept_risk_<=_3.10": kept,
                "removed_risk_>_3.10": removed,
                "filter_applied": "Delete from both input & output if live_risk_usd > 3.10",
                "orders": results
            }

            try:
                with out_path.open("w", encoding="utf-8") as f:
                    json.dump(report, f, indent=2)
                log_and_print(f"SAVED → {out_path} | Kept: {kept} | Removed: {removed}", "SUCCESS")
            except Exception as e:
                log_and_print(f"Save failed: {e}", "ERROR")

            # ------------------- OVERWRITE INPUT: hightolow.json -------------------
            cleaned_input = original_data.copy()
            cleaned_input["entries"] = valid_entries  # Only good ones

            try:
                with json_path.open("w", encoding="utf-8") as f:
                    json.dump(cleaned_input, f, indent=2)
                log_and_print(f"OVERWRITTEN → {json_path} | Now has {len(valid_entries)} entries (removed {removed})", "SUCCESS")
            except Exception as e:
                log_and_print(f"Failed to overwrite input JSON: {e}", "ERROR")

            mt5.shutdown()
            log_and_print(f"FINISHED {user_brokerid} → {kept}/{total} valid orders in BOTH files", "SUCCESS")

        log_and_print("\nALL DONE – BAD ORDERS (> $3.10) DELETED FROM INPUT & OUTPUT!", "SUCCESS")
        return True
    
    def place_3usd_orders():
        

        BASE_INPUT_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
        RISK_FOLDER = "risk_3_usd"
        STRATEGY_FILE = "hightolow.json"
        REPORT_SUFFIX = "forex_order_report.json"
        ISSUES_FILE = "ordersissues.json"

        for user_brokerid, broker_cfg in usersdictionary.items():
            TERMINAL_PATH = broker_cfg["TERMINAL_PATH"]
            LOGIN_ID = broker_cfg["LOGIN_ID"]
            PASSWORD = broker_cfg["PASSWORD"]
            SERVER = broker_cfg["SERVER"]

            log_and_print(f"Processing broker: {user_brokerid} (Balance $12–$20 mode)", "INFO")

            # === MT5 Init ===
            if not os.path.exists(TERMINAL_PATH):
                log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
                continue

            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=30000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue

            if not mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"MT5 login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue



            account_info = mt5.account_info()
            if not account_info:
                log_and_print(f"Failed to get account info: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue
            balance = account_info.balance
            equity = account_info.equity
            log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f}", "INFO")
            if equity < 12.0 and balance >= 12.0:
                log_and_print(f"Equity ${equity:.2f} < $12.0 while Balance ${balance:.2f} ≥ $12.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if equity >= 12.0 and balance < 12.0:
                log_and_print(f"Equity ${equity:.2f} > $12.0 while Balance ${balance:.2f} < $12.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if not (12.0 <= balance < 19.99):
                log_and_print(f"Balance ${balance:.2f} not in $12–$19.99 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue
            # === Only reaches here if: equity >= 8 AND balance in [8, 11.99) ===
            log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f} → Using {RISK_FOLDER} + {STRATEGY_FILE}", "INFO")



            log_and_print(f"Balance: ${balance:.2f} → Using {RISK_FOLDER} + {STRATEGY_FILE}", "INFO")

            # === Load hightolow.json ===
            file_path = Path(BASE_INPUT_DIR) / user_brokerid / RISK_FOLDER / STRATEGY_FILE
            if not file_path.exists():
                log_and_print(f"File not found: {file_path}", "WARNING")
                mt5.shutdown()
                continue

            try:
                with file_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                    entries = data.get("entries", [])
            except Exception as e:
                log_and_print(f"Failed to read {file_path}: {e}", "ERROR")
                mt5.shutdown()
                continue

            if not entries:
                log_and_print("No entries in hightolow.json", "INFO")
                mt5.shutdown()
                continue

            # === Load existing orders & positions ===
            existing_pending = {}  # (symbol, type) → ticket
            running_positions = set()  # symbols with open position

            for order in (mt5.orders_get() or []):
                if order.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT):
                    existing_pending[(order.symbol, order.type)] = order.ticket

            for pos in (mt5.positions_get() or []):
                running_positions.add(pos.symbol)

            # === Reporting ===
            report_file = file_path.parent / REPORT_SUFFIX
            existing_reports = json.load(report_file.open("r", encoding="utf-8")) if report_file.exists() else []
            issues_list = []
            now_str = datetime.now(pytz.timezone("Africa/Lagos")).strftime("%Y-%m-%d %H:%M:%S.%f+01:00")
            placed = failed = skipped = 0

            for entry in entries:
                try:
                    symbol = entry["market"]
                    price = float(entry["entry_price"])
                    sl = float(entry["sl_price"])
                    tp = float(entry["tp_price"])
                    volume = float(entry["volume"])
                    order_type_str = entry["limit_order"]
                    order_type = mt5.ORDER_TYPE_BUY_LIMIT if order_type_str == "buy_limit" else mt5.ORDER_TYPE_SELL_LIMIT

                    # === SKIP: Already running or pending ===
                    if symbol in running_positions:
                        skipped += 1
                        log_and_print(f"{symbol} has running position → SKIPPED", "INFO")
                        continue

                    key = (symbol, order_type)
                    if key in existing_pending:
                        skipped += 1
                        log_and_print(f"{symbol} {order_type_str} already pending → SKIPPED", "INFO")
                        continue

                    # === Symbol check ===
                    symbol_info = mt5.symbol_info(symbol)
                    if not symbol_info or not symbol_info.visible:
                        issues_list.append({"symbol": symbol, "reason": "Symbol not available"})
                        failed += 1
                        continue

                    # === Volume fix ===
                    vol_step = symbol_info.volume_step
                    volume = max(symbol_info.volume_min,
                                round(volume / vol_step) * vol_step)
                    volume = min(volume, symbol_info.volume_max)

                    # === Price distance check ===
                    tick = mt5.symbol_info_tick(symbol)
                    if not tick:
                        issues_list.append({"symbol": symbol, "reason": "No tick data"})
                        failed += 1
                        continue

                    point = symbol_info.point
                    if order_type == mt5.ORDER_TYPE_BUY_LIMIT:
                        if price >= tick.ask or (tick.ask - price) < 10 * point:
                            skipped += 1
                            continue
                    else:
                        if price <= tick.bid or (price - tick.bid) < 10 * point:
                            skipped += 1
                            continue

                    # === Build & send order ===
                    request = {
                        "action": mt5.TRADE_ACTION_PENDING,
                        "symbol": symbol,
                        "volume": volume,
                        "type": order_type,
                        "price": price,
                        "sl": sl,
                        "tp": tp,
                        "deviation": 10,
                        "magic": 123456,
                        "comment": "Risk3_Auto",
                        "type_time": mt5.ORDER_TIME_GTC,
                        "type_filling": mt5.ORDER_FILLING_IOC,
                    }

                    result = mt5.order_send(request)
                    if result is None:
                        result = type('obj', (), {'retcode': 10000, 'comment': 'order_send returned None'})()

                    success = result.retcode == mt5.TRADE_RETCODE_DONE
                    if success:
                        existing_pending[key] = result.order
                        placed += 1
                        log_and_print(f"{symbol} {order_type_str} @ {price} → PLACED (ticket {result.order})", "SUCCESS")
                    else:
                        failed += 1
                        issues_list.append({"symbol": symbol, "reason": result.comment})

                    # === Report ===
                    if "cent" in RISK_FOLDER:
                        risk_usd = 0.5
                    else:
                        risk_usd = float(RISK_FOLDER.split("_")[1].replace("usd", ""))

                    # === Report ===
                    report_entry = {
                        "symbol": symbol,
                        "order_type": order_type_str,
                        "price": price,
                        "volume": volume,
                        "sl": sl,
                        "tp": tp,
                        "risk_usd": risk_usd,   # ← Now correct: 0.5, 1.0, 2.0, 3.0, 4.0
                        "ticket": result.order if success else None,
                        "success": success,
                        "error_code": result.retcode if not success else None,
                        "error_msg": result.comment if not success else None,
                        "timestamp": now_str
                    }
                    existing_reports.append(report_entry)
                    try:
                        with report_file.open("w", encoding="utf-8") as f:
                            json.dump(existing_reports, f, indent=2)
                    except:
                        pass

                except Exception as e:
                    failed += 1
                    issues_list.append({"symbol": symbol, "reason": f"Exception: {e}"})
                    log_and_print(f"Error processing {symbol}: {e}", "ERROR")

            # === Save issues ===
            issues_path = file_path.parent / ISSUES_FILE
            try:
                existing_issues = json.load(issues_path.open("r", encoding="utf-8")) if issues_path.exists() else []
                with issues_path.open("w", encoding="utf-8") as f:
                    json.dump(existing_issues + issues_list, f, indent=2)
            except:
                pass

            mt5.shutdown()
            log_and_print(
                f"{user_brokerid} DONE → Placed: {placed}, Failed: {failed}, Skipped: {skipped}",
                "SUCCESS"
            )

        log_and_print("All $12–$20 accounts processed.", "SUCCESS")
        return True

    def _3usd_history_and_deduplication():
        """
        HISTORY + PENDING + POSITION DUPLICATE DETECTOR + RISK SNIPER
        - Cancels risk > $3.10  (even if TP=0)
        - Cancels HISTORY DUPLICATES
        - Cancels PENDING LIMIT DUPLICATES
        - Cancels PENDING if POSITION already exists
        - Shows duplicate market name on its own line
        ONLY PROCESSES ACCOUNTS WITH BALANCE $12.00 – $19.99
        """
        BASE_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
        REPORT_NAME = "pending_risk_profit_per_order.json"
        MAX_RISK_USD = 3.10
        LOOKBACK_DAYS = 5
        PRICE_PRECISION = 5
        TZ = pytz.timezone("Africa/Lagos")

        five_days_ago = datetime.now(TZ) - timedelta(days=LOOKBACK_DAYS)

        for user_brokerid, cfg in usersdictionary.items():
            TERMINAL_PATH = cfg["TERMINAL_PATH"]
            LOGIN_ID     = cfg["LOGIN_ID"]
            PASSWORD     = cfg["PASSWORD"]
            SERVER       = cfg["SERVER"]

            log_and_print(f"\n{'='*80}", "INFO")
            log_and_print(f"BROKER: {user_brokerid.upper()} | FULL DUPLICATE + RISK GUARD", "INFO")
            log_and_print(f"{'='*80}", "INFO")

            # ---------- MT5 Init ----------
            if not os.path.exists(TERMINAL_PATH):
                log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
                continue
            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=30000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue
            if not mt5.login(int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"Login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account = mt5.account_info()
            if not account:
                log_and_print("No account info.", "ERROR")
                mt5.shutdown()
                continue

            balance = account.balance
            equity = account.equity
            log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f}", "INFO")
            if equity < 12.0 and balance >= 12.0:
                log_and_print(f"Equity ${equity:.2f} < $12.0 while Balance ${balance:.2f} ≥ $12.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if equity >= 12.0 and balance < 12.0:
                log_and_print(f"Equity ${equity:.2f} > $12.0 while Balance ${balance:.2f} < $12.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if not (12.0 <= balance < 19.99):
                log_and_print(f"Balance ${balance:.2f} not in $12–$19.99 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue

            currency = account.currency
            log_and_print(f"Account: {account.login} | Balance: ${balance:.2f} {currency} → Proceeding with risk_3_usd checks", "INFO")

            # ---------- Get Data ----------
            pending_orders = [o for o in (mt5.orders_get() or [])
                            if o.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT)]
            positions = mt5.positions_get()
            history_deals = mt5.history_deals_get(int(five_days_ago.timestamp()), int(datetime.now(TZ).timestamp()))

            if not pending_orders:
                log_and_print("No pending orders.", "INFO")
                mt5.shutdown()
                continue

            # ---------- BUILD DATABASES ----------
            log_and_print(f"Building duplicate databases...", "INFO")

            # 1. Historical Setups
            historical_keys = {}  # (symbol, entry, sl) → details
            if history_deals:
                for deal in history_deals:
                    if deal.entry != mt5.DEAL_ENTRY_IN: continue
                    if deal.type not in (mt5.DEAL_TYPE_BUY, mt5.DEAL_TYPE_SELL): continue

                    order = mt5.history_orders_get(ticket=deal.order)
                    if not order: continue
                    order = order[0]
                    if order.sl == 0: continue

                    symbol = deal.symbol
                    entry = round(deal.price, PRICE_PRECISION)
                    sl = round(order.sl, PRICE_PRECISION)

                    key = (symbol, entry, sl)
                    if key not in historical_keys:
                        profit = sum(d.profit for d in history_deals if d.order == deal.order and d.entry == mt5.DEAL_ENTRY_OUT)
                        historical_keys[key] = {
                            "time": datetime.fromtimestamp(deal.time, TZ).strftime("%Y-%m-%d %H:%M"),
                            "profit": round(profit, 2),
                            "symbol": symbol
                        }

            # 2. Open Positions (by symbol)
            open_symbols = {pos.symbol for pos in positions} if positions else set()

            # 3. Pending Orders Key Map
            pending_keys = {}  # (symbol, entry, sl) → [order_tickets]
            for order in pending_orders:
                key = (order.symbol, round(order.price_open, PRICE_PRECISION), round(order.sl, PRICE_PRECISION))
                pending_keys.setdefault(key, []).append(order.ticket)

            log_and_print(f"Loaded: {len(historical_keys)} history | {len(open_symbols)} open | {len(pending_keys)} unique pending setups", "INFO")

            # ---------- Process & Cancel ----------
            per_order_data = []
            kept = cancelled_risk = cancelled_hist = cancelled_pend_dup = cancelled_pos_dup = skipped = 0

            for order in pending_orders:
                symbol = order.symbol
                ticket = order.ticket
                volume = order.volume_current
                entry = round(order.price_open, PRICE_PRECISION)
                sl = round(order.sl, PRICE_PRECISION)
                tp = order.tp                     # may be 0

                # ---- NEW: ONLY REQUIRE SL, TP CAN BE 0 ----
                if sl == 0:
                    log_and_print(f"SKIP {ticket} | {symbol} | No SL", "WARNING")
                    skipped += 1
                    continue

                info = mt5.symbol_info(symbol)
                if not info or not mt5.symbol_info_tick(symbol):
                    log_and_print(f"SKIP {ticket} | {symbol} | No symbol data", "WARNING")
                    skipped += 1
                    continue

                point = info.point
                contract = info.trade_contract_size
                point_val = contract * point
                if "JPY" in symbol and currency == "USD":
                    point_val /= 100

                # ---- RISK CALCULATION (always possible with SL) ----
                risk_points = abs(entry - sl) / point
                risk_usd = risk_points * point_val * volume
                if currency != "USD":
                    rate = mt5.symbol_info_tick(f"USD{currency}")
                    if not rate:
                        log_and_print(f"SKIP {ticket} | No USD{currency} rate", "WARNING")
                        skipped += 1
                        continue
                    risk_usd /= rate.bid

                # ---- PROFIT CALCULATION (only if TP exists) ----
                profit_usd = None
                if tp != 0:
                    profit_usd = abs(tp - entry) / point * point_val * volume
                    if currency != "USD":
                        profit_usd /= rate.bid

                # ---- DUPLICATE KEYS ----
                key = (symbol, entry, sl)
                dup_hist = historical_keys.get(key)
                is_position_open = symbol in open_symbols
                is_pending_duplicate = len(pending_keys.get(key, [])) > 1

                print(f"\nmarket: {symbol}")
                print(f"risk: {risk_usd:.2f} USD | profit: {profit_usd if profit_usd is not None else 'N/A'} USD")

                cancel_reason = None
                cancel_type = None

                # === 1. RISK CANCEL (works even if TP=0) ===
                if risk_usd > MAX_RISK_USD:
                    cancel_reason = f"RISK > ${MAX_RISK_USD}"
                    cancel_type = "RISK"
                    print(f"{cancel_reason} → CANCELLED")

                # === 2. HISTORY DUPLICATE ===
                elif dup_hist:
                    cancel_reason = "HISTORY DUPLICATE"
                    cancel_type = "HIST_DUP"
                    print("HISTORY DUPLICATE ORDER FOUND!")
                    print(dup_hist["symbol"])
                    print(f"entry: {entry} | sl: {sl}")
                    print(f"used: {dup_hist['time']} | P/L: {dup_hist['profit']:+.2f} {currency}")
                    print("→ HISTORY DUPLICATE CANCELLED")
                    print("!" * 60)

                # === 3. PENDING DUPLICATE ===
                elif is_pending_duplicate:
                    cancel_reason = "PENDING DUPLICATE"
                    cancel_type = "PEND_DUP"
                    print("PENDING LIMIT DUPLICATE FOUND!")
                    print(symbol)
                    print(f"→ DUPLICATE PENDING ORDER CANCELLED")
                    print("-" * 60)

                # === 4. POSITION EXISTS (Cancel Pending) ===
                elif is_position_open:
                    cancel_reason = "POSITION ALREADY OPEN"
                    cancel_type = "POS_DUP"
                    print("POSITION ALREADY RUNNING!")
                    print(symbol)
                    print(f"→ PENDING ORDER CANCELLED (POSITION ACTIVE)")
                    print("^" * 60)

                # === NO ISSUE → KEEP ===
                else:
                    print("No duplicate. Order kept.")
                    kept += 1
                    per_order_data.append({
                        "ticket": ticket,
                        "symbol": symbol,
                        "entry": entry,
                        "sl": sl,
                        "tp": tp,
                        "risk_usd": round(risk_usd, 2),
                        "profit_usd": round(profit_usd, 2) if profit_usd is not None else None,
                        "status": "KEPT"
                    })
                    continue  # Skip cancel

                # === CANCEL ORDER ===
                req = {"action": mt5.TRADE_ACTION_REMOVE, "order": ticket}
                res = mt5.order_send(req)
                if res.retcode == mt5.TRADE_RETCODE_DONE:
                    log_and_print(f"{cancel_type} CANCELLED {ticket} | {symbol} | {cancel_reason}", "WARNING")
                    if cancel_type == "RISK": cancelled_risk += 1
                    elif cancel_type == "HIST_DUP": cancelled_hist += 1
                    elif cancel_type == "PEND_DUP": cancelled_pend_dup += 1
                    elif cancel_type == "POS_DUP": cancelled_pos_dup += 1
                else:
                    log_and_print(f"CANCEL FAILED {ticket} | {res.comment}", "ERROR")

                per_order_data.append({
                    "ticket": ticket,
                    "symbol": symbol,
                    "entry": entry,
                    "sl": sl,
                    "tp": tp,
                    "risk_usd": round(risk_usd, 2),
                    "profit_usd": round(profit_usd, 2) if profit_usd is not None else None,
                    "status": "CANCELLED",
                    "reason": cancel_reason,
                    "duplicate_time": dup_hist["time"] if dup_hist else None,
                    "duplicate_pl": dup_hist["profit"] if dup_hist else None
                })

            # === SUMMARY ===
            log_and_print(f"\nSUMMARY:", "SUCCESS")
            log_and_print(f"KEPT: {kept}", "INFO")
            log_and_print(f"CANCELLED → RISK: {cancelled_risk} | HIST DUP: {cancelled_hist} | "
                        f"PEND DUP: {cancelled_pend_dup} | POS DUP: {cancelled_pos_dup} | SKIPPED: {skipped}", "WARNING")

            # === SAVE REPORT ===
            out_dir = Path(BASE_DIR) / user_brokerid / "risk_3_usd"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / REPORT_NAME

            report = {
                "broker": user_brokerid,
                "checked_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
                "max_risk_usd": MAX_RISK_USD,
                "lookback_days": LOOKBACK_DAYS,
                "summary": {
                    "kept": kept,
                    "cancelled_risk": cancelled_risk,
                    "cancelled_history_duplicate": cancelled_hist,
                    "cancelled_pending_duplicate": cancelled_pend_dup,
                    "cancelled_position_duplicate": cancelled_pos_dup,
                    "skipped": skipped
                },
                "orders": per_order_data
            }

            try:
                with out_path.open("w", encoding="utf-8") as f:
                    json.dump(report, f, indent=2)
                log_and_print(f"Report saved: {out_path}", "SUCCESS")
            except Exception as e:
                log_and_print(f"Save error: {e}", "ERROR")

            mt5.shutdown()

        log_and_print("\nALL $12–$20 ACCOUNTS: DUPLICATE SCAN + RISK GUARD = DONE", "SUCCESS")
        return True

    def _3usd_ratio_levels():
        """
        3USD RATIO LEVELS + TP UPDATE (PENDING + RUNNING POSITIONS) – BROKER-SAFE
        - Balance $12–$19.99 only
        - Auto-supports riskreward: 1, 2, 3, 4... (any integer)
        - Case-insensitive config
        - consistency → Dynamic TP = RISKREWARD × Risk
        - martingale → TP = 1R (always), ignores RISKREWARD
        - Smart ratio ladder (shows 1R, 2R, 3R only when needed)
        """
        TZ = pytz.timezone("Africa/Lagos")

        log_and_print(f"\n{'='*80}", "INFO")
        log_and_print("3USD RATIO LEVELS + TP UPDATE (PENDING + RUNNING) – CONSISTENCY: N×R | MARTINGALE: 1R", "INFO")
        log_and_print(f"{'='*80}", "INFO")

        for user_brokerid, cfg in usersdictionary.items():
            TERMINAL_PATH = cfg.get("TERMINAL_PATH") or cfg.get("terminal_path")
            LOGIN_ID      = cfg.get("LOGIN_ID")      or cfg.get("login_id")
            PASSWORD      = cfg.get("PASSWORD")      or cfg.get("password")
            SERVER        = cfg.get("SERVER")        or cfg.get("server")
            SCALE         = (cfg.get("SCALE")        or cfg.get("scale")        or "").strip().lower()
            STRATEGY      = (cfg.get("STRATEGY")    or cfg.get("strategy")    or "").strip().lower()

            # === Case-insensitive riskreward lookup ===
            riskreward_raw = None
            for key in cfg:
                if key.lower() == "riskreward":
                    riskreward_raw = cfg[key]
                    break

            if riskreward_raw is None:
                riskreward_raw = 2
                log_and_print(f"{user_brokerid}: 'riskreward' not found → using default 2R", "WARNING")

            log_and_print(
                f"\nProcessing broker: {user_brokerid} | Scale: {SCALE.upper()} | "
                f"Strategy: {STRATEGY.upper()} | riskreward: {riskreward_raw}R", "INFO"
            )

            # === Validate required fields ===
            missing = []
            for f in ("TERMINAL_PATH", "LOGIN_ID", "PASSWORD", "SERVER", "SCALE"):
                if not locals()[f]: missing.append(f)
            if missing:
                log_and_print(f"Missing config: {', '.join(missing)} → SKIPPED", "ERROR")
                continue

            # === MT5 Init ===
            if not os.path.exists(TERMINAL_PATH):
                log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
                continue

            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD,
                                server=SERVER, timeout=30000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue

            if not mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"MT5 login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account_info = mt5.account_info()
            if not account_info:
                log_and_print(f"Failed to get account info: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            balance = account_info.balance
            if not (12.0 <= balance < 19.99):
                log_and_print(f"Balance ${balance:.2f} not in $12–$20 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue

            log_and_print(f"Balance: ${balance:.2f} → Scanning positions & pending orders...", "INFO")

            # === Determine effective RR ===
            try:
                config_rr = int(float(riskreward_raw))
                if config_rr < 1: config_rr = 1
            except (ValueError, TypeError):
                config_rr = 2
                log_and_print(f"Invalid riskreward '{riskreward_raw}' → using 2R", "WARNING")

            effective_rr = 1 if SCALE == "martingale" else config_rr
            rr_source = "MARTINGALE (forced 1R)" if SCALE == "martingale" else f"CONFIG ({effective_rr}R)"
            log_and_print(f"Effective TP: {effective_rr}R [{rr_source}]", "INFO")

            # ------------------------------------------------------------------ #
            # 1. PENDING LIMIT ORDERS
            # ------------------------------------------------------------------ #
            pending_orders = [
                o for o in (mt5.orders_get() or [])
                if o.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT)
                and getattr(o, 'sl', 0) != 0 and getattr(o, 'tp', 0) != 0
            ]

            # ------------------------------------------------------------------ #
            # 2. RUNNING POSITIONS
            # ------------------------------------------------------------------ #
            running_positions = [
                p for p in (mt5.positions_get() or [])
                if p.type in (mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_SELL)
                and p.sl != 0 and p.tp != 0
            ]

            # Merge into a single iterable with a flag
            items_to_process = []
            for o in pending_orders:
                items_to_process.append(('PENDING', o))
            for p in running_positions:
                items_to_process.append(('RUNNING', p))

            if not items_to_process:
                log_and_print("No valid pending orders or running positions found.", "INFO")
                mt5.shutdown()
                continue

            log_and_print(f"Found {len(pending_orders)} pending + {len(running_positions)} running → total {len(items_to_process)}", "INFO")

            processed_symbols = set()
            updated_count = 0

            for kind, obj in items_to_process:
                symbol   = obj.symbol
                ticket   = getattr(obj, 'ticket', None) or getattr(obj, 'order', None)
                entry_price = getattr(obj, 'price_open', None) or getattr(obj, 'price_current', None)
                sl_price = obj.sl
                current_tp = obj.tp
                is_buy   = obj.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY)

                if symbol in processed_symbols:
                    continue

                risk_distance = abs(entry_price - sl_price)
                if risk_distance <= 0:
                    log_and_print(f"Zero risk distance on {symbol} ({kind}) → skipped", "WARNING")
                    continue

                symbol_info = mt5.symbol_info(symbol)
                if not symbol_info:
                    log_and_print(f"Symbol info missing: {symbol}", "WARNING")
                    continue

                digits = symbol_info.digits
                def r(p): return round(p, digits)

                entry_price = r(entry_price)
                sl_price    = r(sl_price)
                current_tp  = r(current_tp)
                direction   = 1 if is_buy else -1
                target_tp   = r(entry_price + direction * effective_rr * risk_distance)

                # ----- Ratio ladder (display only) -----
                ratio1 = r(entry_price + direction * 1 * risk_distance)
                ratio2 = r(entry_price + direction * 2 * risk_distance)
                ratio3 = r(entry_price + direction * 3 * risk_distance) if effective_rr >= 3 else None

                print(f"\n{symbol} | {kind} | Target: {effective_rr}R ({SCALE.upper()})")
                print(f"  Entry : {entry_price}")
                print(f"  1R    : {ratio1}")
                print(f"  2R    : {ratio2}")
                if ratio3:
                    print(f"  3R    : {ratio3}")
                print(f"  TP    : {current_tp} → ", end="")

                # ----- Modify TP -----
                tolerance = 10 ** -digits
                if abs(current_tp - target_tp) > tolerance:
                    if kind == "PENDING":
                        # modify pending order
                        request = {
                            "action": mt5.TRADE_ACTION_MODIFY,
                            "order": ticket,
                            "price": entry_price,
                            "sl": sl_price,
                            "tp": target_tp,
                            "type": obj.type,
                            "type_time": obj.type_time,
                            "type_filling": obj.type_filling,
                            "magic": getattr(obj, 'magic', 0),
                            "comment": getattr(obj, 'comment', "")
                        }
                        if hasattr(obj, 'expiration') and obj.expiration:
                            request["expiration"] = obj.expiration
                    else:  # RUNNING
                        # modify open position (SL/TP only)
                        request = {
                            "action": mt5.TRADE_ACTION_SLTP,
                            "position": ticket,
                            "sl": sl_price,
                            "tp": target_tp,
                            "symbol": symbol
                        }

                    result = mt5.order_send(request)
                    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                        print(f"{target_tp} [UPDATED]")
                        log_and_print(
                            f"TP → {effective_rr}R | {symbol} | {kind} | {current_tp} → {target_tp} [{SCALE.upper()}]",
                            "SUCCESS"
                        )
                        updated_count += 1
                    else:
                        err = result.comment if result else "Unknown"
                        print(f"{current_tp} [FAILED: {err}]")
                        log_and_print(f"TP UPDATE FAILED | {symbol} | {kind} | {err}", "ERROR")
                else:
                    print(f"{current_tp} [OK]")

                print(f"  SL    : {sl_price}")
                processed_symbols.add(symbol)

            mt5.shutdown()
            log_and_print(
                f"{user_brokerid} → {len(processed_symbols)} symbol(s) | "
                f"{updated_count} TP(s) set to {effective_rr}R [{SCALE.upper()}]",
                "SUCCESS"
            )

        log_and_print(
            "\nALL $12–$20 ACCOUNTS: R:R UPDATE (PENDING + RUNNING) – "
            "consistency=N×R, martingale=1R = DONE",
            "SUCCESS"
        )
        return True
    _3usd_live_sl_tp_amounts()
    place_3usd_orders()
    _3usd_history_and_deduplication()
    _3usd_ratio_levels()

def _20_80_orders():
    def _4usd_live_sl_tp_amounts():
        
        """
        READS: hightolow.json
        CALCULATES: Live $3 risk & profit
        PRINTS: 3-line block for every market
        SAVES:
            - live_risk_profit_all.json → only valid ≤ $4.10
            - OVERWRITES hightolow.json → REMOVES bad orders PERMANENTLY
        FILTER: Delete any order with live_risk_usd > 4.10 from BOTH files
        """

        BASE_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
        INPUT_FILE = "hightolow.json"
        OUTPUT_FILE = "live_risk_profit_all.json"

        for user_brokerid, cfg in usersdictionary.items():
            TERMINAL_PATH = cfg["TERMINAL_PATH"]
            LOGIN_ID = cfg["LOGIN_ID"]
            PASSWORD = cfg["PASSWORD"]
            SERVER = cfg["SERVER"]

            log_and_print(f"\n{'='*60}", "INFO")
            log_and_print(f"PROCESSING BROKER: {user_brokerid.upper()}", "INFO")
            log_and_print(f"{'='*60}", "INFO")

            # ------------------- CONNECT TO MT5 -------------------
            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=60000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue
            if not mt5.login(int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"Login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account = mt5.account_info()
            if not account:
                log_and_print("No account info", "ERROR")
                mt5.shutdown()
                continue

            balance = account.balance
            if not (20.0 <= balance < 79.99):
                log_and_print(f"Balance ${balance:.2f} not in $12–$20 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue

            currency = account.currency
            log_and_print(f"Connected → Balance: ${balance:.2f} {currency}", "INFO")

            # ------------------- LOAD JSON -------------------
            json_path = Path(BASE_DIR) / user_brokerid / "risk_4_usd" / INPUT_FILE
            if not json_path.exists():
                log_and_print(f"JSON not found: {json_path}", "ERROR")
                mt5.shutdown()
                continue

            try:
                with json_path.open("r", encoding="utf-8") as f:
                    original_data = json.load(f)
                entries = original_data.get("entries", [])
            except Exception as e:
                log_and_print(f"Failed to read JSON: {e}", "ERROR")
                mt5.shutdown()
                continue

            if not entries:
                log_and_print("No entries in JSON.", "INFO")
                mt5.shutdown()
                continue

            log_and_print(f"Loaded {len(entries)} entries → Calculating LIVE risk...", "INFO")

            # ------------------- PROCESS & FILTER -------------------
            valid_entries = []        # For overwriting hightolow.json
            results = []              # For live_risk_profit_all.json
            total = len(entries)
            kept = 0
            removed = 0

            for i, entry in enumerate(entries, 1):
                market = entry["market"]
                try:
                    price = float(entry["entry_price"])
                    sl = float(entry["sl_price"])
                    tp = float(entry["tp_price"])
                    volume = float(entry["volume"])
                    order_type = entry["limit_order"]
                    sl_pips = float(entry.get("sl_pips", 0))
                    tp_pips = float(entry.get("tp_pips", 0))

                    # --- LIVE DATA ---
                    info = mt5.symbol_info(market)
                    tick = mt5.symbol_info_tick(market)

                    if not info or not tick:
                        log_and_print(f"NO LIVE DATA for {market} → Using fallback", "WARNING")
                        pip_value = 0.1
                        risk_usd = volume * sl_pips * pip_value
                        profit_usd = volume * tp_pips * pip_value
                    else:
                        point = info.point
                        contract = info.trade_contract_size

                        risk_points = abs(price - sl) / point
                        profit_points = abs(tp - price) / point

                        point_val = contract * point
                        if "JPY" in market and currency == "USD":
                            point_val /= 100

                        risk_ac = risk_points * point_val * volume
                        profit_ac = profit_points * point_val * volume

                        risk_usd = risk_ac
                        profit_usd = profit_ac

                        if currency != "USD":
                            conv = f"USD{currency}"
                            rate_tick = mt5.symbol_info_tick(conv)
                            rate = rate_tick.bid if rate_tick else 1.0
                            risk_usd /= rate
                            profit_usd /= rate

                    risk_usd = round(risk_usd, 2)
                    profit_usd = round(profit_usd, 2)

                    # --- PRINT ALL ---
                    print(f"market: {market}")
                    print(f"risk: {risk_usd} USD")
                    print(f"profit: {profit_usd} USD")
                    print("---")

                    # --- FILTER: KEEP ONLY <= 4.10 ---
                    if risk_usd <= 4.10:
                        # Keep in BOTH files
                        valid_entries.append(entry)  # Original format
                        results.append({
                            "market": market,
                            "order_type": order_type,
                            "entry_price": round(price, 6),
                            "sl": round(sl, 6),
                            "tp": round(tp, 6),
                            "volume": round(volume, 5),
                            "live_risk_usd": risk_usd,
                            "live_profit_usd": profit_usd,
                            "sl_pips": round(sl_pips, 2),
                            "tp_pips": round(tp_pips, 2),
                            "has_live_tick": bool(info and tick),
                            "current_bid": round(tick.bid, 6) if tick else None,
                            "current_ask": round(tick.ask, 6) if tick else None,
                        })
                        kept += 1
                    else:
                        removed += 1
                        log_and_print(f"REMOVED {market}: live risk ${risk_usd} > $4.10 → DELETED FROM BOTH JSON FILES", "WARNING")

                except Exception as e:
                    log_and_print(f"ERROR on {market}: {e}", "ERROR")
                    removed += 1

                if i % 5 == 0 or i == total:
                    log_and_print(f"Processed {i}/{total} | Kept: {kept} | Removed: {removed}", "INFO")

            # ------------------- SAVE OUTPUT: live_risk_profit_all.json -------------------
            out_path = json_path.parent / OUTPUT_FILE
            report = {
                "broker": user_brokerid,
                "account_currency": currency,
                "generated_at": datetime.now(pytz.timezone("Africa/Lagos")).strftime("%Y-%m-%d %H:%M:%S.%f%z"),
                "source_file": str(json_path),
                "total_entries": total,
                "kept_risk_<=_4.10": kept,
                "removed_risk_>_4.10": removed,
                "filter_applied": "Delete from both input & output if live_risk_usd > 4.10",
                "orders": results
            }

            try:
                with out_path.open("w", encoding="utf-8") as f:
                    json.dump(report, f, indent=2)
                log_and_print(f"SAVED → {out_path} | Kept: {kept} | Removed: {removed}", "SUCCESS")
            except Exception as e:
                log_and_print(f"Save failed: {e}", "ERROR")

            # ------------------- OVERWRITE INPUT: hightolow.json -------------------
            cleaned_input = original_data.copy()
            cleaned_input["entries"] = valid_entries  # Only good ones

            try:
                with json_path.open("w", encoding="utf-8") as f:
                    json.dump(cleaned_input, f, indent=2)
                log_and_print(f"OVERWRITTEN → {json_path} | Now has {len(valid_entries)} entries (removed {removed})", "SUCCESS")
            except Exception as e:
                log_and_print(f"Failed to overwrite input JSON: {e}", "ERROR")

            mt5.shutdown()
            log_and_print(f"FINISHED {user_brokerid} → {kept}/{total} valid orders in BOTH files", "SUCCESS")

        log_and_print("\nALL DONE – BAD ORDERS (> $4.10) DELETED FROM INPUT & OUTPUT!", "SUCCESS")
        return True
    
    def place_4usd_orders():
        

        BASE_INPUT_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
        RISK_FOLDER = "risk_4_usd"
        STRATEGY_FILE = "hightolow.json"
        REPORT_SUFFIX = "forex_order_report.json"
        ISSUES_FILE = "ordersissues.json"

        for user_brokerid, broker_cfg in usersdictionary.items():
            TERMINAL_PATH = broker_cfg["TERMINAL_PATH"]
            LOGIN_ID = broker_cfg["LOGIN_ID"]
            PASSWORD = broker_cfg["PASSWORD"]
            SERVER = broker_cfg["SERVER"]

            log_and_print(f"Processing broker: {user_brokerid} (Balance $12–$20 mode)", "INFO")

            # === MT5 Init ===
            if not os.path.exists(TERMINAL_PATH):
                log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
                continue

            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=30000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue

            if not mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"MT5 login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account_info = mt5.account_info()
            if not account_info:
                log_and_print(f"Failed to get account info: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue
            balance = account_info.balance
            equity = account_info.equity
            log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f}", "INFO")
            if equity < 20.0 and balance >= 20.0:
                log_and_print(f"Equity ${equity:.2f} < $20.0 while Balance ${balance:.2f} ≥ $20.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if equity >= 20.0 and balance < 20.0:
                log_and_print(f"Equity ${equity:.2f} > $20.0 while Balance ${balance:.2f} < $20.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if not (20.0 <= balance < 79.99):
                log_and_print(f"Balance ${balance:.2f} not in $20–$99.99 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue
            # === Only reaches here if: equity >= 8 AND balance in [8, 11.99) ===
            log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f} → Using {RISK_FOLDER} + {STRATEGY_FILE}", "INFO")



            log_and_print(f"Balance: ${balance:.2f} → Using {RISK_FOLDER} + {STRATEGY_FILE}", "INFO")

            # === Load hightolow.json ===
            file_path = Path(BASE_INPUT_DIR) / user_brokerid / RISK_FOLDER / STRATEGY_FILE
            if not file_path.exists():
                log_and_print(f"File not found: {file_path}", "WARNING")
                mt5.shutdown()
                continue

            try:
                with file_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                    entries = data.get("entries", [])
            except Exception as e:
                log_and_print(f"Failed to read {file_path}: {e}", "ERROR")
                mt5.shutdown()
                continue

            if not entries:
                log_and_print("No entries in hightolow.json", "INFO")
                mt5.shutdown()
                continue

            # === Load existing orders & positions ===
            existing_pending = {}  # (symbol, type) → ticket
            running_positions = set()  # symbols with open position

            for order in (mt5.orders_get() or []):
                if order.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT):
                    existing_pending[(order.symbol, order.type)] = order.ticket

            for pos in (mt5.positions_get() or []):
                running_positions.add(pos.symbol)

            # === Reporting ===
            report_file = file_path.parent / REPORT_SUFFIX
            existing_reports = json.load(report_file.open("r", encoding="utf-8")) if report_file.exists() else []
            issues_list = []
            now_str = datetime.now(pytz.timezone("Africa/Lagos")).strftime("%Y-%m-%d %H:%M:%S.%f+01:00")
            placed = failed = skipped = 0

            for entry in entries:
                try:
                    symbol = entry["market"]
                    price = float(entry["entry_price"])
                    sl = float(entry["sl_price"])
                    tp = float(entry["tp_price"])
                    volume = float(entry["volume"])
                    order_type_str = entry["limit_order"]
                    order_type = mt5.ORDER_TYPE_BUY_LIMIT if order_type_str == "buy_limit" else mt5.ORDER_TYPE_SELL_LIMIT

                    # === SKIP: Already running or pending ===
                    if symbol in running_positions:
                        skipped += 1
                        log_and_print(f"{symbol} has running position → SKIPPED", "INFO")
                        continue

                    key = (symbol, order_type)
                    if key in existing_pending:
                        skipped += 1
                        log_and_print(f"{symbol} {order_type_str} already pending → SKIPPED", "INFO")
                        continue

                    # === Symbol check ===
                    symbol_info = mt5.symbol_info(symbol)
                    if not symbol_info or not symbol_info.visible:
                        issues_list.append({"symbol": symbol, "reason": "Symbol not available"})
                        failed += 1
                        continue

                    # === Volume fix ===
                    vol_step = symbol_info.volume_step
                    volume = max(symbol_info.volume_min,
                                round(volume / vol_step) * vol_step)
                    volume = min(volume, symbol_info.volume_max)

                    # === Price distance check ===
                    tick = mt5.symbol_info_tick(symbol)
                    if not tick:
                        issues_list.append({"symbol": symbol, "reason": "No tick data"})
                        failed += 1
                        continue

                    point = symbol_info.point
                    if order_type == mt5.ORDER_TYPE_BUY_LIMIT:
                        if price >= tick.ask or (tick.ask - price) < 10 * point:
                            skipped += 1
                            continue
                    else:
                        if price <= tick.bid or (price - tick.bid) < 10 * point:
                            skipped += 1
                            continue

                    # === Build & send order ===
                    request = {
                        "action": mt5.TRADE_ACTION_PENDING,
                        "symbol": symbol,
                        "volume": volume,
                        "type": order_type,
                        "price": price,
                        "sl": sl,
                        "tp": tp,
                        "deviation": 10,
                        "magic": 123456,
                        "comment": "Risk3_Auto",
                        "type_time": mt5.ORDER_TIME_GTC,
                        "type_filling": mt5.ORDER_FILLING_IOC,
                    }

                    result = mt5.order_send(request)
                    if result is None:
                        result = type('obj', (), {'retcode': 10000, 'comment': 'order_send returned None'})()

                    success = result.retcode == mt5.TRADE_RETCODE_DONE
                    if success:
                        existing_pending[key] = result.order
                        placed += 1
                        log_and_print(f"{symbol} {order_type_str} @ {price} → PLACED (ticket {result.order})", "SUCCESS")
                    else:
                        failed += 1
                        issues_list.append({"symbol": symbol, "reason": result.comment})

                    # === Report ===
                    if "cent" in RISK_FOLDER:
                        risk_usd = 0.5
                    else:
                        risk_usd = float(RISK_FOLDER.split("_")[1].replace("usd", ""))

                    # === Report ===
                    report_entry = {
                        "symbol": symbol,
                        "order_type": order_type_str,
                        "price": price,
                        "volume": volume,
                        "sl": sl,
                        "tp": tp,
                        "risk_usd": risk_usd,   # ← Now correct: 0.5, 1.0, 2.0, 3.0, 4.0
                        "ticket": result.order if success else None,
                        "success": success,
                        "error_code": result.retcode if not success else None,
                        "error_msg": result.comment if not success else None,
                        "timestamp": now_str
                    }
                    existing_reports.append(report_entry)
                    try:
                        with report_file.open("w", encoding="utf-8") as f:
                            json.dump(existing_reports, f, indent=2)
                    except:
                        pass

                except Exception as e:
                    failed += 1
                    issues_list.append({"symbol": symbol, "reason": f"Exception: {e}"})
                    log_and_print(f"Error processing {symbol}: {e}", "ERROR")

            # === Save issues ===
            issues_path = file_path.parent / ISSUES_FILE
            try:
                existing_issues = json.load(issues_path.open("r", encoding="utf-8")) if issues_path.exists() else []
                with issues_path.open("w", encoding="utf-8") as f:
                    json.dump(existing_issues + issues_list, f, indent=2)
            except:
                pass

            mt5.shutdown()
            log_and_print(
                f"{user_brokerid} DONE → Placed: {placed}, Failed: {failed}, Skipped: {skipped}",
                "SUCCESS"
            )

        log_and_print("All $12–$20 accounts processed.", "SUCCESS")
        return True
  
    def _4usd_history_and_deduplication():
        """
        HISTORY + PENDING + POSITION DUPLICATE DETECTOR + RISK SNIPER
        - Cancels risk > $4.10  (even if TP=0)
        - Cancels HISTORY DUPLICATES
        - Cancels PENDING LIMIT DUPLICATES
        - Cancels PENDING if POSITION already exists
        - Shows duplicate market name on its own line
        ONLY PROCESSES ACCOUNTS WITH BALANCE $12.00 – $19.99
        """
        BASE_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
        REPORT_NAME = "pending_risk_profit_per_order.json"
        MAX_RISK_USD = 4.10
        LOOKBACK_DAYS = 5
        PRICE_PRECISION = 5
        TZ = pytz.timezone("Africa/Lagos")

        five_days_ago = datetime.now(TZ) - timedelta(days=LOOKBACK_DAYS)

        for user_brokerid, cfg in usersdictionary.items():
            TERMINAL_PATH = cfg["TERMINAL_PATH"]
            LOGIN_ID     = cfg["LOGIN_ID"]
            PASSWORD     = cfg["PASSWORD"]
            SERVER       = cfg["SERVER"]

            log_and_print(f"\n{'='*80}", "INFO")
            log_and_print(f"BROKER: {user_brokerid.upper()} | FULL DUPLICATE + RISK GUARD", "INFO")
            log_and_print(f"{'='*80}", "INFO")

            # ---------- MT5 Init ----------
            if not os.path.exists(TERMINAL_PATH):
                log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
                continue
            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=30000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue
            if not mt5.login(int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"Login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account = mt5.account_info()
            if not account:
                log_and_print("No account info.", "ERROR")
                mt5.shutdown()
                continue

            balance = account.balance
            equity = account.equity
            log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f}", "INFO")
            if equity < 20.0 and balance >= 20.0:
                log_and_print(f"Equity ${equity:.2f} < $20.0 while Balance ${balance:.2f} ≥ $20.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if equity >= 20.0 and balance < 20.0:
                log_and_print(f"Equity ${equity:.2f} > $20.0 while Balance ${balance:.2f} < $20.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if not (20.0 <= balance < 79.99):
                log_and_print(f"Balance ${balance:.2f} not in $20–$99.99 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue

            currency = account.currency
            log_and_print(f"Account: {account.login} | Balance: ${balance:.2f} {currency} → Proceeding with risk_4_usd checks", "INFO")

            # ---------- Get Data ----------
            pending_orders = [o for o in (mt5.orders_get() or [])
                            if o.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT)]
            positions = mt5.positions_get()
            history_deals = mt5.history_deals_get(int(five_days_ago.timestamp()), int(datetime.now(TZ).timestamp()))

            if not pending_orders:
                log_and_print("No pending orders.", "INFO")
                mt5.shutdown()
                continue

            # ---------- BUILD DATABASES ----------
            log_and_print(f"Building duplicate databases...", "INFO")

            # 1. Historical Setups
            historical_keys = {}  # (symbol, entry, sl) → details
            if history_deals:
                for deal in history_deals:
                    if deal.entry != mt5.DEAL_ENTRY_IN: continue
                    if deal.type not in (mt5.DEAL_TYPE_BUY, mt5.DEAL_TYPE_SELL): continue

                    order = mt5.history_orders_get(ticket=deal.order)
                    if not order: continue
                    order = order[0]
                    if order.sl == 0: continue

                    symbol = deal.symbol
                    entry = round(deal.price, PRICE_PRECISION)
                    sl = round(order.sl, PRICE_PRECISION)

                    key = (symbol, entry, sl)
                    if key not in historical_keys:
                        profit = sum(d.profit for d in history_deals if d.order == deal.order and d.entry == mt5.DEAL_ENTRY_OUT)
                        historical_keys[key] = {
                            "time": datetime.fromtimestamp(deal.time, TZ).strftime("%Y-%m-%d %H:%M"),
                            "profit": round(profit, 2),
                            "symbol": symbol
                        }

            # 2. Open Positions (by symbol)
            open_symbols = {pos.symbol for pos in positions} if positions else set()

            # 3. Pending Orders Key Map
            pending_keys = {}  # (symbol, entry, sl) → [order_tickets]
            for order in pending_orders:
                key = (order.symbol, round(order.price_open, PRICE_PRECISION), round(order.sl, PRICE_PRECISION))
                pending_keys.setdefault(key, []).append(order.ticket)

            log_and_print(f"Loaded: {len(historical_keys)} history | {len(open_symbols)} open | {len(pending_keys)} unique pending setups", "INFO")

            # ---------- Process & Cancel ----------
            per_order_data = []
            kept = cancelled_risk = cancelled_hist = cancelled_pend_dup = cancelled_pos_dup = skipped = 0

            for order in pending_orders:
                symbol = order.symbol
                ticket = order.ticket
                volume = order.volume_current
                entry = round(order.price_open, PRICE_PRECISION)
                sl = round(order.sl, PRICE_PRECISION)
                tp = order.tp                     # may be 0

                # ---- NEW: ONLY REQUIRE SL, TP CAN BE 0 ----
                if sl == 0:
                    log_and_print(f"SKIP {ticket} | {symbol} | No SL", "WARNING")
                    skipped += 1
                    continue

                info = mt5.symbol_info(symbol)
                if not info or not mt5.symbol_info_tick(symbol):
                    log_and_print(f"SKIP {ticket} | {symbol} | No symbol data", "WARNING")
                    skipped += 1
                    continue

                point = info.point
                contract = info.trade_contract_size
                point_val = contract * point
                if "JPY" in symbol and currency == "USD":
                    point_val /= 100

                # ---- RISK CALCULATION (always possible with SL) ----
                risk_points = abs(entry - sl) / point
                risk_usd = risk_points * point_val * volume
                if currency != "USD":
                    rate = mt5.symbol_info_tick(f"USD{currency}")
                    if not rate:
                        log_and_print(f"SKIP {ticket} | No USD{currency} rate", "WARNING")
                        skipped += 1
                        continue
                    risk_usd /= rate.bid

                # ---- PROFIT CALCULATION (only if TP exists) ----
                profit_usd = None
                if tp != 0:
                    profit_usd = abs(tp - entry) / point * point_val * volume
                    if currency != "USD":
                        profit_usd /= rate.bid

                # ---- DUPLICATE KEYS ----
                key = (symbol, entry, sl)
                dup_hist = historical_keys.get(key)
                is_position_open = symbol in open_symbols
                is_pending_duplicate = len(pending_keys.get(key, [])) > 1

                print(f"\nmarket: {symbol}")
                print(f"risk: {risk_usd:.2f} USD | profit: {profit_usd if profit_usd is not None else 'N/A'} USD")

                cancel_reason = None
                cancel_type = None

                # === 1. RISK CANCEL (works even if TP=0) ===
                if risk_usd > MAX_RISK_USD:
                    cancel_reason = f"RISK > ${MAX_RISK_USD}"
                    cancel_type = "RISK"
                    print(f"{cancel_reason} → CANCELLED")

                # === 2. HISTORY DUPLICATE ===
                elif dup_hist:
                    cancel_reason = "HISTORY DUPLICATE"
                    cancel_type = "HIST_DUP"
                    print("HISTORY DUPLICATE ORDER FOUND!")
                    print(dup_hist["symbol"])
                    print(f"entry: {entry} | sl: {sl}")
                    print(f"used: {dup_hist['time']} | P/L: {dup_hist['profit']:+.2f} {currency}")
                    print("→ HISTORY DUPLICATE CANCELLED")
                    print("!" * 60)

                # === 3. PENDING DUPLICATE ===
                elif is_pending_duplicate:
                    cancel_reason = "PENDING DUPLICATE"
                    cancel_type = "PEND_DUP"
                    print("PENDING LIMIT DUPLICATE FOUND!")
                    print(symbol)
                    print(f"→ DUPLICATE PENDING ORDER CANCELLED")
                    print("-" * 60)

                # === 4. POSITION EXISTS (Cancel Pending) ===
                elif is_position_open:
                    cancel_reason = "POSITION ALREADY OPEN"
                    cancel_type = "POS_DUP"
                    print("POSITION ALREADY RUNNING!")
                    print(symbol)
                    print(f"→ PENDING ORDER CANCELLED (POSITION ACTIVE)")
                    print("^" * 60)

                # === NO ISSUE → KEEP ===
                else:
                    print("No duplicate. Order kept.")
                    kept += 1
                    per_order_data.append({
                        "ticket": ticket,
                        "symbol": symbol,
                        "entry": entry,
                        "sl": sl,
                        "tp": tp,
                        "risk_usd": round(risk_usd, 2),
                        "profit_usd": round(profit_usd, 2) if profit_usd is not None else None,
                        "status": "KEPT"
                    })
                    continue  # Skip cancel

                # === CANCEL ORDER ===
                req = {"action": mt5.TRADE_ACTION_REMOVE, "order": ticket}
                res = mt5.order_send(req)
                if res.retcode == mt5.TRADE_RETCODE_DONE:
                    log_and_print(f"{cancel_type} CANCELLED {ticket} | {symbol} | {cancel_reason}", "WARNING")
                    if cancel_type == "RISK": cancelled_risk += 1
                    elif cancel_type == "HIST_DUP": cancelled_hist += 1
                    elif cancel_type == "PEND_DUP": cancelled_pend_dup += 1
                    elif cancel_type == "POS_DUP": cancelled_pos_dup += 1
                else:
                    log_and_print(f"CANCEL FAILED {ticket} | {res.comment}", "ERROR")

                per_order_data.append({
                    "ticket": ticket,
                    "symbol": symbol,
                    "entry": entry,
                    "sl": sl,
                    "tp": tp,
                    "risk_usd": round(risk_usd, 2),
                    "profit_usd": round(profit_usd, 2) if profit_usd is not None else None,
                    "status": "CANCELLED",
                    "reason": cancel_reason,
                    "duplicate_time": dup_hist["time"] if dup_hist else None,
                    "duplicate_pl": dup_hist["profit"] if dup_hist else None
                })

            # === SUMMARY ===
            log_and_print(f"\nSUMMARY:", "SUCCESS")
            log_and_print(f"KEPT: {kept}", "INFO")
            log_and_print(f"CANCELLED → RISK: {cancelled_risk} | HIST DUP: {cancelled_hist} | "
                        f"PEND DUP: {cancelled_pend_dup} | POS DUP: {cancelled_pos_dup} | SKIPPED: {skipped}", "WARNING")

            # === SAVE REPORT ===
            out_dir = Path(BASE_DIR) / user_brokerid / "risk_4_usd"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / REPORT_NAME

            report = {
                "broker": user_brokerid,
                "checked_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
                "max_risk_usd": MAX_RISK_USD,
                "lookback_days": LOOKBACK_DAYS,
                "summary": {
                    "kept": kept,
                    "cancelled_risk": cancelled_risk,
                    "cancelled_history_duplicate": cancelled_hist,
                    "cancelled_pending_duplicate": cancelled_pend_dup,
                    "cancelled_position_duplicate": cancelled_pos_dup,
                    "skipped": skipped
                },
                "orders": per_order_data
            }

            try:
                with out_path.open("w", encoding="utf-8") as f:
                    json.dump(report, f, indent=2)
                log_and_print(f"Report saved: {out_path}", "SUCCESS")
            except Exception as e:
                log_and_print(f"Save error: {e}", "ERROR")

            mt5.shutdown()

        log_and_print("\nALL $12–$20 ACCOUNTS: DUPLICATE SCAN + RISK GUARD = DONE", "SUCCESS")
        return True

    def _4usd_ratio_levels():
        """
        4usd RATIO LEVELS + TP UPDATE (PENDING + RUNNING POSITIONS) – BROKER-SAFE
        - Balance $12–$19.99 only
        - Auto-supports riskreward: 1, 2, 3, 4... (any integer)
        - Case-insensitive config
        - consistency → Dynamic TP = RISKREWARD × Risk
        - martingale → TP = 1R (always), ignores RISKREWARD
        - Smart ratio ladder (shows 1R, 2R, 3R only when needed)
        """
        TZ = pytz.timezone("Africa/Lagos")

        log_and_print(f"\n{'='*80}", "INFO")
        log_and_print("4usd RATIO LEVELS + TP UPDATE (PENDING + RUNNING) – CONSISTENCY: N×R | MARTINGALE: 1R", "INFO")
        log_and_print(f"{'='*80}", "INFO")

        for user_brokerid, cfg in usersdictionary.items():
            TERMINAL_PATH = cfg.get("TERMINAL_PATH") or cfg.get("terminal_path")
            LOGIN_ID      = cfg.get("LOGIN_ID")      or cfg.get("login_id")
            PASSWORD      = cfg.get("PASSWORD")      or cfg.get("password")
            SERVER        = cfg.get("SERVER")        or cfg.get("server")
            SCALE         = (cfg.get("SCALE")        or cfg.get("scale")        or "").strip().lower()
            STRATEGY      = (cfg.get("STRATEGY")    or cfg.get("strategy")    or "").strip().lower()

            # === Case-insensitive riskreward lookup ===
            riskreward_raw = None
            for key in cfg:
                if key.lower() == "riskreward":
                    riskreward_raw = cfg[key]
                    break

            if riskreward_raw is None:
                riskreward_raw = 2
                log_and_print(f"{user_brokerid}: 'riskreward' not found → using default 2R", "WARNING")

            log_and_print(
                f"\nProcessing broker: {user_brokerid} | Scale: {SCALE.upper()} | "
                f"Strategy: {STRATEGY.upper()} | riskreward: {riskreward_raw}R", "INFO"
            )

            # === Validate required fields ===
            missing = []
            for f in ("TERMINAL_PATH", "LOGIN_ID", "PASSWORD", "SERVER", "SCALE"):
                if not locals()[f]: missing.append(f)
            if missing:
                log_and_print(f"Missing config: {', '.join(missing)} → SKIPPED", "ERROR")
                continue

            # === MT5 Init ===
            if not os.path.exists(TERMINAL_PATH):
                log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
                continue

            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD,
                                server=SERVER, timeout=30000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue

            if not mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"MT5 login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account_info = mt5.account_info()
            if not account_info:
                log_and_print(f"Failed to get account info: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            balance = account_info.balance
            if not (20.0 <= balance < 79.99):
                log_and_print(f"Balance ${balance:.2f} not in $12–$20 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue

            log_and_print(f"Balance: ${balance:.2f} → Scanning positions & pending orders...", "INFO")

            # === Determine effective RR ===
            try:
                config_rr = int(float(riskreward_raw))
                if config_rr < 1: config_rr = 1
            except (ValueError, TypeError):
                config_rr = 2
                log_and_print(f"Invalid riskreward '{riskreward_raw}' → using 2R", "WARNING")

            effective_rr = 1 if SCALE == "martingale" else config_rr
            rr_source = "MARTINGALE (forced 1R)" if SCALE == "martingale" else f"CONFIG ({effective_rr}R)"
            log_and_print(f"Effective TP: {effective_rr}R [{rr_source}]", "INFO")

            # ------------------------------------------------------------------ #
            # 1. PENDING LIMIT ORDERS
            # ------------------------------------------------------------------ #
            pending_orders = [
                o for o in (mt5.orders_get() or [])
                if o.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT)
                and getattr(o, 'sl', 0) != 0 and getattr(o, 'tp', 0) != 0
            ]

            # ------------------------------------------------------------------ #
            # 2. RUNNING POSITIONS
            # ------------------------------------------------------------------ #
            running_positions = [
                p for p in (mt5.positions_get() or [])
                if p.type in (mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_SELL)
                and p.sl != 0 and p.tp != 0
            ]

            # Merge into a single iterable with a flag
            items_to_process = []
            for o in pending_orders:
                items_to_process.append(('PENDING', o))
            for p in running_positions:
                items_to_process.append(('RUNNING', p))

            if not items_to_process:
                log_and_print("No valid pending orders or running positions found.", "INFO")
                mt5.shutdown()
                continue

            log_and_print(f"Found {len(pending_orders)} pending + {len(running_positions)} running → total {len(items_to_process)}", "INFO")

            processed_symbols = set()
            updated_count = 0

            for kind, obj in items_to_process:
                symbol   = obj.symbol
                ticket   = getattr(obj, 'ticket', None) or getattr(obj, 'order', None)
                entry_price = getattr(obj, 'price_open', None) or getattr(obj, 'price_current', None)
                sl_price = obj.sl
                current_tp = obj.tp
                is_buy   = obj.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY)

                if symbol in processed_symbols:
                    continue

                risk_distance = abs(entry_price - sl_price)
                if risk_distance <= 0:
                    log_and_print(f"Zero risk distance on {symbol} ({kind}) → skipped", "WARNING")
                    continue

                symbol_info = mt5.symbol_info(symbol)
                if not symbol_info:
                    log_and_print(f"Symbol info missing: {symbol}", "WARNING")
                    continue

                digits = symbol_info.digits
                def r(p): return round(p, digits)

                entry_price = r(entry_price)
                sl_price    = r(sl_price)
                current_tp  = r(current_tp)
                direction   = 1 if is_buy else -1
                target_tp   = r(entry_price + direction * effective_rr * risk_distance)

                # ----- Ratio ladder (display only) -----
                ratio1 = r(entry_price + direction * 1 * risk_distance)
                ratio2 = r(entry_price + direction * 2 * risk_distance)
                ratio3 = r(entry_price + direction * 3 * risk_distance) if effective_rr >= 3 else None

                print(f"\n{symbol} | {kind} | Target: {effective_rr}R ({SCALE.upper()})")
                print(f"  Entry : {entry_price}")
                print(f"  1R    : {ratio1}")
                print(f"  2R    : {ratio2}")
                if ratio3:
                    print(f"  3R    : {ratio3}")
                print(f"  TP    : {current_tp} → ", end="")

                # ----- Modify TP -----
                tolerance = 10 ** -digits
                if abs(current_tp - target_tp) > tolerance:
                    if kind == "PENDING":
                        # modify pending order
                        request = {
                            "action": mt5.TRADE_ACTION_MODIFY,
                            "order": ticket,
                            "price": entry_price,
                            "sl": sl_price,
                            "tp": target_tp,
                            "type": obj.type,
                            "type_time": obj.type_time,
                            "type_filling": obj.type_filling,
                            "magic": getattr(obj, 'magic', 0),
                            "comment": getattr(obj, 'comment', "")
                        }
                        if hasattr(obj, 'expiration') and obj.expiration:
                            request["expiration"] = obj.expiration
                    else:  # RUNNING
                        # modify open position (SL/TP only)
                        request = {
                            "action": mt5.TRADE_ACTION_SLTP,
                            "position": ticket,
                            "sl": sl_price,
                            "tp": target_tp,
                            "symbol": symbol
                        }

                    result = mt5.order_send(request)
                    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                        print(f"{target_tp} [UPDATED]")
                        log_and_print(
                            f"TP → {effective_rr}R | {symbol} | {kind} | {current_tp} → {target_tp} [{SCALE.upper()}]",
                            "SUCCESS"
                        )
                        updated_count += 1
                    else:
                        err = result.comment if result else "Unknown"
                        print(f"{current_tp} [FAILED: {err}]")
                        log_and_print(f"TP UPDATE FAILED | {symbol} | {kind} | {err}", "ERROR")
                else:
                    print(f"{current_tp} [OK]")

                print(f"  SL    : {sl_price}")
                processed_symbols.add(symbol)

            mt5.shutdown()
            log_and_print(
                f"{user_brokerid} → {len(processed_symbols)} symbol(s) | "
                f"{updated_count} TP(s) set to {effective_rr}R [{SCALE.upper()}]",
                "SUCCESS"
            )

        log_and_print(
            "\nALL $12–$20 ACCOUNTS: R:R UPDATE (PENDING + RUNNING) – "
            "consistency=N×R, martingale=1R = DONE",
            "SUCCESS"
        )
        return True
    _4usd_live_sl_tp_amounts()
    place_4usd_orders()
    _4usd_history_and_deduplication()
    _4usd_ratio_levels()

def _80_160_orders():
    def _8usd_live_sl_tp_amounts():
        
        """
        READS: hightolow.json
        CALCULATES: Live $3 risk & profit
        PRINTS: 3-line block for every market
        SAVES:
            - live_risk_profit_all.json → only valid ≤ $8.10
            - OVERWRITES hightolow.json → REMOVES bad orders PERMANENTLY
        FILTER: Delete any order with live_risk_usd > 8.10 from BOTH files
        """

        BASE_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
        INPUT_FILE = "hightolow.json"
        OUTPUT_FILE = "live_risk_profit_all.json"

        for user_brokerid, cfg in usersdictionary.items():
            TERMINAL_PATH = cfg["TERMINAL_PATH"]
            LOGIN_ID = cfg["LOGIN_ID"]
            PASSWORD = cfg["PASSWORD"]
            SERVER = cfg["SERVER"]

            log_and_print(f"\n{'='*60}", "INFO")
            log_and_print(f"PROCESSING BROKER: {user_brokerid.upper()}", "INFO")
            log_and_print(f"{'='*60}", "INFO")

            # ------------------- CONNECT TO MT5 -------------------
            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=60000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue
            if not mt5.login(int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"Login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account = mt5.account_info()
            if not account:
                log_and_print("No account info", "ERROR")
                mt5.shutdown()
                continue

            balance = account.balance
            if not (80.0 <= balance < 159.99):
                log_and_print(f"Balance ${balance:.2f} not in $12–$20 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue

            currency = account.currency
            log_and_print(f"Connected → Balance: ${balance:.2f} {currency}", "INFO")

            # ------------------- LOAD JSON -------------------
            json_path = Path(BASE_DIR) / user_brokerid / "risk_8_usd" / INPUT_FILE
            if not json_path.exists():
                log_and_print(f"JSON not found: {json_path}", "ERROR")
                mt5.shutdown()
                continue

            try:
                with json_path.open("r", encoding="utf-8") as f:
                    original_data = json.load(f)
                entries = original_data.get("entries", [])
            except Exception as e:
                log_and_print(f"Failed to read JSON: {e}", "ERROR")
                mt5.shutdown()
                continue

            if not entries:
                log_and_print("No entries in JSON.", "INFO")
                mt5.shutdown()
                continue

            log_and_print(f"Loaded {len(entries)} entries → Calculating LIVE risk...", "INFO")

            # ------------------- PROCESS & FILTER -------------------
            valid_entries = []        # For overwriting hightolow.json
            results = []              # For live_risk_profit_all.json
            total = len(entries)
            kept = 0
            removed = 0

            for i, entry in enumerate(entries, 1):
                market = entry["market"]
                try:
                    price = float(entry["entry_price"])
                    sl = float(entry["sl_price"])
                    tp = float(entry["tp_price"])
                    volume = float(entry["volume"])
                    order_type = entry["limit_order"]
                    sl_pips = float(entry.get("sl_pips", 0))
                    tp_pips = float(entry.get("tp_pips", 0))

                    # --- LIVE DATA ---
                    info = mt5.symbol_info(market)
                    tick = mt5.symbol_info_tick(market)

                    if not info or not tick:
                        log_and_print(f"NO LIVE DATA for {market} → Using fallback", "WARNING")
                        pip_value = 0.1
                        risk_usd = volume * sl_pips * pip_value
                        profit_usd = volume * tp_pips * pip_value
                    else:
                        point = info.point
                        contract = info.trade_contract_size

                        risk_points = abs(price - sl) / point
                        profit_points = abs(tp - price) / point

                        point_val = contract * point
                        if "JPY" in market and currency == "USD":
                            point_val /= 100

                        risk_ac = risk_points * point_val * volume
                        profit_ac = profit_points * point_val * volume

                        risk_usd = risk_ac
                        profit_usd = profit_ac

                        if currency != "USD":
                            conv = f"USD{currency}"
                            rate_tick = mt5.symbol_info_tick(conv)
                            rate = rate_tick.bid if rate_tick else 1.0
                            risk_usd /= rate
                            profit_usd /= rate

                    risk_usd = round(risk_usd, 2)
                    profit_usd = round(profit_usd, 2)

                    # --- PRINT ALL ---
                    print(f"market: {market}")
                    print(f"risk: {risk_usd} USD")
                    print(f"profit: {profit_usd} USD")
                    print("---")

                    # --- FILTER: KEEP ONLY <= 8.10 ---
                    if risk_usd <= 8.10:
                        # Keep in BOTH files
                        valid_entries.append(entry)  # Original format
                        results.append({
                            "market": market,
                            "order_type": order_type,
                            "entry_price": round(price, 6),
                            "sl": round(sl, 6),
                            "tp": round(tp, 6),
                            "volume": round(volume, 5),
                            "live_risk_usd": risk_usd,
                            "live_profit_usd": profit_usd,
                            "sl_pips": round(sl_pips, 2),
                            "tp_pips": round(tp_pips, 2),
                            "has_live_tick": bool(info and tick),
                            "current_bid": round(tick.bid, 6) if tick else None,
                            "current_ask": round(tick.ask, 6) if tick else None,
                        })
                        kept += 1
                    else:
                        removed += 1
                        log_and_print(f"REMOVED {market}: live risk ${risk_usd} > $8.10 → DELETED FROM BOTH JSON FILES", "WARNING")

                except Exception as e:
                    log_and_print(f"ERROR on {market}: {e}", "ERROR")
                    removed += 1

                if i % 5 == 0 or i == total:
                    log_and_print(f"Processed {i}/{total} | Kept: {kept} | Removed: {removed}", "INFO")

            # ------------------- SAVE OUTPUT: live_risk_profit_all.json -------------------
            out_path = json_path.parent / OUTPUT_FILE
            report = {
                "broker": user_brokerid,
                "account_currency": currency,
                "generated_at": datetime.now(pytz.timezone("Africa/Lagos")).strftime("%Y-%m-%d %H:%M:%S.%f%z"),
                "source_file": str(json_path),
                "total_entries": total,
                "kept_risk_<=_8.10": kept,
                "removed_risk_>_8.10": removed,
                "filter_applied": "Delete from both input & output if live_risk_usd > 8.10",
                "orders": results
            }

            try:
                with out_path.open("w", encoding="utf-8") as f:
                    json.dump(report, f, indent=2)
                log_and_print(f"SAVED → {out_path} | Kept: {kept} | Removed: {removed}", "SUCCESS")
            except Exception as e:
                log_and_print(f"Save failed: {e}", "ERROR")

            # ------------------- OVERWRITE INPUT: hightolow.json -------------------
            cleaned_input = original_data.copy()
            cleaned_input["entries"] = valid_entries  # Only good ones

            try:
                with json_path.open("w", encoding="utf-8") as f:
                    json.dump(cleaned_input, f, indent=2)
                log_and_print(f"OVERWRITTEN → {json_path} | Now has {len(valid_entries)} entries (removed {removed})", "SUCCESS")
            except Exception as e:
                log_and_print(f"Failed to overwrite input JSON: {e}", "ERROR")

            mt5.shutdown()
            log_and_print(f"FINISHED {user_brokerid} → {kept}/{total} valid orders in BOTH files", "SUCCESS")

        log_and_print("\nALL DONE – BAD ORDERS (> $8.10) DELETED FROM INPUT & OUTPUT!", "SUCCESS")
        return True
    
    def place_8usd_orders():
        

        BASE_INPUT_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
        RISK_FOLDER = "risk_8_usd"
        STRATEGY_FILE = "hightolow.json"
        REPORT_SUFFIX = "forex_order_report.json"
        ISSUES_FILE = "ordersissues.json"

        for user_brokerid, broker_cfg in usersdictionary.items():
            TERMINAL_PATH = broker_cfg["TERMINAL_PATH"]
            LOGIN_ID = broker_cfg["LOGIN_ID"]
            PASSWORD = broker_cfg["PASSWORD"]
            SERVER = broker_cfg["SERVER"]

            log_and_print(f"Processing broker: {user_brokerid} (Balance $12–$20 mode)", "INFO")

            # === MT5 Init ===
            if not os.path.exists(TERMINAL_PATH):
                log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
                continue

            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=30000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue

            if not mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"MT5 login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account_info = mt5.account_info()
            if not account_info:
                log_and_print(f"Failed to get account info: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue
            balance = account_info.balance
            equity = account_info.equity
            log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f}", "INFO")
            if equity < 80.0 and balance >= 80.0:
                log_and_print(f"Equity ${equity:.2f} < $20.0 while Balance ${balance:.2f} ≥ $20.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if equity >= 80.0 and balance < 80.0:
                log_and_print(f"Equity ${equity:.2f} > $20.0 while Balance ${balance:.2f} < $20.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if not (80.0 <= balance < 159.99):
                log_and_print(f"Balance ${balance:.2f} not in $20–$99.99 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue
            # === Only reaches here if: equity >= 8 AND balance in [8, 11.99) ===
            log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f} → Using {RISK_FOLDER} + {STRATEGY_FILE}", "INFO")



            log_and_print(f"Balance: ${balance:.2f} → Using {RISK_FOLDER} + {STRATEGY_FILE}", "INFO")

            # === Load hightolow.json ===
            file_path = Path(BASE_INPUT_DIR) / user_brokerid / RISK_FOLDER / STRATEGY_FILE
            if not file_path.exists():
                log_and_print(f"File not found: {file_path}", "WARNING")
                mt5.shutdown()
                continue

            try:
                with file_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                    entries = data.get("entries", [])
            except Exception as e:
                log_and_print(f"Failed to read {file_path}: {e}", "ERROR")
                mt5.shutdown()
                continue

            if not entries:
                log_and_print("No entries in hightolow.json", "INFO")
                mt5.shutdown()
                continue

            # === Load existing orders & positions ===
            existing_pending = {}  # (symbol, type) → ticket
            running_positions = set()  # symbols with open position

            for order in (mt5.orders_get() or []):
                if order.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT):
                    existing_pending[(order.symbol, order.type)] = order.ticket

            for pos in (mt5.positions_get() or []):
                running_positions.add(pos.symbol)

            # === Reporting ===
            report_file = file_path.parent / REPORT_SUFFIX
            existing_reports = json.load(report_file.open("r", encoding="utf-8")) if report_file.exists() else []
            issues_list = []
            now_str = datetime.now(pytz.timezone("Africa/Lagos")).strftime("%Y-%m-%d %H:%M:%S.%f+01:00")
            placed = failed = skipped = 0

            for entry in entries:
                try:
                    symbol = entry["market"]
                    price = float(entry["entry_price"])
                    sl = float(entry["sl_price"])
                    tp = float(entry["tp_price"])
                    volume = float(entry["volume"])
                    order_type_str = entry["limit_order"]
                    order_type = mt5.ORDER_TYPE_BUY_LIMIT if order_type_str == "buy_limit" else mt5.ORDER_TYPE_SELL_LIMIT

                    # === SKIP: Already running or pending ===
                    if symbol in running_positions:
                        skipped += 1
                        log_and_print(f"{symbol} has running position → SKIPPED", "INFO")
                        continue

                    key = (symbol, order_type)
                    if key in existing_pending:
                        skipped += 1
                        log_and_print(f"{symbol} {order_type_str} already pending → SKIPPED", "INFO")
                        continue

                    # === Symbol check ===
                    symbol_info = mt5.symbol_info(symbol)
                    if not symbol_info or not symbol_info.visible:
                        issues_list.append({"symbol": symbol, "reason": "Symbol not available"})
                        failed += 1
                        continue

                    # === Volume fix ===
                    vol_step = symbol_info.volume_step
                    volume = max(symbol_info.volume_min,
                                round(volume / vol_step) * vol_step)
                    volume = min(volume, symbol_info.volume_max)

                    # === Price distance check ===
                    tick = mt5.symbol_info_tick(symbol)
                    if not tick:
                        issues_list.append({"symbol": symbol, "reason": "No tick data"})
                        failed += 1
                        continue

                    point = symbol_info.point
                    if order_type == mt5.ORDER_TYPE_BUY_LIMIT:
                        if price >= tick.ask or (tick.ask - price) < 10 * point:
                            skipped += 1
                            continue
                    else:
                        if price <= tick.bid or (price - tick.bid) < 10 * point:
                            skipped += 1
                            continue

                    # === Build & send order ===
                    request = {
                        "action": mt5.TRADE_ACTION_PENDING,
                        "symbol": symbol,
                        "volume": volume,
                        "type": order_type,
                        "price": price,
                        "sl": sl,
                        "tp": tp,
                        "deviation": 10,
                        "magic": 123856,
                        "comment": "Risk3_Auto",
                        "type_time": mt5.ORDER_TIME_GTC,
                        "type_filling": mt5.ORDER_FILLING_IOC,
                    }

                    result = mt5.order_send(request)
                    if result is None:
                        result = type('obj', (), {'retcode': 10000, 'comment': 'order_send returned None'})()

                    success = result.retcode == mt5.TRADE_RETCODE_DONE
                    if success:
                        existing_pending[key] = result.order
                        placed += 1
                        log_and_print(f"{symbol} {order_type_str} @ {price} → PLACED (ticket {result.order})", "SUCCESS")
                    else:
                        failed += 1
                        issues_list.append({"symbol": symbol, "reason": result.comment})

                    # === Report ===
                    if "cent" in RISK_FOLDER:
                        risk_usd = 0.5
                    else:
                        risk_usd = float(RISK_FOLDER.split("_")[1].replace("usd", ""))

                    # === Report ===
                    report_entry = {
                        "symbol": symbol,
                        "order_type": order_type_str,
                        "price": price,
                        "volume": volume,
                        "sl": sl,
                        "tp": tp,
                        "risk_usd": risk_usd,   # ← Now correct: 0.5, 1.0, 2.0, 3.0, 8.0
                        "ticket": result.order if success else None,
                        "success": success,
                        "error_code": result.retcode if not success else None,
                        "error_msg": result.comment if not success else None,
                        "timestamp": now_str
                    }
                    existing_reports.append(report_entry)
                    try:
                        with report_file.open("w", encoding="utf-8") as f:
                            json.dump(existing_reports, f, indent=2)
                    except:
                        pass

                except Exception as e:
                    failed += 1
                    issues_list.append({"symbol": symbol, "reason": f"Exception: {e}"})
                    log_and_print(f"Error processing {symbol}: {e}", "ERROR")

            # === Save issues ===
            issues_path = file_path.parent / ISSUES_FILE
            try:
                existing_issues = json.load(issues_path.open("r", encoding="utf-8")) if issues_path.exists() else []
                with issues_path.open("w", encoding="utf-8") as f:
                    json.dump(existing_issues + issues_list, f, indent=2)
            except:
                pass

            mt5.shutdown()
            log_and_print(
                f"{user_brokerid} DONE → Placed: {placed}, Failed: {failed}, Skipped: {skipped}",
                "SUCCESS"
            )

        log_and_print("All $12–$20 accounts processed.", "SUCCESS")
        return True
  
    def _8usd_history_and_deduplication():
        """
        HISTORY + PENDING + POSITION DUPLICATE DETECTOR + RISK SNIPER
        - Cancels risk > $8.10  (even if TP=0)
        - Cancels HISTORY DUPLICATES
        - Cancels PENDING LIMIT DUPLICATES
        - Cancels PENDING if POSITION already exists
        - Shows duplicate market name on its own line
        ONLY PROCESSES ACCOUNTS WITH BALANCE $12.00 – $19.99
        """
        BASE_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
        REPORT_NAME = "pending_risk_profit_per_order.json"
        MAX_RISK_USD = 8.10
        LOOKBACK_DAYS = 5
        PRICE_PRECISION = 5
        TZ = pytz.timezone("Africa/Lagos")

        five_days_ago = datetime.now(TZ) - timedelta(days=LOOKBACK_DAYS)

        for user_brokerid, cfg in usersdictionary.items():
            TERMINAL_PATH = cfg["TERMINAL_PATH"]
            LOGIN_ID     = cfg["LOGIN_ID"]
            PASSWORD     = cfg["PASSWORD"]
            SERVER       = cfg["SERVER"]

            log_and_print(f"\n{'='*80}", "INFO")
            log_and_print(f"BROKER: {user_brokerid.upper()} | FULL DUPLICATE + RISK GUARD", "INFO")
            log_and_print(f"{'='*80}", "INFO")

            # ---------- MT5 Init ----------
            if not os.path.exists(TERMINAL_PATH):
                log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
                continue
            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=30000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue
            if not mt5.login(int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"Login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account = mt5.account_info()
            if not account:
                log_and_print("No account info.", "ERROR")
                mt5.shutdown()
                continue

            balance = account.balance
            equity = account.equity
            log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f}", "INFO")
            if equity < 80.0 and balance >= 80.0:
                log_and_print(f"Equity ${equity:.2f} < $20.0 while Balance ${balance:.2f} ≥ $20.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if equity >= 80.0 and balance < 80.0:
                log_and_print(f"Equity ${equity:.2f} > $20.0 while Balance ${balance:.2f} < $20.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if not (80.0 <= balance < 159.99):
                log_and_print(f"Balance ${balance:.2f} not in $20–$99.99 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue

            currency = account.currency
            log_and_print(f"Account: {account.login} | Balance: ${balance:.2f} {currency} → Proceeding with risk_8_usd checks", "INFO")

            # ---------- Get Data ----------
            pending_orders = [o for o in (mt5.orders_get() or [])
                            if o.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT)]
            positions = mt5.positions_get()
            history_deals = mt5.history_deals_get(int(five_days_ago.timestamp()), int(datetime.now(TZ).timestamp()))

            if not pending_orders:
                log_and_print("No pending orders.", "INFO")
                mt5.shutdown()
                continue

            # ---------- BUILD DATABASES ----------
            log_and_print(f"Building duplicate databases...", "INFO")

            # 1. Historical Setups
            historical_keys = {}  # (symbol, entry, sl) → details
            if history_deals:
                for deal in history_deals:
                    if deal.entry != mt5.DEAL_ENTRY_IN: continue
                    if deal.type not in (mt5.DEAL_TYPE_BUY, mt5.DEAL_TYPE_SELL): continue

                    order = mt5.history_orders_get(ticket=deal.order)
                    if not order: continue
                    order = order[0]
                    if order.sl == 0: continue

                    symbol = deal.symbol
                    entry = round(deal.price, PRICE_PRECISION)
                    sl = round(order.sl, PRICE_PRECISION)

                    key = (symbol, entry, sl)
                    if key not in historical_keys:
                        profit = sum(d.profit for d in history_deals if d.order == deal.order and d.entry == mt5.DEAL_ENTRY_OUT)
                        historical_keys[key] = {
                            "time": datetime.fromtimestamp(deal.time, TZ).strftime("%Y-%m-%d %H:%M"),
                            "profit": round(profit, 2),
                            "symbol": symbol
                        }

            # 2. Open Positions (by symbol)
            open_symbols = {pos.symbol for pos in positions} if positions else set()

            # 3. Pending Orders Key Map
            pending_keys = {}  # (symbol, entry, sl) → [order_tickets]
            for order in pending_orders:
                key = (order.symbol, round(order.price_open, PRICE_PRECISION), round(order.sl, PRICE_PRECISION))
                pending_keys.setdefault(key, []).append(order.ticket)

            log_and_print(f"Loaded: {len(historical_keys)} history | {len(open_symbols)} open | {len(pending_keys)} unique pending setups", "INFO")

            # ---------- Process & Cancel ----------
            per_order_data = []
            kept = cancelled_risk = cancelled_hist = cancelled_pend_dup = cancelled_pos_dup = skipped = 0

            for order in pending_orders:
                symbol = order.symbol
                ticket = order.ticket
                volume = order.volume_current
                entry = round(order.price_open, PRICE_PRECISION)
                sl = round(order.sl, PRICE_PRECISION)
                tp = order.tp                     # may be 0

                # ---- NEW: ONLY REQUIRE SL, TP CAN BE 0 ----
                if sl == 0:
                    log_and_print(f"SKIP {ticket} | {symbol} | No SL", "WARNING")
                    skipped += 1
                    continue

                info = mt5.symbol_info(symbol)
                if not info or not mt5.symbol_info_tick(symbol):
                    log_and_print(f"SKIP {ticket} | {symbol} | No symbol data", "WARNING")
                    skipped += 1
                    continue

                point = info.point
                contract = info.trade_contract_size
                point_val = contract * point
                if "JPY" in symbol and currency == "USD":
                    point_val /= 100

                # ---- RISK CALCULATION (always possible with SL) ----
                risk_points = abs(entry - sl) / point
                risk_usd = risk_points * point_val * volume
                if currency != "USD":
                    rate = mt5.symbol_info_tick(f"USD{currency}")
                    if not rate:
                        log_and_print(f"SKIP {ticket} | No USD{currency} rate", "WARNING")
                        skipped += 1
                        continue
                    risk_usd /= rate.bid

                # ---- PROFIT CALCULATION (only if TP exists) ----
                profit_usd = None
                if tp != 0:
                    profit_usd = abs(tp - entry) / point * point_val * volume
                    if currency != "USD":
                        profit_usd /= rate.bid

                # ---- DUPLICATE KEYS ----
                key = (symbol, entry, sl)
                dup_hist = historical_keys.get(key)
                is_position_open = symbol in open_symbols
                is_pending_duplicate = len(pending_keys.get(key, [])) > 1

                print(f"\nmarket: {symbol}")
                print(f"risk: {risk_usd:.2f} USD | profit: {profit_usd if profit_usd is not None else 'N/A'} USD")

                cancel_reason = None
                cancel_type = None

                # === 1. RISK CANCEL (works even if TP=0) ===
                if risk_usd > MAX_RISK_USD:
                    cancel_reason = f"RISK > ${MAX_RISK_USD}"
                    cancel_type = "RISK"
                    print(f"{cancel_reason} → CANCELLED")

                # === 2. HISTORY DUPLICATE ===
                elif dup_hist:
                    cancel_reason = "HISTORY DUPLICATE"
                    cancel_type = "HIST_DUP"
                    print("HISTORY DUPLICATE ORDER FOUND!")
                    print(dup_hist["symbol"])
                    print(f"entry: {entry} | sl: {sl}")
                    print(f"used: {dup_hist['time']} | P/L: {dup_hist['profit']:+.2f} {currency}")
                    print("→ HISTORY DUPLICATE CANCELLED")
                    print("!" * 60)

                # === 3. PENDING DUPLICATE ===
                elif is_pending_duplicate:
                    cancel_reason = "PENDING DUPLICATE"
                    cancel_type = "PEND_DUP"
                    print("PENDING LIMIT DUPLICATE FOUND!")
                    print(symbol)
                    print(f"→ DUPLICATE PENDING ORDER CANCELLED")
                    print("-" * 60)

                # === 8. POSITION EXISTS (Cancel Pending) ===
                elif is_position_open:
                    cancel_reason = "POSITION ALREADY OPEN"
                    cancel_type = "POS_DUP"
                    print("POSITION ALREADY RUNNING!")
                    print(symbol)
                    print(f"→ PENDING ORDER CANCELLED (POSITION ACTIVE)")
                    print("^" * 60)

                # === NO ISSUE → KEEP ===
                else:
                    print("No duplicate. Order kept.")
                    kept += 1
                    per_order_data.append({
                        "ticket": ticket,
                        "symbol": symbol,
                        "entry": entry,
                        "sl": sl,
                        "tp": tp,
                        "risk_usd": round(risk_usd, 2),
                        "profit_usd": round(profit_usd, 2) if profit_usd is not None else None,
                        "status": "KEPT"
                    })
                    continue  # Skip cancel

                # === CANCEL ORDER ===
                req = {"action": mt5.TRADE_ACTION_REMOVE, "order": ticket}
                res = mt5.order_send(req)
                if res.retcode == mt5.TRADE_RETCODE_DONE:
                    log_and_print(f"{cancel_type} CANCELLED {ticket} | {symbol} | {cancel_reason}", "WARNING")
                    if cancel_type == "RISK": cancelled_risk += 1
                    elif cancel_type == "HIST_DUP": cancelled_hist += 1
                    elif cancel_type == "PEND_DUP": cancelled_pend_dup += 1
                    elif cancel_type == "POS_DUP": cancelled_pos_dup += 1
                else:
                    log_and_print(f"CANCEL FAILED {ticket} | {res.comment}", "ERROR")

                per_order_data.append({
                    "ticket": ticket,
                    "symbol": symbol,
                    "entry": entry,
                    "sl": sl,
                    "tp": tp,
                    "risk_usd": round(risk_usd, 2),
                    "profit_usd": round(profit_usd, 2) if profit_usd is not None else None,
                    "status": "CANCELLED",
                    "reason": cancel_reason,
                    "duplicate_time": dup_hist["time"] if dup_hist else None,
                    "duplicate_pl": dup_hist["profit"] if dup_hist else None
                })

            # === SUMMARY ===
            log_and_print(f"\nSUMMARY:", "SUCCESS")
            log_and_print(f"KEPT: {kept}", "INFO")
            log_and_print(f"CANCELLED → RISK: {cancelled_risk} | HIST DUP: {cancelled_hist} | "
                        f"PEND DUP: {cancelled_pend_dup} | POS DUP: {cancelled_pos_dup} | SKIPPED: {skipped}", "WARNING")

            # === SAVE REPORT ===
            out_dir = Path(BASE_DIR) / user_brokerid / "risk_8_usd"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / REPORT_NAME

            report = {
                "broker": user_brokerid,
                "checked_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
                "max_risk_usd": MAX_RISK_USD,
                "lookback_days": LOOKBACK_DAYS,
                "summary": {
                    "kept": kept,
                    "cancelled_risk": cancelled_risk,
                    "cancelled_history_duplicate": cancelled_hist,
                    "cancelled_pending_duplicate": cancelled_pend_dup,
                    "cancelled_position_duplicate": cancelled_pos_dup,
                    "skipped": skipped
                },
                "orders": per_order_data
            }

            try:
                with out_path.open("w", encoding="utf-8") as f:
                    json.dump(report, f, indent=2)
                log_and_print(f"Report saved: {out_path}", "SUCCESS")
            except Exception as e:
                log_and_print(f"Save error: {e}", "ERROR")

            mt5.shutdown()

        log_and_print("\nALL $12–$20 ACCOUNTS: DUPLICATE SCAN + RISK GUARD = DONE", "SUCCESS")
        return True

    def _8usd_ratio_levels():
        """
        8usd RATIO LEVELS + TP UPDATE (PENDING + RUNNING POSITIONS) – BROKER-SAFE
        - Balance $12–$19.99 only
        - Auto-supports riskreward: 1, 2, 3, 8... (any integer)
        - Case-insensitive config
        - consistency → Dynamic TP = RISKREWARD × Risk
        - martingale → TP = 1R (always), ignores RISKREWARD
        - Smart ratio ladder (shows 1R, 2R, 3R only when needed)
        """
        TZ = pytz.timezone("Africa/Lagos")

        log_and_print(f"\n{'='*80}", "INFO")
        log_and_print("8usd RATIO LEVELS + TP UPDATE (PENDING + RUNNING) – CONSISTENCY: N×R | MARTINGALE: 1R", "INFO")
        log_and_print(f"{'='*80}", "INFO")

        for user_brokerid, cfg in usersdictionary.items():
            TERMINAL_PATH = cfg.get("TERMINAL_PATH") or cfg.get("terminal_path")
            LOGIN_ID      = cfg.get("LOGIN_ID")      or cfg.get("login_id")
            PASSWORD      = cfg.get("PASSWORD")      or cfg.get("password")
            SERVER        = cfg.get("SERVER")        or cfg.get("server")
            SCALE         = (cfg.get("SCALE")        or cfg.get("scale")        or "").strip().lower()
            STRATEGY      = (cfg.get("STRATEGY")    or cfg.get("strategy")    or "").strip().lower()

            # === Case-insensitive riskreward lookup ===
            riskreward_raw = None
            for key in cfg:
                if key.lower() == "riskreward":
                    riskreward_raw = cfg[key]
                    break

            if riskreward_raw is None:
                riskreward_raw = 2
                log_and_print(f"{user_brokerid}: 'riskreward' not found → using default 2R", "WARNING")

            log_and_print(
                f"\nProcessing broker: {user_brokerid} | Scale: {SCALE.upper()} | "
                f"Strategy: {STRATEGY.upper()} | riskreward: {riskreward_raw}R", "INFO"
            )

            # === Validate required fields ===
            missing = []
            for f in ("TERMINAL_PATH", "LOGIN_ID", "PASSWORD", "SERVER", "SCALE"):
                if not locals()[f]: missing.append(f)
            if missing:
                log_and_print(f"Missing config: {', '.join(missing)} → SKIPPED", "ERROR")
                continue

            # === MT5 Init ===
            if not os.path.exists(TERMINAL_PATH):
                log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
                continue

            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD,
                                server=SERVER, timeout=30000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue

            if not mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"MT5 login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account_info = mt5.account_info()
            if not account_info:
                log_and_print(f"Failed to get account info: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            balance = account_info.balance
            if not (80.0 <= balance < 159.99):
                log_and_print(f"Balance ${balance:.2f} not in $12–$20 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue

            log_and_print(f"Balance: ${balance:.2f} → Scanning positions & pending orders...", "INFO")

            # === Determine effective RR ===
            try:
                config_rr = int(float(riskreward_raw))
                if config_rr < 1: config_rr = 1
            except (ValueError, TypeError):
                config_rr = 2
                log_and_print(f"Invalid riskreward '{riskreward_raw}' → using 2R", "WARNING")

            effective_rr = 1 if SCALE == "martingale" else config_rr
            rr_source = "MARTINGALE (forced 1R)" if SCALE == "martingale" else f"CONFIG ({effective_rr}R)"
            log_and_print(f"Effective TP: {effective_rr}R [{rr_source}]", "INFO")

            # ------------------------------------------------------------------ #
            # 1. PENDING LIMIT ORDERS
            # ------------------------------------------------------------------ #
            pending_orders = [
                o for o in (mt5.orders_get() or [])
                if o.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT)
                and getattr(o, 'sl', 0) != 0 and getattr(o, 'tp', 0) != 0
            ]

            # ------------------------------------------------------------------ #
            # 2. RUNNING POSITIONS
            # ------------------------------------------------------------------ #
            running_positions = [
                p for p in (mt5.positions_get() or [])
                if p.type in (mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_SELL)
                and p.sl != 0 and p.tp != 0
            ]

            # Merge into a single iterable with a flag
            items_to_process = []
            for o in pending_orders:
                items_to_process.append(('PENDING', o))
            for p in running_positions:
                items_to_process.append(('RUNNING', p))

            if not items_to_process:
                log_and_print("No valid pending orders or running positions found.", "INFO")
                mt5.shutdown()
                continue

            log_and_print(f"Found {len(pending_orders)} pending + {len(running_positions)} running → total {len(items_to_process)}", "INFO")

            processed_symbols = set()
            updated_count = 0

            for kind, obj in items_to_process:
                symbol   = obj.symbol
                ticket   = getattr(obj, 'ticket', None) or getattr(obj, 'order', None)
                entry_price = getattr(obj, 'price_open', None) or getattr(obj, 'price_current', None)
                sl_price = obj.sl
                current_tp = obj.tp
                is_buy   = obj.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY)

                if symbol in processed_symbols:
                    continue

                risk_distance = abs(entry_price - sl_price)
                if risk_distance <= 0:
                    log_and_print(f"Zero risk distance on {symbol} ({kind}) → skipped", "WARNING")
                    continue

                symbol_info = mt5.symbol_info(symbol)
                if not symbol_info:
                    log_and_print(f"Symbol info missing: {symbol}", "WARNING")
                    continue

                digits = symbol_info.digits
                def r(p): return round(p, digits)

                entry_price = r(entry_price)
                sl_price    = r(sl_price)
                current_tp  = r(current_tp)
                direction   = 1 if is_buy else -1
                target_tp   = r(entry_price + direction * effective_rr * risk_distance)

                # ----- Ratio ladder (display only) -----
                ratio1 = r(entry_price + direction * 1 * risk_distance)
                ratio2 = r(entry_price + direction * 2 * risk_distance)
                ratio3 = r(entry_price + direction * 3 * risk_distance) if effective_rr >= 3 else None

                print(f"\n{symbol} | {kind} | Target: {effective_rr}R ({SCALE.upper()})")
                print(f"  Entry : {entry_price}")
                print(f"  1R    : {ratio1}")
                print(f"  2R    : {ratio2}")
                if ratio3:
                    print(f"  3R    : {ratio3}")
                print(f"  TP    : {current_tp} → ", end="")

                # ----- Modify TP -----
                tolerance = 10 ** -digits
                if abs(current_tp - target_tp) > tolerance:
                    if kind == "PENDING":
                        # modify pending order
                        request = {
                            "action": mt5.TRADE_ACTION_MODIFY,
                            "order": ticket,
                            "price": entry_price,
                            "sl": sl_price,
                            "tp": target_tp,
                            "type": obj.type,
                            "type_time": obj.type_time,
                            "type_filling": obj.type_filling,
                            "magic": getattr(obj, 'magic', 0),
                            "comment": getattr(obj, 'comment', "")
                        }
                        if hasattr(obj, 'expiration') and obj.expiration:
                            request["expiration"] = obj.expiration
                    else:  # RUNNING
                        # modify open position (SL/TP only)
                        request = {
                            "action": mt5.TRADE_ACTION_SLTP,
                            "position": ticket,
                            "sl": sl_price,
                            "tp": target_tp,
                            "symbol": symbol
                        }

                    result = mt5.order_send(request)
                    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                        print(f"{target_tp} [UPDATED]")
                        log_and_print(
                            f"TP → {effective_rr}R | {symbol} | {kind} | {current_tp} → {target_tp} [{SCALE.upper()}]",
                            "SUCCESS"
                        )
                        updated_count += 1
                    else:
                        err = result.comment if result else "Unknown"
                        print(f"{current_tp} [FAILED: {err}]")
                        log_and_print(f"TP UPDATE FAILED | {symbol} | {kind} | {err}", "ERROR")
                else:
                    print(f"{current_tp} [OK]")

                print(f"  SL    : {sl_price}")
                processed_symbols.add(symbol)

            mt5.shutdown()
            log_and_print(
                f"{user_brokerid} → {len(processed_symbols)} symbol(s) | "
                f"{updated_count} TP(s) set to {effective_rr}R [{SCALE.upper()}]",
                "SUCCESS"
            )

        log_and_print(
            "\nALL $12–$20 ACCOUNTS: R:R UPDATE (PENDING + RUNNING) – "
            "consistency=N×R, martingale=1R = DONE",
            "SUCCESS"
        )
        return True
    
    _8usd_live_sl_tp_amounts()
    place_8usd_orders()
    _8usd_history_and_deduplication()
    _8usd_ratio_levels()

def _160_320_orders():
    def _16usd_live_sl_tp_amounts():
        
        """
        READS: hightolow.json
        CALCULATES: Live $3 risk & profit
        PRINTS: 3-line block for every market
        SAVES:
            - live_risk_profit_all.json → only valid ≤ $16.10
            - OVERWRITES hightolow.json → REMOVES bad orders PERMANENTLY
        FILTER: Delete any order with live_risk_usd > 16.10 from BOTH files
        """

        BASE_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
        INPUT_FILE = "hightolow.json"
        OUTPUT_FILE = "live_risk_profit_all.json"

        for user_brokerid, cfg in usersdictionary.items():
            TERMINAL_PATH = cfg["TERMINAL_PATH"]
            LOGIN_ID = cfg["LOGIN_ID"]
            PASSWORD = cfg["PASSWORD"]
            SERVER = cfg["SERVER"]

            log_and_print(f"\n{'='*60}", "INFO")
            log_and_print(f"PROCESSING BROKER: {user_brokerid.upper()}", "INFO")
            log_and_print(f"{'='*60}", "INFO")

            # ------------------- CONNECT TO MT5 -------------------
            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=60000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue
            if not mt5.login(int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"Login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account = mt5.account_info()
            if not account:
                log_and_print("No account info", "ERROR")
                mt5.shutdown()
                continue

            balance = account.balance
            if not (160.0 <= balance < 319.99):
                log_and_print(f"Balance ${balance:.2f} not in $12–$20 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue

            currency = account.currency
            log_and_print(f"Connected → Balance: ${balance:.2f} {currency}", "INFO")

            # ------------------- LOAD JSON -------------------
            json_path = Path(BASE_DIR) / user_brokerid / "risk_16_usd" / INPUT_FILE
            if not json_path.exists():
                log_and_print(f"JSON not found: {json_path}", "ERROR")
                mt5.shutdown()
                continue

            try:
                with json_path.open("r", encoding="utf-8") as f:
                    original_data = json.load(f)
                entries = original_data.get("entries", [])
            except Exception as e:
                log_and_print(f"Failed to read JSON: {e}", "ERROR")
                mt5.shutdown()
                continue

            if not entries:
                log_and_print("No entries in JSON.", "INFO")
                mt5.shutdown()
                continue

            log_and_print(f"Loaded {len(entries)} entries → Calculating LIVE risk...", "INFO")

            # ------------------- PROCESS & FILTER -------------------
            valid_entries = []        # For overwriting hightolow.json
            results = []              # For live_risk_profit_all.json
            total = len(entries)
            kept = 0
            removed = 0

            for i, entry in enumerate(entries, 1):
                market = entry["market"]
                try:
                    price = float(entry["entry_price"])
                    sl = float(entry["sl_price"])
                    tp = float(entry["tp_price"])
                    volume = float(entry["volume"])
                    order_type = entry["limit_order"]
                    sl_pips = float(entry.get("sl_pips", 0))
                    tp_pips = float(entry.get("tp_pips", 0))

                    # --- LIVE DATA ---
                    info = mt5.symbol_info(market)
                    tick = mt5.symbol_info_tick(market)

                    if not info or not tick:
                        log_and_print(f"NO LIVE DATA for {market} → Using fallback", "WARNING")
                        pip_value = 0.1
                        risk_usd = volume * sl_pips * pip_value
                        profit_usd = volume * tp_pips * pip_value
                    else:
                        point = info.point
                        contract = info.trade_contract_size

                        risk_points = abs(price - sl) / point
                        profit_points = abs(tp - price) / point

                        point_val = contract * point
                        if "JPY" in market and currency == "USD":
                            point_val /= 100

                        risk_ac = risk_points * point_val * volume
                        profit_ac = profit_points * point_val * volume

                        risk_usd = risk_ac
                        profit_usd = profit_ac

                        if currency != "USD":
                            conv = f"USD{currency}"
                            rate_tick = mt5.symbol_info_tick(conv)
                            rate = rate_tick.bid if rate_tick else 1.0
                            risk_usd /= rate
                            profit_usd /= rate

                    risk_usd = round(risk_usd, 2)
                    profit_usd = round(profit_usd, 2)

                    # --- PRINT ALL ---
                    print(f"market: {market}")
                    print(f"risk: {risk_usd} USD")
                    print(f"profit: {profit_usd} USD")
                    print("---")

                    # --- FILTER: KEEP ONLY <= 16.10 ---
                    if risk_usd <= 16.10:
                        # Keep in BOTH files
                        valid_entries.append(entry)  # Original format
                        results.append({
                            "market": market,
                            "order_type": order_type,
                            "entry_price": round(price, 6),
                            "sl": round(sl, 6),
                            "tp": round(tp, 6),
                            "volume": round(volume, 5),
                            "live_risk_usd": risk_usd,
                            "live_profit_usd": profit_usd,
                            "sl_pips": round(sl_pips, 2),
                            "tp_pips": round(tp_pips, 2),
                            "has_live_tick": bool(info and tick),
                            "current_bid": round(tick.bid, 6) if tick else None,
                            "current_ask": round(tick.ask, 6) if tick else None,
                        })
                        kept += 1
                    else:
                        removed += 1
                        log_and_print(f"REMOVED {market}: live risk ${risk_usd} > $16.10 → DELETED FROM BOTH JSON FILES", "WARNING")

                except Exception as e:
                    log_and_print(f"ERROR on {market}: {e}", "ERROR")
                    removed += 1

                if i % 5 == 0 or i == total:
                    log_and_print(f"Processed {i}/{total} | Kept: {kept} | Removed: {removed}", "INFO")

            # ------------------- SAVE OUTPUT: live_risk_profit_all.json -------------------
            out_path = json_path.parent / OUTPUT_FILE
            report = {
                "broker": user_brokerid,
                "account_currency": currency,
                "generated_at": datetime.now(pytz.timezone("Africa/Lagos")).strftime("%Y-%m-%d %H:%M:%S.%f%z"),
                "source_file": str(json_path),
                "total_entries": total,
                "kept_risk_<=_16.10": kept,
                "removed_risk_>_16.10": removed,
                "filter_applied": "Delete from both input & output if live_risk_usd > 16.10",
                "orders": results
            }

            try:
                with out_path.open("w", encoding="utf-8") as f:
                    json.dump(report, f, indent=2)
                log_and_print(f"SAVED → {out_path} | Kept: {kept} | Removed: {removed}", "SUCCESS")
            except Exception as e:
                log_and_print(f"Save failed: {e}", "ERROR")

            # ------------------- OVERWRITE INPUT: hightolow.json -------------------
            cleaned_input = original_data.copy()
            cleaned_input["entries"] = valid_entries  # Only good ones

            try:
                with json_path.open("w", encoding="utf-8") as f:
                    json.dump(cleaned_input, f, indent=2)
                log_and_print(f"OVERWRITTEN → {json_path} | Now has {len(valid_entries)} entries (removed {removed})", "SUCCESS")
            except Exception as e:
                log_and_print(f"Failed to overwrite input JSON: {e}", "ERROR")

            mt5.shutdown()
            log_and_print(f"FINISHED {user_brokerid} → {kept}/{total} valid orders in BOTH files", "SUCCESS")

        log_and_print("\nALL DONE – BAD ORDERS (> $16.10) DELETED FROM INPUT & OUTPUT!", "SUCCESS")
        return True
    
    def place_16usd_orders():
        

        BASE_INPUT_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
        RISK_FOLDER = "risk_16_usd"
        STRATEGY_FILE = "hightolow.json"
        REPORT_SUFFIX = "forex_order_report.json"
        ISSUES_FILE = "ordersissues.json"

        for user_brokerid, broker_cfg in usersdictionary.items():
            TERMINAL_PATH = broker_cfg["TERMINAL_PATH"]
            LOGIN_ID = broker_cfg["LOGIN_ID"]
            PASSWORD = broker_cfg["PASSWORD"]
            SERVER = broker_cfg["SERVER"]

            log_and_print(f"Processing broker: {user_brokerid} (Balance $12–$20 mode)", "INFO")

            # === MT5 Init ===
            if not os.path.exists(TERMINAL_PATH):
                log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
                continue

            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=30000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue

            if not mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"MT5 login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account_info = mt5.account_info()
            if not account_info:
                log_and_print(f"Failed to get account info: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue
            balance = account_info.balance
            equity = account_info.equity
            log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f}", "INFO")
            if equity < 160.0 and balance >= 160.0:
                log_and_print(f"Equity ${equity:.2f} < $20.0 while Balance ${balance:.2f} ≥ $20.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if equity >= 160.0 and balance < 160.0:
                log_and_print(f"Equity ${equity:.2f} > $20.0 while Balance ${balance:.2f} < $20.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if not (160.0 <= balance < 319.99):
                log_and_print(f"Balance ${balance:.2f} not in $20–$99.99 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue
            # === Only reaches here if: equity >= 8 AND balance in [8, 11.99) ===
            log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f} → Using {RISK_FOLDER} + {STRATEGY_FILE}", "INFO")



            log_and_print(f"Balance: ${balance:.2f} → Using {RISK_FOLDER} + {STRATEGY_FILE}", "INFO")

            # === Load hightolow.json ===
            file_path = Path(BASE_INPUT_DIR) / user_brokerid / RISK_FOLDER / STRATEGY_FILE
            if not file_path.exists():
                log_and_print(f"File not found: {file_path}", "WARNING")
                mt5.shutdown()
                continue

            try:
                with file_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                    entries = data.get("entries", [])
            except Exception as e:
                log_and_print(f"Failed to read {file_path}: {e}", "ERROR")
                mt5.shutdown()
                continue

            if not entries:
                log_and_print("No entries in hightolow.json", "INFO")
                mt5.shutdown()
                continue

            # === Load existing orders & positions ===
            existing_pending = {}  # (symbol, type) → ticket
            running_positions = set()  # symbols with open position

            for order in (mt5.orders_get() or []):
                if order.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT):
                    existing_pending[(order.symbol, order.type)] = order.ticket

            for pos in (mt5.positions_get() or []):
                running_positions.add(pos.symbol)

            # === Reporting ===
            report_file = file_path.parent / REPORT_SUFFIX
            existing_reports = json.load(report_file.open("r", encoding="utf-8")) if report_file.exists() else []
            issues_list = []
            now_str = datetime.now(pytz.timezone("Africa/Lagos")).strftime("%Y-%m-%d %H:%M:%S.%f+01:00")
            placed = failed = skipped = 0

            for entry in entries:
                try:
                    symbol = entry["market"]
                    price = float(entry["entry_price"])
                    sl = float(entry["sl_price"])
                    tp = float(entry["tp_price"])
                    volume = float(entry["volume"])
                    order_type_str = entry["limit_order"]
                    order_type = mt5.ORDER_TYPE_BUY_LIMIT if order_type_str == "buy_limit" else mt5.ORDER_TYPE_SELL_LIMIT

                    # === SKIP: Already running or pending ===
                    if symbol in running_positions:
                        skipped += 1
                        log_and_print(f"{symbol} has running position → SKIPPED", "INFO")
                        continue

                    key = (symbol, order_type)
                    if key in existing_pending:
                        skipped += 1
                        log_and_print(f"{symbol} {order_type_str} already pending → SKIPPED", "INFO")
                        continue

                    # === Symbol check ===
                    symbol_info = mt5.symbol_info(symbol)
                    if not symbol_info or not symbol_info.visible:
                        issues_list.append({"symbol": symbol, "reason": "Symbol not available"})
                        failed += 1
                        continue

                    # === Volume fix ===
                    vol_step = symbol_info.volume_step
                    volume = max(symbol_info.volume_min,
                                round(volume / vol_step) * vol_step)
                    volume = min(volume, symbol_info.volume_max)

                    # === Price distance check ===
                    tick = mt5.symbol_info_tick(symbol)
                    if not tick:
                        issues_list.append({"symbol": symbol, "reason": "No tick data"})
                        failed += 1
                        continue

                    point = symbol_info.point
                    if order_type == mt5.ORDER_TYPE_BUY_LIMIT:
                        if price >= tick.ask or (tick.ask - price) < 10 * point:
                            skipped += 1
                            continue
                    else:
                        if price <= tick.bid or (price - tick.bid) < 10 * point:
                            skipped += 1
                            continue

                    # === Build & send order ===
                    request = {
                        "action": mt5.TRADE_ACTION_PENDING,
                        "symbol": symbol,
                        "volume": volume,
                        "type": order_type,
                        "price": price,
                        "sl": sl,
                        "tp": tp,
                        "deviation": 10,
                        "magic": 123856,
                        "comment": "Risk3_Auto",
                        "type_time": mt5.ORDER_TIME_GTC,
                        "type_filling": mt5.ORDER_FILLING_IOC,
                    }

                    result = mt5.order_send(request)
                    if result is None:
                        result = type('obj', (), {'retcode': 10000, 'comment': 'order_send returned None'})()

                    success = result.retcode == mt5.TRADE_RETCODE_DONE
                    if success:
                        existing_pending[key] = result.order
                        placed += 1
                        log_and_print(f"{symbol} {order_type_str} @ {price} → PLACED (ticket {result.order})", "SUCCESS")
                    else:
                        failed += 1
                        issues_list.append({"symbol": symbol, "reason": result.comment})

                    # === Report ===
                    if "cent" in RISK_FOLDER:
                        risk_usd = 0.5
                    else:
                        risk_usd = float(RISK_FOLDER.split("_")[1].replace("usd", ""))

                    # === Report ===
                    report_entry = {
                        "symbol": symbol,
                        "order_type": order_type_str,
                        "price": price,
                        "volume": volume,
                        "sl": sl,
                        "tp": tp,
                        "risk_usd": risk_usd,   # ← Now correct: 0.5, 1.0, 2.0, 3.0, 8.0
                        "ticket": result.order if success else None,
                        "success": success,
                        "error_code": result.retcode if not success else None,
                        "error_msg": result.comment if not success else None,
                        "timestamp": now_str
                    }
                    existing_reports.append(report_entry)
                    try:
                        with report_file.open("w", encoding="utf-8") as f:
                            json.dump(existing_reports, f, indent=2)
                    except:
                        pass

                except Exception as e:
                    failed += 1
                    issues_list.append({"symbol": symbol, "reason": f"Exception: {e}"})
                    log_and_print(f"Error processing {symbol}: {e}", "ERROR")

            # === Save issues ===
            issues_path = file_path.parent / ISSUES_FILE
            try:
                existing_issues = json.load(issues_path.open("r", encoding="utf-8")) if issues_path.exists() else []
                with issues_path.open("w", encoding="utf-8") as f:
                    json.dump(existing_issues + issues_list, f, indent=2)
            except:
                pass

            mt5.shutdown()
            log_and_print(
                f"{user_brokerid} DONE → Placed: {placed}, Failed: {failed}, Skipped: {skipped}",
                "SUCCESS"
            )

        log_and_print("All $12–$20 accounts processed.", "SUCCESS")
        return True
  
    def _16usd_history_and_deduplication():
        """
        HISTORY + PENDING + POSITION DUPLICATE DETECTOR + RISK SNIPER
        - Cancels risk > $16.10  (even if TP=0)
        - Cancels HISTORY DUPLICATES
        - Cancels PENDING LIMIT DUPLICATES
        - Cancels PENDING if POSITION already exists
        - Shows duplicate market name on its own line
        ONLY PROCESSES ACCOUNTS WITH BALANCE $12.00 – $19.99
        """
        BASE_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
        REPORT_NAME = "pending_risk_profit_per_order.json"
        MAX_RISK_USD = 16.10
        LOOKBACK_DAYS = 5
        PRICE_PRECISION = 5
        TZ = pytz.timezone("Africa/Lagos")

        five_days_ago = datetime.now(TZ) - timedelta(days=LOOKBACK_DAYS)

        for user_brokerid, cfg in usersdictionary.items():
            TERMINAL_PATH = cfg["TERMINAL_PATH"]
            LOGIN_ID     = cfg["LOGIN_ID"]
            PASSWORD     = cfg["PASSWORD"]
            SERVER       = cfg["SERVER"]

            log_and_print(f"\n{'='*80}", "INFO")
            log_and_print(f"BROKER: {user_brokerid.upper()} | FULL DUPLICATE + RISK GUARD", "INFO")
            log_and_print(f"{'='*80}", "INFO")

            # ---------- MT5 Init ----------
            if not os.path.exists(TERMINAL_PATH):
                log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
                continue
            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=30000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue
            if not mt5.login(int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"Login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account = mt5.account_info()
            if not account:
                log_and_print("No account info.", "ERROR")
                mt5.shutdown()
                continue

            balance = account.balance
            equity = account.equity
            log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f}", "INFO")
            if equity < 160.0 and balance >= 160.0:
                log_and_print(f"Equity ${equity:.2f} < $20.0 while Balance ${balance:.2f} ≥ $20.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if equity >= 160.0 and balance < 160.0:
                log_and_print(f"Equity ${equity:.2f} > $20.0 while Balance ${balance:.2f} < $20.0 → IN DRAWDOWN → SKIPPED", "WARNING")
                mt5.shutdown()
                continue
            if not (160.0 <= balance < 319.99):
                log_and_print(f"Balance ${balance:.2f} not in $20–$99.99 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue

            currency = account.currency
            log_and_print(f"Account: {account.login} | Balance: ${balance:.2f} {currency} → Proceeding with risk_16_usd checks", "INFO")

            # ---------- Get Data ----------
            pending_orders = [o for o in (mt5.orders_get() or [])
                            if o.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT)]
            positions = mt5.positions_get()
            history_deals = mt5.history_deals_get(int(five_days_ago.timestamp()), int(datetime.now(TZ).timestamp()))

            if not pending_orders:
                log_and_print("No pending orders.", "INFO")
                mt5.shutdown()
                continue

            # ---------- BUILD DATABASES ----------
            log_and_print(f"Building duplicate databases...", "INFO")

            # 1. Historical Setups
            historical_keys = {}  # (symbol, entry, sl) → details
            if history_deals:
                for deal in history_deals:
                    if deal.entry != mt5.DEAL_ENTRY_IN: continue
                    if deal.type not in (mt5.DEAL_TYPE_BUY, mt5.DEAL_TYPE_SELL): continue

                    order = mt5.history_orders_get(ticket=deal.order)
                    if not order: continue
                    order = order[0]
                    if order.sl == 0: continue

                    symbol = deal.symbol
                    entry = round(deal.price, PRICE_PRECISION)
                    sl = round(order.sl, PRICE_PRECISION)

                    key = (symbol, entry, sl)
                    if key not in historical_keys:
                        profit = sum(d.profit for d in history_deals if d.order == deal.order and d.entry == mt5.DEAL_ENTRY_OUT)
                        historical_keys[key] = {
                            "time": datetime.fromtimestamp(deal.time, TZ).strftime("%Y-%m-%d %H:%M"),
                            "profit": round(profit, 2),
                            "symbol": symbol
                        }

            # 2. Open Positions (by symbol)
            open_symbols = {pos.symbol for pos in positions} if positions else set()

            # 3. Pending Orders Key Map
            pending_keys = {}  # (symbol, entry, sl) → [order_tickets]
            for order in pending_orders:
                key = (order.symbol, round(order.price_open, PRICE_PRECISION), round(order.sl, PRICE_PRECISION))
                pending_keys.setdefault(key, []).append(order.ticket)

            log_and_print(f"Loaded: {len(historical_keys)} history | {len(open_symbols)} open | {len(pending_keys)} unique pending setups", "INFO")

            # ---------- Process & Cancel ----------
            per_order_data = []
            kept = cancelled_risk = cancelled_hist = cancelled_pend_dup = cancelled_pos_dup = skipped = 0

            for order in pending_orders:
                symbol = order.symbol
                ticket = order.ticket
                volume = order.volume_current
                entry = round(order.price_open, PRICE_PRECISION)
                sl = round(order.sl, PRICE_PRECISION)
                tp = order.tp                     # may be 0

                # ---- NEW: ONLY REQUIRE SL, TP CAN BE 0 ----
                if sl == 0:
                    log_and_print(f"SKIP {ticket} | {symbol} | No SL", "WARNING")
                    skipped += 1
                    continue

                info = mt5.symbol_info(symbol)
                if not info or not mt5.symbol_info_tick(symbol):
                    log_and_print(f"SKIP {ticket} | {symbol} | No symbol data", "WARNING")
                    skipped += 1
                    continue

                point = info.point
                contract = info.trade_contract_size
                point_val = contract * point
                if "JPY" in symbol and currency == "USD":
                    point_val /= 100

                # ---- RISK CALCULATION (always possible with SL) ----
                risk_points = abs(entry - sl) / point
                risk_usd = risk_points * point_val * volume
                if currency != "USD":
                    rate = mt5.symbol_info_tick(f"USD{currency}")
                    if not rate:
                        log_and_print(f"SKIP {ticket} | No USD{currency} rate", "WARNING")
                        skipped += 1
                        continue
                    risk_usd /= rate.bid

                # ---- PROFIT CALCULATION (only if TP exists) ----
                profit_usd = None
                if tp != 0:
                    profit_usd = abs(tp - entry) / point * point_val * volume
                    if currency != "USD":
                        profit_usd /= rate.bid

                # ---- DUPLICATE KEYS ----
                key = (symbol, entry, sl)
                dup_hist = historical_keys.get(key)
                is_position_open = symbol in open_symbols
                is_pending_duplicate = len(pending_keys.get(key, [])) > 1

                print(f"\nmarket: {symbol}")
                print(f"risk: {risk_usd:.2f} USD | profit: {profit_usd if profit_usd is not None else 'N/A'} USD")

                cancel_reason = None
                cancel_type = None

                # === 1. RISK CANCEL (works even if TP=0) ===
                if risk_usd > MAX_RISK_USD:
                    cancel_reason = f"RISK > ${MAX_RISK_USD}"
                    cancel_type = "RISK"
                    print(f"{cancel_reason} → CANCELLED")

                # === 2. HISTORY DUPLICATE ===
                elif dup_hist:
                    cancel_reason = "HISTORY DUPLICATE"
                    cancel_type = "HIST_DUP"
                    print("HISTORY DUPLICATE ORDER FOUND!")
                    print(dup_hist["symbol"])
                    print(f"entry: {entry} | sl: {sl}")
                    print(f"used: {dup_hist['time']} | P/L: {dup_hist['profit']:+.2f} {currency}")
                    print("→ HISTORY DUPLICATE CANCELLED")
                    print("!" * 60)

                # === 3. PENDING DUPLICATE ===
                elif is_pending_duplicate:
                    cancel_reason = "PENDING DUPLICATE"
                    cancel_type = "PEND_DUP"
                    print("PENDING LIMIT DUPLICATE FOUND!")
                    print(symbol)
                    print(f"→ DUPLICATE PENDING ORDER CANCELLED")
                    print("-" * 60)

                # === 8. POSITION EXISTS (Cancel Pending) ===
                elif is_position_open:
                    cancel_reason = "POSITION ALREADY OPEN"
                    cancel_type = "POS_DUP"
                    print("POSITION ALREADY RUNNING!")
                    print(symbol)
                    print(f"→ PENDING ORDER CANCELLED (POSITION ACTIVE)")
                    print("^" * 60)

                # === NO ISSUE → KEEP ===
                else:
                    print("No duplicate. Order kept.")
                    kept += 1
                    per_order_data.append({
                        "ticket": ticket,
                        "symbol": symbol,
                        "entry": entry,
                        "sl": sl,
                        "tp": tp,
                        "risk_usd": round(risk_usd, 2),
                        "profit_usd": round(profit_usd, 2) if profit_usd is not None else None,
                        "status": "KEPT"
                    })
                    continue  # Skip cancel

                # === CANCEL ORDER ===
                req = {"action": mt5.TRADE_ACTION_REMOVE, "order": ticket}
                res = mt5.order_send(req)
                if res.retcode == mt5.TRADE_RETCODE_DONE:
                    log_and_print(f"{cancel_type} CANCELLED {ticket} | {symbol} | {cancel_reason}", "WARNING")
                    if cancel_type == "RISK": cancelled_risk += 1
                    elif cancel_type == "HIST_DUP": cancelled_hist += 1
                    elif cancel_type == "PEND_DUP": cancelled_pend_dup += 1
                    elif cancel_type == "POS_DUP": cancelled_pos_dup += 1
                else:
                    log_and_print(f"CANCEL FAILED {ticket} | {res.comment}", "ERROR")

                per_order_data.append({
                    "ticket": ticket,
                    "symbol": symbol,
                    "entry": entry,
                    "sl": sl,
                    "tp": tp,
                    "risk_usd": round(risk_usd, 2),
                    "profit_usd": round(profit_usd, 2) if profit_usd is not None else None,
                    "status": "CANCELLED",
                    "reason": cancel_reason,
                    "duplicate_time": dup_hist["time"] if dup_hist else None,
                    "duplicate_pl": dup_hist["profit"] if dup_hist else None
                })

            # === SUMMARY ===
            log_and_print(f"\nSUMMARY:", "SUCCESS")
            log_and_print(f"KEPT: {kept}", "INFO")
            log_and_print(f"CANCELLED → RISK: {cancelled_risk} | HIST DUP: {cancelled_hist} | "
                        f"PEND DUP: {cancelled_pend_dup} | POS DUP: {cancelled_pos_dup} | SKIPPED: {skipped}", "WARNING")

            # === SAVE REPORT ===
            out_dir = Path(BASE_DIR) / user_brokerid / "risk_16_usd"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / REPORT_NAME

            report = {
                "broker": user_brokerid,
                "checked_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
                "max_risk_usd": MAX_RISK_USD,
                "lookback_days": LOOKBACK_DAYS,
                "summary": {
                    "kept": kept,
                    "cancelled_risk": cancelled_risk,
                    "cancelled_history_duplicate": cancelled_hist,
                    "cancelled_pending_duplicate": cancelled_pend_dup,
                    "cancelled_position_duplicate": cancelled_pos_dup,
                    "skipped": skipped
                },
                "orders": per_order_data
            }

            try:
                with out_path.open("w", encoding="utf-8") as f:
                    json.dump(report, f, indent=2)
                log_and_print(f"Report saved: {out_path}", "SUCCESS")
            except Exception as e:
                log_and_print(f"Save error: {e}", "ERROR")

            mt5.shutdown()

        log_and_print("\nALL $12–$20 ACCOUNTS: DUPLICATE SCAN + RISK GUARD = DONE", "SUCCESS")
        return True

    def _16usd_ratio_levels():
        """
        16usd RATIO LEVELS + TP UPDATE (PENDING + RUNNING POSITIONS) – BROKER-SAFE
        - Balance $12–$19.99 only
        - Auto-supports riskreward: 1, 2, 3, 8... (any integer)
        - Case-insensitive config
        - consistency → Dynamic TP = RISKREWARD × Risk
        - martingale → TP = 1R (always), ignores RISKREWARD
        - Smart ratio ladder (shows 1R, 2R, 3R only when needed)
        """
        TZ = pytz.timezone("Africa/Lagos")

        log_and_print(f"\n{'='*80}", "INFO")
        log_and_print("16usd RATIO LEVELS + TP UPDATE (PENDING + RUNNING) – CONSISTENCY: N×R | MARTINGALE: 1R", "INFO")
        log_and_print(f"{'='*80}", "INFO")

        for user_brokerid, cfg in usersdictionary.items():
            TERMINAL_PATH = cfg.get("TERMINAL_PATH") or cfg.get("terminal_path")
            LOGIN_ID      = cfg.get("LOGIN_ID")      or cfg.get("login_id")
            PASSWORD      = cfg.get("PASSWORD")      or cfg.get("password")
            SERVER        = cfg.get("SERVER")        or cfg.get("server")
            SCALE         = (cfg.get("SCALE")        or cfg.get("scale")        or "").strip().lower()
            STRATEGY      = (cfg.get("STRATEGY")    or cfg.get("strategy")    or "").strip().lower()

            # === Case-insensitive riskreward lookup ===
            riskreward_raw = None
            for key in cfg:
                if key.lower() == "riskreward":
                    riskreward_raw = cfg[key]
                    break

            if riskreward_raw is None:
                riskreward_raw = 2
                log_and_print(f"{user_brokerid}: 'riskreward' not found → using default 2R", "WARNING")

            log_and_print(
                f"\nProcessing broker: {user_brokerid} | Scale: {SCALE.upper()} | "
                f"Strategy: {STRATEGY.upper()} | riskreward: {riskreward_raw}R", "INFO"
            )

            # === Validate required fields ===
            missing = []
            for f in ("TERMINAL_PATH", "LOGIN_ID", "PASSWORD", "SERVER", "SCALE"):
                if not locals()[f]: missing.append(f)
            if missing:
                log_and_print(f"Missing config: {', '.join(missing)} → SKIPPED", "ERROR")
                continue

            # === MT5 Init ===
            if not os.path.exists(TERMINAL_PATH):
                log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
                continue

            if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD,
                                server=SERVER, timeout=30000):
                log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
                continue

            if not mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER):
                log_and_print(f"MT5 login failed: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            account_info = mt5.account_info()
            if not account_info:
                log_and_print(f"Failed to get account info: {mt5.last_error()}", "ERROR")
                mt5.shutdown()
                continue

            balance = account_info.balance
            if not (160.0 <= balance < 319.99):
                log_and_print(f"Balance ${balance:.2f} not in $12–$20 range → SKIPPED", "INFO")
                mt5.shutdown()
                continue

            log_and_print(f"Balance: ${balance:.2f} → Scanning positions & pending orders...", "INFO")

            # === Determine effective RR ===
            try:
                config_rr = int(float(riskreward_raw))
                if config_rr < 1: config_rr = 1
            except (ValueError, TypeError):
                config_rr = 2
                log_and_print(f"Invalid riskreward '{riskreward_raw}' → using 2R", "WARNING")

            effective_rr = 1 if SCALE == "martingale" else config_rr
            rr_source = "MARTINGALE (forced 1R)" if SCALE == "martingale" else f"CONFIG ({effective_rr}R)"
            log_and_print(f"Effective TP: {effective_rr}R [{rr_source}]", "INFO")

            # ------------------------------------------------------------------ #
            # 1. PENDING LIMIT ORDERS
            # ------------------------------------------------------------------ #
            pending_orders = [
                o for o in (mt5.orders_get() or [])
                if o.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT)
                and getattr(o, 'sl', 0) != 0 and getattr(o, 'tp', 0) != 0
            ]

            # ------------------------------------------------------------------ #
            # 2. RUNNING POSITIONS
            # ------------------------------------------------------------------ #
            running_positions = [
                p for p in (mt5.positions_get() or [])
                if p.type in (mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_SELL)
                and p.sl != 0 and p.tp != 0
            ]

            # Merge into a single iterable with a flag
            items_to_process = []
            for o in pending_orders:
                items_to_process.append(('PENDING', o))
            for p in running_positions:
                items_to_process.append(('RUNNING', p))

            if not items_to_process:
                log_and_print("No valid pending orders or running positions found.", "INFO")
                mt5.shutdown()
                continue

            log_and_print(f"Found {len(pending_orders)} pending + {len(running_positions)} running → total {len(items_to_process)}", "INFO")

            processed_symbols = set()
            updated_count = 0

            for kind, obj in items_to_process:
                symbol   = obj.symbol
                ticket   = getattr(obj, 'ticket', None) or getattr(obj, 'order', None)
                entry_price = getattr(obj, 'price_open', None) or getattr(obj, 'price_current', None)
                sl_price = obj.sl
                current_tp = obj.tp
                is_buy   = obj.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY)

                if symbol in processed_symbols:
                    continue

                risk_distance = abs(entry_price - sl_price)
                if risk_distance <= 0:
                    log_and_print(f"Zero risk distance on {symbol} ({kind}) → skipped", "WARNING")
                    continue

                symbol_info = mt5.symbol_info(symbol)
                if not symbol_info:
                    log_and_print(f"Symbol info missing: {symbol}", "WARNING")
                    continue

                digits = symbol_info.digits
                def r(p): return round(p, digits)

                entry_price = r(entry_price)
                sl_price    = r(sl_price)
                current_tp  = r(current_tp)
                direction   = 1 if is_buy else -1
                target_tp   = r(entry_price + direction * effective_rr * risk_distance)

                # ----- Ratio ladder (display only) -----
                ratio1 = r(entry_price + direction * 1 * risk_distance)
                ratio2 = r(entry_price + direction * 2 * risk_distance)
                ratio3 = r(entry_price + direction * 3 * risk_distance) if effective_rr >= 3 else None

                print(f"\n{symbol} | {kind} | Target: {effective_rr}R ({SCALE.upper()})")
                print(f"  Entry : {entry_price}")
                print(f"  1R    : {ratio1}")
                print(f"  2R    : {ratio2}")
                if ratio3:
                    print(f"  3R    : {ratio3}")
                print(f"  TP    : {current_tp} → ", end="")

                # ----- Modify TP -----
                tolerance = 10 ** -digits
                if abs(current_tp - target_tp) > tolerance:
                    if kind == "PENDING":
                        # modify pending order
                        request = {
                            "action": mt5.TRADE_ACTION_MODIFY,
                            "order": ticket,
                            "price": entry_price,
                            "sl": sl_price,
                            "tp": target_tp,
                            "type": obj.type,
                            "type_time": obj.type_time,
                            "type_filling": obj.type_filling,
                            "magic": getattr(obj, 'magic', 0),
                            "comment": getattr(obj, 'comment', "")
                        }
                        if hasattr(obj, 'expiration') and obj.expiration:
                            request["expiration"] = obj.expiration
                    else:  # RUNNING
                        # modify open position (SL/TP only)
                        request = {
                            "action": mt5.TRADE_ACTION_SLTP,
                            "position": ticket,
                            "sl": sl_price,
                            "tp": target_tp,
                            "symbol": symbol
                        }

                    result = mt5.order_send(request)
                    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                        print(f"{target_tp} [UPDATED]")
                        log_and_print(
                            f"TP → {effective_rr}R | {symbol} | {kind} | {current_tp} → {target_tp} [{SCALE.upper()}]",
                            "SUCCESS"
                        )
                        updated_count += 1
                    else:
                        err = result.comment if result else "Unknown"
                        print(f"{current_tp} [FAILED: {err}]")
                        log_and_print(f"TP UPDATE FAILED | {symbol} | {kind} | {err}", "ERROR")
                else:
                    print(f"{current_tp} [OK]")

                print(f"  SL    : {sl_price}")
                processed_symbols.add(symbol)

            mt5.shutdown()
            log_and_print(
                f"{user_brokerid} → {len(processed_symbols)} symbol(s) | "
                f"{updated_count} TP(s) set to {effective_rr}R [{SCALE.upper()}]",
                "SUCCESS"
            )

        log_and_print(
            "\nALL $12–$20 ACCOUNTS: R:R UPDATE (PENDING + RUNNING) – "
            "consistency=N×R, martingale=1R = DONE",
            "SUCCESS"
        )
        return True
    
    _16usd_live_sl_tp_amounts()
    place_16usd_orders()
    _16usd_history_and_deduplication()
    _16usd_ratio_levels()

def restore_missing_orders():
    import json
    from pathlib import Path
    from datetime import datetime
    import pytz

    TZ = pytz.timezone("Africa/Lagos")
    BROKERS_ORDERS_PATH = Path(r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_volumes_points\allowedmarkets\brokerslimitorders.json")
    CALC_BASE_DIR = Path(r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices")

    RISK_MAP = {
        0.5: "risk_0_50cent_usd",
        1.0: "risk_1_usd",
        2.0: "risk_2_usd",
        3.0: "risk_3_usd",
        4.0: "risk_4_usd",
        8.0: "risk_8_usd",
        16.0: "risk_16_usd"
    }
    TOLERANCE = 0.15

    restored_count = 0
    dedup_removed_count = 0
    files_touched = 0

    print(f"\n[Restore + Dedup] Starting cleanup & restoration...\n")

    # ========================================
    # 1. Load & deduplicate brokerslimitorders.json
    # ========================================
    if BROKERS_ORDERS_PATH.exists():
        try:
            data = json.loads(BROKERS_ORDERS_PATH.read_text(encoding="utf-8"))
            original_pending = data.get("pending_orders", [])
            seen_tickets = set()
            clean_pending = []
            for o in original_pending:
                ticket = o.get("ticket")
                if ticket and ticket not in seen_tickets:
                    seen_tickets.add(ticket)
                    clean_pending.append(o)
                elif not ticket:
                    clean_pending.append(o)

            if len(clean_pending) < len(original_pending):
                removed = len(original_pending) - len(clean_pending)
                dedup_removed_count += removed
                data["pending_orders"] = clean_pending
                BROKERS_ORDERS_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
                print(f"[Clean] Removed {removed} duplicate pending order(s) from brokerslimitorders.json")
                files_touched += 1
        except Exception as e:
            print(f"[Error] Failed to clean brokerslimitorders.json → {e}")

    if not BROKERS_ORDERS_PATH.exists():
        print("[Restore] brokerslimitorders.json not found")
        return False

    try:
        brokers_data = json.loads(BROKERS_ORDERS_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[Restore] Cannot read brokerslimitorders.json → {e}")
        return False

    pending_orders = brokers_data.get("pending_orders", [])
    if not pending_orders:
        print("[Restore] No pending orders → nothing to do.")
        return True

    # ========================================
    # 2. Process each pending order
    # ========================================
    for order in pending_orders:
        broker = order.get("broker")
        symbol = order.get("symbol")
        order_type = order.get("type", "")
        entry_price = order.get("entry_price")
        volume = order.get("volume")
        sl_amount = order.get("sl_amount")
        ticket = order.get("ticket")

        if not all([broker, symbol, entry_price is not None, volume, sl_amount is not None]):
            continue

        # Find risk folder
        risk_folder = None
        target_risk = None
        for val, folder in RISK_MAP.items():
            if abs(sl_amount - val) <= TOLERANCE:
                risk_folder = folder
                target_risk = val
                break
        if not risk_folder:
            continue

        broker_risk_dir = CALC_BASE_DIR / broker / risk_folder
        if not broker_risk_dir.exists():
            continue

        limit_side = "buy_limit" if "BUY" in order_type.upper() else "sell_limit"

        for direction_file in ["hightolow.json", "lowtohigh.json"]:
            file_path = broker_risk_dir / direction_file
            if not file_path.exists():
                continue

            try:
                content = json.loads(file_path.read_text(encoding="utf-8"))
                entries = content.get("entries", [])
                summary = content.get("summary", {})
            except:
                continue

            # === DEDUPLICATE FIRST ===
            seen = set()
            unique_entries = []
            local_dedup = 0
            for e in entries:
                if not isinstance(e, dict):
                    continue
                key = (e.get("symbol"), e.get("limit_order"), round(e.get("entry_price", 0), 8), e.get("volume"))
                if key not in seen:
                    seen.add(key)
                    unique_entries.append(e)
                else:
                    local_dedup += 1

            # === CHECK IF ORDER IS MISSING ===
            order_key = (symbol, limit_side, round(entry_price, 8), volume)
            is_missing = order_key not in seen

            action = ""
            if local_dedup > 0:
                dedup_removed_count += local_dedup
                action += f"DEDUPED({local_dedup}) "
            if is_missing:
                new_entry = {
                    "symbol": symbol,
                    "market": symbol,
                    "entry_price": round(entry_price, 8),
                    "volume": volume,
                    "limit_order": limit_side,
                    "sl_pips": None,
                    "tp_pips": None,
                    "timeframe": "restored",
                    "source": "restore_missing_orders",
                    "restored_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
                    "original_ticket": ticket,
                    "original_comment": order.get("comment"),
                    "magic": order.get("magic")
                }
                unique_entries.append(new_entry)
                restored_count += 1
                action += "RESTORED"

            # === UPDATE SUMMARY ===
            unique_symbols = {e.get("symbol") for e in unique_entries if isinstance(e, dict) and e.get("symbol")}
            if isinstance(summary, dict):
                summary["allmarketssymbols"] = len(unique_symbols)
            content["summary"] = summary
            content["entries"] = unique_entries

            # === SAVE ONLY IF CHANGED ===
            if local_dedup > 0 or is_missing:
                try:
                    file_path.write_text(json.dumps(content, indent=2, ensure_ascii=False), encoding="utf-8")
                    files_touched += 1
                    dir_name = "HIGH→LOW" if "hightolow" in direction_file else "LOW→HIGH"
                    status = action.strip() or "OK"
                    print(f"[Done] {status} → {broker} | ${target_risk} | {symbol} {order_type.split()[0]} @ {entry_price} → {dir_name}")
                except Exception as e:
                    print(f"[Error] Failed to save {file_path}: {e}")
            # If no change → do nothing and stay silent

    # ========================================
    # Final Report
    # ========================================
    print(f"\n{'='*70}")
    print(f"[Restore + Dedup] COMPLETED")
    print(f"   • Orders restored         : {restored_count}")
    print(f"   • Duplicates removed      : {dedup_removed_count}")
    print(f"   • Files updated           : {files_touched}")
    if restored_count == 0 and dedup_removed_count == 0:
        print(f"   All files already clean and complete")
    print(f"{'='*70}\n")

    return True 
  
def collect_all_brokers_limit_orders():
    BASE_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_volumes_points\allowedmarkets"
    REPORT_NAME = "brokerslimitorders.json"
    TZ = pytz.timezone("Africa/Lagos")
    OUTPUT_PATH = Path(BASE_DIR) / REPORT_NAME

    MAX_AGE_SECONDS = 2 * 24 * 60 * 60  # 2 days (for stale pending)
    MAX_HISTORY_SECONDS = 5 * 60 * 60   # 5 hours (for recent filled only)

    all_pending_orders = []
    all_open_positions = []
    all_history_orders = []
    total_pending = 0
    total_positions = 0
    total_history = 0
    failed_brokers = []
    deleted_count = 0

    # Helper: Convert seconds to human-readable age string
    def format_age(seconds):
        if seconds < 60:
            return f"{int(seconds)}s"
        minutes = int(seconds // 60)
        if minutes < 60:
            return f"{minutes}m"
        hours = minutes // 60
        minutes = minutes % 60
        if hours < 24:
            return f"{hours}h {minutes}m" if minutes else f"{hours}h"
        days = hours // 24
        hours = hours % 24
        return f"{days}d {hours}h" if hours else f"{days}d"

    # Helper: Calculate monetary value of SL/TP distance
    def calculate_risk_reward(symbol, volume, entry_price, sl_price, tp_price, currency):
        if not sl_price and not tp_price:
            return None, None

        symbol_info = mt5.symbol_info(symbol)
        if not symbol_info:
            return None, None

        tick_value = symbol_info.trade_tick_value
        tick_size = symbol_info.trade_tick_size
        point = symbol_info.point
        digits = symbol_info.digits

        contract_size = symbol_info.trade_contract_size

        sl_amount = None
        tp_amount = None

        if sl_price and sl_price != 0:
            price_diff_sl = abs(entry_price - sl_price)
            ticks_sl = price_diff_sl / tick_size
            sl_amount = round(ticks_sl * tick_value * volume, 2)

        if tp_price and tp_price != 0:
            price_diff_tp = abs(tp_price - entry_price)
            ticks_tp = price_diff_tp / tick_size
            tp_amount = round(ticks_tp * tick_value * volume, 2)

        return sl_amount, tp_amount

    log_and_print(f"\n{'='*100}", "INFO")
    log_and_print(f"COLLECTING PENDING LIMITS + OPEN POSITIONS + RECENT FILLED HISTORY (<5h)", "INFO")
    log_and_print(f"{'='*100}", "INFO")

    broker_symbol_data = {}

    for user_brokerid, cfg in usersdictionary.items():
        TERMINAL_PATH = cfg["TERMINAL_PATH"]
        LOGIN_ID     = cfg["LOGIN_ID"]
        PASSWORD     = cfg["PASSWORD"]
        SERVER       = cfg["SERVER"]

        log_and_print(f"\n→ Broker: {user_brokerid.upper()}", "INFO")

        if not os.path.exists(TERMINAL_PATH):
            log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
            failed_brokers.append(user_brokerid)
            continue

        if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=30000):
            log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
            failed_brokers.append(user_brokerid)
            continue

        if not mt5.login(int(LOGIN_ID), password=PASSWORD, server=SERVER):
            log_and_print(f"Login failed: {mt5.last_error()}", "ERROR")
            mt5.shutdown()
            failed_brokers.append(user_brokerid)
            continue

        account = mt5.account_info()
        if not account:
            log_and_print("No account info.", "ERROR")
            mt5.shutdown()
            failed_brokers.append(user_brokerid)
            continue

        balance = account.balance
        currency = account.currency
        log_and_print(f"Connected: Account {account.login} | Balance: ${balance:.2f} {currency}", "INFO")

        broker_symbol_data[user_brokerid] = {}
        current_time = datetime.now(TZ)

        # ========== 1. PENDING LIMIT ORDERS ==========
        pending_orders_raw = mt5.orders_get() or []
        pending_orders = [
            o for o in pending_orders_raw
            if o.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT)
        ]

        pending_count = len(pending_orders)
        total_pending += pending_count

        to_delete = []

        if pending_count:
            log_and_print(f"Found {pending_count} pending limit order(s).", "INFO")
            for order in pending_orders:
                symbol = order.symbol
                if symbol not in broker_symbol_data[user_brokerid]:
                    broker_symbol_data[user_brokerid][symbol] = {
                        "has_open": False,
                        "pending": {"BUY": None, "SELL": None},
                        "account_login": account.login,
                        "account_currency": currency
                    }

                order_type_str = "BUY LIMIT" if order.type == mt5.ORDER_TYPE_BUY_LIMIT else "SELL LIMIT"
                order_time = datetime.fromtimestamp(order.time_setup, TZ)
                age_seconds = (current_time - order_time).total_seconds()

                side_key = "BUY" if order.type == mt5.ORDER_TYPE_BUY_LIMIT else "SELL"

                sl_amount, tp_amount = calculate_risk_reward(
                    symbol=symbol,
                    volume=order.volume_current,
                    entry_price=order.price_open,
                    sl_price=order.sl,
                    tp_price=order.tp,
                    currency=currency
                )

                broker_symbol_data[user_brokerid][symbol]["pending"][side_key] = {
                    "ticket": order.ticket,
                    "volume": order.volume_current,
                    "entry_price": round(order.price_open, 6),
                    "sl": round(order.sl, 6) if order.sl != 0 else None,
                    "tp": round(order.tp, 6) if order.tp != 0 else None,
                    "sl_amount": sl_amount,
                    "tp_amount": tp_amount,
                    "setup_time": order_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "comment": order.comment.strip() if order.comment else None,
                    "magic": order.magic,
                    "age_seconds": age_seconds
                }

                if age_seconds > MAX_AGE_SECONDS:
                    to_delete.append((order.ticket, symbol, order_type_str, format_age(age_seconds)))

        else:
            log_and_print("No pending limit orders.", "INFO")

        # ========== 2. OPEN POSITIONS ==========
        positions = mt5.positions_get()
        position_count = len(positions) if positions else 0
        total_positions += position_count

        if position_count:
            log_and_print(f"Found {position_count} open position(s).", "INFO")
            for pos in positions:
                symbol = pos.symbol
                pos_type_str = "BUY" if pos.type == mt5.POSITION_TYPE_BUY else "SELL"
                open_time = datetime.fromtimestamp(pos.time, TZ).strftime("%Y-%m-%d %H:%M:%S")

                sl_amount, tp_amount = calculate_risk_reward(
                    symbol=symbol,
                    volume=pos.volume,
                    entry_price=pos.price_open,
                    sl_price=pos.sl,
                    tp_price=pos.tp,
                    currency=currency
                )

                if symbol not in broker_symbol_data[user_brokerid]:
                    broker_symbol_data[user_brokerid][symbol] = {
                        "has_open": True,
                        "pending": {"BUY": None, "SELL": None},
                        "account_login": account.login,
                        "account_currency": currency
                    }
                else:
                    broker_symbol_data[user_brokerid][symbol]["has_open"] = True

                all_open_positions.append({
                    "broker": user_brokerid,
                    "account_login": account.login,
                    "account_currency": currency,
                    "ticket": pos.ticket,
                    "symbol": pos.symbol,
                    "type": pos_type_str,
                    "status": "OPEN",
                    "volume": pos.volume,
                    "entry_price": round(pos.price_open, 6),
                    "current_price": round(pos.price_current, 6),
                    "sl": round(pos.sl, 6) if pos.sl != 0 else None,
                    "tp": round(pos.tp, 6) if pos.tp != 0 else None,
                    "sl_amount": sl_amount,
                    "tp_amount": tp_amount,
                    "open_time": open_time,
                    "profit": round(pos.profit, 2),
                    "swap": round(pos.swap, 2),
                    "comment": pos.comment.strip() if pos.comment else None,
                    "magic": pos.magic
                })

        else:
            log_and_print("No open positions.", "INFO")

        # ========== 3. HISTORY: ONLY FILLED OR CLOSED WITH P/L (<5h) ==========
        from_datetime = datetime.now(TZ) - timedelta(seconds=MAX_HISTORY_SECONDS)
        from_ts = int(from_datetime.timestamp())
        to_ts = int(datetime.now(TZ).timestamp())

        history_orders = mt5.history_orders_get(from_ts, to_ts) or []

        relevant_history = []
        for h in history_orders:
            if h.type not in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT):
                continue
            if (h.state == mt5.ORDER_STATE_FILLED or 
                (h.volume_current == 0 and getattr(h, 'profit', 0) != 0)):
                relevant_history.append(h)

        history_count = len(relevant_history)
        total_history += history_count

        if history_count:
            log_and_print(f"Found {history_count} filled/closed limit order(s) in history (<5h).", "INFO")
            for h in relevant_history:
                symbol = h.symbol
                order_type_str = "BUY LIMIT" if h.type == mt5.ORDER_TYPE_BUY_LIMIT else "SELL LIMIT"
                fill_time = datetime.fromtimestamp(h.time_done, TZ).strftime("%Y-%m-%d %H:%M:%S") if h.time_done else None
                age_seconds = (current_time - datetime.fromtimestamp(h.time_done, TZ)).total_seconds() if h.time_done else 0
                age_str = format_age(age_seconds)

                profit = round(getattr(h, 'profit', 0), 2)

                sl_amount, tp_amount = calculate_risk_reward(
                    symbol=symbol,
                    volume=h.volume_initial,
                    entry_price=h.price_open,
                    sl_price=getattr(h, 'sl', 0),
                    tp_price=getattr(h, 'tp', 0),
                    currency=currency
                )

                entry = {
                    "broker": user_brokerid,
                    "account_login": account.login,
                    "account_currency": currency,
                    "ticket": h.ticket,
                    "symbol": symbol,
                    "type": order_type_str,
                    "status": "FILLED" if h.state == mt5.ORDER_STATE_FILLED else "CLOSED",
                    "volume": h.volume_initial,
                    "filled_volume": h.volume_current if h.volume_current > 0 else h.volume_initial,
                    "entry_price": round(h.price_open, 6),
                    "fill_price": round(h.price_current, 6) if h.price_current != 0 else None,
                    "sl_amount": sl_amount,
                    "tp_amount": tp_amount,
                    "fill_time": fill_time,
                    "setup_time": datetime.fromtimestamp(h.time_setup, TZ).strftime("%Y-%m-%d %H:%M:%S"),
                    "comment": h.comment.strip() if h.comment else None,
                    "magic": h.magic,
                    "profit": profit if profit != 0 else None,
                    "age": age_str
                }
                all_history_orders.append(entry)
        else:
            log_and_print("No filled or closed limit orders in history (<5h).", "INFO")

        mt5.shutdown()

        # ========== 4. DELETE STALE PENDING ORDERS (>2 days) ==========
        if to_delete:
            log_and_print(f"Attempting to delete {len(to_delete)} stale limit order(s) on {user_brokerid.upper()}...", "INFO")
            if mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=30000):
                mt5.login(int(LOGIN_ID), password=PASSWORD, server=SERVER)
                for ticket, symbol, order_type, age_str in to_delete:
                    sym_data = broker_symbol_data[user_brokerid].get(symbol, {})
                    if sym_data.get("has_open", False):
                        log_and_print(f"SKIPPED: {symbol} [{order_type}] has open position", "INFO")
                        continue

                    current_orders = mt5.orders_get(ticket=ticket)
                    if not current_orders:
                        log_and_print(f"SKIP: Order {ticket} no longer exists", "INFO")
                        side = "BUY" if "BUY" in order_type else "SELL"
                        if broker_symbol_data[user_brokerid].get(symbol, {}).get("pending", {}).get(side):
                            broker_symbol_data[user_brokerid][symbol]["pending"][side] = None
                        continue

                    request = {"action": mt5.TRADE_ACTION_REMOVE, "order": ticket}
                    result = mt5.order_send(request)
                    if result.retcode == mt5.TRADE_RETCODE_DONE:
                        log_and_print(f"DELETED: {symbol} [{order_type}] | Ticket: {ticket} | Age: {age_str}", "SUCCESS")
                        deleted_count += 1
                        side = "BUY" if "BUY" in order_type else "SELL"
                        broker_symbol_data[user_brokerid][symbol]["pending"][side] = None
                    else:
                        log_and_print(f"FAILED: {symbol} [{order_type}] | Ticket: {ticket} | {result.comment}", "ERROR")
                mt5.shutdown()

    # ========== POST-PROCESS: Build final pending list ==========
    for user_brokerid, symbols_data in broker_symbol_data.items():
        for symbol, data in symbols_data.items():
            has_open = data["has_open"]
            pending = data["pending"]
            for side, order in [("BUY", pending["BUY"]), ("SELL", pending["SELL"])]:
                if not order:
                    continue
                base_entry = {
                    "broker": user_brokerid,
                    "account_login": data["account_login"],
                    "account_currency": data["account_currency"],
                    "ticket": order["ticket"],
                    "symbol": symbol,
                    "type": f"{side} LIMIT",
                    "status": "PENDING",
                    "volume": order["volume"],
                    "entry_price": order["entry_price"],
                    "sl": order["sl"],
                    "tp": order["tp"],
                    "sl_amount": order["sl_amount"],
                    "tp_amount": order["tp_amount"],
                    "setup_time": order["setup_time"],
                    "comment": order["comment"],
                    "magic": order["magic"]
                }
                if not has_open:
                    base_entry["age"] = format_age(order["age_seconds"])
                all_pending_orders.append(base_entry)

    # ========== FINAL SUMMARY & SAVE ==========
    log_and_print(f"\n{'='*100}", "SUCCESS")
    log_and_print(f"COLLECTION COMPLETE", "SUCCESS")
    log_and_print(f"Total Brokers: {len(usersdictionary)} | Failed: {len(failed_brokers)}", "INFO")
    if failed_brokers:
        log_and_print(f"Failed: {', '.join(failed_brokers)}", "WARNING")
    log_and_print(f"Pending Limit Orders: {len(all_pending_orders)}", "INFO")
    log_and_print(f"Open Positions: {total_positions}", "INFO")
    log_and_print(f"Filled/Closed History (<5h): {total_history}", "INFO")
    log_and_print(f"Stale Orders Deleted: {deleted_count}", "WARNING" if deleted_count else "INFO")

    report = {
        "generated_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
        "total_brokers": len(usersdictionary),
        "failed_brokers": failed_brokers,
        "cleanup": {"stale_orders_deleted": deleted_count},
        "history_window_seconds": MAX_HISTORY_SECONDS,
        "summary": {
            "pending_orders": len(all_pending_orders),
            "open_positions": total_positions,
            "history_orders": total_history,
            "total": len(all_pending_orders) + total_positions + total_history
        },
        "pending_orders": all_pending_orders,
        "open_positions": all_open_positions,
        "history_orders": all_history_orders
    }

    try:
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with OUTPUT_PATH.open("w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        log_and_print(f"REPORT SAVED: {OUTPUT_PATH}", "SUCCESS")
    except Exception as e:
        log_and_print(f"FAILED TO SAVE REPORT: {e}", "ERROR")

    log_and_print(f"{'='*100}", "INFO")
    restore_missing_orders()
    return True  

def deduplicate_pending_orders():
    r"""
    Deduplicate pending BUY_LIMIT / SELL_LIMIT orders.
    Rules:
      1. Only ONE pending BUY_LIMIT per symbol
      2. Only ONE pending SELL_LIMIT per symbol
      3. If a BUY position is open → delete ALL pending BUY_LIMIT on that symbol
      4. If a SELL position is open → delete ALL pending SELL_LIMIT on that symbol
      5. When multiple pendings exist → use STRATEGY (lowtohigh/hightolow) to keep best price
         or keep oldest (lowest ticket) if no strategy.
    """
    BASE_INPUT_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
    DEDUP_REPORT = "dedup_report.json"
    ISSUES_FILE = "ordersissues.json"

    # ------------------------------------------------------------------ #
    def _order_type_str(mt5_type):
        return "BUY_LIMIT" if mt5_type == mt5.ORDER_TYPE_BUY_LIMIT else "SELL_LIMIT"

    def _decide_winner(existing, candidate, order_type, strategy):
        """Return (keep_existing, reason)"""
        is_buy = order_type == mt5.ORDER_TYPE_BUY_LIMIT

        if strategy == "lowtohigh":
            if is_buy:
                better = candidate["price"] > existing["price"]
                reason = f"lowtohigh → new {candidate['price']} > old {existing['price']}"
            else:
                better = candidate["price"] < existing["price"]
                reason = f"lowtohigh → new {candidate['price']} < old {existing['price']}"
        elif strategy == "hightolow":
            if is_buy:
                better = candidate["price"] < existing["price"]
                reason = f"hightolow → new {candidate['price']} < old {existing['price']}"
            else:
                better = candidate["price"] > existing["price"]
                reason = f"hightolow → new {candidate['price']} > old {existing['price']}"
        else:
            better = candidate["ticket"] < existing["ticket"]
            reason = f"no strategy → keep oldest ticket {candidate['ticket']} < {existing['ticket']}"

        return (not better, reason)  # True → keep existing

    # ------------------------------------------------------------------ #
    for user_brokerid, broker_cfg in usersdictionary.items():
        account_type = broker_cfg.get("ACCOUNT", "").lower()
        if account_type not in ("demo", "real"):
            log_and_print(f"Skipping {user_brokerid} (account type: {account_type})", "INFO")
            continue

        strategy_key = broker_cfg.get("STRATEGY", "").lower()
        if strategy_key and strategy_key not in ("lowtohigh", "hightolow"):
            log_and_print(f"{user_brokerid}: Unknown STRATEGY '{strategy_key}' – using oldest ticket", "WARNING")
            strategy_key = ""

        TERMINAL_PATH = broker_cfg["TERMINAL_PATH"]
        LOGIN_ID      = broker_cfg["LOGIN_ID"]
        PASSWORD      = broker_cfg["PASSWORD"]
        SERVER        = broker_cfg["SERVER"]

        log_and_print(f"Deduplicating pending orders for {user_brokerid} ({account_type})", "INFO")

        # ------------------- MT5 connection -------------------
        if not os.path.exists(TERMINAL_PATH):
            log_and_print(f"{user_brokerid}: Terminal path missing", "ERROR")
            continue

        if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD,
                              server=SERVER, timeout=30000):
            log_and_print(f"{user_brokerid}: MT5 init failed: {mt5.last_error()}", "ERROR")
            continue

        if not mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER):
            log_and_print(f"{user_brokerid}: MT5 login failed: {mt5.last_error()}", "ERROR")
            mt5.shutdown()
            continue

        # ------------------- Get running positions -------------------
        running_positions = {}  # symbol → direction: 1=buy, -1=sell
        positions = mt5.positions_get()
        for pos in (positions or []):
            direction = 1 if pos.type == mt5.ORDER_TYPE_BUY else -1
            running_positions[pos.symbol] = direction

        # ------------------- Get pending orders -------------------
        pending = mt5.orders_get()
        pending_by_key = {}  # (symbol, type) → list of {'ticket':, 'price':}
        for order in (pending or []):
            if order.type not in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT):
                continue
            key = (order.symbol, order.type)
            pending_by_key.setdefault(key, []).append({
                "ticket": order.ticket,
                "price":  order.price_open
            })

        # ------------------- Deduplication -------------------
        total_deleted = total_kept = 0
        dedup_report = []
        issues_list   = []
        now_str = datetime.now(pytz.timezone("Africa/Lagos")).strftime(
            "%Y-%m-%d %H:%M:%S.%f+01:00")

        for (symbol, otype), orders in pending_by_key.items():
            new_dir = 1 if otype == mt5.ORDER_TYPE_BUY_LIMIT else -1
            type_str = _order_type_str(otype)

            # === RULE: If same-direction position is running → delete ALL pending of this type ===
            if symbol in running_positions and running_positions[symbol] == new_dir:
                for order in orders:
                    del_req = {"action": mt5.TRADE_ACTION_REMOVE, "order": order["ticket"]}
                    del_res = mt5.order_send(del_req)

                    status = "DELETED"
                    err_msg = None
                    if del_res is None:
                        status = "DELETE FAILED (None)"
                        err_msg = "order_send returned None"
                    elif del_res.retcode != mt5.TRADE_RETCODE_DONE:
                        status = f"DELETE FAILED ({del_res.retcode})"
                        err_msg = del_res.comment

                    log_and_print(
                        f"{user_brokerid} | {symbol} {type_str} "
                        f"ticket {order['ticket']} @ {order['price']} → {status} "
                        f"(running { 'BUY' if new_dir==1 else 'SELL' } position)",
                        "INFO" if status == "DELETED" else "WARNING"
                    )

                    dedup_report.append({
                        "symbol": symbol,
                        "order_type": type_str,
                        "ticket": order["ticket"],
                        "price": order["price"],
                        "action": status.split()[0],
                        "reason": "Deleted: same-direction position already running",
                        "error_msg": err_msg,
                        "timestamp": now_str
                    })

                    if status == "DELETED":
                        total_deleted += 1
                    else:
                        issues_list.append({"symbol": symbol, "diagnosed_reason": f"Delete failed: {err_msg}"})
                continue  # skip to next symbol

            # === RULE: Only one pending per type → deduplicate if >1 ===
            if len(orders) <= 1:
                total_kept += 1
                continue

            # Sort by ticket (oldest first) for fallback
            orders.sort(key=lambda x: x["ticket"])

            keep = orders[0]
            for cand in orders[1:]:
                keep_it, reason = _decide_winner(keep, cand, otype, strategy_key)
                to_delete = cand if keep_it else keep

                del_req = {"action": mt5.TRADE_ACTION_REMOVE, "order": to_delete["ticket"]}
                del_res = mt5.order_send(del_req)

                status = "DELETED"
                err_msg = None
                if del_res is None:
                    status = "DELETE FAILED (None)"
                    err_msg = "order_send returned None"
                elif del_res.retcode != mt5.TRADE_RETCODE_DONE:
                    status = f"DELETE FAILED ({del_res.retcode})"
                    err_msg = del_res.comment

                log_and_print(
                    f"{user_brokerid} | {symbol} {type_str} "
                    f"ticket {to_delete['ticket']} @ {to_delete['price']} → {status} | {reason}",
                    "INFO" if status == "DELETED" else "WARNING"
                )

                dedup_report.append({
                    "symbol": symbol,
                    "order_type": type_str,
                    "ticket": to_delete["ticket"],
                    "price": to_delete["price"],
                    "action": status.split()[0],
                    "reason": reason,
                    "error_msg": err_msg,
                    "timestamp": now_str
                })

                if status == "DELETED":
                    total_deleted += 1
                    if not keep_it:
                        keep = cand  # promote winner
                else:
                    issues_list.append({"symbol": symbol, "diagnosed_reason": f"Delete failed: {err_msg}"})

            total_kept += 1  # one survivor

        # ------------------- Save reports -------------------
        broker_dir = Path(BASE_INPUT_DIR) / user_brokerid
        dedup_file = broker_dir / DEDUP_REPORT
        try:
            existing = json.load(dedup_file.open("r", encoding="utf-8")) if dedup_file.exists() else []
        except:
            existing = []
        all_report = existing + dedup_report
        try:
            with dedup_file.open("w", encoding="utf-8") as f:
                json.dump(all_report, f, indent=2)
        except Exception as e:
            log_and_print(f"{user_brokerid}: Failed to write {DEDUP_REPORT}: {e}", "ERROR")

        issues_path = broker_dir / ISSUES_FILE
        try:
            existing_issues = json.load(issues_path.open("r", encoding="utf-8")) if issues_path.exists() else []
            with issues_path.open("w", encoding="utf-8") as f:
                json.dump(existing_issues + issues_list, f, indent=2)
        except Exception as e:
            log_and_print(f"{user_brokerid}: Failed to update {ISSUES_FILE}: {e}", "ERROR")

        mt5.shutdown()
        log_and_print(
            f"{user_brokerid}: Deduplication complete – Kept: {total_kept}, Deleted: {total_deleted}",
            "SUCCESS"
        )

    log_and_print("All brokers deduplicated successfully.", "SUCCESS")

def BreakevenRunningPositions():
    r"""
    Staged Breakeven:
      • Ratio 1 → SL to 0.25 (actual price shown)
      • Ratio 2 → SL to 0.50 (actual price shown)
    Clean logs, full precision, MT5-safe.
    """
    BASE_INPUT_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
    BREAKEVEN_REPORT = "breakeven_report.json"
    ISSUES_FILE = "ordersissues.json"

    # === BREAKEVEN STAGES ===
    BE_STAGE_1 = 0.25   # SL moves here at ratio 1
    BE_STAGE_2 = 0.50   # SL moves here at ratio 2
    RATIO_1 = 1.0
    RATIO_2 = 2.0

    # === Helper: Round to symbol digits ===
    def _round_price(price, symbol):
        digits = mt5.symbol_info(symbol).digits
        return round(price, digits)

    # === Helper: Price at ratio ===
    def _ratio_price(entry, sl, tp, ratio, is_buy):
        risk = abs(entry - sl) or 1e-9
        return entry + risk * ratio * (1 if is_buy else -1)

    # === Helper: Modify SL ===
    def _modify_sl(pos, new_sl_raw):
        new_sl = _round_price(new_sl_raw, pos.symbol)
        req = {
            "action": mt5.TRADE_ACTION_SLTP,
            "symbol": pos.symbol,
            "position": pos.ticket,
            "sl": new_sl,
            "tp": pos.tp,
            "magic": pos.magic,
            "comment": pos.comment
        }
        return mt5.order_send(req)

    # === Helper: Print block ===
    def _log_block(lines):
        log_and_print("\n".join(lines), "INFO")

    # === Helper: Safe JSON read (handles corrupted/multi-object files) ===
    def _safe_read_json(path):
        if not path.exists():
            return []
        try:
            with path.open("r", encoding="utf-8") as f:
                content = f.read().strip()
                if not content:
                    return []
                # Handle multiple JSON objects by parsing line-by-line
                objs = []
                for line in content.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        if isinstance(obj, list):
                            objs.extend(obj)
                        elif isinstance(obj, dict):
                            objs.append(obj)
                    except json.JSONDecodeError:
                        continue
                return objs
        except Exception as e:
            log_and_print(f"Failed to read {path.name}: {e}. Starting fresh.", "WARNING")
            return []

    # === Helper: Safe JSON write ===
    def _safe_write_json(path, data):
        try:
            # Ensure parent directory exists
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.write("\n")  # Ensure file ends cleanly
            return True
        except Exception as e:
            log_and_print(f"Failed to write {path.name}: {e}", "ERROR")
            return False

    # ------------------------------------------------------------------ #
    for user_brokerid, cfg in usersdictionary.items():
        # ---- MT5 Connection ------------------------------------------------
        if not mt5.initialize(path=cfg["TERMINAL_PATH"], login=int(cfg["LOGIN_ID"]),
                              password=cfg["PASSWORD"], server=cfg["SERVER"], timeout=30000):
            log_and_print(f"{user_brokerid}: MT5 init failed", "ERROR")
            continue
        if not mt5.login(int(cfg["LOGIN_ID"]), cfg["PASSWORD"], cfg["SERVER"]):
            log_and_print(f"{user_brokerid}: MT5 login failed", "ERROR")
            mt5.shutdown()
            continue

        broker_dir = Path(BASE_INPUT_DIR) / user_brokerid
        report_path = broker_dir / BREAKEVEN_REPORT
        issues_path = broker_dir / ISSUES_FILE

        # Load existing report (unchanged)
        existing_report = []
        if report_path.exists():
            try:
                with report_path.open("r", encoding="utf-8") as f:
                    existing_report = json.load(f)
            except Exception as e:
                log_and_print(f"{user_brokerid}: Failed to load breakeven_report.json – {e}", "WARNING")

        issues = []
        now = datetime.now(pytz.timezone("Africa/Lagos")).strftime("%Y-%m-%d %H:%M:%S.%f%z")
        now = f"{now[:-2]}:{now[-2:]}"  # Format +01:00 properly
        updated = pending_info = 0

        positions = mt5.positions_get() or []
        pending   = mt5.orders_get()   or []

        # ---- Group pending orders by symbol ----
        pending_by_sym = {}
        for o in pending:
            if o.type not in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT):
                continue
            pending_by_sym.setdefault(o.symbol, {})[o.type] = {
                "price": o.price_open, "sl": o.sl, "tp": o.tp
            }

        # ==================================================================
        # === PROCESS RUNNING POSITIONS ===
        # ==================================================================
        for pos in positions:
            if pos.sl == 0 or pos.tp == 0:
                continue

            sym = pos.symbol
            tick = mt5.symbol_info_tick(sym)
            info = mt5.symbol_info(sym)
            if not tick or not info:
                continue

            cur_price = tick.ask if pos.type == mt5.ORDER_TYPE_BUY else tick.bid
            is_buy = pos.type == mt5.ORDER_TYPE_BUY
            typ = "BUY" if is_buy else "SELL"

            # Key levels
            r1_price = _ratio_price(pos.price_open, pos.sl, pos.tp, RATIO_1, is_buy)
            r2_price = _ratio_price(pos.price_open, pos.sl, pos.tp, RATIO_2, is_buy)
            be_025   = _ratio_price(pos.price_open, pos.sl, pos.tp, BE_STAGE_1, is_buy)
            be_050   = _ratio_price(pos.price_open, pos.sl, pos.tp, BE_STAGE_2, is_buy)

            stage1 = (cur_price >= r1_price) if is_buy else (cur_price <= r1_price)
            stage2 = (cur_price >= r2_price) if is_buy else (cur_price <= r2_price)

            # Base block
            block = [
                f"┌─ {user_brokerid} ─ {sym} ─ {typ} (ticket {pos.ticket})",
                f"│ Entry : {pos.price_open:.{info.digits}f}   SL : {pos.sl:.{info.digits}f}   TP : {pos.tp:.{info.digits}f}",
                f"│ Now   : {cur_price:.{info.digits}f}"
            ]

            # === STAGE 2: SL to 0.50 ===
            if stage2 and abs(pos.sl - be_050) > info.point:
                res = _modify_sl(pos, be_050)
                if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                    block += [
                        f"│ BE @ 0.25 → {be_025:.{info.digits}f}",
                        f"│ BE @ 0.50 → {be_050:.{info.digits}f}  ← SL MOVED",
                        f"└─ All left to market"
                    ]
                    updated += 1
                else:
                    issues.append({"symbol": sym, "diagnosed_reason": "SL modify failed (stage 2)"})
                    block.append(f"└─ SL move FAILED")
                _log_block(block)
                continue

            # === STAGE 1: SL to 0.25 ===
            if stage1 and abs(pos.sl - be_025) > info.point:
                res = _modify_sl(pos, be_025)
                if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                    block += [
                        f"│ BE @ 0.25 → {be_025:.{info.digits}f}  ← SL MOVED",
                        f"│ Waiting ratio 2 @ {r2_price:.{info.digits}f} → BE @ 0.50 → {be_050:.{info.digits}f}"
                    ]
                    updated += 1
                else:
                    issues.append({"symbol": sym, "diagnosed_reason": "SL modify failed (stage 1)"})
                    block.append(f"└─ SL move FAILED")
                _log_block(block)
                continue

            # === STAGE 1 REACHED, WAITING STAGE 2 ===
            if stage1:
                block += [
                    f"│ BE @ 0.25 → {be_025:.{info.digits}f}",
                    f"│ Waiting ratio 2 @ {r2_price:.{info.digits}f} → BE @ 0.50 → {be_050:.{info.digits}f}"
                ]
            # === WAITING STAGE 1 ===
            else:
                block += [
                    f"│ Waiting ratio 1 @ {r1_price:.{info.digits}f} → BE @ 0.25 → {be_025:.{info.digits}f}"
                ]

            block.append("")
            _log_block(block)

        # ==================================================================
        # === PROCESS PENDING ORDERS (INFO ONLY) ===
        # ==================================================================
        for sym, orders in pending_by_sym.items():
            for otype, o in orders.items():
                if o["sl"] == 0 or o["tp"] == 0:
                    continue
                info = mt5.symbol_info(sym)
                if not info:
                    continue
                is_buy = otype == mt5.ORDER_TYPE_BUY_LIMIT
                typ = "BUY_LIMIT" if is_buy else "SELL_LIMIT"

                r1_price = _ratio_price(o["price"], o["sl"], o["tp"], RATIO_1, is_buy)
                r2_price = _ratio_price(o["price"], o["sl"], o["tp"], RATIO_2, is_buy)
                be_025   = _ratio_price(o["price"], o["sl"], o["tp"], BE_STAGE_1, is_buy)
                be_050   = _ratio_price(o["price"], o["sl"], o["tp"], BE_STAGE_2, is_buy)

                block = [
                    f"┌─ {user_brokerid} ─ {sym} ─ PENDING {typ}",
                    f"│ Entry : {o['price']:.{info.digits}f}   SL : {o['sl']:.{info.digits}f}   TP : {o['tp']:.{info.digits}f}",
                    f"│ Target 1 → {r1_price:.{info.digits}f}  |  BE @ 0.25 → {be_025:.{info.digits}f}",
                    f"│ Target 2 → {r2_price:.{info.digits}f}  |  BE @ 0.50 → {be_050:.{info.digits}f}",
                    f"└─ Order not running – waiting…"
                ]
                _log_block(block)
                pending_info += 1

        # === SAVE BREAKEVEN REPORT (unchanged) ===
        _safe_write_json(report_path, existing_report)

        # === SAVE ISSUES – ROBUST MERGE ===
        current_issues = _safe_read_json(issues_path)
        all_issues = current_issues + issues
        _safe_write_json(issues_path, all_issues)

        mt5.shutdown()
        log_and_print(
            f"{user_brokerid}: Breakeven done – SL Updated: {updated} | Pending Info: {pending_info}",
            "SUCCESS"
        )

    log_and_print("All brokers breakeven processed.", "SUCCESS")

def risk_reward_ratio_levels():
    """
    8usd RATIO LEVELS + TP UPDATE (PENDING + RUNNING POSITIONS) – BROKER-SAFE
    - Works on ANY balance (balance check removed)
    - Auto-supports riskreward: 1, 2, 3, 8... (any integer)
    - Case-insensitive config
    - consistency → Dynamic TP = RISKREWARD × Risk
    - martingale → TP = 1R (always), ignores RISKREWARD
    - Smart ratio ladder (shows 1R, 2R, 3R only when needed)
    """
    TZ = pytz.timezone("Africa/Lagos")

    log_and_print(f"\n{'='*80}", "INFO")
    log_and_print("RATIO LEVELS + TP UPDATE (PENDING + RUNNING) – CONSISTENCY: N×R | MARTINGALE: 1R", "INFO")
    log_and_print(f"{'='*80}", "INFO")

    for user_brokerid, cfg in usersdictionary.items():
        TERMINAL_PATH = cfg.get("TERMINAL_PATH") or cfg.get("terminal_path")
        LOGIN_ID      = cfg.get("LOGIN_ID")      or cfg.get("login_id")
        PASSWORD      = cfg.get("PASSWORD")      or cfg.get("password")
        SERVER        = cfg.get("SERVER")        or cfg.get("server")
        SCALE         = (cfg.get("SCALE")        or cfg.get("scale")        or "").strip().lower()
        STRATEGY      = (cfg.get("STRATEGY")    or cfg.get("strategy")    or "").strip().lower()

        # === Case-insensitive riskreward lookup ===
        riskreward_raw = None
        for key in cfg:
            if key.lower() == "riskreward":
                riskreward_raw = cfg[key]
                break

        if riskreward_raw is None:
            riskreward_raw = 2
            log_and_print(f"{user_brokerid}: 'riskreward' not found → using default 2R", "WARNING")

        log_and_print(
            f"\nProcessing broker: {user_brokerid} | Scale: {SCALE.upper()} | "
            f"Strategy: {STRATEGY.upper()} | riskreward: {riskreward_raw}R", "INFO"
        )

        # === Validate required fields ===
        missing = []
        for f in ("TERMINAL_PATH", "LOGIN_ID", "PASSWORD", "SERVER", "SCALE"):
            if not locals()[f]: missing.append(f)
        if missing:
            log_and_print(f"Missing config: {', '.join(missing)} → SKIPPED", "ERROR")
            continue

        # === MT5 Init ===
        if not os.path.exists(TERMINAL_PATH):
            log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
            continue

        if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD,
                                server=SERVER, timeout=30000):
            log_and_print(f"MT5 init failed: {mt5.last_error()}", "ERROR")
            continue

        if not mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER):
            log_and_print(f"MT5 login failed: {mt5.last_error()}", "ERROR")
            mt5.shutdown()
            continue

        account_info = mt5.account_info()
        if not account_info:
            log_and_print(f"Failed to get account info: {mt5.last_error()}", "ERROR")
            mt5.shutdown()
            continue

        balance = account_info.balance
        # REMOVED: Balance restriction ($12–$20)
        log_and_print(f"Balance: ${balance:.2f} → Scanning positions & pending orders...", "INFO")

        # === Determine effective RR ===
        try:
            config_rr = int(float(riskreward_raw))
            if config_rr < 1: config_rr = 1
        except (ValueError, TypeError):
            config_rr = 2
            log_and_print(f"Invalid riskreward '{riskreward_raw}' → using 2R", "WARNING")

        effective_rr = 1 if SCALE == "martingale" else config_rr
        rr_source = "MARTINGALE (forced 1R)" if SCALE == "martingale" else f"CONFIG ({effective_rr}R)"
        log_and_print(f"Effective TP: {effective_rr}R [{rr_source}]", "INFO")

        # ------------------------------------------------------------------ #
        # 1. PENDING LIMIT ORDERS
        # ------------------------------------------------------------------ #
        pending_orders = [
            o for o in (mt5.orders_get() or [])
            if o.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT)
            and getattr(o, 'sl', 0) != 0 and getattr(o, 'tp', 0) != 0
        ]

        # ------------------------------------------------------------------ #
        # 2. RUNNING POSITIONS
        # ------------------------------------------------------------------ #
        running_positions = [
            p for p in (mt5.positions_get() or [])
            if p.type in (mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_SELL)
            and p.sl != 0 and p.tp != 0
        ]

        # Merge into a single iterable with a flag
        items_to_process = []
        for o in pending_orders:
            items_to_process.append(('PENDING', o))
        for p in running_positions:
            items_to_process.append(('RUNNING', p))

        if not items_to_process:
            log_and_print("No valid pending orders or running positions found.", "INFO")
            mt5.shutdown()
            continue

        log_and_print(f"Found {len(pending_orders)} pending + {len(running_positions)} running → total {len(items_to_process)}", "INFO")

        processed_symbols = set()
        updated_count = 0

        for kind, obj in items_to_process:
            symbol   = obj.symbol
            ticket   = getattr(obj, 'ticket', None) or getattr(obj, 'order', None)
            entry_price = getattr(obj, 'price_open', None) or getattr(obj, 'price_current', None)
            sl_price = obj.sl
            current_tp  = obj.tp
            is_buy   = obj.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY)

            if symbol in processed_symbols:
                continue

            risk_distance = abs(entry_price - sl_price)
            if risk_distance <= 0:
                log_and_print(f"Zero risk distance on {symbol} ({kind}) → skipped", "WARNING")
                continue

            symbol_info = mt5.symbol_info(symbol)
            if not symbol_info:
                log_and_print(f"Symbol info missing: {symbol}", "WARNING")
                continue

            digits = symbol_info.digits
            def r(p): return round(p, digits)

            entry_price = r(entry_price)
            sl_price    = r(sl_price)
            current_tp  = r(current_tp)
            direction   = 1 if is_buy else -1
            target_tp   = r(entry_price + direction * effective_rr * risk_distance)

            # ----- Ratio ladder (display only) -----
            ratio1 = r(entry_price + direction * 1 * risk_distance)
            ratio2 = r(entry_price + direction * 2 * risk_distance)
            ratio3 = r(entry_price + direction * 3 * risk_distance) if effective_rr >= 3 else None

            print(f"\n{symbol} | {kind} | Target: {effective_rr}R ({SCALE.upper()})")
            print(f"  Entry : {entry_price}")
            print(f"  1R    : {ratio1}")
            print(f"  2R    : {ratio2}")
            if ratio3:
                print(f"  3R    : {ratio3}")
            print(f"  TP    : {current_tp} → ", end="")

            # ----- Modify TP -----
            tolerance = 10 ** -digits
            if abs(current_tp - target_tp) > tolerance:
                if kind == "PENDING":
                    request = {
                        "action": mt5.TRADE_ACTION_MODIFY,
                        "order": ticket,
                        "price": entry_price,
                        "sl": sl_price,
                        "tp": target_tp,
                        "type": obj.type,
                        "type_time": obj.type_time,
                        "type_filling": obj.type_filling,
                        "magic": getattr(obj, 'magic', 0),
                        "comment": getattr(obj, 'comment', "")
                    }
                    if hasattr(obj, 'expiration') and obj.expiration:
                        request["expiration"] = obj.expiration
                else:  # RUNNING
                    request = {
                        "action": mt5.TRADE_ACTION_SLTP,
                        "position": ticket,
                        "sl": sl_price,
                        "tp": target_tp,
                        "symbol": symbol
                    }

                result = mt5.order_send(request)
                if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                    print(f"{target_tp} [UPDATED]")
                    log_and_print(
                        f"TP → {effective_rr}R | {symbol} | {kind} | {current_tp} → {target_tp} [{SCALE.upper()}]",
                        "SUCCESS"
                    )
                    updated_count += 1
                else:
                    err = result.comment if result else "Unknown"
                    print(f"{current_tp} [FAILED: {err}]")
                    log_and_print(f"TP UPDATE FAILED | {symbol} | {kind} | {err}", "ERROR")
            else:
                print(f"{current_tp} [OK]")

            print(f"  SL    : {sl_price}")
            processed_symbols.add(symbol)

        mt5.shutdown()
        log_and_print(
            f"{user_brokerid} → {len(processed_symbols)} symbol(s) | "
            f"{updated_count} TP(s) set to {effective_rr}R [{SCALE.upper()}]",
            "SUCCESS"
        )

    log_and_print(
        "\nALL ACCOUNTS: R:R UPDATE (PENDING + RUNNING) – "
        "consistency=N×R, martingale=1R = DONE",
        "SUCCESS"
    )
    return True

def martingale_enforcement():
    """
    MARTINGALE ENFORCER v5.2 – SMART KILL + REAL HISTORY SCALING
    ------------------------------------------------------------
    • Kills unwanted pending orders
    • Uses mt5.history_deals_get() with smart filtering
    • Checks last 2 closed trades per symbol
    • Scales pending limit order volume ×2 for each losing symbol
    • Delete + recreate if volume change needed
    • Works on Bybit MT5 (tested with real history)
    """
    import time
    from collections import defaultdict, deque
    from datetime import datetime, timedelta

    log_and_print(f"\n{'='*100}", "INFO")
    log_and_print("MARTINGALE ENFORCER v5.2 – SMART KILL + HISTORY SCALING", "INFO")
    log_and_print(f"{'='*100}", "INFO")

    for user_brokerid, cfg in usersdictionary.items():
        SCALE = (cfg.get("SCALE") or cfg.get("scale") or "").lower()
        if SCALE != "martingale":
            continue

        TERMINAL_PATH = cfg["TERMINAL_PATH"]
        LOGIN_ID      = int(cfg["LOGIN_ID"])
        PASSWORD      = cfg["PASSWORD"]
        SERVER        = cfg["SERVER"]
        raw           = cfg.get("MARTINGALE_MARKETS", "")
        allowed       = {s.strip().lower() for s in raw.replace(",", " ").split() if s.strip()}

        if not allowed:
            continue

        log_and_print(f"\n{user_brokerid.upper()} → LOCKING TO: {', '.join(sorted(allowed)).upper()}", "INFO")

        # ------------------------------------------------------------------ #
        # 1. CONNECT / RECONNECT
        # ------------------------------------------------------------------ #
        def connect():
            mt5.shutdown()
            time.sleep(0.3)
            if not mt5.initialize(path=TERMINAL_PATH, login=LOGIN_ID,
                                  password=PASSWORD, server=SERVER, timeout=60000):
                return False
            if not mt5.login(LOGIN_ID, password=PASSWORD, server=SERVER):
                return False
            time.sleep(0.7)
            return True

        if not connect():
            log_and_print("INITIAL CONNECTION FAILED", "ERROR")
            continue

        # ------------------------------------------------------------------ #
        # 2. KILL UNWANTED PENDING ORDERS
        # ------------------------------------------------------------------ #
        def get_orders():
            return mt5.orders_get() or []

        orders = get_orders()
        unwanted = [
            o for o in orders
            if o.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT)
            and o.symbol.lower() not in allowed
        ]

        killed = skipped = failed = 0
        for order in unwanted:
            symbol = order.symbol
            ticket = order.ticket
            log_and_print(f"{symbol} PENDING → Attempting removal...", "WARNING")

            if not connect():
                log_and_print(f"{symbol} → Reconnect failed", "ERROR")
                failed += 1
                continue

            req = {"action": mt5.TRADE_ACTION_REMOVE, "order": ticket}
            res = mt5.order_send(req)

            if not res:
                log_and_print(f"{symbol} → No response", "ERROR")
                failed += 1
                continue

            if res.retcode == mt5.TRADE_RETCODE_DONE:
                log_and_print(f"{symbol} → REMOVED", "SUCCESS")
                killed += 1
            elif "market closed" in res.comment.lower():
                log_and_print(f"{symbol} → Market closed → SKIPPED (safe)", "INFO")
                skipped += 1
            elif res.retcode in (mt5.TRADE_RETCODE_TRADE_DISABLED, mt5.TRADE_RETCODE_NO_CONNECTION):
                log_and_print(f"{symbol} → {res.comment} → SKIPPED", "INFO")
                skipped += 1
            else:
                log_and_print(f"{symbol} → FAILED: {res.comment}", "ERROR")
                failed += 1
            time.sleep(0.4)

        # ------------------------------------------------------------------ #
        # 3. GET CLOSED HISTORY (LAST 2 TRADES PER SYMBOL)
        # ------------------------------------------------------------------ #
        if not connect():
            mt5.shutdown()
            continue

        # Pull recent deals (last 24h should be enough)
        to_date = datetime.now()
        from_date = to_date - timedelta(hours=24)
        all_deals = mt5.history_deals_get(from_date, to_date) or []

        # Filter: only closed positions (DEAL_ENTRY_OUT) and our symbols
        closed_deals = [
            d for d in all_deals
            if d.entry == mt5.DEAL_ENTRY_OUT
            and d.symbol.lower() in allowed
            and d.profit is not None
        ]

        # Sort newest first
        closed_deals.sort(key=lambda x: x.time, reverse=True)

        log_and_print(f"Found {len(closed_deals)} closed deal(s) in last 24h for Martingale markets", "INFO")

        # Build: symbol → list of (deal, volume, profit) — newest first
        history_per_symbol = defaultdict(list)
        for deal in closed_deals:
            sym = deal.symbol.lower()
            history_per_symbol[sym].append({
                'deal': deal,
                'volume': deal.volume,
                'profit': deal.profit,
                'time': deal.time
            })

        # ------------------------------------------------------------------ #
        # 4. DETERMINE WHICH SYMBOLS TO SCALE
        # ------------------------------------------------------------------ #
        symbols_to_scale = {}  # sym → (original_volume, price, order_type)

        # We look at **last 2 closed trades globally**, but per symbol
        recent_losses = []
        for deal in closed_deals[:10]:  # safety cap
            if deal.profit < 0:
                recent_losses.append({
                    'symbol': deal.symbol.lower(),
                    'volume': deal.volume,
                    'profit': deal.profit,
                    'time': deal.time
                })
            if len(recent_losses) >= 2:
                break

        log_and_print(f"Last {len(recent_losses)} losing trade(s): {[d['symbol'].upper() for d in recent_losses]}", "INFO")

        # Rule: If last 2 are losses → scale both (if different), or only last (if same)
        if len(recent_losses) >= 1:
            last = recent_losses[0]
            sym1 = last['symbol']
            vol1 = last['volume']

            # Find pending order
            pending = [o for o in get_orders() if o.symbol.lower() == sym1
                       and o.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT)]

            if pending:
                order = pending[0]
                if order.volume_current < vol1 * 2:
                    symbols_to_scale[sym1] = (vol1, order.price_open, order.type)
                    log_and_print(f"{sym1.upper()} → Last loss {vol1} → will scale pending to {vol1*2}", "INFO")
                else:
                    log_and_print(f"{sym1.upper()} → Already scaled (current {order.volume_current} ≥ {vol1*2})", "INFO")
            else:
                log_and_print(f"{sym1.upper()} → No pending order → cannot scale", "INFO")

            # If 2nd loss exists and is DIFFERENT symbol
            if len(recent_losses) >= 2:
                second = recent_losses[1]
                sym2 = second['symbol']
                vol2 = second['volume']

                if sym2 != sym1:
                    pending2 = [o for o in get_orders() if o.symbol.lower() == sym2
                                and o.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT)]
                    if pending2:
                        order2 = pending2[0]
                        if order2.volume_current < vol2 * 2:
                            symbols_to_scale[sym2] = (vol2, order2.price_open, order2.type)
                            log_and_print(f"{sym2.upper()} → 2nd loss {vol2} → will scale pending to {vol2*2}", "INFO")
                        else:
                            log_and_print(f"{sym2.upper()} → Already scaled", "INFO")
                    else:
                        log_and_print(f"{sym2.upper()} → No pending order → cannot scale", "INFO")

        # ------------------------------------------------------------------ #
        # 5. APPLY SCALING: DELETE + RECREATE
        # ------------------------------------------------------------------ #
        scaled = not_scaled = 0
        for sym, (orig_vol, price, order_type) in symbols_to_scale.items():
            if not connect():
                log_and_print(f"{sym.upper()} → Reconnect failed before scaling", "ERROR")
                continue

            # Re-get orders
            current_orders = get_orders()
            pending = [o for o in current_orders if o.symbol.lower() == sym
                       and o.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT)]

            if not pending:
                log_and_print(f"{sym.upper()} → Pending order vanished → SKIPPED", "WARNING")
                not_scaled += 1
                continue

            order = pending[0]
            new_vol = orig_vol * 2

            if order.volume_current >= new_vol:
                log_and_print(f"{sym.upper()} → Already at {order.volume_current} → SKIPPED", "INFO")
                not_scaled += 1
                continue

            # DELETE
            del_req = {"action": mt5.TRADE_ACTION_REMOVE, "order": order.ticket}
            del_res = mt5.order_send(del_req)
            if del_res.retcode != mt5.TRADE_RETCODE_DONE:
                log_and_print(f"{sym.upper()} → DELETE FAILED: {del_res.comment}", "ERROR")
                continue

            time.sleep(0.3)

            # RECREATE
            new_req = {
                "action": mt5.TRADE_ACTION_PENDING,
                "symbol": sym.upper(),
                "volume": new_vol,
                "type": order_type,
                "price": price,
                "sl": order.sl,
                "tp": order.tp,
                "deviation": 20,
                "magic": order.magic,
                "comment": f"MartingaleScaled_{new_vol}",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }

            new_res = mt5.order_send(new_req)
            if new_res and new_res.retcode == mt5.TRADE_RETCODE_DONE:
                log_and_print(f"{sym.upper()} → SCALED {order.volume_current} → {new_vol} @ {price}", "SUCCESS")
                scaled += 1
            else:
                comment = new_res.comment if new_res else "None"
                log_and_print(f"{sym.upper()} → PLACE FAILED: {comment}", "ERROR")

            time.sleep(0.5)

        # ------------------------------------------------------------------ #
        # 6. 1R ENFORCEMENT (placeholder)
        # ------------------------------------------------------------------ #
        if connect():
            for pos in mt5.positions_get() or []:
                if pos.symbol.lower() in allowed:
                    pass  # ← your 1R logic

        mt5.shutdown()

        # ------------------------------------------------------------------ #
        # 7. FINAL REPORT
        # ------------------------------------------------------------------ #
        log_and_print(f"\n{user_brokerid.upper()} → ENFORCEMENT COMPLETE", "SUCCESS")
        log_and_print(f"   REMOVED     : {killed}", "SUCCESS")
        log_and_print(f"   SKIPPED     : {skipped} (market closed / safe)", "INFO")
        log_and_print(f"   Failed      : {failed}", "WARNING")
        log_and_print(f"   SCALED      : {scaled}", "SUCCESS")
        log_and_print(f"   NOT SCALED  : {not_scaled}", "INFO")

    log_and_print("\nMARTINGALE v5.2 → HISTORY CHECKED. SCALED. DONE.", "SUCCESS")
    return True

def place_2usd_orders():
    BASE_INPUT_DIR = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
    RISK_FOLDER = "risk_2_usd"
    STRATEGY_FILE = "hightolow.json"
    REPORT_SUFFIX = "forex_order_report.json"
    ISSUES_FILE = "ordersissues.json"

    for user_brokerid, broker_cfg in usersdictionary.items():
        TERMINAL_PATH = broker_cfg["TERMINAL_PATH"]
        LOGIN_ID = broker_cfg["LOGIN_ID"]
        PASSWORD = broker_cfg["PASSWORD"]
        SERVER = broker_cfg["SERVER"]

        log_and_print(f"Processing broker: {user_brokerid} (Balance $8–$11.99 → 2 USD risk mode)", "INFO")

        # === MT5 Init ===
        if not os.path.exists(TERMINAL_PATH):
            log_and_print(f"Terminal not found: {TERMINAL_PATH}", "ERROR")
            continue

        if not mt5.initialize(path=TERMINAL_PATH, login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=60000):
            log_and_print(f"MT5 initialize failed: {mt5.last_error()}", "ERROR")
            continue

        if not mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER):
            log_and_print(f"MT5 login failed: {mt5.last_error()}", "ERROR")
            mt5.shutdown()
            continue

        account_info = mt5.account_info()
        if not account_info:
            log_and_print(f"Failed to get account info: {mt5.last_error()}", "ERROR")
            mt5.shutdown()
            continue

        balance = account_info.balance
        equity = account_info.equity
        log_and_print(f"Balance: ${balance:.2f}, Equity: ${equity:.2f}", "INFO")

        # Strict balance check for 2 USD risk mode
        if not (8.0 <= balance < 11.99):
            log_and_print(f"Balance ${balance:.2f} not in $8.00–$11.99 → SKIPPED", "INFO")
            mt5.shutdown()
            continue

        if equity < 8.0:
            log_and_print(f"Equity ${equity:.2f} < $8.0 → In drawdown → SKIPPED", "WARNING")
            mt5.shutdown()
            continue

        log_and_print(f"Account valid → Proceeding with {RISK_FOLDER} strategy", "INFO")

        # === Load hightolow.json ===
        file_path = Path(BASE_INPUT_DIR) / user_brokerid / RISK_FOLDER / STRATEGY_FILE
        if not file_path.exists():
            log_and_print(f"Strategy file not found: {file_path}", "WARNING")
            mt5.shutdown()
            continue

        try:
            with file_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
                entries = data.get("entries", [])
        except Exception as e:
            log_and_print(f"Failed to load JSON: {e}", "ERROR")
            mt5.shutdown()
            continue

        if not entries:
            log_and_print("No entries found in hightolow.json", "INFO")
            mt5.shutdown()
            continue

        # === Track existing orders & positions ===
        existing_pending = {}   # (symbol, type) → ticket
        running_positions = set()

        for order in (mt5.orders_get() or []):
            if order.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT):
                existing_pending[(order.symbol, order.type)] = order.ticket

        for pos in (mt5.positions_get() or []):
            running_positions.add(pos.symbol)

        # === Reporting setup ===
        report_file = file_path.parent / REPORT_SUFFIX
        issues_path = file_path.parent / ISSUES_FILE
        existing_reports = []
        if report_file.exists():
            try:
                with report_file.open("r", encoding="utf-8") as f:
                    existing_reports = json.load(f)
            except:
                existing_reports = []

        issues_list = []
        now_str = datetime.now(pytz.timezone("Africa/Lagos")).strftime("%Y-%m-%d %H:%M:%S.%f+01:00")
        placed = failed = skipped = 0

        for entry in entries:
            try:
                symbol = entry["market"]
                price = float(entry["entry_price"])
                sl = float(entry["sl_price"])
                tp = float(entry["tp_price"])
                volume = float(entry["volume"])
                order_type_str = entry["limit_order"]
                order_type = mt5.ORDER_TYPE_BUY_LIMIT if order_type_str == "buy_limit" else mt5.ORDER_TYPE_SELL_LIMIT

                # Skip if already running or pending
                if symbol in running_positions:
                    skipped += 1
                    log_and_print(f"{symbol} → Already has open position → SKIPPED", "INFO")
                    continue

                key = (symbol, order_type)
                if key in existing_pending:
                    skipped += 1
                    log_and_print(f"{symbol} {order_type_str} → Already pending → SKIPPED", "INFO")
                    continue

                # Symbol info
                symbol_info = mt5.symbol_info(symbol)
                if not symbol_info or not symbol_info.visible:
                    issues_list.append({"symbol": symbol, "reason": "Symbol not visible"})
                    failed += 1
                    continue

                if not mt5.symbol_select(symbol, True):
                    issues_list.append({"symbol": symbol, "reason": "Failed to select symbol"})
                    failed += 1
                    continue

                tick = mt5.symbol_info_tick(symbol)
                if not tick:
                    issues_list.append({"symbol": symbol, "reason": "No tick data"})
                    failed += 1
                    continue

                point = symbol_info.point

                # === DERIV-SPECIFIC MINIMUM DISTANCE ===
                is_synthetic = any(x in symbol for x in ["Volatility", "Boom", "Crash", "Jump", "Step"])
                min_distance_points = 120 if is_synthetic else 30  # 120+ safe for all Deriv synthetics

                if order_type == mt5.ORDER_TYPE_BUY_LIMIT:
                    current_price = tick.ask
                    if price >= current_price or (current_price - price) < min_distance_points * point:
                        skipped += 1
                        log_and_print(f"{symbol} BUY_LIMIT price too close ({current_price - price:.1f} points < {min_distance_points}) → SKIPPED", "INFO")
                        continue
                else:
                    current_price = tick.bid
                    if price <= current_price or (price - current_price) < min_distance_points * point:
                        skipped += 1
                        log_and_print(f"{symbol} SELL_LIMIT price too close ({price - current_price:.1f} points < {min_distance_points}) → SKIPPED", "INFO")
                        continue

                # SL/TP distance check
                min_sl_tp = min_distance_points * point
                if abs(price - sl) < min_sl_tp or abs(price - tp) < min_sl_tp:
                    issues_list.append({"symbol": symbol, "reason": "SL/TP too close"})
                    failed += 1
                    log_and_print(f"{symbol} SL/TP too tight (< {min_distance_points} points) → REJECTED", "WARNING")
                    continue

                # Volume correction
                vol_step = symbol_info.volume_step
                vol_min = symbol_info.volume_min
                vol_max = symbol_info.volume_max
                volume = max(vol_min, round(volume / vol_step) * vol_step)
                volume = min(volume, vol_max)

                if volume < vol_min:
                    issues_list.append({"symbol": symbol, "reason": f"Volume too small: {volume} < {vol_min}"})
                    failed += 1
                    continue

                # === FINAL ORDER REQUEST (DERIV-PROVEN SETTINGS) ===
                request = {
                    "action": mt5.TRADE_ACTION_PENDING,
                    "symbol": symbol,
                    "volume": volume,
                    "type": order_type,
                    "price": price,
                    "sl": sl,
                    "tp": tp,
                    "deviation": 20,
                    "magic": 123456,
                    "comment": "Risk2_Auto",
                    "type_time": mt5.ORDER_TIME_GTC,
                    "type_filling": mt5.ORDER_FILLING_FOK,   # Critical for Deriv!
                }

                result = mt5.order_send(request)

                # Enhanced error reporting
                if result is None:
                    retcode = 10000
                    comment = "order_send returned None"
                else:
                    retcode = result.retcode
                    comment = result.comment

                success = (result and result.retcode == mt5.TRADE_RETCODE_DONE)

                if success:
                    placed += 1
                    existing_pending[key] = result.order
                    log_and_print(f"{symbol} {order_type_str.upper()} @ {price:.5f} → PLACED (Ticket: {result.order})", "SUCCESS")
                else:
                    failed += 1
                    error_msg = f"Retcode: {retcode} | {comment}"
                    issues_list.append({"symbol": symbol, "reason": error_msg})
                    log_and_print(f"{symbol} → FAILED → {error_msg}", "ERROR")

                # Save report entry
                report_entry = {
                    "symbol": symbol,
                    "order_type": order_type_str,
                    "price": price,
                    "volume": volume,
                    "sl": sl,
                    "tp": tp,
                    "risk_usd": 2.0,
                    "ticket": result.order if success else None,
                    "success": success,
                    "error_code": retcode if not success else None,
                    "error_msg": comment if not success else None,
                    "timestamp": now_str
                }
                existing_reports.append(report_entry)

            except Exception as e:
                failed += 1
                issues_list.append({"symbol": symbol if 'symbol' in locals() else "Unknown", "reason": f"Exception: {str(e)}"})
                log_and_print(f"Exception processing entry: {e}", "ERROR")

        # === Save reports ===
        try:
            with report_file.open("w", encoding="utf-8") as f:
                json.dump(existing_reports, f, indent=2)
        except Exception as e:
            log_and_print(f"Failed to save report: {e}", "ERROR")

        try:
            existing_issues = []
            if issues_path.exists():
                with issues_path.open("r", encoding="utf-8") as f:
                    existing_issues = json.load(f)
            with issues_path.open("w", encoding="utf-8") as f:
                json.dump(existing_issues + issues_list, f, indent=2)
        except Exception as e:
            log_and_print(f"Failed to save issues: {e}", "ERROR")

        mt5.shutdown()
        log_and_print(f"{user_brokerid} → Placed: {placed} | Failed: {failed} | Skipped: {skipped}", "SUCCESS")

    log_and_print("All 2 USD risk accounts processed successfully.", "SUCCESS")
    return True

def purge_non_allowed_orders():
    """
    Enhanced Purge: 
      • Removes non-allowed orders from JSON files (original behavior)
      • ALSO cancels any PENDING LIMIT orders in MT5 that belong to restricted markets
    """
    from pathlib import Path
    import json
    from datetime import datetime
    import MetaTrader5 as mt5
    import pytz

    BASE_DIR = Path(r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices")
    NON_ALLOWED_OUT = BASE_DIR / "nonallowedorders.json"

    ALLOWED_MARKETS_PATH = Path(r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_volumes_points\allowedmarkets\allowedmarkets.json")
    ALL_SYMBOLS_PATH      = Path(r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_volumes_points\allowedmarkets\allsymbolsvolumesandrisk.json")
    SYMBOL_MATCH_PATH     = Path(r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_volumes_points\allowedmarkets\symbolsmatch.json")

    print("[PURGE] Starting detection + REMOVAL of non-allowed orders (JSON + LIVE PENDING)...")

    # ========================= LOAD CONTROL FILES =========================
    try:
        allowed_cfg = json.loads(ALLOWED_MARKETS_PATH.read_text(encoding="utf-8"))
        all_symbols_data = json.loads(ALL_SYMBOLS_PATH.read_text(encoding="utf-8"))
        symbol_match_raw = json.loads(SYMBOL_MATCH_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[PURGE] Failed to load control file: {e}")
        return False

    # === Build symbol → asset class mapping ===
    symbol_to_asset = {}
    for risk_key, asset_groups in all_symbols_data.items():
        for raw_asset_class, entries in asset_groups.items():
            asset_key = raw_asset_class.lower().replace(" ", "").replace("_", "")
            if asset_key == "basket_indices":
                asset_key = "basketindices"
            for entry in entries:
                sym = entry.get("symbol")
                if sym:
                    symbol_to_asset[sym.strip()] = asset_key

    # === Broker variant → main symbol ===
    main_symbol_lookup = {}
    for item in symbol_match_raw.get("main_symbols", []):
        main = item.get("symbol")
        if not main: continue
        for broker in ["deriv", "bybit", "exness"]:
            for variant in item.get(broker, []):
                if variant:
                    main_symbol_lookup[variant] = main

    # === Build allowed config ===
    allowed_config = {}
    for raw_cls, cfg in allowed_cfg.items():
        cls_key = raw_cls.lower().replace("_", "")
        if cls_key == "basket_indices":
            cls_key = "basketindices"
        allowed_config[cls_key] = {
            "limited": bool(cfg.get("limited", False)),
            "whitelist": {s.strip().upper() for s in cfg.get("allowed", []) if s.strip()}
        }

    # ========================= PURGE FROM JSON FILES (Original Logic) =========================
    non_allowed_orders = []
    total_removed_json = 0
    files_modified = 0

    for broker_dir in BASE_DIR.iterdir():
        if not broker_dir.is_dir():
            continue

        for risk_folder in broker_dir.iterdir():
            if not risk_folder.is_dir() or not risk_folder.name.startswith("risk_"):
                continue

            for json_file in ["hightolow.json", "lowtohigh.json"]:
                fpath = risk_folder / json_file
                if not fpath.exists():
                    continue

                try:
                    data = json.loads(fpath.read_text(encoding="utf-8"))
                except:
                    continue

                original_entries = data.get("entries", [])
                if not original_entries:
                    continue

                clean_entries = []
                file_removed = 0

                for entry in original_entries:
                    market = entry.get("market", "").strip()
                    if not market:
                        clean_entries.append(entry)
                        continue

                    resolved = main_symbol_lookup.get(market, market)
                    asset_class = symbol_to_asset.get(resolved) or symbol_to_asset.get(market)

                    # Fallback classification
                    if not asset_class:
                        lower = market.lower()
                        if market.endswith("USD") and len(market) <= 10:
                            asset_class = "crypto"
                        elif any(c in market for c in ["AUD","EUR","GBP","USD","JPY","CAD","CHF","NZD"]):
                            asset_class = "forex"
                        elif "volatility" in lower or "index" in lower:
                            asset_class = "synthetics"
                        else:
                            asset_class = "unknown"

                    config = allowed_config.get(asset_class, {"limited": False, "whitelist": set()})
                    is_whitelisted = resolved.upper() in config["whitelist"] or market.upper() in config["whitelist"]

                    if config["limited"] and not is_whitelisted:
                        non_allowed_orders.append({
                            **entry,
                            "purged_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "source_file": str(fpath.relative_to(BASE_DIR)),
                            "broker": broker_dir.name,
                            "resolved_symbol": resolved,
                            "detected_asset_class": asset_class,
                            "reason": f"LIMITED asset class '{asset_class}' – not in whitelist",
                            "purged_from": "JSON"
                        })
                        file_removed += 1
                        total_removed_json += 1
                    else:
                        clean_entries.append(entry)

                if file_removed > 0:
                    data["entries"] = clean_entries
                    summary = data.get("summary", {})
                    # Rebuild summary counts
                    for key in list(summary.keys()):
                        if "symbols" in key:
                            if key == "allmarketssymbols":
                                summary[key] = len(clean_entries)
                            else:
                                asset = key.replace("symbols", "")
                                summary[key] = sum(1 for e in clean_entries if (symbol_to_asset.get(e.get("market",""), "") or "unknown") == asset)
                    data["summary"] = summary
                    data["purged_non_allowed"] = file_removed
                    data["purged_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    try:
                        fpath.write_text(json.dumps(data, indent=2), encoding="utf-8")
                        files_modified += 1
                        print(f"  [JSON PURGED] {file_removed} → {fpath.relative_to(BASE_DIR)}")
                    except Exception as e:
                        print(f"  [ERROR] Failed to save {fpath}: {e}")

    # ========================= CANCEL PENDING LIMIT ORDERS IN MT5 =========================
    total_removed_pending = 0
    for user_brokerid, cfg in usersdictionary.items():  # Assuming you have this dict defined globally
        print(f"[PURGE] Connecting to {user_brokerid} to cancel non-allowed pending orders...")

        if not mt5.initialize(path=cfg["TERMINAL_PATH"], login=int(cfg["LOGIN_ID"]),
                              password=cfg["PASSWORD"], server=cfg["SERVER"], timeout=60000):
            print(f"[PURGE] {user_brokerid}: MT5 initialize failed")
            continue

        if not mt5.login(int(cfg["LOGIN_ID"]), password=cfg["PASSWORD"], server=cfg["SERVER"]):
            print(f"[PURGE] {user_brokerid}: Login failed")
            mt5.shutdown()
            continue

        orders = mt5.orders_get()
        if not orders:
            mt5.shutdown()
            continue

        canceled_this_broker = 0
        for order in orders:
            if order.type not in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT):
                continue

            sym = order.symbol
            resolved = main_symbol_lookup.get(sym, sym)
            asset_class = symbol_to_asset.get(resolved) or symbol_to_asset.get(sym)

            # Fallback
            if not asset_class:
                lower = sym.lower()
                if sym.endswith("USD") and len(sym) <= 10:
                    asset_class = "crypto"
                elif any(c in sym for c in ["AUD","EUR","GBP","USD","JPY","CAD","CHF","NZD"]):
                    asset_class = "forex"
                elif "volatility" in lower or "index" in lower:
                    asset_class = "synthetics"
                else:
                    asset_class = "unknown"

            config = allowed_config.get(asset_class, {"limited": False, "whitelist": set()})
            is_whitelisted = resolved.upper() in config["whitelist"] or sym.upper() in config["whitelist"]

            if config["limited"] and not is_whitelisted:
                # CANCEL THE ORDER
                request = {
                    "action": mt5.TRADE_ACTION_REMOVE,
                    "order": order.ticket,
                }
                result = mt5.order_send(request)
                if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                    print(f"  [CANCELED PENDING] {sym} (ticket {order.ticket}) → {asset_class}")
                    canceled_this_broker += 1
                    total_removed_pending += 1

                    non_allowed_orders.append({
                        "symbol": sym,
                        "ticket": order.ticket,
                        "type": "BUY_LIMIT" if order.type == mt5.ORDER_TYPE_BUY_LIMIT else "SELL_LIMIT",
                        "price_open": order.price_open,
                        "sl": order.sl,
                        "tp": order.tp,
                        "purged_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "broker": user_brokerid,
                        "resolved_symbol": resolved,
                        "detected_asset_class": asset_class,
                        "reason": f"LIMITED asset class '{asset_class}' – not in whitelist",
                        "purged_from": "PENDING_ORDER_MT5"
                    })
                else:
                    print(f"  [FAILED CANCEL] {sym} (ticket {order.ticket}) – {result.comment if result else 'No result'}")

        print(f"[PURGE] {user_brokerid}: Canceled {canceled_this_broker} non-allowed pending orders.")
        mt5.shutdown()

    # ========================= FINAL REPORT =========================
    total_removed = total_removed_json + total_removed_pending
    result = {
        "purged_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_purged_orders": total_removed,
        "purged_from_json": total_removed_json,
        "purged_from_pending_mt5": total_removed_pending,
        "files_cleaned": files_modified,
        "purged_orders": non_allowed_orders
    }

    try:
        NON_ALLOWED_OUT.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\n[PURGE] SUCCESS → {total_removed} non-allowed orders removed!")
        print(f"        → {total_removed_json} from JSON files")
        print(f"        → {total_removed_pending} pending orders canceled in MT5")
        print(f"        Full log saved: {NON_ALLOWED_OUT}\n")
        if total_removed == 0:
            print("[PURGE] SYSTEM CLEAN – No non-allowed orders found anywhere.")
    except Exception as e:
        print(f"[PURGE] Failed to write final log: {e}")

    return total_removed == 0

def print_user_brokerids():
    base_path = r"C:\xampp\htdocs\chronedge\synarex\usersdata\symbols_calculated_prices"
    
    if not os.path.exists(base_path):
        print(f"ERROR: Base directory does not exist:\n    {base_path}")
        return
    
    if not usersdictionary:
        print("No brokers found in usersdictionary.")
        return

    print("Configured Brokers & Folder Check:")
    print("=" * 90)
    
    configured_names = set()
    broker_details = []
    existing = 0
    missing = 0
    
    for user_brokerid in usersdictionary.keys():
        configured_names.add(user_brokerid.strip())
        safe_user_brokerid = "".join(c if c not in r'\/:*?"<>|' else "_" for c in user_brokerid.strip())
        folder_path = os.path.join(base_path, safe_user_brokerid)
        
        exists = os.path.isdir(folder_path)
        marker = "Success" if exists else "Error"
        status = "EXISTS" if exists else "MISSING"
        
        print(f"{marker} {user_brokerid.ljust(30)} → {status}")
        print(f"    Path: {folder_path}\n")
        
        broker_details.append({
            'full': user_brokerid.strip(),
            'safe': safe_user_brokerid,
            'exists': exists
        })
        
        if exists:
            existing += 1
        else:
            missing += 1
    
    print("=" * 90)

    # ——————————————————————————————
    # Unique configured broker bases
    # ——————————————————————————————
    print("\nUnique Configured Broker Bases:")
    print("-" * 60)
    
    base_names = {}
    for broker in broker_details:
        full = broker['full']
        match = re.match(r"([a-zA-Z_]+)\d*$", full)
        base = match.group(1) if match else full
        base_names.setdefault(base, []).append(full)
    
    for base, instances in sorted(base_names.items()):
        print(f"• {base.ljust(15)} → {len(instances)} configured account(s): {', '.join(instances)}")
    print("-" * 60)
    print(f"Unique configured broker types: {len(base_names)}")

    # ——————————————————————————————
    # AUTO-DELETE ORPHANED FOLDERS (NO CONFIRMATION)
    # ——————————————————————————————
    print("\nScanning and AUTO-DELETING Orphaned Broker Folders...")
    print("-" * 70)
    
    if not os.path.isdir(base_path):
        print("Base path not accessible. Skipping cleanup.")
        return
    
    orphaned_to_delete = []
    all_folders = [f for f in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, f))]
    
    for folder_name in all_folders:
        clean_name = folder_name.strip()
        original_folder_path = os.path.join(base_path, folder_name)
        
        # Try to extract base broker name (e.g., "deriv" from "deriv7" or "deriv8_something")
        match = re.match(r"([a-zA-Z_]+)\d*", clean_name.split('_')[0])
        base = match.group(1) if match else None
        
        # Case 1: Folder name doesn't match any configured broker exactly
        if clean_name not in configured_names:
            if base and any(b['full'].startswith(base) for b in broker_details):
                reason = "same broker family but unconfigured account"
            elif base:
                reason = "completely unknown broker type"
            else:
                reason = "invalid naming pattern"
            
            orphaned_to_delete.append((folder_name, base or "unknown", reason, original_folder_path))

    deleted_count = 0
    if orphaned_to_delete:
        print("Deleting orphaned folders immediately:")
        for folder, base, reason, full_path in orphaned_to_delete:
            try:
                shutil.rmtree(full_path)  # Permanently deletes folder + all contents
                print(f"  DELETED: {folder.ljust(25)} → {base.ljust(12)} | {reason}")
                deleted_count += 1
            except Exception as e:
                print(f"  FAILED to delete: {folder} → {str(e)}")
        
        print(f"\nCleanup complete: {deleted_count} orphaned folder(s) permanently deleted.")
    else:
        print("No orphaned folders found. Nothing to delete.")

    print("-" * 70)
    
    if missing > 0:
        print(f"\nReminder: {missing} configured broker(s) are missing their folder!")

def main():
    print_user_brokerids()
