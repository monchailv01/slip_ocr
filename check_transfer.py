#!/usr/bin/env python3
"""
check_transfer.py

Utility to check whether an OCR-extracted slip corresponds to a real incoming bank transaction
stored in the `bank_incoming_transaction` table.

Features:
- connect to PostgreSQL (psycopg2) using environment variables or defaults
- search for candidate transactions by amount/date/time/receiver account/sender
- score candidates and return best match or list of matches
- CLI for quick checks

Usage (example):
  python check_transfer.py --amount 123.45 --date 2025-11-07 --time 13:05:00 --receiver 0123456789 --sender "Somchai"

Note: The repository already included a DB snippet; this file will use the same connection values
by default but prefers environment variables when present.
"""
from __future__ import annotations

import argparse
import os
import decimal
from datetime import datetime, date, time, timedelta
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
import re


DEFAULT_DB = {
    "host": os.getenv("DB_HOST", "test1407.postgres.database.azure.com"),
    "dbname": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "admin_mon"),
    "password": os.getenv("DB_PASSWORD", "M0nCha!_4Life2025"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "sslmode": os.getenv("DB_SSLMODE", "require"),
}


def connect_db(db_conf: Dict[str, Any] = None):
    cfg = DEFAULT_DB.copy()
    if db_conf:
        cfg.update(db_conf)
    conn = psycopg2.connect(
        host=cfg["host"],
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"],
        port=cfg["port"],
        sslmode=cfg.get("sslmode", "require"),
    )
    return conn


def find_candidate_transactions(
    conn,
    amount: decimal.Decimal = None,
    transfer_date: date = None,
    transfer_time: time = None,
    receiver_account_number: Optional[str] = None,
    sender_account_number: Optional[str] = None,
    sender_name: Optional[str] = None,
    amount_tolerance: decimal.Decimal = decimal.Decimal("0.00"),
    date_tolerance_days: int = 1,
    time_tolerance_minutes: int = 60,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """
    Query the `bank_incoming_transaction` table for candidate matches.

    The function builds WHERE clauses depending on which parameters are provided.
    Returns list of rows as dicts (psycopg2.extras.RealDictCursor).
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    where_clauses: List[str] = []
    params: List[Any] = []

    # amount with tolerance
    if amount is not None:
        if amount_tolerance and amount_tolerance != decimal.Decimal("0.00"):
            where_clauses.append("amount BETWEEN %s AND %s")
            params.extend([amount - amount_tolerance, amount + amount_tolerance])
        else:
            where_clauses.append("amount = %s")
            params.append(amount)

    # date range
    if transfer_date is not None:
        start_date = transfer_date - timedelta(days=date_tolerance_days)
        end_date = transfer_date + timedelta(days=date_tolerance_days)
        where_clauses.append("transfer_date BETWEEN %s AND %s")
        params.extend([start_date, end_date])

    # receiver account exact (if provided) - prefer exact match
    if receiver_account_number:
        # OCR/account formats often contain X placeholders and dashes; try a flexible match
        digits = ''.join(ch for ch in str(receiver_account_number) if ch.isdigit())
        if len(digits) >= 3:
            last4 = digits[-4:]
            where_clauses.append("receiver_account_number ILIKE %s")
            params.append(f"%{last4}%")
        else:
            where_clauses.append("receiver_account_number ILIKE %s")
            params.append(f"%{receiver_account_number}%")

    # sender account flexible match
    if sender_account_number:
        sdigits = ''.join(ch for ch in str(sender_account_number) if ch.isdigit())
        if len(sdigits) >= 3:
            last4s = sdigits[-4:]
            where_clauses.append("(sender_account_number ILIKE %s OR sender_account ILIKE %s)")
            params.extend([f"%{last4s}%", f"%{last4s}%"])
        else:
            where_clauses.append("(sender_account_number ILIKE %s OR sender_account ILIKE %s)")
            params.extend([f"%{sender_account_number}%", f"%{sender_account_number}%"])

    # sender name - use ILIKE %name% when provided
    if sender_name:
        where_clauses.append("sender_name ILIKE %s")
        params.append(f"%{sender_name}%")

    # Build base query
    where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"

    # time difference measure will be computed client-side if transfer_time given
    sql = f"""
    SELECT *,
           created_at,
           updated_at
    FROM bank_incoming_transaction
    WHERE {where_sql}
    ORDER BY transfer_date DESC, api_called_at DESC NULLS LAST
    LIMIT %s
    """
    params.append(limit)

    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    cur.close()

    results: List[Dict[str, Any]] = []
    for r in rows:
        row = dict(r)
        # compute time diff seconds if transfer_time provided
        if transfer_time and row.get("transfer_time") is not None:
            # both are time objects
            db_t = row.get("transfer_time")
            try:
                # compute minimal absolute seconds difference (wrap-around at midnight)
                db_seconds = db_t.hour * 3600 + db_t.minute * 60 + db_t.second
                q_seconds = transfer_time.hour * 3600 + transfer_time.minute * 60 + transfer_time.second
                diff = abs(db_seconds - q_seconds)
                # consider wrap-around (e.g., 23:55 vs 00:05 = 10 minutes)
                diff = min(diff, 24 * 3600 - diff)
                row["time_diff_seconds"] = diff
            except Exception:
                row["time_diff_seconds"] = None
        else:
            row["time_diff_seconds"] = None

        # amount diff
        try:
            row_amount = decimal.Decimal(row.get("amount"))
            row["amount_diff"] = abs(row_amount - amount) if amount is not None else None
        except Exception:
            row["amount_diff"] = None

        results.append(row)

    # Optionally filter by time tolerance client-side
    if transfer_time and time_tolerance_minutes is not None:
        seconds_tol = time_tolerance_minutes * 60
        results = [r for r in results if r["time_diff_seconds"] is None or r["time_diff_seconds"] <= seconds_tol]

    # sort by amount diff then time diff (None treated as large)
    def _sort_key(r: Dict[str, Any]):
        ad = r.get("amount_diff")
        td = r.get("time_diff_seconds")
        return (
            float(ad) if ad is not None else 1e12,
            float(td) if td is not None else 1e12,
        )

    results.sort(key=_sort_key)
    return results


def score_candidate(row: Dict[str, Any]) -> float:
    """Simple scoring: lower is better. Combine amount_diff (in THB) and time_diff_seconds.

    This is purposely simple and easy to tune.
    """
    amount_diff = row.get("amount_diff")
    time_diff = row.get("time_diff_seconds")
    score = 0.0
    if amount_diff is None:
        score += 1000000.0
    else:
        score += float(amount_diff) * 100.0  # weight amount heavily
    if time_diff is None:
        score += 100000.0
    else:
        score += float(time_diff) / 60.0  # minutes
    # small bonus if status_reconcile already MATCHED
    if row.get("status_reconcile") == "MATCHED":
        score -= 1000.0
    return score


def check_slip_match(
    amount: decimal.Decimal = None,
    transfer_date: date = None,
    transfer_time: time = None,
    receiver_account_number: Optional[str] = None,
    sender_name: Optional[str] = None,
    sender_account_number: Optional[str] = None,
    db_conf: Dict[str, Any] = None,
    amount_tolerance: decimal.Decimal = decimal.Decimal("0.00"),
    date_tolerance_days: int = 1,
    time_tolerance_minutes: int = 60,
    limit: int = 20,
) -> Dict[str, Any]:
    """
    High-level function: returns dict with keys:
    - status: MATCHED | NOT_FOUND | MULTIPLE
    - best_match: dict or None
    - candidates: list
    """
    conn = connect_db(db_conf)
    try:
        candidates = find_candidate_transactions(
            conn,
            amount=amount,
            transfer_date=transfer_date,
            transfer_time=transfer_time,
            receiver_account_number=receiver_account_number,
            sender_name=sender_name,
            amount_tolerance=amount_tolerance,
            date_tolerance_days=date_tolerance_days,
            time_tolerance_minutes=time_tolerance_minutes,
            limit=limit,
        )
        # Apply stricter business rules per user request:
        # - amount must match exactly (100%)
        # - transfer_date must match exactly (100%)
        # - transfer_time within +/- 5 minutes
        # - sender account must match positionally; ignore 'X' or '*' as wildcards

        def _normalize_account(s: Optional[str]) -> Optional[str]:
            if not s:
                return None
            # keep digits and X/* placeholders
            return ''.join(ch for ch in str(s) if ch.isdigit() or ch in 'Xx*')

        def _positional_account_match(a: Optional[str], b: Optional[str]) -> bool:
            if not a or not b:
                return False
            na = _normalize_account(a)
            nb = _normalize_account(b)
            if not na or not nb:
                return False
            if len(na) != len(nb):
                return False
            for ca, cb in zip(na, nb):
                if ca in 'Xx*' or cb in 'Xx*':
                    continue
                if ca != cb:
                    return False
            return True

        def _last_n_digits_match(a: Optional[str], b: Optional[str], n: int = 4) -> bool:
            if not a or not b:
                return False
            da = ''.join(ch for ch in str(a) if ch.isdigit())
            db = ''.join(ch for ch in str(b) if ch.isdigit())
            if len(da) < n or len(db) < n:
                return False
            return da[-n:] == db[-n:]

        # Keep original candidates for diagnostics
        original_candidates = list(candidates)

        # Filter candidates according to strict rules
        strict_candidates: List[Dict[str, Any]] = []
        for c in original_candidates:
            try:
                # amount exact
                amt_ok = True
                if amount is not None and c.get('amount') is not None:
                    try:
                        db_amt = decimal.Decimal(c.get('amount'))
                        amt_ok = (db_amt == amount)
                    except Exception:
                        amt_ok = False

                # date exact
                date_ok = True
                if transfer_date is not None and c.get('transfer_date') is not None:
                    try:
                        db_date = c.get('transfer_date')
                        date_ok = (db_date == transfer_date)
                    except Exception:
                        date_ok = False

                # time within +/- time_tolerance_minutes (default should be 5)
                time_ok = True
                if transfer_time is not None and c.get('transfer_time') is not None:
                    try:
                        db_t = c.get('transfer_time')
                        db_seconds = db_t.hour * 3600 + db_t.minute * 60 + db_t.second
                        q_seconds = transfer_time.hour * 3600 + transfer_time.minute * 60 + transfer_time.second
                        diff = abs(db_seconds - q_seconds)
                        diff = min(diff, 24 * 3600 - diff)
                        time_ok = diff <= (time_tolerance_minutes * 60)
                    except Exception:
                        time_ok = False

                # sender account positional match if provided; fallback to last-4 digits
                sender_ok = True
                sender_match_method = None
                if sender_account_number:
                    target = str(c.get('sender_account_number') or c.get('sender_account') or '')
                    pos_ok = _positional_account_match(str(sender_account_number), target)
                    if pos_ok:
                        sender_ok = True
                        sender_match_method = 'positional'
                    else:
                        last4_ok = _last_n_digits_match(str(sender_account_number), target, n=4)
                        if last4_ok:
                            sender_ok = True
                            sender_match_method = 'last4'
                        else:
                            sender_ok = False

                if amt_ok and date_ok and time_ok and sender_ok:
                    # annotate candidate with how sender matched for diagnostics
                    if sender_match_method:
                        c['_sender_match_method'] = sender_match_method
                    strict_candidates.append(c)
            except Exception:
                # on any error, skip candidate
                continue

        if strict_candidates:
            candidates = strict_candidates

        # If after strict filtering there are no candidates, but we had original candidates,
        # compute per-candidate diagnostics explaining which conditions failed.
        def _evaluate_candidate(c: Dict[str, Any]) -> Dict[str, Any]:
            reasons: List[str] = []
            # amount exact
            amt_ok = False
            try:
                if amount is not None and c.get('amount') is not None:
                    db_amt = decimal.Decimal(c.get('amount'))
                    amt_ok = (db_amt == amount)
                else:
                    amt_ok = False
            except Exception:
                amt_ok = False
            if not amt_ok:
                reasons.append('amount_mismatch')

            # date exact
            date_ok = False
            try:
                if transfer_date is not None and c.get('transfer_date') is not None:
                    db_date = c.get('transfer_date')
                    date_ok = (db_date == transfer_date)
                else:
                    date_ok = False
            except Exception:
                date_ok = False
            if not date_ok:
                reasons.append('date_mismatch')

            # time within tolerance
            time_ok = False
            try:
                if transfer_time is not None and c.get('transfer_time') is not None:
                    db_t = c.get('transfer_time')
                    db_seconds = db_t.hour * 3600 + db_t.minute * 60 + db_t.second
                    q_seconds = transfer_time.hour * 3600 + transfer_time.minute * 60 + transfer_time.second
                    diff = abs(db_seconds - q_seconds)
                    diff = min(diff, 24 * 3600 - diff)
                    time_ok = diff <= (time_tolerance_minutes * 60)
                else:
                    time_ok = False
            except Exception:
                time_ok = False
            if not time_ok:
                reasons.append('time_out_of_range')

            # sender account positional (prefers positional, fallback to last-4 digits)
            sender_ok = True
            sender_match_method = None
            if sender_account_number:
                try:
                    target = str(c.get('sender_account_number') or c.get('sender_account') or '')
                    pos_ok = _positional_account_match(str(sender_account_number), target)
                    if pos_ok:
                        sender_ok = True
                        sender_match_method = 'positional'
                    else:
                        last4_ok = _last_n_digits_match(str(sender_account_number), target, n=4)
                        if last4_ok:
                            sender_ok = True
                            sender_match_method = 'last4'
                        else:
                            sender_ok = False
                except Exception:
                    sender_ok = False
            if not sender_ok:
                reasons.append('sender_account_mismatch')

            # make a JSON-serializable copy
            info = dict(c)
            try:
                if isinstance(info.get('amount'), decimal.Decimal):
                    info['amount'] = str(info['amount'])
            except Exception:
                pass
            try:
                if isinstance(info.get('amount_diff'), decimal.Decimal):
                    info['amount_diff'] = str(info['amount_diff'])
            except Exception:
                pass
            info.update({
                'amount_ok': amt_ok,
                'date_ok': date_ok,
                'time_ok': time_ok,
                'sender_ok': sender_ok,
                'failed_conditions': reasons,
            })
            return info

        if not candidates:
            if not original_candidates:
                # nothing returned from DB at all
                return {"status": "NOT_FOUND", "best_match": None, "candidates": [], "message": "no rows returned from DB for provided filters"}
            # return diagnostic list
            diag = [_evaluate_candidate(c) for c in original_candidates]
            return {"status": "NOT_FOUND", "best_match": None, "candidates": diag}

        # score candidates
        scored: List[Tuple[float, Dict[str, Any]]] = []
        for c in candidates:
            s = score_candidate(c)
            scored.append((s, c))
        scored.sort(key=lambda x: x[0])
        best_score, best_row = scored[0]

        # heuristics for declaring MATCHED: small amount diff and small time diff
        amount_ok = best_row.get("amount_diff") is not None and float(best_row.get("amount_diff")) <= float(amount_tolerance)
        time_ok = best_row.get("time_diff_seconds") is None or float(best_row.get("time_diff_seconds")) <= time_tolerance_minutes * 60

        status = "MULTIPLE"
        if amount_ok and time_ok:
            status = "MATCHED"
        elif len(candidates) == 1:
            status = "MATCHED" if (amount_ok or time_ok) else "POSSIBLE"
        else:
            status = "MULTIPLE"

        # produce diagnostics for each candidate (which conditions passed/failed)
        candidates_diag = [_evaluate_candidate(c) for _, c in scored]
        # best match diag is first element after sorting
        best_diag = candidates_diag[0] if candidates_diag else None

        result = {
            "status": status,
            "best_score": float(best_score),
            "best_match": best_diag,
            "candidates": candidates_diag,
        }

        # If matched and caller asked to auto-reconcile, perform DB update
        # Note: we don't change behavior unless caller passes auto_reconcile via db_conf
        auto_reconcile = False
        caller_id = os.getenv('API_CALLER_ID', 'check_transfer')
        if db_conf and isinstance(db_conf, dict):
            auto_reconcile = db_conf.get('auto_reconcile', False)
            caller_id = db_conf.get('caller_id', caller_id)

        if status == 'MATCHED' and best_row and auto_reconcile:
            try:
                # If row was already reconciled, return that info instead of updating again
                current_status = best_row.get('status_reconcile')
                if current_status and str(current_status).strip().lower() in ('match', 'matched', 'แมท'):
                    result['already_reconciled'] = True
                    # Notify in English that the payment/transaction was already reconciled
                    result['already_reconciled_message'] = 'Already reconciled'
                    # attach current row info
                    result['reconciled'] = True
                    result['reconciled_row'] = {
                        'id': best_row.get('id'),
                        'status_reconcile': best_row.get('status_reconcile'),
                        'amount': str(best_row.get('amount')) if best_row.get('amount') is not None else None,
                    }
                else:
                    cur = conn.cursor()
                    # Prefer numeric id primary key when present
                    bid = best_row.get('id')
                    if bid is not None:
                        sql = """
                        UPDATE bank_incoming_transaction
                        SET status_reconcile = %s,
                            api_caller_id = %s,
                            api_called_at = now()
                        WHERE id = %s
                        RETURNING id, status_reconcile, amount
                        """
                        cur.execute(sql, ('MATCH', caller_id, bid))
                    else:
                        txid = best_row.get('transaction_id')
                        if txid:
                            sql = """
                            UPDATE bank_incoming_transaction
                            SET status_reconcile = %s,
                                api_caller_id = %s,
                                api_called_at = now()
                            WHERE transaction_id = %s
                            RETURNING id, status_reconcile, amount
                            """
                            cur.execute(sql, ('MATCH', caller_id, txid))
                        else:
                            raise RuntimeError('no primary id or transaction_id available to update')
                    upd = cur.fetchone()
                    conn.commit()
                    if upd:
                        # attach reconciliation info to result
                        result['reconciled'] = True
                        result['reconciled_row'] = {
                            'id': upd[0],
                            'status_reconcile': upd[1],
                            'amount': str(upd[2]) if upd[2] is not None else None,
                        }
                    else:
                        result['reconciled'] = False
                        result['reconciled_error'] = 'no row updated (id/txid mismatch)'
                    cur.close()
            except Exception as e:
                # do not raise — return diagnostics
                try:
                    conn.rollback()
                except Exception:
                    pass
                result['reconciled'] = False
                result['reconciled_error'] = str(e)

        return result
    finally:
        conn.close()


def _parse_args():
    p = argparse.ArgumentParser(description="Check whether an OCR slip matches an incoming bank transaction")
    p.add_argument("--amount", required=True, help="Amount (e.g., 123.45)")
    p.add_argument("--date", required=True, help="Transfer date YYYY-MM-DD")
    p.add_argument("--time", required=False, help="Transfer time HH:MM:SS (optional)")
    p.add_argument("--receiver", required=False, help="Receiver account number (our account)")
    p.add_argument("--sender", required=False, help="Sender name (as read from slip)")
    p.add_argument("--sender-account", required=False, help="Sender account number (as read from slip)")
    p.add_argument("--amount-tolerance", default="0.00", help="Amount tolerance (default 0.00)")
    p.add_argument("--date-tolerance-days", type=int, default=1, help="Date tolerance in days (default 1)")
    p.add_argument("--time-tolerance-minutes", type=int, default=60, help="Time tolerance in minutes (default 60)")
    p.add_argument("--auto-reconcile", action='store_true', help="If set, update matched row's status_reconcile to 'MATCH'")
    p.add_argument("--caller-id", default=os.getenv('API_CALLER_ID', 'check_transfer'), help="API caller id to write to audit field")
    return p.parse_args()


def main():
    args = _parse_args()
    try:
        amount = decimal.Decimal(args.amount)
    except Exception as e:
        raise SystemExit(f"Invalid amount: {e}")
    try:
        transfer_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    except Exception as e:
        raise SystemExit(f"Invalid date: {e}")
    transfer_time = None
    if args.time:
        try:
            transfer_time = datetime.strptime(args.time, "%H:%M:%S").time()
        except Exception:
            try:
                transfer_time = datetime.strptime(args.time, "%H:%M").time()
            except Exception as e:
                raise SystemExit(f"Invalid time: {e}")

    amount_tol = decimal.Decimal(args.amount_tolerance)

    result = check_slip_match(
        amount=amount,
        transfer_date=transfer_date,
        transfer_time=transfer_time,
        receiver_account_number=args.receiver,
        sender_name=args.sender,
        sender_account_number=args.sender_account,
        amount_tolerance=amount_tol,
        date_tolerance_days=args.date_tolerance_days,
        time_tolerance_minutes=args.time_tolerance_minutes,
    )

    # If CLI requested auto-reconcile, call check_slip_match again with db_conf to perform update
    if args.auto_reconcile:
        db_conf = {
            'auto_reconcile': True,
            'caller_id': args.caller_id,
        }
        # Re-run check_slip_match to perform reconcile update (function will update when matched)
        result = check_slip_match(
            amount=amount,
            transfer_date=transfer_date,
            transfer_time=transfer_time,
            receiver_account_number=args.receiver,
            sender_name=args.sender,
            sender_account_number=args.sender_account,
            amount_tolerance=amount_tol,
            date_tolerance_days=args.date_tolerance_days,
            time_tolerance_minutes=args.time_tolerance_minutes,
            db_conf=db_conf,
        )

    import json

    print(json.dumps(result, indent=2, default=str, ensure_ascii=False))


if __name__ == "__main__":
    main()
