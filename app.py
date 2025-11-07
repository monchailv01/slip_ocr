# app.py
from flask import Flask, render_template, request, jsonify
import logging
from logging.handlers import RotatingFileHandler
from datetime import date, time, datetime as _dt
from decimal import Decimal
from typing import Any
from ocr_utils import process_slip, _norm_amount, _norm_date_th, _clean_name
import decimal
from datetime import datetime
from check_transfer import check_slip_match
import os

app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
# Ensure Flask does not propagate exceptions to the Werkzeug debugger in production runs
app.config.setdefault('PROPAGATE_EXCEPTIONS', False)

# Setup simple rotating file logging for server-side errors
log_handler = RotatingFileHandler('server.log', maxBytes=2_000_000, backupCount=3)
log_handler.setLevel(logging.INFO)
log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
log_handler.setFormatter(log_formatter)
if not app.logger.handlers:
    app.logger.addHandler(log_handler)

@app.route('/')
def index():
    return render_template('review.html')


@app.route('/result')
def result_page():
    return render_template('result.html')

@app.route('/upload', methods=['POST'])
def upload():
    try:
        file = request.files.get('file')
        if not file:
            return jsonify({"error": "no file"}), 400
        path = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(path)

        ocr_result = process_slip(path)

        # prepare a normalized OCR dict to send to DB checker when possible
        ocr_data = {}
        # If LLM flow returned parsed keys, prefer them
        keys = ['amount', 'date', 'sender_account', 'recipient_account', 'sender_name', 'recipient_name', 'transaction_id', 'bank']
        if isinstance(ocr_result, dict):
            for k in keys:
                if k in ocr_result:
                    ocr_data[k] = ocr_result.get(k)

        # fallback: try to extract amount/date from raw text if possible
        if not ocr_data.get('amount') and isinstance(ocr_result, dict) and ocr_result.get('raw_text'):
            ocr_data['amount'] = _norm_amount(ocr_result.get('raw_text'))

        if not ocr_data.get('date') and isinstance(ocr_result, dict) and ocr_result.get('raw_text'):
            # try to normalize thai date/time
            d = _norm_date_th(ocr_result.get('raw_text'))
            if d:
                ocr_data['date'] = d

        # normalize names/accounts
        if ocr_data.get('sender_name'):
            ocr_data['sender_name'] = _clean_name(ocr_data.get('sender_name'))
        if ocr_data.get('recipient_account'):
            ocr_data['recipient_account'] = str(ocr_data.get('recipient_account')).strip()

        db_check = None
        # only attempt DB check if we have an amount and a date
        if ocr_data.get('amount') and ocr_data.get('date'):
            # parse amount to Decimal
            try:
                amount = decimal.Decimal(str(ocr_data.get('amount')))
            except Exception:
                amount = None

            # parse date which may include time
            transfer_date = None
            transfer_time = None
            dstr = ocr_data.get('date')
            if dstr:
                try:
                    # try YYYY-MM-DD HH:MM
                    dt = datetime.strptime(dstr, "%Y-%m-%d %H:%M")
                    transfer_date = dt.date()
                    transfer_time = dt.time()
                except Exception:
                    try:
                        dt = datetime.strptime(dstr, "%Y-%m-%d")
                        transfer_date = dt.date()
                    except Exception:
                        transfer_date = None

            try:
                db_check = check_slip_match(
                        amount=amount,
                        transfer_date=transfer_date,
                        transfer_time=transfer_time,
                        receiver_account_number=ocr_data.get('recipient_account'),
                        sender_name=ocr_data.get('sender_name'),
                        sender_account_number=ocr_data.get('sender_account'),
                        db_conf={'auto_reconcile': True, 'caller_id': 'web_ui'},
                        amount_tolerance=decimal.Decimal('0.00'),
                        date_tolerance_days=0,
                        time_tolerance_minutes=5,
                    )
            except Exception as e:
                # capture DB/check errors so frontend can display them
                db_check = {"error": str(e)}

        # Make sure all returned objects are JSON serializable (convert date/time/Decimal)
        def make_json_safe(obj: Any):
            if obj is None:
                return None
            if isinstance(obj, (str, int, float, bool)):
                return obj
            if isinstance(obj, Decimal):
                return str(obj)
            if isinstance(obj, (_dt, date, time)):
                # datetime/time/date -> ISO format
                try:
                    return obj.isoformat()
                except Exception:
                    return str(obj)
            if isinstance(obj, dict):
                return {k: make_json_safe(v) for k, v in obj.items()}
            if isinstance(obj, (list, tuple)):
                return [make_json_safe(v) for v in obj]
            # fallback
            return str(obj)

        safe_response = {
            "mode": os.getenv("OCR_MODE", "ocr"),
            "ocr": make_json_safe(ocr_result),
            "ocr_parsed": make_json_safe(ocr_data),
            "db_check": make_json_safe(db_check),
        }
        return jsonify(safe_response)
    except Exception as exc:
        # Log and return JSON for unexpected errors (prevents HTML traceback pages)
        app.logger.exception("Unhandled exception in /upload")
        return jsonify({"error": f"{type(exc).__name__}: {str(exc)}"}), 500



@app.errorhandler(Exception)
def handle_exception(e):
    # Return JSON for any unhandled exception (helps the frontend avoid parsing HTML)
    app.logger.exception("Unhandled exception: %s", e)
    return jsonify({"error": f"{type(e).__name__}: {str(e)}"}), 500

if __name__ == "__main__":
    import os
    port = int(os.getenv("PORT", 5000))  # Render จะส่งค่า PORT มาให้
    # Run without debug=True so Flask/Werkzeug doesn't show interactive HTML tracebacks
    app.run(host="0.0.0.0", port=port, debug=False)
    
#if __name__ == "__main__":
   # app.run(debug=True)
