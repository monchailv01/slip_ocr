# ocr_utils.py
import os
import base64
import json
import re
import datetime
import mimetypes
from dotenv import load_dotenv
from openai import OpenAI

# ============= Utilities =============

TH_MONTH = {
    "ม.ค.":1,"ก.พ.":2,"มี.ค.":3,"เม.ย.":4,"พ.ค.":5,"มิ.ย.":6,
    "ก.ค.":7,"ส.ค.":8,"ก.ย.":9,"ต.ค.":10,"พ.ย.":11,"ธ.ค.":12,
    "ม.ค":1,"ก.พ":2,"มี.ค":3,"เม.ย":4,"พ.ค":5,"มิ.ย":6,
    "ก.ค":7,"ส.ค":8,"ก.ย":9,"ต.ค":10,"พ.ย":11,"ธ.ค":12,
}

def _safe_json(s: str) -> dict | None:
    """ลบ code fence แล้วลอง parse เป็น JSON"""
    s = s.strip()
    s = re.sub(r"^```(json)?", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"```$", "", s).strip()
    try:
        return json.loads(s)
    except Exception:
        return None

def _norm_amount(s: str | None) -> str | None:
    if not s:
        return None
    s = str(s)
    s = s.replace("บาท","").replace("THB","").replace("฿","")
    s = s.replace(",","").strip()
    m = re.search(r"\d+(?:\.\d+)?", s)
    return f"{float(m.group()):.2f}" if m else None

def _norm_bank(s: str | None) -> str | None:
    if not s:
        return None
    t = s.lower()
    if "กสิกร" in t or "k+" in t or "kbank" in t: return "KBank"
    if "ไทยพาณิชย์" in t or "scb" in t: return "SCB"
    if "กรุงไทย" in t or "ktb" in t: return "KTB"
    if "กรุงเทพ" in t or "bbl" in t: return "BBL"
    if "กรุงศรี" in t or "bay" in t: return "BAY"
    if "ออมสิน" in t or "gsb" in t: return "GSB"
    if "ทหารไทย" in t or "ttb" in t: return "TTB"
    if "uob" in t: return "UOB"
    if "cimb" in t: return "CIMB"
    return s

def _norm_date_th(s: str | None) -> str | None:
    if not s:
        return None
    s = s.replace("น.", "").strip()
    # 21 ต.ค. 68 12:16 / 21 ต.ค. 2568 12:16
    m = re.search(r"(\d{1,2})\s+([ก-힣\.]+)\s+(\d{2,4})\s+(\d{1,2}):(\d{2})", s)
    if m:
        d, mon, y, hh, mm = m.groups()
        mon_num = TH_MONTH.get(mon, None)
        if mon_num:
            y = int(y)
            if y < 100: y += 2000
            if y > 2400: y -= 543  # พ.ศ. → ค.ศ.
            try:
                dt = datetime.datetime(int(y), mon_num, int(d), int(hh), int(mm))
                return dt.strftime("%Y-%m-%d %H:%M")
            except ValueError:
                pass
    # fallback dd/mm/yyyy hh:mm
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{2,4})\s+(\d{1,2}):(\d{2})", s)
    if m:
        d,mn,y,hh,mm = m.groups()
        y = int(y)
        if y < 100: y += 2000
        if y > 2400: y -= 543
        try:
            dt = datetime.datetime(y, int(mn), int(d), int(hh), int(mm))
            return dt.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            return None
    return None

# ทำความสะอาดชื่อ (ไทย/อังกฤษ) และ normalize คำนำหน้าชื่อ
HONORIFICS = {
    "น.ส.":"น.ส.", "น.ส":"น.ส.", "นางสาว":"นางสาว", "นาง":"นาง", "นาย":"นาย",
    "ด.ช.":"ด.ช.", "ด.ญ.":"ด.ญ.", "mr":"Mr.", "mrs":"Mrs.", "ms":"Ms.", "miss":"Miss"
}
def _clean_name(name: str | None) -> str | None:
    if not name:
        return None
    s = str(name)
    # ตัดอักขระแปลก ๆ เหลือไทย/อังกฤษ/จุด/เว้นวรรค/ขีด
    s = re.sub(r"[^A-Za-zก-๙\.\-\s]", " ", s)
    # ยุบช่องว่างซ้ำ
    s = re.sub(r"\s{2,}", " ", s).strip()

    # แก้จุดซ้ำ "น.ส.." → "น.ส."
    s = re.sub(r"\.{2,}", ".", s)

    # ปรับคำนำหน้า
    tokens = s.split()
    if tokens:
        first = tokens[0].lower()
        norm = HONORIFICS.get(first, HONORIFICS.get(first.replace(".", ""), None))
        if norm:
            tokens[0] = norm
            s = " ".join(tokens)

    # ถ้าสั้นเกินไปให้คืนค่าว่าง
    if len(s) < 2:
        return ""
    return s

# ============= Load env & OpenAI client =============
load_dotenv()
API_KEY = os.getenv("OPENAI_API_KEY")
OCR_MODE = os.getenv("OCR_MODE", "tesseract")
client = OpenAI(api_key=API_KEY) if API_KEY else None

# ============= LLM Flow =============
def process_slip_llm(image_path: str):
    """
    เรียก GPT-4o mini ด้วย data URL (base64) ให้ตอบเป็น JSON แท้
    และ normalize ฟิลด์สำคัญ + ทำความสะอาดชื่อคนโอน/คนรับ
    """
    if not os.path.isfile(image_path):
        return {"error": f"image not found: {image_path}"}
    if not client:
        return {"error": "OPENAI_API_KEY not found. Set it in .env"}

    mime, _ = mimetypes.guess_type(image_path)
    if mime is None: mime = "image/jpeg"
    with open(image_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")
    data_url = f"data:{mime};base64,{b64}"

    prompt = (
        "คุณคือระบบวิเคราะห์สลิปโอนเงินของธนาคารไทย "
        "ตอบกลับเป็น JSON เท่านั้น (ไม่ใส่โค้ดบล็อก/คำอธิบายอื่น) "
        'รูปแบบคีย์: {'
        '"bank":"", "date":"", "amount":"", "account":"", "transaction_id":"", '
        '"sender_name":"", "recipient_name":""'
        '} '
        "หากไม่พบคีย์ใดให้ใส่ค่าว่าง \"\""
    )

    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": data_url}},
                ],
            }],
            temperature=0,
            max_tokens=600,
            response_format={"type": "json_object"},
        )
        raw_text = resp.choices[0].message.content.strip()
        data = _safe_json(raw_text) or {}

        bank       = _norm_bank(data.get("bank", ""))
        date_iso   = _norm_date_th(data.get("date", ""))
        amount     = _norm_amount(data.get("amount", ""))
        account    = data.get("account", "") or data.get("to_account", "")
        txid       = data.get("transaction_id", "") or data.get("ref", "")
        sender     = _clean_name(data.get("sender_name", ""))
        recipient  = _clean_name(data.get("recipient_name", ""))

        result = {
            "bank": bank or "",
            "date": date_iso or "",
            "amount": amount or "",
            "account": account or "",
            "transaction_id": txid or "",
            "sender_name": sender or "",
            "recipient_name": recipient or "",
            "_llm_raw": raw_text,  # เก็บไว้ให้ฝั่ง app.py ทำ log ภายใน (ไม่โชว์หน้าเว็บ)
        }
        # ลบคีย์ที่ว่างออกไป
        return {k: v for k, v in result.items() if v != ""}

    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}

# ============= Fallback OCR (optional) =============
def process_slip_ocr(image_path: str):
    """สำรอง: OCR ปกติด้วย pytesseract (ไม่แม่นเท่า LLM)"""
    try:
        import pytesseract
        from PIL import Image
        text = pytesseract.image_to_string(Image.open(image_path), lang="tha+eng")
        return {"mode": "ocr", "raw_text": text}
    except Exception as e:
        return {"error": str(e)}

# ============= Dispatcher =============
def process_slip(image_path: str):
    if os.getenv("OCR_MODE", OCR_MODE).lower() == "llm":
        return process_slip_llm(image_path)
    return process_slip_ocr(image_path)
