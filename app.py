# app.py
from flask import Flask, render_template, request, jsonify
from ocr_utils import process_slip
import os

app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route('/')
def index():
    return render_template('review.html')

@app.route('/upload', methods=['POST'])
def upload():
    file = request.files['file']
    if not file:
        return jsonify({"error": "no file"}), 400
    path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(path)

    result = process_slip(path)
    return jsonify(result)

if __name__ == "__main__":
    import os
    port = int(os.getenv("PORT", 5000))  # Render จะส่งค่า PORT มาให้
    app.run(host="0.0.0.0", port=port, debug=True)
    
#if __name__ == "__main__":
   # app.run(debug=True)
