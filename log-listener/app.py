from flask import Flask, request, jsonify
import os, requests, time

app = Flask(__name__)
COLLECTOR_URL = os.environ.get("COLLECTOR_URL")  # e.g. http://EC2_PUBLIC/api/collect

if not COLLECTOR_URL:
    print("WARNING: COLLECTOR_URL env not set. Set COLLECTOR_URL when running this container.")

def forward(event):
    retries = int(os.environ.get("RETRIES", "3"))
    backoff = float(os.environ.get("BACKOFF", "1"))
    for i in range(retries):
        try:
            r = requests.post(COLLECTOR_URL, json=event, timeout=5)
            return r.status_code, r.text
        except Exception as e:
            time.sleep(backoff)
            backoff *= 2
    return None, "failed"

@app.route("/health")
def health():
    return {"status":"ok"}, 200

@app.route("/logs", methods=["POST"])
def receive():
    event = request.get_json()
    if not event:
        return {"error":"invalid"}, 400
    status, info = forward(event)
    if status:
        return {"forwarded": True, "status_code": status}, 200
    else:
        return {"forwarded": False, "error": info}, 502

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
