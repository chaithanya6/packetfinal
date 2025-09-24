from flask import Flask, request, jsonify
import os, json

app = Flask(__name__)
STORE = os.environ.get("STORE_FILE", "/data/logs.json")
os.makedirs(os.path.dirname(STORE), exist_ok=True)

@app.route("/persist", methods=["POST"])
def persist():
    event = request.get_json()
    if not event:
        return {"error": "invalid"}, 400
    try:
        with open(STORE, "a") as f:
            f.write(json.dumps(event) + "\n")
        return {"status": "ok"}, 200
    except Exception as e:
        return {"error": str(e)}, 500

@app.route("/health")
def health():
    return {"status":"ok"}, 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6000)
