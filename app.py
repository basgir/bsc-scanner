from flask import Flask, request, render_template
from flask_cors import CORS
from data.data import retrieve_all_transaction, get_tokens_list, get_tokens_stats
from data.data import query_bsc

app = Flask(__name__)

CORS(app)

@app.route('/', methods=['GET'])
def home():
    return "Unauthorized"

@app.route('/transactions', methods=['GET'])
def get_all_transactions():
    return retrieve_all_transaction().to_json(orient="records")

@app.route('/tokens', methods=['GET'])
def get_all_tokens_list():
    return get_tokens_list()

@app.route('/tokens/stats', methods=['GET'])
def get_tokens_statistics():
    return get_tokens_stats().to_json(orient="records")

@app.route('/transactions/refresh', methods=['GET'])
def refresh_database():
    query_bsc()
    return retrieve_all_transaction().to_json(orient="index")

if __name__ == "__main__":
    app.run(host='0.0.0.0')