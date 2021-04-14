from flask import Flask, request, render_template, abort
from flask_cors import CORS
from data.data import retrieve_all_transaction, get_tokens_list, get_tokens_stats, read_token_stats, save_token_stats, query_bsc


ip_ban_list = ['185.156.72.12']

app = Flask(__name__)

CORS(app)


@app.before_request
def block_method():
    ip = request.environ.get('REMOTE_ADDR')
    if ip in ip_ban_list:
        abort(403)

@app.route('/', methods=['GET'])
def home():
    return "Unauthorized"

@app.route('/transactions', methods=['GET'])
def get_all_transactions():
    return retrieve_all_transaction().to_csv()

@app.route('/transactions/json', methods=['GET'])
def get_all_transactions_json():
    return retrieve_all_transaction().to_json(orient="records")

@app.route('/tokens', methods=['GET'])
def get_all_tokens_list():
    return get_tokens_list()

@app.route('/tokens/stats', methods=['GET'])
def get_tokens_statistics():
    return read_token_stats().to_csv()

@app.route('/tokens/stats/json', methods=['GET'])
def get_tokens_statistics_json():
    return get_tokens_stats().to_json(orient="records")

@app.route('/tokens/stats/update', methods=['GET'])
def save_new_token_stats():
    save_token_stats()
    return {"message": "ok"}

@app.route('/transactions/refresh', methods=['GET'])
def refresh_database():
    query_bsc()
    return retrieve_all_transaction().to_csv()

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5786)
