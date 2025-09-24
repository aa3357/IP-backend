from flask import Flask, jsonify
from db import get_conn
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

@app.route('/')
def home():
    return "Home"

@app.route('/top-films', methods=['GET'])
def top_films():
    sql = """select f.film_id, f.title, c.name, count(*) from rental r join inventory i on r.inventory_id=i.inventory_id join film f on i.film_id=f.film_id join film_category fc on f.film_id=fc.film_id join category c on fc.category_id=c.category_id group by f.film_id, f.title, c.name order by count(*) desc limit 5;"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        return jsonify(rows)
    finally:
        conn.close()


if __name__ == '__main__':
    app.run(debug=True, port=5000)

