from flask import Flask, jsonify
from flask_cors import CORS
from db import get_conn

app = Flask(__name__)
CORS(app)


# --- Feature 1: Top 5 rented films ---
@app.route('/api/films/top', methods=['GET'])
def top_films():
    sql = """
        SELECT f.film_id, f.title, c.name AS category, COUNT(*) AS rentals
        FROM rental r
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN film f ON i.film_id = f.film_id
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        GROUP BY f.film_id, f.title, c.name
        ORDER BY rentals DESC
        LIMIT 5;
    """
    conn = get_conn()
    try:
        cur = conn.cursor()  # correct for mysql-connector-python
        cur.execute(sql)
        rows = cur.fetchall()  # list of dicts
        cur.close()
        return jsonify(rows)
    finally:
        conn.close()


@app.route("/api/films/<int:film_id>", methods=["GET"])
def get_film_details(film_id):
    sql = """
        SELECT film_id AS film_id, title, description, release_year,
               rating, rental_duration, rental_rate, length, replacement_cost
        FROM film
        WHERE film_id = %s;
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, (film_id,))
        row = cur.fetchone()  # just one film
        cur.close()
        if row:
            return jsonify(row)
        else:
            return jsonify({"error": "Film not found"}), 404
    finally:
        conn.close()



if __name__ == '__main__':
    app.run(debug=True, port=5000)
