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

@app.route("/api/actors/top", methods=["GET"])
def top_actors():
    sql = """
        SELECT a.actor_id, CONCAT(a.first_name, ' ', a.last_name) AS name, COUNT(*) AS film_count
        FROM actor a
        JOIN film_actor fa ON a.actor_id = fa.actor_id
        JOIN film f ON fa.film_id = f.film_id
        JOIN inventory i ON f.film_id = i.film_id
        JOIN rental r ON i.inventory_id = r.inventory_id
        GROUP BY a.actor_id, a.first_name, a.last_name
        ORDER BY film_count DESC
        LIMIT 5;
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return jsonify(rows)
    finally:
        conn.close()

@app.route("/api/categories/top", methods=["GET"])
def top_categories():
    sql = """
        SELECT c.name AS category, COUNT(*) AS rental_count
        FROM rental r
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN film f ON i.film_id = f.film_id
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        GROUP BY c.name
        ORDER BY rental_count DESC
        LIMIT 5;
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return jsonify(rows)
    finally:
        conn.close()

@app.route("/api/customers/top", methods=["GET"])
def top_customers():
    sql = """
        SELECT c.customer_id, c.first_name, c.last_name, COUNT(*) AS rental_count
        FROM rental r
        JOIN customer c ON r.customer_id = c.customer_id
        GROUP BY c.customer_id, c.first_name, c.last_name
        ORDER BY rental_count DESC
        LIMIT 5;
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return jsonify(rows)
    finally:
        conn.close()


@app.route("/api/films/revenue", methods=["GET"])
def top_films_revenue():
    sql = """
        SELECT f.film_id, f.title, SUM(p.amount) AS revenue
        FROM payment p
        JOIN rental r ON p.rental_id = r.rental_id
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN film f ON i.film_id = f.film_id
        GROUP BY f.film_id, f.title
        ORDER BY revenue DESC
        LIMIT 5;
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return jsonify(rows)
    finally:
        conn.close()

if __name__ == '__main__':
    app.run(debug=True, port=5000)
