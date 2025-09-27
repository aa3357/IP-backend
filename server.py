from flask import Flask, jsonify, request
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

@app.route("/api/customers", methods=["GET"])
def get_customers():
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 10))

    customer_id = request.args.get("id")
    first_name = request.args.get("first_name")
    last_name = request.args.get("last_name")

    offset = (page - 1) * per_page

    conn = get_conn()
    cur = conn.cursor()

    # Base query
    query = "SELECT customer_id, first_name, last_name, email FROM customer WHERE 1=1"
    params = []

    if customer_id:
        query += " AND customer_id = %s"
        params.append(customer_id)
    if first_name:
        query += " AND first_name LIKE %s"
        params.append(f"%{first_name}%")
    if last_name:
        query += " AND last_name LIKE %s"
        params.append(f"%{last_name}%")

    query += " LIMIT %s OFFSET %s"
    params.extend([per_page, offset])

    cur.execute(query, params)
    customers = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify(customers)

#Actor's Details 
@app.route("/api/actors/<int:actor_id>", methods=["GET"])
def get_actor_details(actor_id):
    conn = get_conn()
    cursor = conn.cursor()

    # 1. Get actor details
    cursor.execute(
        """
        SELECT actor_id, first_name, last_name
        FROM actor
        WHERE actor_id = %s
        """,
        (actor_id,),
    )
    actor = cursor.fetchone()

    if not actor:
        return jsonify({"error": "Actor not found"}), 404

    # 2. Get top 5 rented films for this actor
    cursor.execute(
        """
        SELECT f.film_id, f.title, COUNT(r.rental_id) AS rental_count
        FROM film f
        INNER JOIN film_actor fa ON f.film_id = fa.film_id
        INNER JOIN inventory i ON f.film_id = i.film_id
        INNER JOIN rental r ON i.inventory_id = r.inventory_id
        WHERE fa.actor_id = %s
        GROUP BY f.film_id, f.title
        ORDER BY rental_count DESC
        LIMIT 5
        """,
        (actor_id,),
    )
    films = cursor.fetchall()

    cursor.close()

    return jsonify({
        "actor": actor,
        "top_films": films
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)
