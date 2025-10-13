from flask import Flask, jsonify, request
from flask_cors import CORS
from db import get_conn

app = Flask(__name__)
CORS(app)


#Feature 1 top 5 films
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

#Feature 2 film details
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

#Feature 4 view top 5 actors
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


#Feature 5,6 view and search customers
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

#Feature 4: Actor Details
@app.route("/api/actors/<int:actor_id>", methods=["GET"])
def get_actor_details(actor_id):
    conn = get_conn()
    cursor = conn.cursor()

    #actor details
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

    #top 5 rented films for this actor
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


#Feature 7
@app.route('/api/customers', methods=['POST'])
def add_customer():
    data = request.get_json()
    first_name = data.get('first_name')
    last_name = data.get('last_name')
    email = data.get('email')
    address_id = data.get('address_id', 1)
    store_id = data.get('store_id', 1)
    active = 1

    conn = get_conn()
    cur = conn.cursor()
    sql = """
        INSERT INTO customer (store_id, first_name, last_name, email, address_id, active, create_date)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
    """
    cur.execute(sql, (store_id, first_name, last_name, email, address_id, active))
    conn.commit()

    new_id = cur.lastrowid
    cur.close()
    conn.close()

    return jsonify({
        "customer_id": new_id,
        "first_name": first_name,
        "last_name": last_name,
        "email": email
    }), 201

#Feature 8: Update customer
@app.route('/api/customers/<int:customer_id>', methods=['PUT'])
def update_customer(customer_id):
    data = request.get_json()
    first_name = data.get('first_name')
    last_name = data.get('last_name')
    email = data.get('email')

    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("SELECT customer_id FROM customer WHERE customer_id = %s", (customer_id,))
    if not cur.fetchone():
        cur.close()
        conn.close()
        return jsonify({"error": "Customer not found"}), 404
    

    sql = """
        UPDATE customer 
        SET first_name = %s, last_name = %s, email = %s
        WHERE customer_id = %s
    """
    cur.execute(sql, (first_name, last_name, email, customer_id))
    conn.commit()
    
    cur.close()
    conn.close()

    return jsonify({
        "customer_id": customer_id,
        "first_name": first_name,
        "last_name": last_name,
        "email": email
    }), 200

#Feature 9: Delete customer
@app.route('/api/customers/<int:customer_id>', methods=['DELETE'])
def delete_customer(customer_id):
    conn = get_conn()
    cur = conn.cursor()
    

    cur.execute("SELECT customer_id, first_name, last_name FROM customer WHERE customer_id = %s", (customer_id,))
    customer = cur.fetchone()
    if not customer:
        cur.close()
        conn.close()
        return jsonify({"error": "Customer not found"}), 404
    

    cur.execute("DELETE FROM customer WHERE customer_id = %s", (customer_id,))
    conn.commit()
    
    cur.close()
    conn.close()

    return jsonify({
        "message": f"Customer {customer[1]} {customer[2]} has been deleted successfully",
        "customer_id": customer_id
    }), 200

#Feature 10: Get customer details and rental history
@app.route('/api/customers/<int:customer_id>/details', methods=['GET'])
def get_customer_details(customer_id):
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT customer_id, first_name, last_name, email, 
               address_id, active, create_date
        FROM customer 
        WHERE customer_id = %s
    """, (customer_id,))
    customer = cur.fetchone()
    
    if not customer:
        cur.close()
        conn.close()
        return jsonify({"error": "Customer not found"}), 404
    
    cur.execute("""
        SELECT r.rental_id, f.title, f.rental_rate, r.rental_date, r.return_date,
               CASE 
                   WHEN r.return_date IS NULL THEN 'Currently Rented'
                   ELSE 'Returned'
               END as status,
               DATEDIFF(COALESCE(r.return_date, NOW()), r.rental_date) as days_rented
        FROM rental r
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN film f ON i.film_id = f.film_id
        WHERE r.customer_id = %s
        ORDER BY r.rental_date DESC
    """, (customer_id,))
    rentals = cur.fetchall()
    

    cur.execute("""
        SELECT 
            COUNT(*) as total_rentals,
            COUNT(CASE WHEN return_date IS NULL THEN 1 END) as current_rentals,
            COUNT(CASE WHEN return_date IS NOT NULL THEN 1 END) as completed_rentals,
            SUM(CASE WHEN return_date IS NOT NULL THEN f.rental_rate ELSE 0 END) as total_spent
        FROM rental r
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN film f ON i.film_id = f.film_id
        WHERE r.customer_id = %s
    """, (customer_id,))
    stats = cur.fetchone()
    
    cur.close()
    conn.close()
    
    return jsonify({
        "customer": customer,
        "rentals": rentals,
        "statistics": stats
    })

#Feature 11: Return movie
@app.route('/api/rentals/<int:rental_id>/return', methods=['PUT'])
def return_rental(rental_id):
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("SELECT rental_id FROM rental WHERE rental_id = %s", (rental_id,))
    rental = cur.fetchone()
    if not rental:
        cur.close()
        conn.close()
        return jsonify({"error": "Rental not found"}), 404

    cur.execute("UPDATE rental SET return_date = NOW() WHERE rental_id = %s AND return_date IS NULL", (rental_id,))
    conn.commit()
    
    cur.close()
    conn.close()

    return jsonify({"message": f"Rental {rental_id} marked as returned"}), 200



if __name__ == '__main__':
    app.run(debug=True, port=5000)
