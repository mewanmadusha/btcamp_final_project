import os
import hashlib
import datetime
from flask import Flask, request, jsonify, redirect, render_template, url_for, session
from flask_jwt_extended import (
    JWTManager,
    create_access_token,
    jwt_required,
    get_jwt_identity,
)
from pymongo import MongoClient
import snowflake.connector
import logging
import pandas as pd
import plotly
import plotly.express as px
import json
from flask_caching import Cache


app = Flask(__name__)
app.secret_key = "12345678"

# JWT session manegement
jwt = JWTManager(app)
app.config["JWT_SECRET_KEY"] = "12343242"
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = datetime.timedelta(days=1)

# Task 8. Implement caching in your API for frequently requested data.
# Redis cache
cache = Cache(
    app, config={"CACHE_TYPE": "redis", "CACHE_REDIS_URL": "redis://localhost:6379"}
)

# Task 4. API Development with Python:
# Connecting mongo DB
client = MongoClient("mongodb+srv://mewan:admin@clusterdemo.ueewbek.mongodb.net/")
db = client["finalproject"]
users_collection = db["users"]


# login route
@app.route("/login", methods=["GET"])
def login_page():
    return render_template("login.html")


# register route
@app.route("/api/v1/users", methods=["POST"])
def register():
    new_user = request.get_json()  # store the json body request
    new_user["password"] = hashlib.sha256(
        new_user["password"].encode("utf-8")
    ).hexdigest()  # encrpt password
    doc = users_collection.find_one(
        {"username": new_user["username"]}
    )  # check if user exist
    if not doc:
        users_collection.insert_one(new_user)
        return jsonify({"msg": "User created successfully"}), 201
    else:
        return jsonify({"msg": "Username already exists"}), 409


# login route implementation
@app.route("/api/v1/login", methods=["POST"])
def login():
    login_details = request.get_json()  # store the json body request
    user_from_db = users_collection.find_one(
        {"username": login_details["username"]}
    )  # search for user in database

    if user_from_db:
        encrpted_password = hashlib.sha256(
            login_details["password"].encode("utf-8")
        ).hexdigest()
        if encrpted_password == user_from_db["password"]:
            access_token = create_access_token(
                identity=user_from_db["username"]
            )  # create jwt token
            return jsonify(access_token=access_token), 200

    return jsonify({"msg": "The username or password is incorrect"}), 401


# get profile data
@app.route("/api/v1/user", methods=["GET"])
@jwt_required
def profile():
    current_user = get_jwt_identity()  # Get the identity of the current user
    user_from_db = users_collection.find_one({"username": current_user})
    if user_from_db:
        del (
            user_from_db["_id"],
            user_from_db["password"],
        )  # delete data we don't want to return
        return jsonify({"profile": user_from_db}), 200
    else:
        return jsonify({"msg": "Profile not found"}), 404


# opening route ridirect to login
@app.route("/")
def index():
    return redirect("/login")


# render dashboard via ajax call
@app.route("/dashboard")
def dashboard():
    return render_template("dashboard.html")


@app.route("/logout")
def logout():
    # Clear the user session
    session.clear()
    # Redirect to the login page, or another appropriate page
    return redirect(url_for("login_page"))


# Snowflake connection
def get_snowflake_connection():
    return snowflake.connector.connect(
        user="mewanmadhusha",
        password="/Applejg8269",
        account="whvtigk-as57496",
        warehouse="COMPUTE_WH",
    )


# get datawarehouse connection
# this method bind to documentload call in javascript, due to high call rate
# I have changed the storing mechanism to session storage save, untill logout datawill be there
@app.route("/data-warehouses")
def data_warehouses():
    conn = get_snowflake_connection()
    logging.info("success")
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        return {"databases": databases}
    finally:
        cursor.close()
        conn.close()


# execute dynamic sql queries
@app.route("/execute-query", methods=["POST"])
def execute_query():
    data = request.get_json()
    database = data["warehouse"]
    schema = data["schema"]
    sql_query = data["sqlQuery"]
    conn = get_snowflake_connection()
    conn.cursor().execute(f"USE DATABASE {database}")
    conn.cursor().execute(f"USE SCHEMA {schema}")

    dataret = pd.read_sql(sql_query, conn)
    json_result = dataret.to_json(orient="records")
    # Connect to Snowflake and execute the query
    # Return the results
    return json_result


# get all the tables from datawarehouse
def get_tables():
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE COVID19_EPIDEMIOLOGICAL_DATA")
        cursor.execute(f"USE SCHEMA PUBLIC")
        cursor.execute("SHOW TABLES")
        tables = [row[1] for row in cursor.fetchall()]
        return tables
    finally:
        cursor.close()
        conn.close()


# once get the table selection dynamically load the table headers
def get_table_headers(table_name):
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE COVID19_EPIDEMIOLOGICAL_DATA")
        cursor.execute(f"USE SCHEMA PUBLIC")
        cursor.execute(f"DESCRIBE TABLE {table_name}")
        headers = [row[0] for row in cursor.fetchall()]
        return headers
    finally:
        cursor.close()
        conn.close()


@app.route("/get-tables")
def get_tables_route():
    tables = get_tables()
    return jsonify(tables)


@app.route("/get-headers/<table_name>")
@cache.cached(timeout=100000)
def get_headers_route(table_name):
    headers = get_table_headers(table_name)
    return jsonify(headers)


# visualize data
@app.route("/visualize", methods=["POST"])
def visualize():
    content = request.json
    table_name = content["table_name"]
    column_name1 = content["column_name1"]
    column_name2 = content["column_name2"]

    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute(f"USE DATABASE COVID19_EPIDEMIOLOGICAL_DATA")
    cursor.execute(f"USE SCHEMA PUBLIC")
    try:
        query = f"SELECT {column_name1}, SUM({column_name2}) AS DEATHS FROM {table_name} GROUP BY {column_name1}"
        cursor.execute(query)
        rows = cursor.fetchall()
        # Convert rows to a format that can be easily converted to JSON
        result = [{"column1": row[0], "column2": row[1]} for row in rows]
        return jsonify(result)
    finally:
        cursor.close()
        conn.close()

# bonus task
@app.route('/save-comment', methods=['POST'])
def save_comment():
    comment_data = {
        'comment': request.form['comment'],
        'selected_table': request.form['selected_table'],
        'header1': request.form['header1'],
        'header2': request.form['header2'],
        'commented_at': datetime.datetime.utcnow()
    }

    # Insert the comment data into MongoDB
    db.comments_collection.insert_one(comment_data)
    return redirect(url_for('dashboard'))

# get comments if there any
@app.route('/get-comments', methods=['GET'])
def get_comments():
    table_name = request.args.get('table_name')
    header1 = request.args.get('header1')
    header2 = request.args.get('header2')

    comments = list(db.comments_collection.find(
        {'selected_table': table_name, 'header1': header1, 'header2': header2},
        {'_id': 0, 'comment': 1, 'commented_at' : 1}
    ))

    return jsonify(comments)

if __name__ == "__main__":
    app.run(debug=True)
