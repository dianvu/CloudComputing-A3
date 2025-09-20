from flask import Flask, render_template, send_from_directory, request, redirect, url_for, jsonify, make_response, session
import os
import uuid
from datetime import datetime, timezone, timedelta
from typing import List
import boto3
from botocore.exceptions import ClientError
from werkzeug.utils import secure_filename
from dotenv import load_dotenv
import requests
import pg8000.dbapi
from functools import wraps
import ssl


# Load environment variables first
load_dotenv()

# Database configuration - load after .env file
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_NAME = os.environ.get('DB_NAME', 'bankapp')
DB_USER = os.environ.get('DB_USER', 'postgres')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'password')

def create_app() -> Flask:
    app = Flask(__name__, static_folder='static')
    
    # Configure session secret key
    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'default-secret-key-for-development')

    aws_region = os.environ.get("AWS_REGION", "ap-southeast-1")
    s3_bucket = os.environ.get("S3_BUCKET_NAME", "")
    app.config["AWS_REGION"] = aws_region
    app.config["S3_BUCKET_NAME"] = s3_bucket
    s3_client = boto3.client("s3", region_name=aws_region)

    # Connect to RDS
    def get_db_connection():
        # create SSL context and disable certificate validation
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        return pg8000.dbapi.connect(
            host=DB_HOST, 
            database=DB_NAME, 
            user=DB_USER, 
            password=DB_PASSWORD,
            ssl_context=ssl_context)
    
    def login_required(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'user_id' not in session:
                return redirect(url_for('login'))
            return f(*args, **kwargs)
        return decorated_function

    # Check user login
    def get_current_user():
        user_id = session.get('user_id')
        if not user_id:
            return None
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT user_id, email FROM users WHERE user_id = %s", (user_id,))
        user = cur.fetchone()
        cur.close()
        conn.close()
        return {"user_id": user[0], "email": user[1]} if user else None

    # S3 uploading file
    def is_allowed_file(filename: str) -> bool:
        return "." in filename and filename.rsplit(".", 1)[1].lower() == "pdf"

    def generate_statement_key(user_id: str, filename: str) -> str:
        base = os.path.splitext(secure_filename(filename))[0] or "statement"
        ict = timezone(timedelta(hours=7))
        ts = datetime.now(ict).strftime("%Y%m%d%H%M%S")
        unique = uuid.uuid4().hex[:8]
        return f"statements/{user_id}/{base}-{ts}-{unique}.pdf"

    def build_dashboard_presigned_urls(user_id: str, limit: int = 20) -> List[str]:
        prefix = f"dashboard/{user_id}/"
        urls: List[str] = []
        try:
            paginator = s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
                for obj in page.get("Contents", [])[:limit]:
                    key = obj["Key"]
                    try:
                        url = s3_client.generate_presigned_url(
                            ClientMethod="get_object",
                            Params={"Bucket": s3_bucket, "Key": key},
                            ExpiresIn=3600,
                        )
                        urls.append(url)
                    except ClientError:
                        continue
        except ClientError:
            pass
        # Newest first
        return list(reversed(urls))

    # main page
    @app.route("/", methods=["GET"])
    def index():
        user = get_current_user()
        dashboards = []
        if user:
            dashboards = build_dashboard_presigned_urls(user['user_id'])
        return render_template(
            "index.html",
            user=user,
            dashboards=dashboards,
            logged_in=bool(user)
        )
    
    # register
    @app.route("/register", methods=["GET", "POST"])
    def register():
        if request.method == "POST":
            email = request.form.get("email")
            password = request.form.get("password")

            if not email or not password:
                return "Enter email and password", 400

            conn = get_db_connection()
            cur = conn.cursor()
            try:
                user_id = str(uuid.uuid4())
                cur.execute(
                    "INSERT INTO users (user_id, email, password) VALUES (%s, %s, %s)",
                    (user_id, email, password)
                )
                conn.commit()
                session['user_id'] = user_id
                return redirect(url_for('index'))
            except pg8000.dbapi.IntegrityError:
                conn.rollback()
                return "Email already existed", 409
            except Exception as e:
                conn.rollback()
                print(e)
                return "Error", 500
            finally:
                cur.close()
                conn.close()
        return render_template("register.html")
    
    # login
    @app.route("/login", methods=["GET", "POST"])
    def login():
        if request.method == "POST":
            email = request.form.get("email")
            password = request.form.get("password")

            if not email or not password:
                return "Enter email and password", 400

            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute(
                    "SELECT user_id, password FROM users WHERE email = %s",
                    (email,)
                )
                user = cur.fetchone()

                if not user or user[1] != password:
                    return "Sai email hoặc mật khẩu", 401

                user_id = user[0]
                session['user_id'] = user_id
                return redirect(url_for('index'))

            except Exception as e:
                print(e)
                return "Error", 500
            finally:
                cur.close()
                conn.close()
        return render_template("login.html")
    
    # logout
    @app.route("/logout")
    def logout():
        session.pop('user_id', None)
        return redirect(url_for('index'))


    @app.route("/upload", methods=["POST"])
    def upload():
        user = get_current_user()
        if not user:
            return jsonify({"error": "Please login"}), 401

        file = request.files.get("file")
        if file is None or file.filename == "":
            return jsonify({"error": "No file provided"}), 400
        if not is_allowed_file(file.filename):
            return jsonify({"error": "Only PDF files are allowed"}), 400

        key = generate_statement_key(user['user_id'], file.filename)
        try:
            s3_client.upload_fileobj(
                Fileobj=file,
                Bucket=s3_bucket,
                Key=key,
                ExtraArgs={
                    "ContentType": "application/pdf",
                    "Metadata": {"user_id": user['user_id']},
                },
            )
        except ClientError as e:
            return jsonify({"error": str(e)}), 500

        return jsonify({"message": "Uploaded", "s3_key": key, "user_id": user['user_id']}), 200

    @app.route("/dashboards/<user_id>", methods=["GET"])
    def dashboards(user_id: str):
        current_user = get_current_user()
        if not current_user or current_user['user_id'] != user_id:
            return jsonify({"error": "No access"}), 403

        urls = build_dashboard_presigned_urls(user_id)
        return jsonify({"user_id": user_id, "dashboards": urls})

    @app.route("/chat", methods=["POST"])
    @login_required
    def chat():
        try:
            data = request.get_json()
            if not data or 'question' not in data:
                return jsonify({"error": "Missing 'question' in request body"}), 400

            chatbot_url = os.environ.get("CHATBOT_API_URL")
            if not chatbot_url:
                return jsonify({"error": "Chatbot API URL not configured"}), 500
            
            # Get user_id from session, then send to lambda
            user = get_current_user()
            if not user:
                return jsonify({"error": "User not authenticated"}), 401
            
            payload = {
                "question": data["question"],
                "user_id": user["user_id"]
            }

            response = requests.post(
                chatbot_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )

            if response.status_code == 200:
                return jsonify(response.json())
            else:
                return jsonify({"error": "Chatbot service error"}), response.status_code

        except Exception as e:
            return jsonify({"error": f"Internal error: {str(e)}"}), 500

    # Serve static files
    @app.route('/static/<path:filename>')
    def static_files(filename):
        return send_from_directory(app.static_folder, filename)

    return app

application = create_app()

# # for local deployment
# if __name__ == "__main__":
#     port = int(os.environ.get("PORT", "5000"))  # Beanstalk recommends 5000
#     application.run(host="0.0.0.0", port=port, debug=False)  # Disable debug in production