import boto3
import os
import json
import urllib.parse
import pg8000.dbapi
from datetime import date, datetime
import traceback
import tempfile
import ssl

# Library imports with fallback handling
try:
    import pdfplumber
    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False

try:
    from pypdf import PdfReader
    PYPDF_AVAILABLE = True
except ImportError:
    PYPDF_AVAILABLE = False

# Initialize AWS clients
s3_client = boto3.client('s3')
bedrock_runtime_client = boto3.client('bedrock-runtime')
lambda_client = boto3.client('lambda')

try:
    textract_client = boto3.client('textract')
    TEXTRACT_AVAILABLE = True
except Exception:
    textract_client = None
    TEXTRACT_AVAILABLE = False

# Database configuration
DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_PORT = os.environ['DB_PORT']

def get_db_connection():
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    return pg8000.dbapi.connect(
        host=DB_HOST, 
        database=DB_NAME, 
        user=DB_USER, 
        password=DB_PASSWORD,
        port=DB_PORT,
        ssl_context=ssl_context)

def trigger_dashboard_creation(statement_id):
    try:
        payload = {
            "trigger_source": "bank_extract",
            "statement_id": statement_id
        }
        lambda_client.invoke(
            FunctionName='VisualizeDashboardtoS3',
            InvocationType='Event',  # async
            Payload=json.dumps(payload)
        )
        print(f"Triggered dashboard creation for statement_id={statement_id}")
    except Exception as e:
        print(f"Error triggering dashboard: {str(e)}")
        traceback.print_exc()

def check_user_exists(conn, user_id):
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT user_id, email FROM users WHERE user_id = %s", (user_id,))
        existing_user = cursor.fetchone()
        if existing_user:
            return existing_user
        else:
            raise ValueError(f"User with ID {user_id} does not exist. Please register first.")
    except Exception as e:
        conn.rollback()
        raise e

def extract_with_pdfplumber(file_path):
    if not PDFPLUMBER_AVAILABLE:
        return ""
    try:
        with pdfplumber.open(file_path) as pdf:
            text = ""
            max_pages = min(len(pdf.pages), 20)
            for page in pdf.pages[:max_pages]:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
                del page
            return text.strip()
    except Exception:
        return ""

def extract_with_pypdf(file_path):
    if not PYPDF_AVAILABLE:
        return ""
    try:
        reader = PdfReader(file_path, strict=False)
        text = ""
        max_pages = min(len(reader.pages), 20)
        for i in range(max_pages):
            try:
                page_text = reader.pages[i].extract_text()
                if page_text:
                    text += page_text + "\n"
                del reader.pages[i]
            except Exception:
                continue
        return text.strip()
    except Exception:
        return ""

def extract_with_textract(bucket, key):
    if not TEXTRACT_AVAILABLE:
        return ""
    try:
        response = textract_client.detect_document_text(
            Document={'S3Object': {'Bucket': bucket, 'Name': key}})
        text = ""
        for item in response.get('Blocks', []):
            if item['BlockType'] == 'LINE':
                text += item.get('Text', '') + '\n'
        return text.strip()
    except Exception:
        return ""

def textract_from_s3(bucket, key):
    temp_file_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
            temp_file_path = temp_file.name

        s3_client.download_file(bucket, key, temp_file_path)
        file_size = os.path.getsize(temp_file_path)
        
        if file_size == 0 or file_size > 20 * 1024 * 1024:
            return extract_with_textract(bucket, key) if file_size > 0 else ""

        text = extract_with_pdfplumber(temp_file_path)
        if not text or len(text.strip()) < 50:
            text = extract_with_pypdf(temp_file_path)
        if not text or len(text.strip()) < 50:
            text = extract_with_textract(bucket, key)

        return text if text else ""
    except Exception:
        return ""
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except Exception:
                pass

def banktract_from_text(text):
    if not text or len(text.strip()) < 10:
        return {"transactions": [], "analysis": {"total_income": 0, "total_expense": 0, "net_amount": 0, "categories": []}}
    
    model_id = os.environ.get('BEDROCK_MODEL_ID', 'anthropic.claude-3-haiku-20240307-v1:0')
    if len(text) > 30000:
        text = text[:30000] + "\n... (truncated)"
    
    prompt = f"""
    Analyze the following bank statement transaction data and extract the information in JSON format.
    
    Transaction data: 
    {text}
    
    Extract the transactions and return a JSON object with the following exact format:
    {{
        "transactions": [
            {{
                "date": "YYYY-MM-DD",
                "description": "Transaction description",
                "amount": -123.45,
                "category": "food"
            }}
        ],
        "analysis": {{
            "total_income": 1000.00,
            "total_expense": -500.00,
            "net_amount": 500.00,
            "categories": ["food", "transport"],
            "categories_amount": {{
                "food": {{
                    "total_amount": -100.00,
                    "transaction_count": 5,
                    "type": "expense"
                }}
            }}
        }}
    }}
    
    Important rules:
    - Use negative amounts for expenses and positive for income
    - Use YYYY-MM-DD format for dates
    - Return only valid JSON, no markdown or extra text
    - If you cannot parse dates, use "2024-01-01"
    - Categorize transactions appropriately (food, transport, salary, entertainment, etc.)
    """
    
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4096,
        "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
        "temperature": 0.1
    })

    try:
        response = bedrock_runtime_client.invoke_model(body=body, modelId=model_id)
        response_body = json.loads(response['body'].read())
        analysis_result_text = response_body['content'][0]['text'].strip()
        
        if analysis_result_text.startswith('```json'):
            analysis_result_text = analysis_result_text[7:]
        elif analysis_result_text.startswith('```'):
            analysis_result_text = analysis_result_text[3:]
        if analysis_result_text.endswith('```'):
            analysis_result_text = analysis_result_text[:-3]
        
        start_idx = analysis_result_text.find('{')
        end_idx = analysis_result_text.rfind('}') + 1
        
        if start_idx != -1 and end_idx != -1:
            json_content = analysis_result_text[start_idx:end_idx]
            return json.loads(json_content)
        else:
            return json.loads(analysis_result_text)
    except Exception:
        return {"transactions": [], "analysis": {"total_income": 0, "total_expense": 0, "net_amount": 0, "categories": [], "categories_amount": {}}}

def save_analysis_to_database(conn, statement_id, analysis_result, user_id):
    cursor = conn.cursor()
    try:
        transactions = analysis_result.get('transactions', [])
        summary = analysis_result.get('analysis', {})
        
        if transactions:
            insert_transaction_query = """
            INSERT INTO transactions (statement_id, transaction_date, description, amount, category, user_id)
            VALUES (%s, %s, %s, %s, %s, %s);
            """
            
            successful_transactions = 0
            for trans in transactions:
                try:
                    trans_date = None
                    if trans.get('date'):
                        if isinstance(trans['date'], str):
                            try:
                                trans_date = datetime.strptime(trans['date'], '%Y-%m-%d').date()
                            except ValueError:
                                for date_format in ['%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d']:
                                    try:
                                        trans_date = datetime.strptime(trans['date'], date_format).date()
                                        break
                                    except ValueError:
                                        continue
                                if not trans_date:
                                    trans_date = date.today()
                        else:
                            trans_date = trans['date']
                    else:
                        trans_date = date.today()
                    
                    try:
                        amount = float(trans.get('amount', 0.0))
                    except (ValueError, TypeError):
                        amount = 0.0
                    
                    description = str(trans.get('description', ''))[:500]
                    category = str(trans.get('category', 'Other'))[:100]
                    
                    cursor.execute(insert_transaction_query, (statement_id, trans_date, description, amount, category, user_id))
                    successful_transactions += 1
                except Exception:
                    continue
        
        enhanced_summary = {**summary, "transaction_count": len(transactions), 
                          "categories": list(set(trans.get('category', 'Other') for trans in transactions if trans.get('category')))}
        
        cursor.execute("UPDATE bank_statements SET analysis_summary = %s WHERE statement_id = %s;", 
                      (json.dumps(enhanced_summary), statement_id))
        conn.commit()
        
        return len(transactions)
    except Exception as e:
        conn.rollback()
        raise e

def process_s3_upload(bucket, key):
    conn = None
    statement_id = None
    
    try:
        # Determine user_id from metadata or path
        user_id = None

        # Try metadata first
        try:
            response = s3_client.head_object(Bucket=bucket, Key=key)
            user_id = response['Metadata'].get('user-id')
            print(f"Found user_id from metadata: {user_id}") if user_id else None
        except Exception as e:
            print(f"Failed to read metadata: {str(e)}")

        # Fallback to path parsing
        if not user_id:
            path_parts = key.split('/')
            user_id = path_parts[1] if len(path_parts) >= 2 and path_parts[0] == 'statements' else None
            print(f"Found user_id from path: {user_id}") if user_id else None

        # Final fallback
        user_id = user_id or "default_user"
        print(f"Using user_id: {user_id}")

        # Database operations
        conn = get_db_connection()
        check_user_exists(conn, user_id)
        cursor = conn.cursor()

        # Create statement record
        cursor.execute("""
        INSERT INTO bank_statements (user_id, s3_key, process_status, created_at)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (s3_key)
        DO UPDATE SET process_status = EXCLUDED.process_status, created_at = CURRENT_TIMESTAMP
        RETURNING statement_id;
        """, (user_id, key, 'PROCESSING'))

        statement_id = cursor.fetchone()[0]
        conn.commit()

        # Extract and analyze
        extracted_text = textract_from_s3(bucket, key)
        if not extracted_text or len(extracted_text.strip()) < 10:
            raise ValueError("Could not extract text from PDF")

        analysis_result = banktract_from_text(extracted_text)
        transaction_count = save_analysis_to_database(conn, statement_id, analysis_result, user_id)

        # Update status and trigger dashboard
        cursor.execute("UPDATE bank_statements SET process_status = 'COMPLETED' WHERE statement_id = %s;", (statement_id,))
        conn.commit()

        trigger_dashboard_creation(statement_id)

    except Exception as e:
        print(f"Error processing file: {str(e)}")
        traceback.print_exc()

        if conn and statement_id:
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE bank_statements SET process_status = 'FAILED' WHERE statement_id = %s;",
                    (statement_id,)
                )
                conn.commit()
            except Exception as inner_e:
                print(f"Failed to update error status: {str(inner_e)}")
        raise e
    finally:
        if conn:
            conn.close()

def lambda_handler(event, context):
    print(f"Lambda started. Event: {json.dumps(event, default=str)}")
    try:
        if 'Records' in event:
            # S3 event trigger
            for record in event['Records']:
                if record['eventSource'] == 'aws:s3':
                    bucket = record['s3']['bucket']['name']
                    key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
                    process_s3_upload(bucket, key)
        else:
            # Direct invocation
            bucket = event.get('bucket')
            key = event.get('key')
            if bucket and key:
                process_s3_upload(bucket, key)
            else:
                return {'statusCode': 400, 'body': json.dumps('Missing bucket or key parameters')}

        return {'statusCode': 200, 'body': json.dumps('Processing completed successfully')}
    except Exception as e:
        print(f"Lambda handler error: {e}")
        print(f"Error traceback: {traceback.format_exc()}")
        return {'statusCode': 500, 'body': json.dumps(f'Processing failed: {str(e)}')}