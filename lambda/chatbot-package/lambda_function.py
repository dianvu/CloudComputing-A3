import json
import os
import boto3
import pg8000.dbapi
import ssl

# Initialize Bedrock client
bedrock_runtime = boto3.client('bedrock-runtime')

def lambda_handler(event, context):
    try:
        # request from API Gateway as JSON in `event['body']` ---
        body_str = event.get("body")
        if not body_str:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "Request body is missing."})
            }

        # convert JSON to dictionary
        body_dict = json.loads(body_str)
        user_question = body_dict.get("question")
        user_id = body_dict.get("user_id")

        if not user_question:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "Missing 'question' in the request body."})
            }
        
        if not user_id:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "Missing 'user_id' in the request body."})
            }       

        # connect to PostgresSQL
        try:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            conn = pg8000.dbapi.connect(
                host=os.environ.get("DB_HOST"),
                port=int(os.environ.get("DB_PORT", 5432)),
                user=os.environ.get("DB_USER"),
                password=os.environ.get("DB_PASSWORD"),
                database=os.environ.get("DB_NAME"),
                ssl_context=ssl_context
            )
            cursor = conn.cursor()
        except Exception as e:
            return {
                "statusCode": 500,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": f"Database connection failed: {str(e)}"})
            }

        # query data from db to form context
        cursor.execute(
            "SELECT transaction_date, description, amount, category FROM transactions WHERE user_id = %s",
            (user_id,)
        )
        transactions = cursor.fetchall()
        transaction_context = "\n".join([f"- Date: {row[0]}, Description: {row[1]}, Amount: {row[2]}, Categories: {row[3]}" for row in transactions])

        cursor.execute(
            "SELECT analysis_summary FROM bank_statements WHERE user_id = %s",
            (user_id,)
        )
        bank_statements = cursor.fetchall()
        analysis_context = "\n".join([json.dumps(row[0], ensure_ascii=False, indent=2) for row in bank_statements])

        cursor.close()
        conn.close()

        prompt = f"""
        You are assisting user with ID: {user_id}.
        Answer the user's financial question concisely using the context below.
        Constraints:
        - Max 120 words
        - Start directly with the answer (no preamble)
        - Use at most 3 short bullet points when appropriate
        - If data is insufficient, say so briefly and request the minimum extra info

        Context — Transaction history for user {user_id}
        ```
        {transaction_context}
        ```

        Context — Analysis summaries for user {user_id}
        ```
        {analysis_context}
        ```

        User question: "{user_question}"
        """

        # invoke claude3 haiku model from bedrock
        model_request_body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "system": "You are a helpful financial assistant. Be concise, clear, and avoid filler.",
            "max_tokens": 200,
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": prompt}]
                }
            ]
        })

        modelId = 'anthropic.claude-3-haiku-20240307-v1:0'
        response = bedrock_runtime.invoke_model(
            body=model_request_body,
            modelId=modelId,
            accept='application/json',
            contentType='application/json'
        )
        response_body = json.loads(response.get('body').read())
        assistant_response = response_body['content'][0]['text']

        # format the reponse returned to API Gateway
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                # add CORS header if API is called from a different domain
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "POST,OPTIONS"
            },
            "body": json.dumps({"response": assistant_response})
        }

    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Invalid JSON format in request body."})
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": f"An internal error occurred: {str(e)}"})
        }