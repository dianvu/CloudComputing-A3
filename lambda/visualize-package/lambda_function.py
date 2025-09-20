import json
import os
import boto3
import pg8000
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError
import os.path as osp
import traceback
import re
import ssl

# Initialize S3 client
s3_client = boto3.client('s3')
S3_BUCKET = os.environ.get("S3_BUCKET")

def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event, ensure_ascii=False)}")

    try:
        # check trigger từ BankExtract
        if event.get("trigger_source") == "bank_extract":
            statement_id = event.get("statement_id")
            if not statement_id:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Missing statement_id'})
                }

            print(f"Processing dashboard for statement_id={statement_id}")

            # create HTML dashboard
            dashboard_html = create_dashboard_html(statement_id)
            if not dashboard_html:
                return {
                    'statusCode': 500,
                    'body': json.dumps({'error': 'Failed to generate dashboard HTML'})
                }

            # get info from db then consucting dashboard key
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT user_id, s3_key
                FROM bank_statements
                WHERE statement_id = %s
            """, (statement_id,))
            row = cursor.fetchone()
            cursor.close()
            conn.close()

            if not row:
                return {
                    'statusCode': 404,
                    'body': json.dumps({'error': 'Statement not found'})
                }

            user_id, s3_key = row # Only unpack user_id and s3_key

            dashboard_s3_key = build_dashboard_key(user_id, s3_key)
            print(f"Dashboard S3 key: {dashboard_s3_key}")

            # Upload lên S3
            upload_dashboard_to_s3(S3_BUCKET, dashboard_s3_key, dashboard_html) # Note: s3_bucket needs to be defined or fetched differently now

            # Tạo URL
            dashboard_url = get_dashboard_url(S3_BUCKET, dashboard_s3_key) # Note: s3_bucket needs to be defined or fetched differently now

            print(f"Dashboard created successfully: {dashboard_url}")

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Dashboard created successfully',
                    'dashboard_url': dashboard_url,
                    'dashboard_s3_key': dashboard_s3_key,
                    'user_id': user_id,
                    'statement_id': statement_id
                })
            }

        else:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Unsupported event format'})
            }

    except Exception as e:
        print(f"Error: {str(e)}")
        traceback.print_exc() # traceback for debug
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def create_dashboard_html(statement_id):
    """Create HTML dashboard with data from analysis_summary"""
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get data from analysis_summary
        cursor.execute("""
            SELECT user_id, analysis_summary, created_at
            FROM bank_statements
            WHERE statement_id = %s AND analysis_summary IS NOT NULL
        """, (statement_id,))
        current_statement = cursor.fetchone()

        if not current_statement:
            print("⚠️ No analysis found for statement_id")
            return generate_default_dashboard()

        # 5 latest statements from users
        user_id = current_statement[0]
        cursor.execute("""
            SELECT user_id, analysis_summary, created_at
            FROM bank_statements
            WHERE user_id = %s AND analysis_summary IS NOT NULL AND statement_id != %s
            ORDER BY created_at DESC
            LIMIT 5
        """, (user_id, statement_id))

        other_statements = cursor.fetchall()
        all_statements = [current_statement]
        all_statements.extend(other_statements)
        return generate_dashboard_html(all_statements)

    except Exception as e:
        print(f"Error in create_dashboard_html: {str(e)}")
        return generate_default_dashboard() # Return default on error
    finally:
        # Ensure resources are closed
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def extract_timestamp_from_filename(filename: str) -> str:
    # pattern: -20250917191200-
    match = re.search(r'-\d{14}-', filename)
    if match:
        ts_str = match.group(0)[1:-1]  # excluded '-'
        try:
            # Validate format
            datetime.strptime(ts_str, '%Y%m%d%H%M%S')
            return ts_str
        except ValueError:
            pass
    # Fallback
    return datetime.now().strftime('%Y%m%d%H%M%S')

def build_dashboard_key(user_id: str, source_key: str) -> str:
    """Build dashboard key based on uploaded statements"""
    filename = osp.basename(source_key)
    base = osp.splitext(filename)[0] # excluded .pdf
    if not base:
        base = 'statement'

    # extract time stamp from function
    ts = extract_timestamp_from_filename(filename)

    return f"dashboard/{user_id}/{base}.html"

def extract_data_from_statements(statements):
    """Extract data from analysis_summary of statements"""
    total_income = 0
    total_expense = 0
    net_amount = 0
    total_transactions = 0
    categories_data = []
    category_totals = {}
    
    for statement in statements:
        user_id, analysis_summary, created_at = statement
        try:
            if analysis_summary:
                summary = json.loads(analysis_summary) if isinstance(analysis_summary, str) else analysis_summary
                
                # Get overview data
                total_income += summary.get('total_income', 0)
                total_expense += summary.get('total_expense', 0)
                net_amount += summary.get('net_amount', 0)
                total_transactions += summary.get('transaction_count', 0)
                
                # Get categories_amount data
                categories_amount = summary.get('categories_amount', {})
                for category, data in categories_amount.items():
                    amount = data.get('total_amount', 0)
                    count = data.get('transaction_count', 0)
                    
                    if category in category_totals:
                        category_totals[category]['amount'] += amount
                        category_totals[category]['count'] += count
                    else:
                        category_totals[category] = {
                            'amount': amount,
                            'count': count
                        }
        except Exception as e:
            print(f"Error parsing analysis_summary: {str(e)}")
            continue
    
    # Convert to list format
    for category, data in category_totals.items():
        categories_data.append((category, data['amount'], data['count']))
    
    # Sort by absolute amount
    categories_data.sort(key=lambda x: abs(x[1]), reverse=True)
    
    return {
        'total_income': total_income,
        'total_expense': total_expense,
        'net_amount': net_amount,
        'total_transactions': total_transactions,
        'categories_data': categories_data
    }

def get_db_connection():
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    return pg8000.dbapi.connect(
        host=os.environ.get("DB_HOST"),
        database=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        port=int(os.environ.get("DB_PORT", 5432)),
        ssl_context=ssl_context
    )

def generate_dashboard_html(statements):
    """Generate HTML dashboard with Chart.js"""
    
    # Extract data from statements
    data = extract_data_from_statements(statements)
    
    # Prepare data for charts
    pie_data = []
    bar_data = []
    colors = ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40', '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4']
    
    for i, (category, amount, count) in enumerate(data['categories_data']):
        pie_data.append({
            'category': category,
            'amount': float(amount),
            'count': count,
            'color': colors[i % len(colors)]
        })
        bar_data.append({
            'category': category,
            'count': count,
            'color': colors[i % len(colors)]
        })
    
    # Use extracted data
    total_income = data['total_income']
    total_expense = data['total_expense']
    net_amount = data['net_amount']
    total_transactions = data['total_transactions']
    
    html_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Bank Statement Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 20px 40px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        
        .header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
        }}
        
        .header p {{
            font-size: 1.2em;
            opacity: 0.9;
        }}
        
        .summary-cards {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            padding: 30px;
            background: #f8f9fa;
        }}
        
        .card {{
            background: white;
            padding: 25px;
            border-radius: 10px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.08);
            text-align: center;
            transition: transform 0.3s ease;
        }}
        
        .card:hover {{
            transform: translateY(-5px);
        }}
        
        .card h3 {{
            color: #666;
            margin-bottom: 15px;
            font-size: 1.1em;
        }}
        
        .card .value {{
            font-size: 2em;
            font-weight: bold;
            color: #333;
        }}
        
        .expense {{ color: #e74c3c; }}
        .income {{ color: #27ae60; }}
        .neutral {{ color: #3498db; }}
        
        .charts-container {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 30px;
            padding: 30px;
        }}
        
        .chart-card {{
            background: white;
            padding: 25px;
            border-radius: 10px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.08);
        }}
        
        .chart-card h3 {{
            margin-bottom: 20px;
            color: #333;
            text-align: center;
        }}
        
        .chart-container {{
            position: relative;
            height: 400px;
        }}
        
        .recent-statements {{
            padding: 30px;
            background: #f8f9fa;
        }}
        
        .recent-statements h3 {{
            margin-bottom: 20px;
            color: #333;
        }}
        
        .statement-item {{
            background: white;
            padding: 15px;
            margin-bottom: 10px;
            border-radius: 8px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.05);
            border-left: 4px solid #667eea;
        }}
        
        .statement-item .date {{
            color: #666;
            font-size: 0.9em;
        }}
        
        .statement-item .user {{
            font-weight: bold;
            color: #333;
            margin: 5px 0;
        }}
        
        .footer {{
            background: #333;
            color: white;
            text-align: center;
            padding: 20px;
        }}
        
        @media (max-width: 768px) {{
            .charts-container {{
                grid-template-columns: 1fr;
            }}
            
            .summary-cards {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Bank Statement Dashboard</h1>
        </div>
        
        <div class="summary-cards">
            <div class="card">
                <h3>💰 Total Income</h3>
                <div class="value income" id="totalIncome">{total_income:,.0f} VND</div>
            </div>
            <div class="card">
                <h3>💸 Total Expense</h3>
                <div class="value expense" id="totalExpense">{total_expense:,.0f} VND</div>
            </div>
            <div class="card">
                <h3>📈 Net Amount</h3>
                <div class="value neutral" id="netAmount">{total_income + total_expense:,.0f} VND</div>
            </div>
            <div class="card">
                <h3>📋 Total Transactions</h3>
                <div class="value neutral" id="totalTransactions">{total_transactions}</div>
            </div>
        </div>
        
        <div class="charts-container">
            <div class="chart-card">
                <h3>🥧 Expenses by Category</h3>
                <div class="chart-container">
                    <canvas id="pieChart"></canvas>
                </div>
            </div>
            <div class="chart-card">
                <h3>📊 Transactions by Category</h3>
                <div class="chart-container">
                    <canvas id="barChart"></canvas>
                </div>
            </div>
        </div>
        
        <div class="recent-statements">
            <h3>📄 Recent Statements</h3>
            <div id="statementsList">
                {generate_statements_list(statements)}
            </div>
        </div>
        
        <div class="footer">
            <p>🔄 Auto-updated when new PDF files are uploaded | Powered by AWS Lambda</p>
        </div>
    </div>

    <script>
        // Data for charts
        const pieData = {json.dumps(pie_data)};
        const barData = {json.dumps(bar_data)};
        
        // Create Pie Chart
        const pieCtx = document.getElementById('pieChart').getContext('2d');
        new Chart(pieCtx, {{
            type: 'doughnut',
            data: {{
                labels: pieData.map(item => item.category),
                datasets: [{{
                    data: pieData.map(item => Math.abs(item.amount)),
                    backgroundColor: pieData.map(item => item.color),
                    borderWidth: 2,
                    borderColor: '#fff'
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{
                        position: 'bottom',
                        labels: {{
                            padding: 20,
                            usePointStyle: true
                        }}
                    }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                const value = new Intl.NumberFormat('vi-VN', {{
                                    style: 'currency',
                                    currency: 'VND'
                                }}).format(context.parsed);
                                return context.label + ': ' + value;
                            }}
                        }}
                    }}
                }}
            }}
        }});
        
        // Create Bar Chart
        const barCtx = document.getElementById('barChart').getContext('2d');
        new Chart(barCtx, {{
            type: 'bar',
            data: {{
                labels: barData.map(item => item.category),
                datasets: [{{
                    label: 'Transactions',
                    data: barData.map(item => item.count),
                    backgroundColor: barData.map(item => item.color),
                    borderColor: barData.map(item => item.color),
                    borderWidth: 1
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{
                        display: false
                    }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: true,
                        ticks: {{
                            stepSize: 1
                        }}
                    }}
                }}
            }}
        }});
        
        // Auto refresh every 30 seconds
        setInterval(() => {{
            location.reload();
        }}, 30000);
    </script>
</body>
</html>
"""
    
    return html_template

def generate_statements_list(statements):
    """Generate list of recent statements"""
    if not statements:
        return "<p>No statements processed yet.</p>"
    
    html = ""
    for statement in statements[:5]:  # Show 5 most recent statements
        user_id, analysis_summary, created_at = statement
        try:
            summary = json.loads(analysis_summary) if isinstance(analysis_summary, str) else analysis_summary
            net_amount = summary.get('net_amount', 0)
            transaction_count = summary.get('transaction_count', 0)
            
            # Handle both datetime objects and string dates
            if hasattr(created_at, 'strftime'):
                date_str = created_at.strftime('%d/%m/%Y %H:%M')
            else:
                # If it's a string, try to parse it
                try:
                    from datetime import datetime
                    if isinstance(created_at, str):
                        dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        date_str = dt.strftime('%d/%m/%Y %H:%M')
                    else:
                        date_str = str(created_at)
                except:
                    date_str = str(created_at)
            
            html += f"""
            <div class="statement-item">
                <div class="date">{date_str}</div>
                <div class="user">User: {user_id}</div>
                <div>Net Amount: <strong>{net_amount:,.0f} VND</strong> | Transactions: {transaction_count}</div>
            </div>
            """
        except Exception as e:
            # Handle both datetime objects and string dates
            if hasattr(created_at, 'strftime'):
                date_str = created_at.strftime('%d/%m/%Y %H:%M')
            else:
                date_str = str(created_at)
            
            html += f"""
            <div class="statement-item">
                <div class="date">{date_str}</div>
                <div class="user">User: {user_id}</div>
                <div>Processing...</div>
            </div>
            """
    
    return html

def generate_default_dashboard():
    """Generate default dashboard when error occurs"""
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Bank Statement Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; text-align: center; padding: 50px; }
        .error { color: #e74c3c; }
    </style>
</head>
<body>
    <h1>📊 Bank Statement Dashboard</h1>
    <p class="error">Unable to load data. Please try again later.</p>
</body>
</html>
"""

def upload_dashboard_to_s3(bucket: str, dashboard_s3_key: str, html_content: str):
    """Upload dashboard HTML to S3 at the provided key (no ACL required)."""
    try:
        s3_client = boto3.client('s3')
        
        # Upload dashboard
        s3_client.put_object(
            Bucket=bucket,
            Key=dashboard_s3_key,
            Body=html_content,
            ContentType='text/html'
        )
        
        print(f"Dashboard uploaded to s3://{bucket}/{dashboard_s3_key}")
        
    except Exception as e:
        print(f"Error uploading dashboard: {str(e)}")

def get_dashboard_url(bucket: str, dashboard_s3_key: str) -> str:
    """Create URL for dashboard. Returns a 1-hour pre-signed URL (works for private buckets)."""
    s3_client = boto3.client('s3')
    try:
        return s3_client.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': bucket, 'Key': dashboard_s3_key},
            ExpiresIn=3600
        )
    except Exception as e:
        print(f"Error generating presigned URL: {str(e)}")
        region = os.environ.get('AWS_REGION', 'ap-southeast-1')
        return f"https://{bucket}.s3.{region}.amazonaws.com/{dashboard_s3_key}"
