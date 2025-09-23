import json
import boto3
import pymysql
import os
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS Lambda client
lambda_client = boto3.client('lambda')

def lambda_handler(event, context):
    """
    Batch processor Lambda function that:
    1. Reads IDs from MariaDB table
    2. Invokes target Lambda function for each ID
    
    Expected event structure:
    {
        "target_lambda_function": "your-target-lambda-function-name",
        "batch_size": 100,  // Optional: number of IDs to process per batch
        "concurrent_executions": 5,  // Optional: number of parallel invocations
        "invocation_type": "Event",  // Optional: "Event" (async) or "RequestResponse" (sync)
        "table_name": "your_mariadb_table",  // Optional: override default table
        "where_clause": "status = 'pending'"  // Optional: filter condition
    }
    """
    
    try:
        # Extract configuration from event
        target_function = event.get('target_lambda_function', os.environ.get('TARGET_LAMBDA_FUNCTION'))
        batch_size = event.get('batch_size', 100)
        concurrent_executions = event.get('concurrent_executions', 5)
        invocation_type = event.get('invocation_type', 'Event')  # Event = async, RequestResponse = sync
        table_name = event.get('table_name', os.environ.get('MARIADB_TABLE_NAME', 'your_mariadb_table'))
        where_clause = event.get('where_clause', '')
        
        if not target_function:
            raise ValueError("target_lambda_function is required")
        
        # Read IDs from MariaDB
        ids = read_ids_from_mariadb(table_name, where_clause, batch_size)
        
        if not ids:
            logger.info("No IDs found to process")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No IDs found to process',
                    'processed_count': 0
                })
            }
        
        logger.info(f"Found {len(ids)} IDs to process")
        
        # Process IDs (invoke target Lambda for each)
        if invocation_type == 'RequestResponse':
            # Synchronous processing with controlled concurrency
            results = process_ids_sync(target_function, ids, concurrent_executions)
        else:
            # Asynchronous processing (fire and forget)
            results = process_ids_async(target_function, ids)
        
        # Count successful and failed invocations
        successful = sum(1 for r in results if r.get('success', False))
        failed = len(results) - successful
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Batch processing completed',
                'total_ids': len(ids),
                'successful_invocations': successful,
                'failed_invocations': failed,
                'target_function': target_function,
                'invocation_type': invocation_type,
                'results': results[:10]  # Return first 10 results for debugging
            })
        }
        
    except Exception as e:
        logger.error(f"Error in batch processor: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Batch processing failed'
            })
        }

def get_mariadb_connection():
    """Create and return MariaDB connection"""
    try:
        connection = pymysql.connect(
            host=os.environ['MARIADB_HOST'],
            user=os.environ['MARIADB_USER'],
            password=os.environ['MARIADB_PASSWORD'],
            database=os.environ['MARIADB_DATABASE'],
            port=int(os.environ.get('MARIADB_PORT', 3306)),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to MariaDB: {str(e)}")
        raise

def read_ids_from_mariadb(table_name, where_clause='', limit=100):
    """
    Read IDs from MariaDB table
    Returns list of IDs
    """
    connection = None
    try:
        connection = get_mariadb_connection()
        
        with connection.cursor() as cursor:
            # Build the query - only select tid
            base_query = f"SELECT tid FROM {table_name}"
            
            if where_clause:
                query = f"{base_query} WHERE {where_clause}"
            else:
                query = base_query
                
            if limit:
                query += f" LIMIT {limit}"
            
            logger.info(f"Executing query: {query}")
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Extract just the tid values from the results
            ids = [row['tid'] for row in results]
            
            logger.info(f"Retrieved {len(ids)} IDs from MariaDB")
            return ids
            
    except Exception as e:
        logger.error(f"Error reading from MariaDB: {str(e)}")
        raise
    finally:
        if connection:
            connection.close()

def process_ids_async(target_function, ids):
    """
    Process IDs asynchronously (fire and forget)
    Fast but no response handling
    """
    results = []
    
    for idx, tid in enumerate(ids):
        try:
            # Prepare payload for target Lambda - just the ID
            payload = {
                'tid': tid
            }
            
            # Invoke target Lambda asynchronously
            response = lambda_client.invoke(
                FunctionName=target_function,
                InvocationType='Event',  # Async invocation
                Payload=json.dumps(payload)
            )
            
            results.append({
                'tid': tid,
                'success': True,
                'status_code': response['StatusCode'],
                'index': idx
            })
            
            logger.info(f"Successfully invoked for tid: {tid}")
            
        except Exception as e:
            logger.error(f"Failed to invoke for tid {tid}: {str(e)}")
            results.append({
                'tid': tid,
                'success': False,
                'error': str(e),
                'index': idx
            })
    
    return results

def process_ids_sync(target_function, ids, max_workers=5):
    """
    Process IDs synchronously with controlled concurrency
    Slower but with response handling and error details
    """
    results = []
    
    def invoke_single_lambda(tid_with_index):
        tid, idx = tid_with_index
        try:
            # Prepare payload for target Lambda - just the ID
            payload = {
                'tid': tid
            }
            
            # Invoke target Lambda synchronously
            response = lambda_client.invoke(
                FunctionName=target_function,
                InvocationType='RequestResponse',  # Sync invocation
                Payload=json.dumps(payload)
            )
            
            # Parse response
            response_payload = json.loads(response['Payload'].read())
            
            return {
                'tid': tid,
                'success': True,
                'status_code': response['StatusCode'],
                'response': response_payload,
                'index': idx
            }
            
        except Exception as e:
            logger.error(f"Failed to invoke for tid {tid}: {str(e)}")
            return {
                'tid': tid,
                'success': False,
                'error': str(e),
                'index': idx
            }
    
    # Use ThreadPoolExecutor for controlled concurrency
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_tid = {
            executor.submit(invoke_single_lambda, (tid, idx)): tid 
            for idx, tid in enumerate(ids)
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_tid):
            result = future.result()
            results.append(result)
            
            if result['success']:
                logger.info(f"Successfully processed tid: {result['tid']}")
            else:
                logger.error(f"Failed to process tid: {result['tid']}")
    
    return results

# Optional: Scheduled batch processor
def scheduled_batch_processor(event, context):
    """
    Alternative handler for scheduled processing (e.g., CloudWatch Events)
    """
    # Default configuration for scheduled runs
    batch_event = {
        'target_lambda_function': os.environ.get('TARGET_LAMBDA_FUNCTION'),
        'batch_size': int(os.environ.get('BATCH_SIZE', 50)),
        'concurrent_executions': int(os.environ.get('CONCURRENT_EXECUTIONS', 3)),
        'invocation_type': os.environ.get('INVOCATION_TYPE', 'Event'),
        'where_clause': os.environ.get('WHERE_CLAUSE', "status = 'pending'")
    }
    
    return lambda_handler(batch_event, context)
