import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def get_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def get_watermark(pipeline_name):
    conn=get_connection()
    cur=conn.cursor()

    cur.execute("""
     SELECT last_pulled_at FROM pipeline_watermark
                WHERE pipeline_name=%s
    """,(pipeline_name,))

    result=cur.fetchone()

    cur.close()
    conn.close()

    return result[0]

def update_watermark(pipeline_name,new_timestamp):
    conn=get_connection()
    cur=conn.cursor()

    cur.execute("""
      UPDATE pipeline_watermark
                SET last_pulled_at = %s
                WHERE pipeline_name = %s
      """,(new_timestamp,pipeline_name))
    
    conn.commit()
    

    cur.close()
    conn.close()

def log_run(pipeline_name,status,rows_fetched,rows_loaded,error_message,started_at,finished_at):
    conn=get_connection()
    cur=conn.cursor()

    cur.execute("""
    INSERT INTO pipeline_logs(
                pipeline_name,status,rows_fetched,rows_loaded,
                error_message,started_at,finished_at
        )
     VALUES(%s,%s,%s,%s,%s,%s,%s)
""",(
     pipeline_name,status,rows_fetched,rows_loaded,
     error_message,started_at,finished_at
))  
    
    conn.commit()
    cur.close()
    conn.close()