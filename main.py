import psycopg2
import json

# Connect to your postgres DB
def load_config():
    with open('config.json') as f:
        return json.load(f)

def save_config(config):
    with open('config.json', 'w') as f:
        json.dump(config, f)
    return True

def get_connection():
    config = load_config()    
    try:
        return psycopg2.connect(
            database="postgres",
            user=config["user"],
            password=config["password"],
            host="127.0.0.1", # should be standard for localhost
            port=5432,
        )
    except:
        return False


if __name__ == "main":    
    conn = get_connection()
    if conn:
        print("Connection to the PostgreSQL established successfully.")
    else:
        print("Connection to the PostgreSQL encountered and error.")
