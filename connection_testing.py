import phiOp
import op2python as o2p
import psycopg2
import os
from dotenv import load_dotenv
import psycopg2.extras
import polars as pl

query = """select FromAC, FromTel, R.ToAC, R.Length, sum(R.Length)
from CALLS
group by FromAC, FromTel : R
suchthat R.Date > "96/05/31" AND R.Date < "96/09/01"
having sum(R.Length)*3 > sum(Length) AND R.Length = max(R.Length)"""


sales_query = """select prod, cust, state, sum(quant), sum(x.quant), sum(y.quant)
from sales
group by prod, cust: x, y
suchthat x.state = 'NY' and x.quant > 15, y.state = 'NJ'"""

queryParsed = phiOp.parse_query(sales_query)

print(f"S: {queryParsed['S']}")
print(f"N: {queryParsed['N']}")
print(f"V: {queryParsed['V']}")
print(f"F: {queryParsed['F']}")
print(f"R: {queryParsed['R']}")
print(f"H: {queryParsed['H']}")

# test connection

def query():
  
  load_dotenv()

  user = os.getenv('USER')
  password = os.getenv('PASSWORD')
  dbname = os.getenv('DBNAME')

  conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password)
  cur = conn.cursor()
  cur.execute("SELECT * FROM sales")
  tuples = cur.fetchall()
  cur.close()
  conn.close()
  
  df = pl.DataFrame(tuples, {'cust': pl.String, 'prod': pl.String, 'day': pl.Int32, 
                              'month': pl.Int32, 'year': pl.Int32, 'state': pl.String, 'quant': pl.Int32, 'date': pl.Date})
  
  print(df.head())
  
  operator = o2p.op2python(queryParsed)
  result = operator.construct_queries(df=df)
  
  print(result)

query()