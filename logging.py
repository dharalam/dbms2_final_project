import phiOp

query = """select FromAC, FromTel, R.ToAC, R.Length, sum(R.Length)
from CALLS
group by FromAC, FromTel : R
suchthat R.Date > "96/05/31" AND R.Date < "96/09/01"
having sum(R.Length)*3 > sum(Length) AND R.Length = max(R.Length)"""

queryParsed = phiOp.parse_query(query)

print(f"S: {queryParsed["S"]}")
print(f"N: {queryParsed["N"]}")
print(f"V: {queryParsed["V"]}")
print(f"F: {queryParsed["F"]}")
print(f"R: {queryParsed["R"]}")
print(f"H: {queryParsed["H"]}")

mf_data = {
  "prod_cust": [],
  "GV0": {
      "sum_length": []
    },
  "GV1": {
      "sum_length": [],
      "max_length": []
    },
  }

# how do you know when R_n is satisfied






for gv in queryParsed["N"]:
  if (queryParsed["R"][gv]):
    for each *_gv_* in mf_data:
      perform agg in mf_data