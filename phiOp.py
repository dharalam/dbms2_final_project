import regex

'''
Parses text of ESQL query and converts it to a dict of parameters that are equivalent to the Φ operator.

The ESQL query is expected to be in the following format:

select FromAC, FromTel, R.ToAC, R.Length
from CALLS
group by FromAC, FromTel : R
suchthat R.Date > "96/05/31" AND R.Date < "96/09/01"
having sum(R.Length)*3 > sum(Length) AND R.Length = max(R.Length)
 
where each clause is separated by a newline for our parsing convenience
'''

def grab_aggregates(condition):
    regex_pattern = r"(sum|max|min|avg|count)(\([^\)]*\))"
    matches = regex.findall(regex_pattern, condition)
    return matches

def remove_from_agg(agg):
    regex_pattern = r"(sum|max|min|avg|count)(\([^\)]*\))"
    match = regex.match(regex_pattern, agg)
    if match is not None:
        return (match.group(1), match.group(2))
    else:
        return (None, agg)

def isolate_gv(gv):
    regex_pattern = r"(.*)\.(.*)"
    match = regex.match(regex_pattern, gv)
    if match is not None:
        return (match.group(1), match.group(2))
    else:
        return (gv, None)
  
def fTupleToStr(tuple):
  retString = f"{tuple[0]}_{tuple[1].replace('.', '_').replace('(', '').replace(')', '')}"
  if (len(retString.split("_")) == 2):
    func, attr = retString.split("_")
    retString = f"{func}_GV0_{attr}"
  return retString

def replace_aggregate(condition):
    aggregate = grab_aggregates(condition)
    if aggregate is not None and len(aggregate) > 0:
        return fTupleToStr(aggregate[0])
    else:
        return fTupleToStr(("None", condition))

def parse_query(query):
    # Initialize dictionary to store the parameters
    phi_op = {"S": None, "N": None, "V": None, "F": None, "R": None, "H": None}
    statements = {"select": "", "from": "", "group by": "", "suchthat": "", "having": ""}
    
    # Split the query into individual conditions
    query_components = query.split("\n")
    
    # Iterate over each condition in the query
    for condition in query_components:
        components = condition.split(" ")
        if components[0] in statements:
            statements[components[0]] = " ".join(components[1:])
        elif (components[0] + " " + components[1]) in statements:
            statements[components[0] + " " + components[1]] = " ".join(components[2:])
        else:
            raise ValueError("Invalid query format")
    
    # Extract the parameters from the conditions and store them in the dictionary
    phi_op["S"] = list(map(lambda x: replace_aggregate(x.strip()), statements["select"].split(",")))
    phi_op["N"] = 1 + len(statements["group by"].split(":")[1].split(","))
    phi_op["V"] = list(map(lambda x: x.strip(), statements["group by"].split(":")[0].split(",")))
    phi_op["F"] = list(map(fTupleToStr, list(grab_aggregates(statements["select"]) + grab_aggregates(statements["having"]))))
    phi_op["R"] = list(map(lambda x: x.strip(), statements["suchthat"].split(",")))
    phi_op["H"] = list(map(lambda x: x.strip(), statements["having"].split(",")))
    
    # Return the dictionary of parameters
    return phi_op
