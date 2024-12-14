'''
Based on the Phi Operator structure, construct python queries to handle the row selection.
The result of the queries should be combined to create the result of the full ESQL query.
'''
import polars as pl
import regex as re
import phiOp as po

class op2python:
    def __init__(self, phiOp):
        self.S = phiOp["S"]
        self.N = phiOp["N"]
        self.V = phiOp["V"]
        self.F = phiOp["F"]
        self.R = phiOp["R"]
        self.H = phiOp["H"]
        self.queries = {}
        self.gvs_in_queries = set()
    
    def construct_queries(self, df:pl.DataFrame):
        # Construct queries based on the Phi Operator structure
        # Should store each query as a string in the self.queries list
        # Will be executed in the body section of the generator
        
        operator_map = {"=": "==", ">": ">", "<": "<", ">=": ">=", "<=": "<=", "!=": "!=", "<>": "!=", "and": "&", "or": "|", "not": "~"}
        
        # Process group-by variables and aggregation functions

        def split_gvs(gvs:list[str]):
            res = {}
            for expr in gvs:
                agg, gv, col = expr.split("_")
                if gv not in res:
                    res[gv] = [(agg, col)]
                else:
                    res[gv].append((agg, col))
            return res
        
        def parse_suchthat(st:list[str]):
            res = {}
            kws = {}
            for expr in st:
                keywords = re.findall(r"\b(?:and|or|not)\b", expr, re.IGNORECASE) # splits keywords like and, or, and not
                if keywords is not None and len(keywords) > 0:
                    for keyword in keywords:
                        expr = expr.replace(keyword, "|") # ensures we can get our components from the conditions by giving us something consistent to split on
                matches = re.findall(r"([^\s\|]+)\s+([^\s\|]+)\s+([^\s\|]+)", expr)
                if matches is not None:
                    for match in matches:
                        gv = None
                        if len(match) == 3:
                            col, op, val = match # gets the column, operator, and value to check against from the match groups
                            get_gv = re.findall(r"(.*)\.(.*)", col) # checks to see if there is a grouping variable assigned
                            if get_gv is not None and len(get_gv) > 0:
                                gv, col = get_gv[0]
                            else:
                                gv = "GV0" # otherwise assume GV0
                            if gv not in res:
                                res[gv] = {} # dict by gv primary
                            if col not in res[gv]:
                                res[gv][col] = [(op, val)] # for each gv, dict by column name so that we can perform aggregates if necessary even without a condition
                            else:
                                res[gv][col].append((op, val))
                    if gv not in kws:
                        kws[gv] = keywords # get the keywords per gv as well
            return res, kws

        # begin constructing the queries
                    
        gvs = split_gvs(list(set(self.F) | set(self.S)))
        
        st_conds, st_kws = parse_suchthat(self.R)
        
        print(f"GVs: {gvs}")
        print(f"ST: {st_conds}")
        print(f"ST KW: {st_kws}")
        
        for gv, condict in st_conds.items():
            if gv not in gvs:
                return KeyError(f"GV {gv} not in GVs")
            
            gvkws = st_kws[gv] if gv in st_kws else None

            filter_query = "df.filter("
            
            index = 0
            
            for (i, (col, conds)) in enumerate(condict.items()):
                stillAgged = po.grab_aggregates(col)
                if stillAgged is not None and len(stillAgged) > 0:
                    col = stillAgged[0][1]
                for op, val in conds:
                    filter_query += f"(pl.col('{col}') {operator_map[op]} {val})"
                    filter_query += f" {operator_map[gvkws[index]]} " if index < len(gvkws) else ""
                    index+=1
            
            filter_query += ")"
                
            print(filter_query)
            
            gvdf = eval(filter_query)
            
            self.queries[gv] = gvdf
            self.gvs_in_queries.add(gv)

        for gv in gvs:
            if gv not in self.gvs_in_queries:
                self.queries[gv] = df
                
            groupby_query = f"gvdf.with_columns(["
            
            for (index, (agg, col)) in enumerate(gvs[gv]):
                groupby_query += f"pl.col('{col}')" 
                groupby_query += f".{agg}()" if agg != "None" else ""
                alias = f".alias('{agg}_{gv}_{col}')" if agg != "None" else f".alias('{gv}_{col}')"
                print(f"Alias: {alias}")
                if agg == "None":
                    groupby_query += alias
                    if index < len(gvs[gv]) - 1:
                        groupby_query += ", "
                    continue
                groupby_query += f".over({self.V})" if index < len(gvs[gv]) else ""
                groupby_query += alias
                if index < len(gvs[gv]) - 1:
                    groupby_query += ", "
                

            groupby_query += "])"
            
            print(groupby_query)
            
            gvdf = self.queries[gv]
            
            gvdf = eval(groupby_query)
            
            self.queries[gv] = gvdf
            
            print(f"{gv} GVDF: {gvdf.head()}")
        
        frame = self.queries.pop("GV0")
        query_keys = list(self.queries.keys())
        
        if len(self.queries) > 0:
            while len(self.queries) > 0:
                suff = query_keys.pop(0)
                to_join = self.queries.pop(suff)
                frame = frame.join(to_join, how="inner", on=self.V, suffix=f"_{suff}")
        
        def convert_col_name(col_name):
            agg, gv, col = col_name.split("_")
            if agg =="None":
                if gv == "GV0":
                    gv = ""
                agg = ""
            return "_".join(list(filter(lambda x: x != "", [agg, gv, col])))
            
        to_select = list(map(convert_col_name, self.S))


        return frame.select(to_select).unique()




    

    
    
    
    








