'''
Based on the Phi Operator structure, construct python queries to handle the row selection.
The result of the queries should be combined to create the result of the full ESQL query.
'''
import polars as pl
import regex as re

class op2python:
    def __init__(self, phiOp):
        self.S = phiOp["S"]
        self.N = phiOp["N"]
        self.V = phiOp["V"]
        self.F = phiOp["F"]
        self.R = phiOp["R"]
        self.H = phiOp["H"]
        self.queries = []
    
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
                keywords = re.findall(r"\b(?:and|or|not)\b", expr, re.IGNORECASE)
                if keywords is not None and len(keywords) > 0:
                    for keyword in keywords:
                        expr = expr.replace(keyword, "|")
                matches = re.findall(r"([^\s\|]+)\s+([^\s\|]+)\s+([^\s\|]+)", expr)
                if matches is not None:
                    for match in matches:
                        gv = None
                        if len(match) == 3:
                            col, op, val = match
                            get_gv = re.findall(r"(.*)\.(.*)", col)
                            if get_gv is not None and len(get_gv) > 0:
                                gv, col = get_gv[0]
                            else:
                                gv = "GV0"
                            if gv not in res:
                                res[gv] = [(col, op, val)]
                            else:
                                res[gv].append((col, op, val))
                    if gv not in kws:
                        kws[gv] = keywords
            return res, kws

        # begin constructing the queries
                    
        gvs = split_gvs(list(set(self.F) | set(self.S)))
        
        st_conds, st_kws = parse_suchthat(self.R)
        
        print(f"GVs: {gvs}")
        print(f"ST: {st_conds}")
        print(f"ST KW: {st_kws}")
        
        for gv, conds in st_conds.items():
            if gv not in gvs:
                return KeyError(f"GV {gv} not in GVs")
            
            gvkws = st_kws[gv] if gv in st_kws else None

            filter_query = "df.filter("
            
            if len(conds) > 1:
                for (index, (col, op, val)) in enumerate(conds):
                    filter_query += f"(pl.col('{col}') {operator_map[op]} {val})"
                    filter_query += f" {operator_map[gvkws[index]]} " if index < len(gvkws) else ""
            else:
                filter_query += f"(pl.col('{conds[0][0]}') {operator_map[conds[0][1]]} {conds[0][2]})"
            
            filter_query += ")"
                
            print(filter_query)
            
            gvdf = eval(filter_query)
            
            groupby_query = f"gvdf.group_by({self.V}).agg(["
            
            for (index, (agg, col)) in enumerate(gvs[gv]):
                groupby_query += f"pl.{agg}('{col}')" if agg != "None" else f"pl.col('{col}')"
                alias = f".alias('{agg}_{gv}_{col}')" if agg != "None" else ""
                print(f"Alias: {alias}")
                groupby_query += alias
                if index < len(gvs[gv]) - 1:
                    groupby_query += ", "

            groupby_query += "])"
            
            print(groupby_query)
            
            gvdf = eval(groupby_query)
            
            print(f"{gv} GVDF: {gvdf.head()}")
            
            self.queries.append(gvdf)
        
        frame = self.queries.pop(0)
        if len(self.queries) > 0:
            while len(self.queries) > 0:
                frame = frame.join(self.queries.pop(0), how="inner", on=self.V)

        return frame




    

    
    
    
    








