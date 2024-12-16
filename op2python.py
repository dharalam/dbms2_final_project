'''
Based on the Phi Operator structure, construct python queries to handle the row selection.
The result of the queries should be combined to create the result of the full ESQL query.
'''
import polars as pl
import regex as re
import phiOp as po
import copy

class op2python:
    def __init__(self, phiOp):
        self.S = phiOp["S"]
        self.N = phiOp["N"]
        self.W = phiOp["W"]
        self.V = phiOp["V"]
        self.F = phiOp["F"]
        self.R = phiOp["R"]
        self.H = phiOp["H"]
        self.queries = {}
        self.gvs_in_queries = set()
        self.operator_map = {"=": "==", ">": ">", "<": "<", ">=": ">=", "<=": "<=", "!=": "!=", "<>": "!=", "and": "&", "or": "|", "not": "~"}
        self.agg_map = {"sum": "sum", "avg": "mean", "count": "count", "min": "min", "max": "max"}
        
    def split_gvs(self, gvs:list[str]):
            res = {}
            for expr in gvs:
                agg, gv, col = expr.split("_")
                if gv not in res:
                    res[gv] = [(agg, col)]
                else:
                    res[gv].append((agg, col))
            return res
    
    def parse_suchthat(self, st:list[str], gvlist):
            res = {}
            kws = {}
            deps = {gv: set() for gv in gvlist} # keeps track of the dependencies between GVs, default to GV0 having no dependencies
            allcols = set()

            for expr in st:
                keywords = re.findall(r"\b(?:and|or|not)\b", expr, re.IGNORECASE) # splits keywords like and, or, and not
                if keywords is not None and len(keywords) > 0:
                    for keyword in keywords:
                        expr = expr.replace(keyword, "|") # ensures we can get our components from the conditions by giving us something consistent to split on
                matches = re.findall(r"([^\s\|]+)\s+([^\s\|]+)\s+([^\s\|]+)", expr)
                if matches is not None:
                    cgv = None
                    vgv = None
                    for match in matches:
                        if len(match) == 3:
                            col, op, val = match # gets the column, operator, and value to check against from the match groups
                            
                            get_gv = re.findall(r"(.*)\.(.*)", col) # checks to see if there is a grouping variable assigned
                            if get_gv is not None and len(get_gv) > 0:
                                cgv, col = get_gv[0]
                            else:
                                cgv = "GV0" # otherwise assume GV0
                            
                            
                            check_gv = re.findall(r"(.*)\.(.*)", val)
                            if check_gv is not None and len(check_gv) > 0:
                                vgv, valcol = check_gv[0]
                                if cgv not in deps:
                                    deps[cgv] = set([vgv])
                                else: 
                                    deps[cgv].add(vgv)
                                
                                val = f"{valcol}_{vgv}"
                                if val not in allcols:
                                    allcols.add(val)
                                    
                                val = f"pl.col('{val}')"
                            
                            
                            check_agg = po.replace_aggregate(val).split("_") # check to see if we have an aggregate and replace it if necessary
                            if len(check_agg) == 3:
                                agg, vgv, _ = check_agg
                                if agg != "None":
                                    val = po.fTupleToStr(po.grab_aggregates(val)[0])
                                    if val not in allcols:
                                        allcols.add(val)
                                    val = f"pl.col('{val}')" # replace the value with the column name that we will have to aggregate on
                                    if cgv not in deps:
                                        deps[cgv] = set([vgv])
                                    else: 
                                        deps[cgv].add(vgv)
                                    
                            if cgv not in res:
                                res[cgv] = {} # dict by gv primary
                            if col not in res[cgv]:
                                res[cgv][col] = [(op, val)] # for each gv, dict by column name so that we can perform aggregates if necessary even without a condition
                            else:
                                res[cgv][col].append((op, val))
                            
                            if col not in allcols:
                                   allcols.add(col)
                              
                        
                    if cgv not in kws:
                        kws[cgv] = keywords # get the keywords per gv as well
            return res, kws, deps, allcols
    
    def dep_cardinality(self, deps:dict):
        # calculate the cardinality of each gv based on its dependencies
        nodes = list(deps.keys())
        nodes.sort(key=lambda x: 0 if x == "GV0" else 1)
        order = []
        while nodes:
            leaves = [node for node in nodes if all(node not in deps[other] for other in nodes)]
            if not leaves:  # cycle detected
                raise ValueError("Cycle detected in the dependency graph.")
            nodes = [node for node in nodes if node not in leaves]
            order.extend(leaves)
        return order[::-1]
    
    def join_deps(self, deps:dict): # consolidate dependencies for each gv based on its dependencies
        # Initialize a dictionary to store the combined dependencies for each node
        ret_deps = copy.deepcopy(deps) # copy the dependencies over to the new dictionary

        for node in ret_deps:
            # For each node, traverse its dependencies and add them to the set for that node
            to_visit = ret_deps[node].copy()
            while to_visit:
                curr = to_visit.pop()
                if curr in ret_deps:
                    to_visit |= ret_deps[curr]
                    ret_deps[node] |= ret_deps[curr]
                    
        return ret_deps
    
    def simplify_joins(self, deps:dict):
        to_join = set()
        gvset = set(deps.keys())
        deplist = list(deps.items())
        
        while len(gvset) > 0:
            if deplist == []:
                to_join |= gvset
                break
            max_dep = set()
            max_ind = -1
            max_node, _ = deplist[0]
            #print(max_node)
            #print(f"gvset: {gvset}")

            for index, (node, l) in enumerate(deplist):
                if len(l) > len(max_dep):
                    max_dep = l
                    max_node = node
                    max_ind = index
            #print(f"max_dep: {max_dep}")
            if max_dep < gvset:
                #print("is subset!")
                gvset -= max_dep
                to_join.add(max_node)
                deplist.pop(max_ind)
                #print(len(gvset))
            else:
                #print("not subset!")
                to_join |= gvset
                break
        return to_join
    
    def convert_col_name(self, col_name): # converts column names that don't have aggs or have no agg and are GV0 to the form we want them in
        agg, gv, col = col_name.split("_")
        if agg =="None":
            if gv == "GV0":
                gv = ""
            agg = ""
        return "_".join(list(filter(lambda x: x != "", [agg, gv, col])))
    
    def extract_col_from_select(self, col):
        matches = re.findall(r"([^\s]+)_([^\s]+)_([^\s]+)", col)
        
        if len(matches) > 0:
            _, _, col = matches[0]
        
        return col
    
    def construct_queries(self, df:pl.DataFrame):
        # Construct queries based on the Phi Operator structure
        # Should store each query as a string in the self.queries list
        # Will be executed in the body section of the generator
                    
        gvs = self.split_gvs(list(set(self.F) | set(self.S))) # get full set of unique gvs with their aggs and columns from select and aggregate parameter 
        gvlist = list(gvs.keys())
        gvlist.sort(key=lambda x: 0 if x == "GV0" else 1)
        to_select = list(map(self.convert_col_name, self.S))
        #print(gvlist)

        st_conds, st_kws, deps, condcols = self.parse_suchthat(self.R, gvlist) # gets the conditions and logical operators from the such that clause
        gvlist = self.dep_cardinality(deps)
        join_deps = self.join_deps(deps)
        
        where_conds = {"GV0": {} }

        if self.W != "":
            where_conds, where_kws, _, _ = self.parse_suchthat([self.W], gvlist)
            where_query = "df.filter("
            index = 0 # global index for logical operators
            for (i, (col, conds)) in enumerate(where_conds["GV0"].items()):
                stillAgged = po.grab_aggregates(col) # sometimes there are cases that GV0 columns that are being aggregated are used in conditions
                if stillAgged is not None and len(stillAgged) > 0:
                    col = stillAgged[0][1] # change the column name to reflect the actual col name
                for op, val in conds:
                    if val in df.columns:
                        val = "pl.col('" + val + "')"
                    where_query += f"(pl.col('{col}') {self.operator_map[op]} {val})"
                    where_query += f" {self.operator_map[where_kws['GV0'][index]]} " if index < len(where_kws["GV0"]) else "" # makes sure we add the correct number of logical operators. It is order-based however.
                    index+=1
            
            where_query += ")"
            #print(where_query)
            df = eval(where_query)


        #print(f"GVs: {gvs}")
        #print(f"ST: {st_conds}")
        #print(f"ST KW: {st_kws}")
        #print(f"Where Conditions: {where_conds}")
        #print(f"Dependencies: {deps}")
        #print(f"Dep Cardinality: {gvlist}")
        #print(f"Expanded Dependencies: {join_deps}")
        #print(f"Columns in Conditions: {condcols}")
        #print(f"Columns in Select: {to_select}")
        
        simplified_joins = self.simplify_joins(join_deps)
        #print(f"Simplified Joins: {simplified_joins}")
        
        
        
        for gv in gvlist:
            gvdf = df

            if gv in st_conds:
                condict = st_conds[gv]
                gvkws = st_kws[gv] if gv in st_kws else None
                gvdeps = deps[gv] if gv in deps else None
                
                for dep in gvdeps:
                    gvdf = gvdf.join(self.queries[f"{dep}"], on=self.V, how='inner', suffix=f"_{dep}").unique() # join the dataframes based on the dependencies (other gvdf's) that this gvdf depends on
                    #print(f"JOINING: {dep} to {gv}")
                #print(gvdf.columns)
                curcols = set(list(gvdf.columns)) & (condcols | set(to_select) | set(list(map(self.extract_col_from_select, to_select))))
                #print(f"Cols we care about pre-filter: {curcols}")
                
                #print(f"{gv} GVDF pre-filter: {gvdf.head()}")

                gvdf = gvdf.select(list(curcols))

                filter_query = "gvdf.filter("
                
                index = 0 # global index for logical operators
                
                for (i, (col, conds)) in enumerate(condict.items()):
                    stillAgged = po.grab_aggregates(col) # sometimes there are cases that GV0 columns that are being aggregated are used in conditions

                    if stillAgged is not None and len(stillAgged) > 0:
                        col = stillAgged[0][1] # change the column name to reflect the actual col name    
                    
                    for op, val in conds:
                        filter_query += f"(pl.col('{col}') {self.operator_map[op]} {val})"
                        filter_query += f" {self.operator_map[gvkws[index]]} " if index < len(gvkws) else "" # makes sure we add the correct number of logical operators. It is order-based however.
                        index+=1
                
                filter_query += ")"
                #print(filter_query)
                gvdf = eval(filter_query) # eval query
                #print(f"{gv} GVDF post-filter: {gvdf}")
            
            self.queries[gv] = gvdf # stuff into queries dict
            self.gvs_in_queries.add(gv) # add the gv that we filtered to this set

            if gv not in self.gvs_in_queries: # if we haven't filtered this gv, we add a copy of df to the queries dict
                self.queries[gv] = df
            
            #print(f"Cols after filter: {gvdf.columns}")
            
            curcols = set(list(gvdf.columns)) & (condcols | set(to_select) | set(list(map(self.extract_col_from_select, to_select))))
            #print(f"Cols we care about pre-groupby: {curcols}")
                
            groupby_query = f"gvdf.with_columns(["
            
            for (i, (agg, col)) in enumerate(gvs[gv]):
                groupby_query += f"pl.col('{col}')" 
                if agg == "None":
                    alias = f".alias('{col}_{gv}')"
                else:
                    groupby_query += f".{self.agg_map[agg]}()"
                    groupby_query += f".over({self.V})"
                    alias = f".alias('{agg}_{gv}_{col}')" 
                #print(f"Alias: {alias}")
                groupby_query += alias
                if i < len(gvs[gv]) - 1:
                    groupby_query += ", "
                    
            groupby_query += "])"
                
            #print(groupby_query)
                
            gvdf = self.queries[gv] # retrieve stashed query
                
            gvdf = eval(groupby_query) # apply groupby query

            self.queries[gv] = gvdf # restash
                
            #print(f"{gv} GVDF: {gvdf.head()}")
        
        if "GV0" in simplified_joins:
            frame = self.queries.pop("GV0") # start with GV0 for convenience and convention
            simplified_joins.remove("GV0")
            to_join = list(simplified_joins)
        else:
            to_join = list(simplified_joins)
            frame = self.queries.pop(to_join.pop(0)) # start with an arbitrary GV, pop it from to_join
        
        if len(self.queries) > 0:
            while len(to_join) > 0:
                suff = to_join.pop(0)
                other = self.queries.pop(suff)
                frame = frame.join(other, how="inner", on=self.V, suffix=f"_{suff}").unique() # inner join and duplicate column names are suffixed with _{suff} to distinguish between GVs
                curcols = set(list(frame.columns)) & (condcols | set(to_select) | set(list(map(self.extract_col_from_select, to_select))))
                frame = frame.select(list(curcols))
                
        
       
        
        have = self.H.split(" ")
        have = list(map(lambda x: po.fTupleToStr(po.grab_aggregates(x)[0]) if x not in self.operator_map else x, have))
        have = " ".join(have)
        hcconds, hckws, _, _ = self.parse_suchthat([have], gvlist)
        having_query = "frame.filter("
        index = 0 # global index for logical operators
        for (i, (col, conds)) in enumerate(hcconds["GV0"].items()):
            stillAgged = po.grab_aggregates(col) # sometimes there are cases that GV0 columns that are being aggregated are used in conditions
            if stillAgged is not None and len(stillAgged) > 0:
                col = stillAgged[0][1] # change the column name to reflect the actual col name
            for op, val in conds:
                if val in frame.columns:
                    val = "pl.col('" + val + "')"
                having_query += f"(pl.col('{col}') {self.operator_map[op]} {val})"
                having_query += f" {self.operator_map[hckws['GV0'][index]]} " if index < len(hckws["GV0"]) else "" # makes sure we add the correct number of logical operators. It is order-based however.
                index+=1
        
        having_query += ")"
        
        #print(having_query)
        
        frame = eval(having_query)
    
        frame = frame.select(to_select).unique()
    
        return frame