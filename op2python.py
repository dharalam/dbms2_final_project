'''
Based on the Phi Operator structure, construct python queries to handle the row selection.
The result of the queries should be combined to create the result of the full ESQL query.
'''

class op2python:
    def __init__(self, phiOp):
        self.S = phiOp["S"]
        self.N = phiOp["N"]
        self.V = phiOp["V"]
        self.F = phiOp["F"]
        self.R = phiOp["R"]
        self.H = phiOp["H"]
        self.queries = []
    
    def construct_queries(self):
        # Construct queries based on the Phi Operator structure
        # Should store each query as a string in the self.queries list
        # Will be executed in the body section of the generator
        if self.queries is not None and len(self.queries) > 0:
            return self.queries
        pass
    
    








