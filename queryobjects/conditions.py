
class Cols:
    '''column names from SQL represented as an object'''
    def __init__(self, cols_string):
        self.colstr = cols_string

    def __str__(self):
        return self.colstr

class Where:
    '''WHERE clause from SQL represented as an object'''
    def __init__(self, conditions_string):
        self.condstr = conditions_string

    def __str__(self):
        return f"WHERE {self.condstr}"

class Order_by:
    '''ORDER BY clause from SQL represented as an object'''
    def __init__(self, cols_obj, direction='asc'):
        self.cols = cols_obj
        self.dir = direction

    def __str__(self):
        return f"ORDER BY {str(self.cols)} {self.dir}"

class Limit:
    '''LIMIT clause from SQL represented as an object'''
    def __init__(self, number):
        self.amount = number
    
    def __str__(self):
        return f"LIMIT {self.amount}"
