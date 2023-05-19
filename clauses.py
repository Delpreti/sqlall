'''
Helper classes that should be used to correctly form the statement of a SELECT query.

They MUST redefine the __str__(self): method in order to provide the correct statement
Obs.: the string can also be passed into the query directly,
and it should be enough for simple situations

Also, these objects must be passed in the correct order. ## TODO: check the order
'''

class Where:
    '''WHERE clause from SQL represented as an object'''
    def __init__(self, compose=False, separator=" AND ", **kargs):
        self.sep = separator
        self.condstr = ""
        if compose:
            cond_list = [wobj.condstr for wobj in kargs.values()]
            self.condstr = separator.join(cond_list)
        else:
            pairs = []
            for key, value in kargs.items():
                if isinstance(value, str):
                    pairs.append(f"{key}=\'{value}\'")
                else:
                    pairs.append(f"{key}={value}")
            self.condstr = separator.join(pairs)

    def __str__(self):
        return f"WHERE {self.condstr}"

class Limit:
    '''LIMIT clause from SQL represented as an object'''
    def __init__(self, amount):
        self.amount = amount

    def __str__(self):
        return f"LIMIT {self.amount}"

class Order_by:
    '''ORDER BY clause from SQL represented as an object'''
    def __init__(self, col_name, direction="asc"):
        self.col = col_name
        if direction not in ["asc", "desc"]:
            direction = "asc"
        self.dir = direction

    def __str__(self):
        return f"ORDER BY {self.col} {self.dir}"

class Join:
    '''JOIN clause from SQL represented as an object'''
    def __init__(self, table_A, table_B):
        self.table_A = table_A
        self.table_B = table_B
        self.join_condition = ""
        for key in table_A.foreign_key:
            if key in table_B.primary_key:
                self.join_condition += f"{table_A}.{key}={table_B}.{key}"
                break # only one condition for join

    def __str__(self):
        if self.join_condition != "":
            return f"INNER JOIN {self.table_B.e_name} ON {self.join_condition}"
        return f"JOIN {self.table_B.e_name}"

## Statements -> separate on another file
## [insert, select, update, delete] are necessary for triggers

class Select_query:
    '''
    A complete SELECT statement, represented as an object
    Used for repeated select calls, and for view creation
    '''
    def __init__(self, tables_obj, cols_obj="*", *args):
        # I should perform some kind of type checking here, and throw an error if needed
        self.sql = f"SELECT {str(cols_obj)} FROM {str(tables_obj)}"
        for arg in args:
            self.sql += f" {str(arg)}" # whitespace is relevant here

    def __str__(self):
        return self.sql
