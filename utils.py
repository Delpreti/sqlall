'''
Wrapper for sql-all utilities.
'''
from importlib import util
import sys
from clauses import *

class utils:
    '''
    A wrapper for some utilities provided in this module,
    most come from clauses.py and should be accessed through here.
    Should not be initialized, all methods are static
    '''
    @staticmethod
    def where(compose=False, separator=" AND ", **kargs):
        return Where(compose, separator, **kargs)

    @staticmethod
    def order_by(column_name, direction="asc"):
        return Order_by(column_name, direction)

    @staticmethod
    def limit(number):
        return Limit(number)

    @staticmethod
    def select_query(tables_obj, cols_obj="*", *args):
        return Select_query(tables_obj, cols_obj, *args)

    @staticmethod
    def join(table_A, table_B):
        return Join(table_A, table_B)

    @staticmethod
    def load_module(module_name, module_path, piece=None):
        '''Function to import modules dinamically'''
        # print(f"Loading module {module_name}")
        spec = util.spec_from_file_location(module_name, module_path)
        module = util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        if piece:
            return getattr(module, piece)
