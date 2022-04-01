'''
Class used to instantiate the appropriate DatabaseManager
'''
from utils import utils as sql_utils
import aiosqlite

class ManagerFactory:
    '''
    Class used to instantiate the appropriate DatabaseManager
    (maybe I just need a factory method, maybe merge this with utils.py)
    '''
    @staticmethod
    def load_manager(database_location, database_type, filepath="resources/"):
        '''Calls the appropriate constructor for the corresponding manager implementation.'''
        connection = sqlite3.connect(database_location)

        module_name = "manager_" + database_type.lower()
        manager = sql_utils.load_module(module_name, f"Managers/{module_name}.py", "Manager" + database_type)
        return manager(connection, filepath)

    @staticmethod
    async def load_manager_async(database_location, database_type, filepath="resources/"):
        '''Calls the appropriate constructor for the corresponding manager implementation.'''
        connection = await aiosqlite.connect(database_location) # connection should have separate implementations
        module_name = "manager_" + database_type.lower()
        manager = sql_utils.load_module(module_name, f"Managers/{module_name}.py", "Manager" + database_type)
        return manager(connection, filepath)
 