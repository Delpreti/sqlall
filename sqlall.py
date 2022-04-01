'''
SQL-all provides a standard SQL ORM to perform asynchronous connection within multiple database types.
currently supported databases: SQLite (async)

Current Version: 0.6 (under development)
'''
import aiosqlite
import os
from Managers.manager_factory import ManagerFactory

class sqlall:
    '''
    A wrapper with the same name as the module, to make things easier.
    Should not be initialized, all methods are static
    '''
    _manager_instances = None

    @staticmethod
    def _database_location(dbname='database.db', dbpath="resources/"):
        '''Retrieve the folder where the database is located.'''
        return f"{dbpath}{dbname}"

    @staticmethod
    def clear_database(dbname='database.db', dbpath="resources/"):
        '''Destroy the database file, if it exists.'''
        # this works for local files, other databases may need a different implementation
        db = sqlall._database_location(dbname, dbpath)
        if os.path.exists(db):
            os.remove(db)

    @staticmethod
    def _get_instance(dbname, dbtype, dbpath): # change implementation to call factory instead
        '''internal method to get the single database manager instance'''
        if not os.path.exists(dbpath):
            os.mkdir(dbpath)
        if sqlall._manager_instances is None:
            sqlall._manager_instances = dict()
        if dbtype not in sqlall._manager_instances:
            sqlall._manager_instances[dbtype] = ManagerFactory.load_manager(sqlall._database_location(dbname, dbpath), dbtype, dbpath)
            sqlall._manager_instances[dbtype].load_entities()
        return sqlall._manager_instances[dbtype]

    @staticmethod
    async def _get_instance_async(dbname, dbtype, dbpath): # change implementation to call factory instead
        '''internal method to get the single database manager instance'''
        if not os.path.exists(dbpath):
            os.mkdir(dbpath)
        if sqlall._manager_instances is None:
            sqlall._manager_instances = dict()
        if dbtype not in sqlall._manager_instances:
            sqlall._manager_instances[dbtype] = await ManagerFactory.load_manager_async(sqlall._database_location(dbname, dbpath), dbtype, dbpath)
            await sqlall._manager_instances[dbtype].load_entities()
        return sqlall._manager_instances[dbtype]

    @classmethod
    def manager(cls, dbname='database.db', dbtype="SQLite", dbpath="resources/"):
        '''Get the single database manager instance (use this)'''
        return cls._get_instance(dbname, dbtype, dbpath)

    @classmethod
    async def manager_async(cls, dbname='database.db', dbtype="SQLite", dbpath="resources/"):
        '''Get the single database manager instance (use this)'''
        return await cls._get_instance_async(dbname, dbtype, dbpath)

    # to be tested -> manager specific functionality!!
    @staticmethod
    async def compress_database_into(dbnewname, dbname='database.db', dbpath="resources/"):
        '''
        Compress the database file into a new file, dbnewname is mandatory.
        Implemented outside the manager, because the user must retrieve a new manager/
        for the new file after this operation is performed
        As a suggestion, your implementation may want to iterate over a number in the/
        dbname parameter, to use as reference for version control
        '''
        connection = await aiosqlite.connect(sqlall._database_location(dbname, dbpath))
        await connection.execute(f"VACUUM INTO {dbnewname}") # verificar a path
        await connection.close()
