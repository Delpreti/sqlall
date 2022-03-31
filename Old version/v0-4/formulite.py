'''
FORMUlite provides a "simple" ORM to perform asynchronous connection within an sqlite database.
currently under development.

Current Version 0.4
'''

import aiosqlite
import os
from importlib import util
import sys
from .entity import Entity
import re

def load_module(module_name, module_path, piece=None):
    '''Function to import modules dinamically'''
    # print(f"Loading module {module_name}")

    spec = util.spec_from_file_location(module_name, module_path)
    module = util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    if piece:
        return getattr(module, piece)

class formulite:
    '''
    A wrapper with the same name as the module, to make things easier.
    Should not be initialized, all methods are static
    '''
    # In case of singleton instances, place them here
    _manager_instance = None

    @staticmethod
    def clear_database(dbname='database.db'):
        '''Destroy the database file, if it exists.'''
        if os.path.exists(dbname):
            os.remove(dbname)

    @staticmethod
    async def _getInstance(dbname):
        '''internal method to get the single database manager instance'''
        if formulite._manager_instance is None:
            connection = await aiosqlite.connect(dbname)
            formulite._manager_instance = DatabaseManager(connection)
            await formulite._manager_instance._load_entities()
        return formulite._manager_instance

    @classmethod
    async def manager(cls, dbname='database.db'):
        '''Get the single database manager instance (use this)'''
        return await cls._getInstance(dbname)

class DatabaseManager:
    '''
    The main goal of this module.
    It is responsible for communicating with the SQLite database.
    '''
    def __init__(self, connection, objfile="dbobjects.py"):
        self.conn = connection
        self._typeflag = None
        self.objects_file = objfile
        self.entities = {}

    async def close(self):
        '''should be called at the end of execution'''
        await self.conn.close()

    ### SETUP OPERATIONS ###
    # these operations should initialize the ORM
    # They must be performed BEFORE any tables are created

    def set_entity(self, name, **kargs):
        '''
        The user must define the database entities
        Internally, a class for access to the table will be stored in the file dbobjects.py
        attribute names and their respective types should be provided,
        as there is no mapping between python duck typing and SQL types
        '''
        self.entities[name] = Entity(name, kargs)

    def append_primary_key(self, entity, *attrs):
        '''appends the given attributes to the given entity primary key list'''
        for att in attrs:
            self.entities[entity].primary_key.append(att)

    def set_primary_key(self, entity, *attrs):
        ''' makes the given attributes primary key for the given entity'''
        self.entities[entity].primary_key = attrs

    def set_foreign_key(self, entity, attrib, ref_entity):
        '''
        makes the given attribute a foreign key for the given entity
        If append is set to true, the attribute will be appended to the primary key
        foreign keys should only be set after primary keys are set
        '''
        self.entities[entity].foreign_key[attrib] = ref_entity
        
    def set_clear(self):
        '''Destroy the database objects file, if it exists.'''
        if os.path.exists(self.objects_file):
            os.remove(self.objects_file)

    ### CREATE ###

    async def create_tables(self):
        '''
        sends in the queries for creating all the tables predicted in the setup operations
        this operation should only be called once. To add new tables after the database is created, see add_table()
        '''
        for ename, entity in self.entities.items():
            entity.writedown(self.objects_file)
            #print(entity.create_table_query(self.objects_file, True))
            await self.conn.execute(entity.create_table_query(self.objects_file))
        await self.conn.commit()

    async def add_table(self, entity):
        '''Adds a single table to the database, entity must be generated / set separately'''
        entity.writedown(self.objects_file)
        await self.conn.execute(entity.create_table_query(self.objects_file))
        await self.conn.commit()

    ### LOAD ###
    # The manager should be able to work with existing databases, after their creation.
    # Entities should therefore be loaded in when the manager is initialized, if they exist

    def loaded(self):
        return not not self.entities

    async def _load_entities(self):
        tablenames = await self.conn.execute_fetchall(f"SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%';")
        if tablenames:
            #print(tablenames)
            for table, in tablenames:
                e_class = load_module(table.lower(), self.objects_file, table)
                self.entities[table] = Entity(table, e_class._attribute_types)

    ### BUILD ###
    # The object constructor will not be available by default, this would require a complex dynamic import
    # so build functions are provided to work as a factory for instances / table rows, which can later be included to the DB

    def build(self, tablename, **kargs): # needs improvement
        '''Calls the appropriate constructor for the corresponding table.'''
        e_class = load_module(tablename.lower(), self.objects_file, tablename)
        return e_class(**kargs)

    ### INSERT ###

    async def insert(self, Obj):
        '''Insert instance into database.'''
        c_name = Obj.__class__.__name__

        # make sure the object has not been previously inserted
        pk_dict = {}
        for pk in Obj.__class__._primary_key:
            pk_dict[pk] = getattr(Obj, pk)
        if await self.exists(c_name, **pk_dict):
            return

        vals = []
        for key in self.entities[c_name].args_dict.keys():
            vals.append( (getattr(Obj, key)) )
        #q_marks = " ?" * len(vals)
        vals_concat = ""
        for value in vals:
            if isinstance(value, str):
                vals_concat += f"\"{value}\""
            else:
                vals_concat += str(value)
            if value != vals[-1]:
                vals_concat += ", "

        keyjoin = ", ".join( self.entities[c_name].args_dict.keys() )
        sql = f"INSERT INTO {c_name} ({keyjoin}) VALUES ({vals_concat})"
        #print(sql)
        #print(vals)
        await self.conn.execute(sql)
        await self.conn.commit()

    async def build_and_insert(self, tablename, **kargs):
        '''Insert instance into database right after instantiation, then returns it'''
        obj = self.build(tablename, **kargs)
        await self.insert(obj)
        return obj

    ### UPDATE ###

    async def update(self, Obj):
        '''Update a database instance (single row)'''
        c_name = Obj.__class__.__name__

        pk_list = []
        for pk in Obj.__class__._primary_key:
            pk_list.append(f"{pk}={getattr(Obj, pk)}")
        cond_string = " AND ".join(pk_list)        

        up_list = []
        for attribute in Obj.__class__._attribute_types.keys():
            if attribute not in Obj.__class__._primary_key:
                up_list.append(f"{attribute}={getattr(Obj, attribute)}")
        set_string = ", ".join(up_list)

        sql = f"UPDATE {c_name} SET {set_string} WHERE {cond_string}"
        #print(sql)
        await self.conn.execute(sql)
        await self.conn.commit()

    ### SELECT ###

    async def exists(self, tablename, **kargs):
        '''
        check if an object already exists in the database
        only use this if you dont need the returned object further in your application
        '''
        obj_list = await self.select_from(tablename, **kargs)
        return len(obj_list) > 0

    async def select_from(self, tablename, **kargs):
        '''Returns a list of objects from the database that match the passed in conditions, if any'''
        sql = f"SELECT * FROM {tablename}"
        if kargs:
            conditions = []
            for key, val in kargs.items():
                if isinstance(val, str):
                    conditions.append(f"{key}=\'{val}\'")
                else:
                    conditions.append(f"{key}={val}")
            joined_conditions = " AND ".join(conditions)
            sql += f" WHERE {joined_conditions}"
        rows = await self.conn.execute_fetchall(sql)
        
        result = []
        for row in rows:
            arg_dict = dict( zip(self.entities[tablename].args_dict.keys(), row) )
            result.append(self.build(tablename, **arg_dict))

        return result

    async def select_ordered(self, tablename, order_by=None, **kargs):
        '''Returns a list of objects from the database that match the passed in conditions, if any'''
        sql = f"SELECT * FROM {tablename}"
        if kargs:
            conditions = []
            for key, val in kargs.items():
                if isinstance(val, str):
                    conditions.append(f"{key}=\'{val}\'")
                else:
                    conditions.append(f"{key}={val}")
            joined_conditions = " AND ".join(conditions)
            sql += f" WHERE {joined_conditions}"
        if order_by:
            sql += f" ORDER BY {order_by}"
        rows = await self.conn.execute_fetchall(sql)
        
        result = []
        for row in rows:
            arg_dict = dict( zip(self.entities[tablename].args_dict.keys(), row) )
            result.append(self.build(tablename, **arg_dict))

        return result

    async def count(self, tablename, **kargs):
        '''Returns the count of rows from the database that match the passed in conditions, if any'''
        sql = f"SELECT count(*) FROM {tablename}"
        if kargs:
            conditions = []
            for key, val in kargs.items():
                if isinstance(val, str):
                    conditions.append(f"{key}=\'{val}\'")
                else:
                    conditions.append(f"{key}={val}")
            joined_conditions = " AND ".join(conditions)
            sql += f" WHERE {joined_conditions}"
        count_list = await self.conn.execute_fetchall(sql)

        return count_list[0][0]
