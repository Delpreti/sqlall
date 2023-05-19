'''
Async manager for SQLite databases
'''
from entity import Entity
from utils import utils as sql_utils
from Managers.database_manager import DatabaseManager

class ManagerSQLite(DatabaseManager):

    def __init__(self, connection, filepath="resources/"):
        super(ManagerSQLite, self).__init__(connection, filepath)

    async def close(self):
        '''should be called at the end of execution'''
        #await await self.conn.execute("PRAGMA optimize;")
        await self.conn.close()

    ### CREATE ###

    def _create_table_query(self, entity, readable=False):
        '''returns the query used for table creation'''
        endl = " "
        if readable:
            endl = "\n"
        sql = f"CREATE TABLE IF NOT EXISTS {entity.e_name}({endl}"
        for key, value in entity.args_dict.items():
            sql += f"{key} {value},{endl}"
        #pkeys = ", ".join(self.primary_key)
        sql += f"PRIMARY KEY({entity.joined_primary_key(entity.primary_key)})"
        if entity.foreign_key:
            for key, value in entity.foreign_key.items():
                sql += F",{endl}FOREIGN KEY ({key}) REFERENCES {value} ({key})"
        return sql + f"{endl})"

    async def create_tables(self):
        '''
        sends in the queries for creating all the tables predicted in the setup operations
        this operation should only be called once. To add new tables after the database is created, see add_table()
        '''
        for entity in self.entities.values():
            entity.writedown(self.file_path)
            #print(entity._create_table_query(None, True))
            await self.conn.execute(self._create_table_query(entity))
        await self.conn.commit()

    async def add_table(self, entity):
        '''Adds a single table to the database, entity must be generated / set separately'''
        entity.writedown(self.file_path)
        await self.conn.execute(self._create_table_query(entity))
        await self.conn.commit()

    async def create_view(self, view_name, select_obj):
        '''
        Creates a view
        The select_obj is a non-executed select statement, built using sql_utils.select_query()
        '''
        sql = f"CREATE VIEW IF NOT EXISTS {view_name} AS {str(select_obj)}"
        await self.conn.execute(sql)
        await self.conn.commit()

    ### USER OPERATIONS ###
    # Consider if its worthy over having your own user system

    async def create_user(self, user_name):
        '''Creates a user'''
        pass

    async def grant(self, user_name):
        '''Grants a privilege to a user'''
        pass

    async def revoke(self, user_name):
        '''Revokes a privilege from a user'''
        pass

    ### LOAD ###
    # The manager should be able to work with existing databases, after their creation.
    # Entities should therefore be loaded in when the manager is initialized, if they exist

    async def load_entities(self):
        tablenames = await self.conn.execute_fetchall(f"SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%';")
        if tablenames:
            #print(tablenames)
            for table, in tablenames:
                e_class = sql_utils.load_module(table.lower(), self.file_path + Entity.get_filename(table), table)
                self.entities[table] = Entity(table, e_class._attribute_types)

    ### INSERT ###

    async def insert(self, Obj, replace=False):
        '''
        Insert instance into database.
        Will fail by default if the instance is already inserted,
        unless replace is set to true
        '''
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
        vals_concat = ""
        for value in vals:
            if isinstance(value, str):
                vals_concat += f"\"{value}\""
            else:
                vals_concat += str(value)
            if value != vals[-1]:
                vals_concat += ", "

        keyjoin = ", ".join( self.entities[c_name].args_dict.keys() )

        command = f"INSERT"
        if replace:
            command = "REPLACE"

        sql = f"{command} INTO {c_name} ({keyjoin}) VALUES ({vals_concat})"
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
        await self.conn.execute(sql)
        await self.conn.commit()

    ### SELECT ###

    async def exists(self, tablename, **kargs):
        '''
        check if an object already exists in the database
        only use this if you dont need the returned object further in your application
        '''
        obj_count = await self.count(tablename, sql_utils.where(**kargs))
        return obj_count > 0

    async def _select(self, tables_obj, cols_obj="*", *args):
        '''Use select_from() to select items from a database'''
        # I should perform some kind of type checking here, and throw an error if needed
        sql = f"SELECT {str(cols_obj)} FROM {str(tables_obj)}"
        for arg in args:
            sql += f" {str(arg)}" # whitespace is relevant here
        return await self.conn.execute_fetchall(sql)

    async def select_from(self, tables_obj, cols_obj="*", *args):
        '''
        Returns a list of objects from the database that match the passed in conditions, if any.
        User must know some prior SQL to write the proper query (args should be in correct order)
        A view is also a valid argument to pass as tables_obj
        '''
        tablename = str(tables_obj)
        rows = await self._select(tables_obj, cols_obj, *args)

        result = []
        for row in rows:
            arg_dict = dict( zip(self.entities[tablename].args_dict.keys(), row) )
            result.append(self.build(tablename, **arg_dict))

        return result

    async def select_all_from(self, tables_obj, *args):
        '''Helper'''
        return await self.select_from(tables_obj, "*", *args)

    async def count(self, tables_obj, *args):
        '''Helper for selecting the count of rows from a given table'''
        count_tuple = await self._select(tables_obj, "count(*)", *args)
        return count_tuple[0][0]

    ######## ALL FUNCTIONS BELOW THIS POINT ARE UNTESTED !!! some are not even done yet

    ### DROP / DELETE ### 

    async def drop_table(self, tablename):
        '''Delete a table from the database'''
        sql = f"DROP TABLE {tablename}"
        await self.conn.execute(sql)
        await self.conn.commit()

    async def drop_tables(self, *tables):
        '''Helper to delete multiple tables'''
        for t in tables:
            await self.drop_table(t)

    async def reset(self):
        '''Erases all tables, but keeps the file. See sqlall.clear_database()'''
        await self.drop_tables(*list(self.entities.keys()))

    async def delete_table_contents(self, tablename):
        '''Delete all contents within a table, the schema is preserved'''
        sql = f"DELETE FROM {tablename}"
        await self.conn.execute(sql)
        await self.conn.commit()

    async def clear_contents(self):
        '''Delete all database contents, preserving the schemas'''
        for entity in self.entities.keys():
            self.delete_table_contents(entity)

    ### ALTER ###
    # These should be used with caution
    # Upon altering a table structure, the object structure should be manually altered to match it
    # Remember to update rows / reload objects after using these methods

    async def add_column(self, tablename, col_name, col_type):
        '''Adds a new column to a table.'''
        self.entities[tablename].add_attribute(col_name, col_type, self.file_path)
        sql = f"ALTER TABLE {tablename} ADD {col_name} {col_type}"
        await self.conn.execute(sql)
        await self.conn.commit()

    async def add_columns(self, tablename, **columns):
        '''Adds multiple columns to a table, in a more pythonic syntax'''
        for key, value in columns.items():
            await self.add_column(tablename, key, value)

    async def drop_column(self, tablename, column):
        '''
        Removes a column / attribute from a table.
        Only supported in newer versions of sqlite
        '''
        # verify if column is part of primary_key before removal

        # adjust the constructor in the corresponding object file

        # only now drop the column in the database
        sql = f"ALTER TABLE {tablename} DROP COLUMN {column}"
        await self.conn.execute(sql)
        await self.conn.commit()

    async def rename_table(self, tablename, new_tablename):
        '''
        Renames a table/entity
        Objects for this table will be instantiated with the new name within select and other functionalities
        but the previous class definition will be preserved
        Old instances will carry the old table name, however. They must be manually updated, if needed.
        '''
        # I need to check if new_tablename is available
        entity = self.entities.pop(tablename)
        entity.change_name(new_tablename)
        self.entities[new_tablename] = entity

        entity.writedown(self.file_path)

        sql = f"ALTER TABLE {tablename} RENAME TO {new_tablename}"
        await self.conn.execute(sql)
        await self.conn.commit()

    async def rename_column(self, tablename, col_name, new_col_name):
        '''
        Renames a column on a table.
        Existing instances will not have their attributes automatically renamed.
        '''
        # verify if its a primary / foreign key

        # need to rename inside the obects aswell
        sql = f"ALTER TABLE {tablename} RENAME COLUMN {col_name} TO {new_col_name}"
        await self.conn.execute(sql)
        await self.conn.commit()

    ### EVENTS ### (not sure if these should be implemented)
    # In future implementations, these might trigger changes accross the whole application, not only on the database.

    async def create_trigger(self, trigger_name, before_after, event, target_table, action):
        '''
        Add an action to be executed when a certain condition is met
        the action passed in sould be a lambda function containing one of the implemented queries. (or the sql_utils query object?)
        '''
        sql = f"CREATE TRIGGER IF NOT EXISTS {trigger_name} {before_after}"
        pass

    ### INDEX ### (maybe implement on a future version?)
    # create, drop, reindex

    ### NOT IMPLEMENTED ### A decision was made, for these not to be implemented. It may change in the future.
    # Attach and Detach
    # Begin/Commit/Rollback and Savepoint/Release are done automatically (intended for user specified behaviour)
    # [EXPLAIN, ON CONFLICT, UPSERT, RETURNING]
    # PRAGMA

    ## Aggregation functions can theoretically be done on the user side. These are the most likely to get implemented in the near future.
