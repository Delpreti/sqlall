'''
Main sql-all class, used to interact with the database.
Use the factory to obtain a manager for a specific domain.

    This class is abstract, it will depend on specific DB implementations.
    Classes that inherit from this must implement the following methods:

    ### CONNECTION OPERATIONS ###
    def close(self):

    ### CREATE ###
    def create_tables(self):
    def add_table(self, entity):
    def create_view(self, view_name, select_obj):

    ### USER OPERATIONS ###
    def create_user(self, user_name):
    def grant(self, user_name):
    def revoke(self, user_name):

    ### LOAD ###
    def load_entities(self):

    ### INSERT ###
    def insert(self, Obj, replace=False):
    def build_and_insert(self, tablename, **kargs): # helper

    ### UPDATE ###
    def update(self, Obj):

    ### SELECT ###
    def exists(self, tablename, **kargs): # helper
    def select_from(self, tables_obj, cols_obj="*", *args):
    def select_all_from(self, tables_obj, *args):
    def count(self, tables_obj, *args):

    ### DROP / DELETE ### 
    def drop_table(self, tablename):
    def drop_tables(self, *tables):
    def reset(self):
    def delete_table_contents(self, tablename):
    def clear_contents(self):

    ### ALTER ###
    def add_column(self, tablename, col_name, col_type):
    def add_columns(self, tablename, **columns): # helper
    def drop_column(self, tablename, column):
    def rename_table(self, tablename, new_tablename):
    def rename_column(self, tablename, col_name, new_col_name):

    ### EVENTS ###
    def create_trigger(self, trigger_name, before_after, event, target_table, action):

    ### INDEX ### (maybe implement on a future version?)
    # create, drop, reindex

    ### NOT IMPLEMENTED ### A decision was made, for these not to be implemented. It may change in the future.
    # Attach and Detach
    # Begin/Commit/Rollback and Savepoint/Release are done automatically (intended for user specified behaviour)
    # [EXPLAIN, ON CONFLICT, UPSERT, RETURNING]
    # PRAGMA

    ## Aggregation functions can theoretically be done on the user side. These are the most likely to get implemented in the near future.

'''

from entity import Entity
import os
from utils import utils as sql_utils

class DatabaseManager:
    '''
    Abstract class responsible for communicating with the database.
    '''
    def __init__(self, connection, filepath="resources/"):
        self.conn = connection
        self._typeflag = None
        self.file_path = filepath
        self.entities = {}

    def set_entity(self, name, **kargs):
        '''
        The user must define the database entities
        Internally, a class for access to the table will be stored in the file dbobjects.py
        attribute names and their respective types should be provided,
        as there is no mapping between python duck typing and SQL types
        '''
        self.entities[name] = Entity(name, kargs)

    def get_table_object(self, name):
        '''
        Return a database entity object
        Used for metadata processing of statements
        '''
        return self.entities[name]

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
        
    def set_clear(self, entity_obj):
        '''Destroy the database objects file, if it exists.'''
        if os.path.exists(entity_obj.auto_filename()):
            os.remove(entity_obj.auto_filename())

    def set_clear_all(self):
        '''Destroy all database objects files'''
        for entity in self.entities.values():
            self.set_clear(entity)

    def loaded(self):
        '''Returns True if entities have been successfully loaded'''
        return not not self.entities

    ### BUILD ###
    # The object constructor will not be available by default, this would require a complex dynamic import
    # so this build method is provided to work as a factory for instances / table rows, which can later be included to the DB

    def build(self, tablename, **kargs):
        '''Calls the appropriate constructor for the corresponding table.'''
        e_class = sql_utils.load_module(tablename.lower(), self.file_path + Entity.get_filename(tablename), tablename)
        return e_class(**kargs)
