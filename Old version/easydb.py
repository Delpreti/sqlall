import sqlite3
import os
from pydantic import BaseModel

# easyDB provides a simple interface to execute CRUD functions within a sqlite database.
# It is supposed to support object oriented programs by taking objects as function arguments directly.
# currently under development, not recommended for professional purposes.

# All object attributes should have type annotations and default values (None is allowed).

# Current Version 0.1
# Dependencies: Pydantic, sqlite3
# Should be migrating to async. SOON!

class easydb:

    typedict = {"str":"VARCHAR(50)",
                "Optional[str]":"VARCHAR(50)",
                "int":"INTEGER",
                "Optional[int]":"INTEGER"}

    _manager_instance = None # This is a singleton.
    
    @staticmethod
    def _getInstance():
        if easydb._manager_instance == None:
            easydb._manager_instance = DatabaseManager()
        return easydb._manager_instance

    @classmethod
    def manager(cls):
        return cls._getInstance()

class DatabaseManager:
    def __init__(self):
        self.conn = sqlite3.connect('database.db')
        self.c = self.conn.cursor()

    def __del__(self):
        self.c.close()
        self.conn.close()

    # Auxiliary internal functions
    def _tablename(self, Obj):
        return Obj.__name__.lower() + "s" # s makes it plural, a dumb yet effective solution

    def _table_nonexistent(self, tname):
        self.c.execute(f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{tname}'")
        if self.c.fetchone()[0] == 1:
            return False
        return True

    def _objprops(self, Obj):
        out = {}
        chaves = list(Obj.__fields__.keys())
        tipos = list(Obj.__fields__.values())
        for i in range(len(chaves)):
            out.update( {chaves[i]:str(tipos[i]._type_display())} )
        return out

    def _translate_type(self, pytype):
        return easydb.typedict[pytype]

    # CREATE
    def create_table(self, Obj): # This requires a mapping between python types and sqlite types to be done before instantiation. Pydantic makes it possible.
        props = self._objprops(Obj)
        sql = f"CREATE TABLE IF NOT EXISTS {self._tablename(Obj)} ({Obj.__name__.lower()}_id INTEGER PRIMARY KEY"

        # check for superclass
        if Obj.__Bases__ != (BaseModel, ):
            super_Obj, = Obj.__Bases__ # The comma is going to unpack the tuple
            sup_name = f"{super_Obj.__name__.lower()}"
            sql += f", {sup_name}_id INTEGER NOT NULL"
            foreign = f"FOREIGN KEY ({sup_name}_id) REFERENCES {self._tablename(super_Obj)} ({sup_name}_id) ON UPDATE CASCADE ON DELETE CASCADE"

        for key, value in list(props.items()):
            sql += f", {key} {self._translate_type(value)}"

        if Obj.__Bases__ != (BaseModel, ):
            sql += foreign

        sql += " )"
        self.c.execute(sql)
        self.conn.commit()

    # READ
    def search(self, Obj, exact=True, **kargs):
        att = vars(Obj()).keys()
        rows = ", ".join(att) # Maybe could substitute for .Row object (read documentation)
        sql = f"SELECT {rows} FROM {self._tablename(Obj)}"
        if kargs:
            key, value = list(kargs.items())[0]
            if not exact:
                self.c.execute(sql + f" WHERE {key} LIKE ?", (f"%{value}%",))
            else:
                self.c.execute(sql + f" WHERE {key} = {value}")
            fetch = self.c.fetchall()
        else:
            self.c.execute(sql)
            fetch = self.c.fetchall()

        out = []
        for result in fetch:
            attrib = list(att) # att is a dict_keys object, so the loop would fail without this conversion
            aux = {}
            for i in range(len(attrib)):
                aux.update( {attrib[i] : result[i]} )
            out.append(Obj(**aux))
        return out

    # UPDATE
    def add_one(self, instance):
        # check for superclass
        Obj = instance.__class__
        if self._table_nonexistent(self._tablename(Obj)):
            create_table(Obj)
        attrib = list(vars(instance).keys())
        sql = f"INSERT INTO {self._tablename(Obj)} ({", ".join(attrib)}) VALUES ({", ".join(["?"] * len(attrib))})"
        parsed_instance = tuple(vars(instance).values())
        self.c.execute(sql, parsed_instance)
        self.conn.commit()

    def add_many(self, instance_list):
        # check for superclass
        if instance_list == []:
            return
        Obj = instance_list[0].__class__
        if self._table_nonexistent(self._tablename(Obj)):
            create_table(Obj)
        attrib = list(vars(instance_list[0]).keys())
        sql = f"INSERT INTO {self._tablename(Obj)} ({", ".join(attrib)}) VALUES ({", ".join(["?"] * len(attrib))})"
        parsed_list = [tuple(vars(instance).values()) for instance in instance_list]
        self.c.executemany(sql, parsed_list)
        self.conn.commit() 

    def update_attribute(self, Obj, cname, change): # change should be a {old:new} dictionary
        self.c.execute(f"UPDATE {self._tablename(Obj)} SET {cname}={list(change.values())[0]} WHERE {cname} is {list(change.keys())[0]}")
        self.conn.commit()

    c.execute('''UPDATE pokemons
            SET evospecies='Bulbasaur', evoname='Bulbasaur'
            WHERE card_id=88''')

    update_item(Pokemon(evospecies='Bulbasaur', evoname='Bulbasaur'), old_item)

    def update_item(self, instance, previous_instance):
        Obj = instance.__class__
        self.c.execute(f"UPDATE {self._tablename(Obj)} SET {instance} WHERE {card_id}={list(change.keys())[0]}")
        self.conn.commit()

    # DELETE
    def clear_database(self): # Destroy the file database.db
        os.remove("database.db")

