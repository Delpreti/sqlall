import aiosqlite
import os
from pydantic import BaseModel
import inspect

# FORMUlite provides a simple ORM to perform asynchronous connection within an sqlite database.
# currently under development, there are probably several bugs just waiting to be found.

# Current Version 0.1
# Dependencies: Pydantic, aiosqlite

# Next features: tokens for safe connections, let the user name his own database, 'delete' functions, custom sql functions.

class formulite:
    typedict = {"str":"TEXT",
                "Optional[str]":"TEXT",
                "int":"INT",
                "Optional[int]":"INT"}

    _manager_instance = None # This is a singleton.

    # Using vars() on a pydantic class is kinda frustrating.
    # The couple next methods are a way to go around it.
    # They also remove the base class attributes that would be returned.
    @staticmethod
    def vars_keys(Obj): # for instances, use .__class__ to call this method.
        Sup = Obj.mro()[1]
        this = list(Obj.__fields__.keys())
        those = list(Sup.__fields__.keys())
        for item in those:
            this.remove(item)
        return this

    @staticmethod
    def vars_values(instance): # using an empty constructor will retrieve the class default values.
        Sup = instance.__class__.mro()[1]
        unwanted = list(Sup().__dict__.values())
        ret = list(instance.__dict__.values())
        for _ in range(len(unwanted)):
            ret.pop(0)
        return ret

    @staticmethod
    def clear_database(): # Destroy the file database.db, if it exists
        if os.path.exists("database.db"):
            os.remove("database.db")

    @staticmethod
    async def _getInstance():
        if formulite._manager_instance == None:
            connection = await aiosqlite.connect('database.db')
            formulite._manager_instance = DatabaseManager(connection)
        return formulite._manager_instance

    @classmethod
    async def manager(cls):
        return await cls._getInstance()

class DatabaseManager:
    def __init__(self, connection):
        self.conn = connection

    # should be called at the end of execution
    async def close(self):
        await self.conn.close()

    # Auxiliary internal functions
    def _tablename(self, Obj):
        return Obj.__name__.lower() + "s" # s makes it plural, a dumb yet effective solution

    async def _table_nonexistent(self, tname):
        c = await self.conn.execute(f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{tname}'")
        test, = await c.fetchone()
        if test == 1:
            return False
        return True

    def _objprops(self, Obj):
        out = {}
        chaves = list(Obj.__fields__.keys())
        tipos = list(Obj.__fields__.values())
        for i in range(len(chaves)):
            out.update( {chaves[i]:str(tipos[i]._type_display())} )

        # This method must not return base class attributes
        Sup = Obj.mro()[1]
        if Sup != BaseModel:
            # So we remove them here
            for k, v in self._objprops(Sup).items():
                del out[k]

        return out

    def _translate_type(self, pytype):
        return formulite.typedict[pytype]

    def _parse_instance(self, instance, cursor, Supper):
        vals = formulite.vars_values(instance)
        if Supper != BaseModel:
            vals.insert(0, cursor.lastrowid)
        return tuple(vals)

    async def _extract_from_instance(self, Sup, instance): # didn't neeeeed to be async but wynaut
        sup_args = {}
        y = list(vars(instance).values())
        x = list(vars(Sup()).keys())
        for i in range(len(x)):
            sup_args.update({ x[i]:y[i] })
        return Sup(**sup_args)

    # CREATE
    async def create_table(self, Obj): # This requires a mapping between python types and sqlite types to be done before instantiation. Pydantic makes it possible.
        props = self._objprops(Obj)
        sql = f"CREATE TABLE IF NOT EXISTS {self._tablename(Obj)} ( {Obj.__name__.lower()}_id INTEGER PRIMARY KEY"

        Sup = Obj.mro()[1]
        sup_name = f"{Sup.__name__.lower()}"
        if Sup != BaseModel:
            sql += f", {sup_name}_id INTEGER" # NOT NULL"

        for key, value in list(props.items()):
            sql += f", {key} {self._translate_type(value)}"

        # check for superclass (again)
        if Sup != BaseModel:
            sql += f''', FOREIGN KEY ({sup_name}_id) REFERENCES {self._tablename(Sup)} ({sup_name}_id) ON UPDATE CASCADE ON DELETE CASCADE'''

        sql += " )"
        await self.conn.execute(sql)
        await self.conn.commit()

    # READ
    async def search(self, Obj, exact=True, **kargs):
        # if you also want to retrieve the superclass attributes, use search_joined instead.
        att = formulite.vars_keys(Obj)
        rows = ", ".join(att) # Maybe could substitute for .Row object (read documentation)
        sql = f"SELECT {rows} FROM {self._tablename(Obj)}"
        fetch = None
        if kargs:
            key, value = list(kargs.items())[0]
            c = None
            if not exact:
                c = await self.conn.execute(sql + f" WHERE {key} LIKE ?", (f"%{value}%",))
            else:
                c = await self.conn.execute(sql + f" WHERE {key} = ?", (value, ))
            fetch = await c.fetchall()
        else:
            fetch = await self.conn.execute_fetchall(sql)

        out = []
        for result in fetch:
            aux = {}
            for i in range(len(att)):
                aux.update( {att[i] : result[i]} )
            out.append(Obj(**aux))
        return out

    async def search_joined(self, Obj, exact=True, **kargs): # joins subclass tables and then searches.
        att = formulite.vars_keys(Obj)
        rows = ", ".join(att)

        sql = f"SELECT * FROM {self._tablename(Obj)}"

        joined_columns = [f"{Obj.__name__.lower()}_id"]
        joined_columns.extend(formulite.vars_keys(Obj))

        for subc in Obj.__subclasses__():
            sql += f" LEFT JOIN {self._tablename(subc)} USING ({Obj.__name__.lower()}_id)"
            joined_columns.extend([f"{subc.__name__.lower()}_id"])
            joined_columns.extend(formulite.vars_keys(subc))

        placeholders = []
        if kargs:
            sql += " WHERE "
            conditionals = []
            for key, value in kargs.items():
                if not key:
                    raise ValueError("Invalid keyword argument.")
                if exact:
                    conditionals.append(f"{key} = ?")
                    placeholders.append(value)
                else:
                    conditionals.append(f"{key} LIKE ?")
                    placeholders.append(f"%{value}%")
            sql += " AND ".join(conditionals)

        fetch = await self.conn.execute_fetchall(sql, tuple(placeholders))

        results = []
        for result in fetch:
            ret = dict((joined_columns[i], result[i]) for i in range(len(joined_columns)))
            for subc in Obj.__subclasses__():
                if ret.get(f"{subc.__name__.lower()}_id"):
                    results.append(subc(**ret))
        return results

    # UPDATE
    async def add_one(self, instance, propagate=False, _cursed=None):
        Obj = instance.__class__
        if await self._table_nonexistent(self._tablename(Obj)):
            await self.create_table(Obj)
        attrib = formulite.vars_keys(Obj)
        
        c = _cursed
        if c == None:
            c = await self.conn.cursor()

        Sup = Obj.mro()[1]
        if Sup != BaseModel:
            attrib.insert(0, f"{Sup.__name__.lower()}_id")
            if propagate:
                before = await self._extract_from_instance(Sup, instance)
                await self.add_one(before, _cursed=c)

        attributes = ", ".join(attrib)
        interr = ", ".join(["?"] * len(attrib))
        sql = f"INSERT INTO {self._tablename(Obj)} ({attributes}) VALUES ({interr})"
        
        await c.execute(sql, self._parse_instance(instance, c, Sup))
        await self.conn.commit()
        if c != _cursed:
            await c.close()

    async def add_many(self, instance_list, propagate=False):
        for instance in instance_list:
            await self.add_one(instance, propagate)

    async def _add_many(self, instance_list, propagate=False):
        if instance_list == []:
            # should raise something here
            return
        Obj = instance_list[0].__class__
        if await self._table_nonexistent(self._tablename(Obj)):
            await self.create_table(Obj)
        attrib = formulite.vars_keys(Obj)

        Sup = Obj.mro()[1]
        if Sup != BaseModel:
            attrib.insert(0, f"{Sup.__name__.lower()}_id")
            if propagate:
                before = [await self._extract_from_instance(Sup, instance) for instance in instance_list]
                await self.add_many(before)

        attributes = ", ".join(attrib)
        interr = ", ".join(["?"] * len(attrib))
        sql = f"INSERT INTO {self._tablename(Obj)} ({attributes}) VALUES ({interr})"

        c = await self.conn.cursor()
        parsed_list = [self._parse_instance(instance, c, Sup) for instance in instance_list]
        await c.executemany(sql, parsed_list)
        await self.conn.commit()
        await c.close()

    async def update_attribute(self, Obj, cname, change): # change should be a {old:new} dictionary
        await self.conn.execute(f"UPDATE {self._tablename(Obj)} SET {cname}={list(change.values())[0]} WHERE {cname} is {list(change.keys())[0]}")
        await self.conn.commit()

    async def update_item(self, instance, **kargs): # pass in an updated instance and the old values go in kargs
        Obj = instance.__class__
        attrib = formulite.vars_keys(Obj)
        vals = formulite.vars_values(instance)
        it = 0
        for _ in range(len(attrib)):
            if vals[it] == None:
                vals.pop(it)
                attrib.pop(it)
            else:
                it += 1
        changes = [f"{attrib[i]}='{vals[i]}'" for i in range(len(attrib))]
        changes_string = ", ".join(changes)
        k = list(kargs.keys())[0]
        v = list(kargs.values())[0]
        await self.conn.execute(f"UPDATE {self._tablename(Obj)} SET {changes_string} WHERE {k}={v}")
        await self.conn.commit()

    async def update_item_joined(self, Sup, instance, **kargs):
        pass # yet to be implemented 

    # DELETE
    # async def remove_item(self, instance): # yet to be implemented

