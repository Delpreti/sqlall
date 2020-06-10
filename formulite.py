import aiosqlite
import os, importlib
from pydantic import BaseModel
import inspect
import math, re

# FORMUlite provides a simple ORM to perform asynchronous connection within an sqlite database.
# currently under development, there are probably several bugs just waiting to be found.

# Current Version 0.3
# Dependencies: Pydantic, aiosqlite, async-property

# Next features: tokens for safe connections, 'delete/remove' functions, more custom sql functions (maybe), allowing typedict customization, support for multiple inheritance

class formulite:
    typedict = {"str":"TEXT",
                "Optional[str]":"TEXT",
                "int":"INT",
                "Optional[int]":"INT"}

    # Here goes some singleton classes
    _manager_instance = None

    # Using vars() on a pydantic class can be kinda frustrating.
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
    def paged_list(page_size, item_list, wrap=True):
        return Paged_list(page_size=page_size, item_list=item_list, wrap=wrap)

    # Note that the album turns out to be an "async constructor" because of its dependencies,
    # in contrast with paged_list. This partially hides its implementation
    @staticmethod
    async def album(page_size, album_Type, **kwargs):
        man = await formulite.manager()
        return Album(page_size=page_size, manage=man, album_Type=album_Type, **kwargs)

    # I thought of making this a singleton, but the user may want multiple instances
    @staticmethod
    def album_manager(limit=20):
        return AlbumManager(limit=limit)

    # Destroy the database file, if it exists.
    @staticmethod
    def clear_database(dbname='database.db'):
        if os.path.exists(dbname):
            os.remove(dbname)

    # internal method to get the single database manager instance
    @staticmethod
    async def _getInstance(dbname):
        if formulite._manager_instance is None:
            connection = await aiosqlite.connect(dbname)
            formulite._manager_instance = DatabaseManager(connection)
        return formulite._manager_instance

    # Get the single database manager instance (use this)
    @classmethod
    async def manager(cls, dbname='database.db'):
        return await cls._getInstance(dbname)

class DatabaseManager:
    def __init__(self, connection):
        self.conn = connection
        self._typeflag = None

    # should be called at the end of execution
    async def close(self):
        await self.conn.close()

    ### Auxiliary internal functions ###

    # Function to retieve a class from a string (copied from stackoverflow, don't judge me, we all do it sometimes)
    # module_name is the file where the class was defined (dbobjects.py as standard)
    def string_to_Obj(self, class_name, module_name='dbobjects'):
        mod = importlib.import_module(module_name)
        Obj = getattr(mod, class_name)
        return Obj

    # tablenames are hidden from the user and should always be obtained through these 2 functions
    def _tablename(self, Obj, key=None):
        if key:
            return f"{Obj.__name__.lower()}_{key}s"
        return Obj.__name__.lower() + "s" # s makes it plural, a dumb yet effective solution

    def _string_totablename(self, string):
        return string.lower() + "s"

    # This method returns true if the table does not exist in the database, and false otherwise
    async def _table_nonexistent(self, tname):
        c = await self.conn.execute(f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{tname}'")
        test, = await c.fetchone()
        if test == 1:
            return False
        return True

    # This method acquires and returns object attributes in a dictionary. It does not return base class attributes.
    # Does not support multiple inheritance. (I should probably fix this)
    # Dictionary format is {attribute name : attribute type}
    def _objprops(self, Obj):
        out = {}
        chaves = list(Obj.__fields__.keys())
        tipos = list(Obj.__fields__.values())
        for i in range(len(chaves)):
            out.update( {chaves[i]:str(tipos[i]._type_display())} )

        # Base class attributes are removed before returning
        Sup = Obj.mro()[1]
        if Sup != BaseModel:
            # So we remove them here in recursive style
            for k, v in self._objprops(Sup).items():
                del out[k]

        return out

    # This method is responsible for accessing the typedict dictionary and inferring sqlite types from pydantic types
    # pytype argument should be passed as a string
    def _translate_type(self, pytype):
        # If the type is a list, it should flag for another table to be created and return the type inside the list
        checklist = re.match("\[(\w+)]", pytype)
        if checklist is not None:
            self._typeflag = True
            # This try except will return as a class name if the captured type is a user defined class
            # Otherwise it will return a string containing the sqlite type as usual
            try:
                Custom_Obj = self.string_to_Obj(checklist.group(1))
                return Custom_Obj.__name__
            except AttributeError:
                pytype = checklist.group(1)
        return formulite.typedict[pytype]

    # This method needs a closer look
    # The goal is to insert the cursor.lastrowid property into the instance before adding it to the database, to act as a foreign key
    def _parse_instance(self, instance, cursor, Supper):
        vals = formulite.vars_values(instance)
        if Supper != BaseModel:
            vals.insert(0, cursor.lastrowid)
        return tuple(vals)

    # didn't neeeeed to be async but wynaut
    async def _extract_from_instance(self, Sup, instance):
        sup_args = {}
        y = list(vars(instance).values())
        x = list(vars(Sup()).keys())
        for i in range(len(x)):
            sup_args.update({ x[i]:y[i] })
        return Sup(**sup_args)

    ### CREATE ###

    # Internal method to create a table that links an object to a list of other objects
    async def _create_table_link_obj(self, Obj, prop_name, Custom_Obj):
        sql = f"CREATE TABLE IF NOT EXISTS {self._tablename(Obj, prop_name)} ( {Obj.__name__.lower()}_{prop_name}_id INTEGER PRIMARY KEY"
        sql += f"{Obj.__name__.lower()}_id INTEGER NOT NULL,"
        sql += f"{Custom_Obj.__name__.lower()}_id INTEGER NOT NULL,"

        sql += f"quantity INTEGER," # This should be optional somehow

        sql += f''', FOREIGN KEY ({Obj.__name__.lower()}_id) REFERENCES {self._tablename(Obj)} ({Obj.__name__.lower()}_id) ON UPDATE CASCADE ON DELETE CASCADE'''
        sql += f''', FOREIGN KEY ({Custom_Obj.__name__.lower()}_id) REFERENCES {self._tablename(Custom_Obj)} ({Custom_Obj.__name__.lower()}_id) ON UPDATE CASCADE ON DELETE CASCADE'''

        await self.conn.execute(sql)
        await self.conn.commit()

    # Internal method to create a table that links an object to a list of some builtin type
    async def _create_table_link_items(self, Obj, prop_name, sqlite_type):
        sql = f"CREATE TABLE IF NOT EXISTS {self._tablename(Obj, prop_name)} ( {Obj.__name__.lower()}_{prop_name}_id INTEGER PRIMARY KEY"
        sql += f"{Obj.__name__.lower()}_id INTEGER NOT NULL,"
        sql += f"{prop_name} {sqlite_type},"

        sql += f"quantity INTEGER," # This should be optional somehow

        sql += f''', FOREIGN KEY ({Obj.__name__.lower()}_id) REFERENCES {self._tablename(Obj)} ({Obj.__name__.lower()}_id) ON UPDATE CASCADE ON DELETE CASCADE'''
        
        await self.conn.execute(sql)
        await self.conn.commit()

    # This method creates a table, very straightforward
    async def create_table(self, Obj):
        props = self._objprops(Obj)
        sql = f"CREATE TABLE IF NOT EXISTS {self._tablename(Obj)} ( {Obj.__name__.lower()}_id INTEGER PRIMARY KEY"

        Sup = Obj.mro()[1] # mro() gets the classes that are used to build the object.
        sup_name = f"{Sup.__name__.lower()}"
        if Sup != BaseModel:
            sql += f", {sup_name}_id INTEGER" # NOT NULL"

        for prop_name, prop_type in list(props.items()):
            sqlite_type = self._translate_type(prop_type)
            if self._typeflag:
                try:
                    # Verify if sqlite_type is a class defined in dbobjects or a builtin type
                    Custom_Obj = self.string_to_Obj(sqlite_type)
                    self._create_table_link_obj(Obj, prop_name, Custom_Obj)
                except AttributeError:
                    # If it's a builtin type, it will fall here
                    self._create_table_link_items(Obj, prop_name, sqlite_type)
                self._typeflag = None
                continue
            else:
                sql += f", {prop_name} {sqlite_type}"

        # check for superclass (again)
        if Sup != BaseModel:
            sql += f''', FOREIGN KEY ({sup_name}_id) REFERENCES {self._tablename(Sup)} ({sup_name}_id) ON UPDATE CASCADE ON DELETE CASCADE'''

        sql += " )"
        await self.conn.execute(sql)
        await self.conn.commit()

    # This method creates a table for every class it finds in the file passed as parameter. Might be useful, idk
    async def create_all_tables(self, filename='dbobjects.py'):
        # Get the file
        if os.path.exists(filename):
            file = open(filename)
            # Find matching classes using regex
            results = re.findall(r"class ([A-z]*)", file.read())
            # Create the tables
            for string in results:
                mod_name = filename[:-3]
                # Need to convert this string to an object before converting
                Obj = self.string_to_Obj(string, mod_name)
                await self.create_table(Obj)
        else:
            # raise an exception if the file does not exist
            raise FileNotFoundError(f"Couldn't find the file {filename}")

    ### READ ###
    # Many functions inside this category are similar, should study a way to join them together.

    # This method is the basic method for reading data from an sqlite table
    # if you also want to retrieve the superclass attributes, use search_joined instead.
    async def search(self, Obj, exact=True, **kargs):
        # 1 - Define which columns will be selected by the query
        att = formulite.vars_keys(Obj)
        rows = ", ".join(att) # Maybe could substitute for .Row object (read documentation)
        sql = f"SELECT {rows} FROM {self._tablename(Obj)}"

        # 2 - Define the criteria for the search results, and store them into fetch
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
            # sql = f"SELECT * FROM {self._tablename(Obj)}" is not a good idea here
            # because we may have more parameters than the ones that the object requires

        # 3 - Build the list of objects retrieved, and return it
        out = []
        for result in fetch:
            aux = {}
            for i in range(len(att)):
                aux.update( {att[i] : result[i]} )
            out.append(Obj(**aux))
        return out

    async def select_some(self, Obj, exact=True, limit=None, offset=None, **kargs): # Unfinished
        # 1 - Define which columns will be selected by the query
        att = formulite.vars_keys(Obj)
        rows = ", ".join(att)
        sql = f"SELECT * FROM {self._tablename(Obj)}"

        # joined_columns is a list of all columns that appear on the table after the join is executed
        joined_columns = [f"{Obj.__name__.lower()}_id"]
        joined_columns.extend(formulite.vars_keys(Obj))

        for subc in Obj.__subclasses__():
            sql += f" LEFT JOIN {self._tablename(subc)} USING ({Obj.__name__.lower()}_id)"
            joined_columns.extend([f"{subc.__name__.lower()}_id"])
            joined_columns.extend(formulite.vars_keys(subc))

        # 2 - Define the criteria for the search results
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
        
        # 2.5 - Check some optional args
        if limit:
            sql += f" LIMIT {limit}"
        if offset:
            sql += f" OFFSET {offset}"

        # 3 - store results in fetch
        fetch = await self.conn.execute_fetchall(sql, tuple(placeholders))

        # 4 - Build the list of objects retrieved, and return it
        # I guess this should be a separate object builder function
        results = []
        for result in fetch:
            # ret is a dictionary of {column:value}
            ret = dict((joined_columns[i], result[i]) for i in range(len(joined_columns)))
            for subc in Obj.__subclasses__():
                # if the current result has the subclass id, then the object to be returned is of that subclass
                if ret.get(f"{subc.__name__.lower()}_id"):
                    results.append(subc(**ret))
        return results

    # joins subclass tables and returns the amount of items
    # Should be the same result as doing len() on a regular search
    async def count_joined(self, Obj, exact=True, **kargs): 
        # 1 - Define which columns will be selected by the query
        att = formulite.vars_keys(Obj)
        rows = ", ".join(att)
        sql = f"SELECT count(*) FROM {self._tablename(Obj)}"

        # joined_columns is a list of all columns that appear on the table after the join is executed
        joined_columns = [f"{Obj.__name__.lower()}_id"]
        joined_columns.extend(formulite.vars_keys(Obj))

        for subc in Obj.__subclasses__():
            sql += f" LEFT JOIN {self._tablename(subc)} USING ({Obj.__name__.lower()}_id)"
            joined_columns.extend([f"{subc.__name__.lower()}_id"])
            joined_columns.extend(formulite.vars_keys(subc))

        # 2 - Define the criteria for the search results
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

        # 3 - store result in fetch
        fetch = await self.conn.execute_fetchall(sql, tuple(placeholders))

        # 4 - return the amount of items retrieved (int)
        return fetch[0]['count(*)']

    # joins subclass tables and then searches
    async def search_joined(self, Obj, exact=True, **kargs):
        # 1 - Define which columns will be selected by the query
        att = formulite.vars_keys(Obj)
        rows = ", ".join(att)
        sql = f"SELECT * FROM {self._tablename(Obj)}"

        # joined_columns is a list of all columns that appear on the table after the join is executed
        joined_columns = [f"{Obj.__name__.lower()}_id"]
        joined_columns.extend(formulite.vars_keys(Obj))

        for subc in Obj.__subclasses__():
            sql += f" LEFT JOIN {self._tablename(subc)} USING ({Obj.__name__.lower()}_id)"
            joined_columns.extend([f"{subc.__name__.lower()}_id"])
            joined_columns.extend(formulite.vars_keys(subc))

        # 2 - Define the criteria for the search results
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
            sql += "COLLATE NOCASE"

        # 3 - store results in fetch
        fetch = await self.conn.execute_fetchall(sql, tuple(placeholders))

        # 4 - Build the list of objects retrieved, and return it
        # I guess this should be a separate object builder function
        results = []
        for result in fetch:
            # ret is a dictionary of {column:value}
            ret = dict((joined_columns[i], result[i]) for i in range(len(joined_columns)))
            for subc in Obj.__subclasses__():
                # if the current result has the subclass id, then the object to be returned is of that subclass
                if ret.get(f"{subc.__name__.lower()}_id"):
                    results.append(subc(**ret))
        return results

    ### UPDATE ###

    # Insert an instance at the appropriate table
    async def add_one(self, instance, propagate=False, _cursed=None):
        # Get the table based on the object class and verify its existence
        Obj = instance.__class__
        if await self._table_nonexistent(self._tablename(Obj)):
            await self.create_table(Obj)

        # Column names will be needed (maybe should have some layer of abstraction for this)
        attrib = formulite.vars_keys(Obj)
        
        # A cursor can be passed here for some reason I forgot
        c = _cursed
        if c == None:
            c = await self.conn.cursor()

        # Checks for superclass and adds it recursively
        Sup = Obj.mro()[1]
        if Sup != BaseModel:
            attrib.insert(0, f"{Sup.__name__.lower()}_id")
            if propagate:
                before = await self._extract_from_instance(Sup, instance)
                await self.add_one(before, _cursed=c)

        # Insert the values into the table
        attributes = ", ".join(attrib)
        interr = ", ".join(["?"] * len(attrib))
        sql = f"INSERT INTO {self._tablename(Obj)} ({attributes}) VALUES ({interr})"
        await c.execute(sql, self._parse_instance(instance, c, Sup))
        await self.conn.commit()
        if c != _cursed:
            await c.close()

    # This is likely the wrong way to do it, but works
    async def add_many(self, instance_list, propagate=False):
        for instance in instance_list:
            await self.add_one(instance, propagate)

    # This is likely the right way to do it, but does not work (yet)
    async def _add_many(self, instance_list, propagate=False):
        if instance_list == []:
            # should raise some error here
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

    async def update_attribute(self, Obj, cname, change):
        # change parameter should be a {old:new} dictionary
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

    ### DELETE ###
    # async def remove_item(self, instance): # yet to be implemented

    ### OTHER ### - I mean, the user could run the aiosqlite library for these, maybe I'll drop this idea.
    async def custom_fetchall(self, sql, params=None):
        if params:
            return await self.conn.execute_fetchall(sql, params)
        else:
            return await self.conn.execute_fetchall(sql)