from formulite import *
from async_property import async_property
import inspect

# Many times, the amount of information to be extracted from the database is too long to be viewed in a single list,
# Instead, users might want to skip through pages with limited amount of content in each.
# This file provides 2 different implementations of this same functionality, and a manager to handle/store them.

class AlbumManager:
    def __init__(self, limit=20):
        self.album_dict = {}
        self.limit = limit
        self.auto_id = 0

    # Function to prevent a dictionary of infinite size
    # Could be a timeout function instead
    def update_manager(self):
        if len(self.album_dict) > self.limit:
            # Bad practice, as the order may not be guaranteed
            firstkey = list(self.album_dict.keys())[0]
            self.album_dict.pop(firstkey)

    # In case of a paged_list, you may want to only subscribe the parameters needed to rebuild it
    # (the parameter "album" does not necessairly takes an album)
    def subscribe(self, identifier, album):
        self.album_dict[identifier] = album
        self.update_manager()

    # Internal helper to automatically subscribe an album with a padronized key
    def _auto_subscribe(self, album):
        key = self.auto_id
        self.subscribe(self.auto_id, album)
        self.auto_id += 1
        return key

    # Just an abstraction
    def get_album(self, key):
        return self.album_dict[key]

    # This method will automatically subscribe the album when its created. Instead of returning the album,
    # it returns a key. The album can be obtained from the get_album() method above.
    async def create_album(self, page_size, album_Type, **kwargs):
        alb = await formulite.album(page_size, album_Type, **kwargs)
        return self._auto_subscribe(album)

    # This method does the same as above for paged lists.
    # Note that you can have both implementations with unique ids on a single manager instance
    def create_paged_list(self, page_size, item_list, wrap=True):
        alb = formulite.paged_list(page_size, item_list, wrap=wrap)
        return self._auto_subscribe(album)

# This implementation dispenses use of LIMIT/OFFSET by storing all the items it needs
# It's simple, but may consume too much memory at runtime
# note that, since the database access is made externally, this implementation is not asynchronous
# I mean, you can do an asynchronous version of it if you really want to
# Does not support an async page_factory!
class Paged_list:
    def __init__(self, page_size, item_list, wrap=True, page_factory=None):
        if page_size <= 0:
            raise ValueError('page_size should be greater than 0')
        self.items = item_list
        self.page_size = page_size
        self.current_page = 0

        # This could be a calculated property but I decided to keep it here since it should only be calculated once
        self.album_size = math.ceil(len(self.items) / self.page_size)

        # Wrap means that if you try to go beyond the last page you get sent back to the beginning, and vice-versa
        # This behaviour does not apply on the iterator
        self.wrap = wrap

        # Page factory is a function passed in as parameter. This allows you to change the way a page is returned.
        self.page_factory = page_factory

    def get_current_page(self):
        page = []
        if self.items:
            # Last page may have less than (page_size) elements, so it must be treated different
            if self.current_page == self.album_size - 1:
                page = self.items[self.page_size * self.current_page : len(self.items)]
            else:
                page = self.items[self.page_size * self.current_page : self.page_size * (1 + self.current_page)]
        # Call the page_factory if there is one
        if self.page_factory:
            page = self.page_factory(page)
        return page

    def __iter__(self):
        # iterates through multiple pages
        self.current_page = 0
        return self

    def __next__(self):
        if self.current_page < self.album_size:
            page = self.get_current_page()
            self.current_page += 1
            return page
        else:
            raise StopIteration

    # Goes to the next page (if possible) and returns it
    def next_page(self):
        if self.wrap:
            self.current_page += 1
            if self.current_page >= self.album_size:
                self.current_page = 0
            return self.get_current_page()
        else:
            if self.current_page == self.album_size - 1:
                return self.get_current_page()
            self.current_page += 1
            return self.get_current_page()

    # Goes to the previous page (if possible) and returns it
    def prev_page(self):
        if self.wrap:
            self.current_page -= 1
            if self.current_page < 0:
                self.current_page = self.album_size - 1
            return self.get_current_page()
        else:
            if self.current_page == 0:
                return self.get_current_page()
            self.current_page -= 1
            return self.get_current_page()

# This implementation uses LIMIT/OFFSET to build pages during runtime by accessing the database.
# Consumes less memory, at the cost of requesting access to the database for every page.
# Since aiosqlite is asynchronous, this implementation had to be aswell
# I personally recommend it over the Paged_list, but that might depend on your application.
class Album:
    def __init__(self, page_size, dbmanager, album_Type, wrap=True, page_factory=None, **kwargs):
        self.page_size = page_size
        self.dbmanager = dbmanager
        self.current_page = 0

        # These are specific to the user:
        # The class (not an instance) of the objects that the album should retrieve
        self.Type = album_Type

        # Wrap means that if you try to go beyond the last page you get sent back to the beginning, and vice-versa
        # This behaviour does not apply on the iterator
        self.wrap = wrap

        # Page factory is a function passed in as parameter. This allows you to change the way a page is returned.
        self.page_factory = page_factory

        # A dictionary to specify the data that will be selected from the db
        self.data_column = kwargs # user_id

    # async_property is an extra dependecy (see the imports) that fits really well here
    @async_property
    async def album_size(self):
        item_count = await self.dbmanager.count_joined(self.Type, self.data_column)
        return math.ceil(item_count / self.page_size)

    # Since the database requests are async, the iterator must be async.
    # I won't teach you how to use generators, good luck.
    async def async_iter(self):
        while self.current_page < await self.album_size:
            page = await self.view()
            self.current_page += 1
            yield page

    async def view(self):
        page = await self.dbmanager.search(self.Type, limit=self.page_size, offset=self.current_page * self.page_size, joined=True, self.data_column)
        if self.page_factory:
            if inspect.iscoroutinefunction(self.page_factory):
                page = await self.page_factory(page)
            else:
                page = self.page_factory(page)
        return page

    # Goes to the next page (if possible) and returns it
    async def view_next(self):
        if self.wrap:
            self.current_page += 1
            if self.current_page >= self.album_size:
                self.current_page = 0
            return await self.view()
        else:
            if self.current_page == self.album_size - 1:
                return self.view()
            self.current_page += 1
            return self.view()

    # Goes to the previous page (if possible) and returns it
    async def view_prev(self):
        if self.wrap:
            self.current_page -= 1
            if self.current_page < 0:
                self.current_page = self.album_size - 1
            return await self.view()
        else:
            if self.current_page == 0:
                return self.view()
            self.current_page -= 1
            return self.view()

