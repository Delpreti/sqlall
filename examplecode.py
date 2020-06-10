import asyncio
from formulite import *
from dbobjects import *

def get_some_users():
    John = Sample_Admin(username="John", user_discriminator=123, credentials=456789)
    Sarah = Sample_Member(username="Sarah", user_discriminator=789)
    Lee = Sample_Member(username="Rocky leemon", user_discriminator=321)
    return [John, Sarah, Lee]

async def main():
    # This will delete an existing database, so that we can create another instead of writing over it
    formulite.clear_database()

    # Get the manager instance ready
    manager = await formulite.manager()

    # create the tables for all objects defined inside the dbobjects.py file
    await manager.create_all_tables()

    # At the time of this writing, you cannot add this userlist with a single command, because of the different classes.
    userlist = get_some_users()

    # So this is what you can do instead: (which I guess is good enough)
    Jhonny = userlist.pop(0) # Remove the admin before adding it to the db
    await manager.add_one(Jhonny, propagate=True)
    await manager.add_many(userlist, propagate=True) # then add the rest

    # the method below is specially useful if you want to increase warnings by 1 for example.
    Jhonny.credentials = 356272
    await manager.update_item(Jhonny, credentials=456789) # it gets an updated instance and some unique value it had before (its not intuitive, ik)

    Sarah = userlist[0]
    old_warn = Sarah.warnings
    Sarah.warnings += 1
    await manager.update_item(Sarah, warnings=0) # so incrementing something is relatively easy.
    # however this fails because warnings are not unique. (mr. Leemon also got a warning)

    # Now let's see if the things we added are correctly inserted into the database
    adm = await manager.search_joined(Sample_User, credentials=356272)
    for item in adm:
        print(item)

    cool_members = await manager.search_joined(Sample_User, warnings=1)
    for item in cool_members:
        print(item)

    # we can also remove stuff if we want -to be implemented-

    # And that's it for the basics,
    # just don't forget to close it!
    await manager.close()

asyncio.run(main())
