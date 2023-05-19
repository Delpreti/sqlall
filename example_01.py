import asyncio
from sqlall import sqlall
from utils import utils

async def initialize(manager):
    manager.set_clear_all()

    # Define the tables before creating the database
    manager.set_entity("Product", prod_id="INT", prod_name="TEXT", prod_spec="TEXT")
    manager.set_primary_key("Product", "prod_id")

    # Then build the database
    await manager.create_tables()

async def main():

    manager = await sqlall.manager_async()

    if not manager.loaded():
        await initialize(manager)

    # Example entry
    prod_dict = { "prod_id": 1, "prod_name": "shampoo", "prod_spec": "a nice thing to use!" }

    # Will both build the object and insert it into the database
    prod_object = await manager.build_and_insert("Product", **prod_dict)

    # You can use the object that is built
    print(prod_object.prod_name)

    # You can also query it from the database
    prod_list = await manager.select_all_from("Product", utils.where(prod_id=1))
    prod = prod_list[0]
    print(prod.prod_spec)

    # Equals method will return True if objects belong to the same table
    # and their primary keys are equal, False otherwise
    if prod_object == prod:
        print(f"Objects {prod_object} and {prod} are equal")

    # Remember to close the connection when done
    await manager.close()

asyncio.run(main())
