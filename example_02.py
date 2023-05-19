import asyncio
from sqlall import sqlall
from utils import utils

async def initialize(manager):
    manager.set_clear_all()

    manager.set_entity("Product", prod_id="INT", prod_name="TEXT", prod_spec="TEXT")
    manager.set_primary_key("Product", "prod_id")

    manager.set_entity("Store", l_nick="TEXT", l_name="TEXT", l_credit="BOOL", l_delivery="BOOL", l_address="TEXT")
    manager.set_primary_key("Store", "l_nick")

    manager.set_entity("Advertisement", l_nick="TEXT", prod_id="INT", prod_price="FLOAT", time_catch="TEXT")
    manager.set_primary_key("Advertisement", "l_nick", "prod_id", "prod_price")
    manager.set_foreign_key("Advertisement", "l_nick", "Store")
    manager.set_foreign_key("Advertisement", "prod_id", "Product")

    await manager.create_tables()

async def main():

    try: # try except block to prevent async cmd from blocking

        manager = await sqlall.manager_async()

        if not manager.loaded():
            await initialize(manager)

        # Example entries
        prod_dict = { "prod_id": 1, "prod_name": "shampoo", "prod_spec": "a nice thing to use!" }
        store_dict = { "l_nick": "snack donald", "l_name": "snd enterprise", "l_credit": True, "l_delivery": False, "l_address": "Louisianna" }
        ad_dict = { "prod_id": 1, "l_nick": "snack donald", "prod_price": 59.99, "time_catch": "10:00" }

        # Insert objects into database
        prod_object = await manager.build_and_insert("Product", **prod_dict)
        store_object = await manager.build_and_insert("Store", **store_dict)
        ad_object = await manager.build_and_insert("Advertisement", **ad_dict)

        # Performing a complex query:
        # Grab the name of all products that can be delivered by a store
        prod_table = manager.get_table_object("Product")
        store_table = manager.get_table_object("Store")
        ad_table = manager.get_table_object("Advertisement")
        prod_list = await manager.select_all_from("Product",
            utils.join(prod_table, ad_table),
            utils.join(ad_table, store_table),
            utils.where(l_delivery=True)
            )
        if len(prod_list) > 0:
            result = prod_list[0]
            print(result.prod_name)

        # Remember to close the connection when done
        await manager.close()

    except Exception as e:
        print(e)

asyncio.run(main())
