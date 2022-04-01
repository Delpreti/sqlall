import asyncio
from sqlall import sqlall

async def initialize(manager):
    manager.set_clear_all()

    manager.set_entity("Produto", prod_id="INT", prod_name="TEXT", prod_spec="TEXT")
    manager.set_primary_key("Produto", "prod_id")

    manager.set_entity("Loja", l_nick="TEXT", l_name="TEXT", l_credit="BOOL", l_delivery="BOOL", l_address="TEXT")
    manager.set_primary_key("Loja", "l_nick")

    manager.set_entity("Anuncio", l_nick="TEXT", prod_id="INT", prod_price="FLOAT", time_catch="TEXT")
    manager.set_primary_key("Anuncio", "l_nick", "prod_id", "prod_price")
    manager.set_foreign_key("Anuncio", "l_nick", "Loja")
    manager.set_foreign_key("Anuncio", "prod_id", "Produto")

    manager.set_entity("Meta", metakey="INT", range_start="INT", range_end="INT")
    manager.set_primary_key("Meta", "metakey")

    await manager.create_tables()

async def main():

    manager = await sqlall.manager_async()

    if not manager.loaded():
        await initialize(manager)

    await manager.close()

asyncio.run(main())
