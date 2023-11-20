# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


class WebscrapingFincaraizPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # strip whitespaces for all fields and replace "/" if not link
        field_names = adapter.field_names()
        for field_name in field_names:
            if field_name != 'link':
                value = adapter.get(field_name)
                if value:
                    adapter[field_name] = str(value).replace("/","").strip()

                else:
                    adapter[field_name] = "No info"

        # price to float
        price = adapter.get("Precio")
        value1 = price.replace('$','')
        value2 = value1.replace('.','')
        adapter["Precio"] = float(value2)

        # Habitaciones, baños, estudio, cuarto util, parqueaderos to int

        int_columns = ['Baños', 'Habitaciones', 'Parqueaderos', 'Estudio', 'cuarto_util']

        for col in int_columns:
            value = adapter.get(col)
            if value != "No info":
                adapter[col] = int(value)

        # Area 

        area = adapter.get("Área")
        adapter["Área"] = float(area.split(" ")[0])
    

        return item
    
class NoDuplicates:

    def __init__(self):

        self.seen_nombres = set()
        self.seen_prices = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        nombre = adapter.get("nombre")
        price = adapter.get("Precio")
        
        if nombre is not None and price is not None and (nombre, price) not in self.seen_prices:
            # If the (Nombre, Precio) pair is not seen before, add it to the set and process the item
            self.seen_nombres.add(nombre)
            self.seen_prices.add((nombre, price))

            return item
        
        else:
            # If the (Nombre, Precio) pair is already seen, drop the item
            raise DropItem(f"Duplicate item for Nombre: {nombre} and Precio: {price}")

        
# Create class to save data SQL (Posgres)



