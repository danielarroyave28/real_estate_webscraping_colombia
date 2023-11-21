# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from itemloaders.processors import TakeFirst, MapCompose

def try_float(value):
    try:
        return float(value)
    except ValueError:
        return value
    
def remove_points(value):
    return value.replace(".","")

def remove_dollar_sign(value):
    return value.replace("$","")


class WebscrapingFincaraizItem(scrapy.Item):
    nombre = scrapy.Field()
    tipo = scrapy.Field()
    ciudad = scrapy.Field()
    barrio = scrapy.Field()
    link = scrapy.Field()
    # Precio = scrapy.Field(
    #     input_processor = MapCompose(remove_points, remove_dollar_sign, try_float),
    #     output_processor = TakeFirst()
    # )
    Precio = scrapy.Field()
    Área = scrapy.Field()
    Entrega = scrapy.Field()
    Habitaciones = scrapy.Field()
    cuarto_util = scrapy.Field()
    Baños = scrapy.Field()
    Parqueaderos = scrapy.Field()
    Estudio = scrapy.Field()
    

