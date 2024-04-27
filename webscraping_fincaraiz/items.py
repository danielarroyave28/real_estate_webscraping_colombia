# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class WebscrapingFincaraizItem(scrapy.Item):
    nombre = scrapy.Field()
    tipo = scrapy.Field()
    propiedad = scrapy.Field()
    ciudad = scrapy.Field()
    barrio = scrapy.Field()
    link = scrapy.Field()
    Precio = scrapy.Field()
    Área = scrapy.Field()
    Entrega = scrapy.Field()
    Habitaciones = scrapy.Field()
    cuarto_util = scrapy.Field()
    Baños = scrapy.Field()
    Parqueaderos = scrapy.Field()
    Estudio = scrapy.Field()
    website_updated = scrapy.Field()

    

