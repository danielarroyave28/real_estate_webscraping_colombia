import scrapy
from scrapy.exceptions import CloseSpider, DontCloseSpider
from webscraping_fincaraiz.items import WebscrapingFincaraizItem

class InformeinmobiliarioSpider(scrapy.Spider):
    name = "informeinmobiliario"
    allowed_domains = ["informeinmobiliario.com"]
    start_urls = ["https://www.informeinmobiliario.com/venta/proyecto?concepto=3&order=valor&ordert=asc&busqueda=venta&tipo_negocio=proyecto&tipo_proyecto=&pag=1"]
    page_number = 1

    custom_settings = {
         'FEED_EXPORT_FIELDS': ['nombre', 'tipo', 'ciudad','barrio','link','Precio','Área','Baños',
                                'Habitaciones','Entrega','Parqueaderos','Estudio','cuarto_util'],
         'HTTPERROR_ALLOWED_CODES': [500],
     }

    def start_requests(self):
         yield scrapy.Request(url=self.start_urls[0], callback=self.parse)

    def parse(self, response):
        
        proyectos = response.css('div.styles__DetailsContainer-sc-1nn8twz-7')

        if response.status == 404: 
            raise CloseSpider('Recieve 404 response')
         
        if len(proyectos) == 0 and response.status != 500:
            raise CloseSpider('No quotes in response')


        for proyecto in proyectos:
            relative_url = proyecto.css("a ::attr(href)").get()
            
            #property_url = "https://www.informeinmobiliario.com" + relative_url
            property_url = response.urljoin(relative_url)

            yield scrapy.Request(url=property_url, callback=self.parse_area_data)

        self.page_number += 1

        next_page = f"https://www.informeinmobiliario.com/venta/proyecto?concepto=3&order=valor&ordert=asc&busqueda=venta&tipo_negocio=proyecto&pag={self.page_number}"

        yield scrapy.Request(url=next_page, callback=self.parse)
        

    def parse_area_data(self,response):

        property_item = WebscrapingFincaraizItem()
        slider = response.css('div.horizontalSlider___281Ls')

        for item in slider[:-1]:

            # get the titles and static data first
            property_item["nombre"] = response.css("h1[class*='styles__Title-sc-1owo49i-2'] ::text").get()
            property_item["tipo"] = response.css("h1[class*='styles__Title-sc-1owo49i-2'] div ::text").getall()[1]
            property_item["ciudad"] = response.css("h1[class*='styles__Title-sc-1owo49i-2'] div ::text").getall()[5]
            property_item["barrio"] = response.css("h1[class*='styles__Title-sc-1owo49i-2'] div ::text").getall()[-1]
            property_item["link"] = response.url


            # Search for the data for each aparment offer

            data_list = item.css('div > div ::text').getall()

            data_dict = {data_list[i]: data_list[i + 1] for i in range(0, len(data_list), 2)}

            if "Cuarto Útil" in data_dict.keys():
                data_dict["cuarto_util"] = data_dict.pop("Cuarto Útil")


            for k, v  in data_dict.items():
                try:
                    property_item[k] = v
                
                except KeyError:
                    print("key not found for this column")

            yield property_item


        

    



