from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
#import mysql.connector
import sqlite3
import psycopg2
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
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
                    adapter[field_name] = None

        # price to float
        # price = adapter.get("Precio")
        # if price is not None:
        #     # Replace unwanted characters and convert to float
        #     price_cleaned = price.replace('$', '').replace('.', '').replace(' ', '')
        #     try:
        #         adapter["Precio"] = float(price_cleaned)
        #     except ValueError:
        #         raise DropItem("Invalid price format")

        # elif price is None or price == ' ' or price == '' or not price:
        #     raise DropItem("Price is None")


        # Habitaciones, baños, estudio, cuarto util, parqueaderos to int

        int_columns = ['Baños', 'Habitaciones', 'Parqueaderos', 'Estudio']

        for col in int_columns:
            value = adapter.get(col)
            if value != None:
                adapter[col] = int(value)

        # Area 

        area = adapter.get("Área")
        if area != None:
            adapter["Área"] = float(area.split(" ")[0])


        # Barrio

        barrio = adapter.get("barrio")
        if barrio == '':
            adapter['barrio'] = 'sin barrio'
    

        return item
    
class NoDuplicates:

    def __init__(self):

        self.seen_nombres = set()
        self.seen_prices = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        nombre = adapter.get("nombre")
        price = adapter.get("Precio")
        
        if nombre != None and price != None and (nombre, price) not in self.seen_prices:
            # If the (Nombre, Precio) pair is not seen before, add it to the set and process the item
            self.seen_nombres.add(nombre)
            self.seen_prices.add((nombre, price))

            return item
        
        else:
            # If the (Nombre, Precio) pair is already seen, drop the item
            raise DropItem(f"Duplicate item for Nombre: {nombre} and Precio: {price}")
        
        

class SaveToPostgreSQLPipeline:
    def __init__(self):
        # Connect to PostgreSQL database
        load_dotenv()
        self.conn = psycopg2.connect(
            database="daniel",
            user="postgres",
            password=os.getenv('PASSWORD'),
            host=os.getenv('HOST'),
            port="5432"
        )
        self.cur = self.conn.cursor()

        self.unique_ciudades = set()
        self.unique_barrios = set()

        self.conn.commit()

    def process_item(self, item, spider):
        try:
            # Get or create Ciudad and Barrio IDs
            ciudad_id = self.get_or_create_id('ciudad', 'name', item["ciudad"])
            barrio_id = self.get_or_create_id('barrio', 'name', item["barrio"])

            current_datetime = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            price = float(item['Precio'].replace('$', '').replace('.', '').replace(' ', ''))

            self.cur.execute('''
                SELECT id FROM proyectos WHERE nombre = %s AND precio::numeric = %s AND ciudad_id = %s AND barrio_id = %s AND area = %s
            ''', (item['nombre'], price, ciudad_id, barrio_id, item['Área']))
            
            existing_project_id = self.cur.fetchone()

            if existing_project_id is not None:
                # Project already exists, update the existing row
                print(f"Updating existing project: {item['nombre']}, {item['ciudad']}, {item['barrio']}, {item['Área']}")
                self.cur.execute('''
                    UPDATE proyectos
                    SET tipo = %s, link = %s, precio = %s, area = %s, entrega = %s,
                        habitaciones = %s, cuarto_util = %s, baños = %s, parqueaderos = %s, estudio = %s, last_updated = %s, website_updated = %s
                    WHERE id = %s
                ''', (
                    item['tipo'],
                    item['link'],
                    price,
                    item['Área'],
                    item['Entrega'],
                    item['Habitaciones'],
                    item['cuarto_util'],
                    item['Baños'],
                    item['Parqueaderos'],
                    item['Estudio'],
                    current_datetime,
                    item['website_updated'],
                    existing_project_id[0]  # ID of the existing project
                ))
            else:
                # Insert data into the projects table
                self.cur.execute('''
                    INSERT INTO proyectos (
                        nombre, tipo, ciudad_id, barrio_id, link, precio, area, entrega,
                        habitaciones, cuarto_util, baños, parqueaderos, estudio, last_updated, website_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    item['nombre'],
                    item['tipo'],
                    ciudad_id,
                    barrio_id,
                    item['link'],
                    price,
                    item['Área'],
                    item['Entrega'],
                    item['Habitaciones'],
                    item['cuarto_util'],
                    item['Baños'],
                    item['Parqueaderos'],
                    item['Estudio'],
                    current_datetime,
                    item['website_updated']
                ))

            self.unique_ciudades.add(item["ciudad"])
            self.unique_barrios.add(item["barrio"])

            return item

        except Exception as e:
            print("Error processing item:", e)
            self.conn.rollback()
            raise

    def close_spider(self, spider):
        try:
            # Close the database connection when the spider is closed
            for ciudad_name in self.unique_ciudades:
                self.get_or_create_id('ciudad', 'name', ciudad_name)

            for barrio_name in self.unique_barrios:
                if barrio_name:
                    self.get_or_create_id('barrio', 'name', barrio_name)

            self.conn.commit()
        finally:
            self.cur.close()
            self.conn.close()

    def get_or_create_id(self, table, column, value):
        # Get or create ID for a given value in a table
        self.cur.execute(f'''
            INSERT INTO {table} ({column}) VALUES (%s) ON CONFLICT DO NOTHING
        ''', (value,))
        self.cur.execute(f'''
            SELECT id FROM {table} WHERE {column} = %s
        ''', (value,))
        result = self.cur.fetchone()
        return result[0] if result else None


class SaveToSQLitePipelineUpdated:
    def __init__(self):
        # Connect to SQLite database (creates a new file if it doesn't exist)
        self.conn = sqlite3.connect('/Users/Usuario/Documents/real_estate_webscraping_api/instance/informeinmobiliario.db')
        self.cur = self.conn.cursor()

        self.unique_ciudades = set()
        self.unique_barrios = set()

        self.conn.commit()
        

    def process_item(self, item, spider):
        # Get or create Ciudad and Barrio IDs
        ciudad_id = self.get_or_create_id('ciudad', 'name', item["ciudad"])
 
        barrio_id = self.get_or_create_id('barrio', 'name', item["barrio"])
        

        current_datetime = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        price = float(item['Precio'].replace('$', '').replace('.', '').replace(' ', '')) # Had to make the transformation here not working in the first process

        # Check if the project already exists
        existing_project = self.cur.execute('''
            SELECT id FROM proyectos WHERE nombre = ? AND precio = ? AND ciudad_id = ? AND barrio_id = ? AND area = ?
        ''', (item['nombre'], price, ciudad_id, barrio_id, item['Área'])).fetchone()

        if existing_project:
            # Project already exists, update the existing row
            print(f"Updating existing project: {item['nombre']}, {item['ciudad']}, {item['barrio']}, {item['Área']}")
            self.cur.execute('''
                UPDATE proyectos
                SET tipo = ?, propiedad = ?, link = ?, precio = ?, area = ?, entrega = ?,
                    habitaciones = ?, cuarto_util = ?, baños = ?, parqueaderos = ?, estudio = ?, last_updated = ?, website_updated = ?
                WHERE id = ?
            ''', (
                item['tipo'],
                item['propiedad'],
                item['link'],
                price,
                item['Área'],
                item['Entrega'],
                item['Habitaciones'],
                item['cuarto_util'],
                item['Baños'],
                item['Parqueaderos'],
                item['Estudio'],
                current_datetime,
                item['website_updated'],
                existing_project[0]  # ID of the existing project
            ))
        else:
            # Insert data into the projects table
            self.cur.execute('''
                INSERT INTO proyectos (
                    nombre, tipo, propiedad, ciudad_id, barrio_id, link, precio, area, entrega,
                    habitaciones, cuarto_util, baños, parqueaderos, estudio, last_updated, website_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                item['nombre'],
                item['tipo'],
                item['propiedad'],
                ciudad_id,
                barrio_id,
                item['link'],
                price,
                item['Área'],
                item['Entrega'],
                item['Habitaciones'],
                item['cuarto_util'],
                item['Baños'],
                item['Parqueaderos'],
                item['Estudio'],
                current_datetime,
                item['website_updated']
            ))

        self.unique_ciudades.add(item["ciudad"])
        self.unique_barrios.add(item["barrio"])

        self.conn.commit()
        return item

    def close_spider(self, spider):
        # Close the database connection when the spider is closed
        for ciudad_name in self.unique_ciudades:
            self.get_or_create_id('ciudad', 'name', ciudad_name)

        for barrio_name in self.unique_barrios:
            if barrio_name:
                self.get_or_create_id('barrio', 'name', barrio_name)

        self.conn.commit()
        self.conn.close()

    def get_or_create_id(self, table, column, value):
        # Get or create ID for a given value in a table
        if value != '':
            self.cur.execute(f'''
                INSERT OR IGNORE INTO {table} ({column}) VALUES (?)
            ''', (value,))  # inserts the barrio/ciudad name into its respective table
        self.cur.execute(f'''
            SELECT id FROM {table} WHERE {column} = ?
        ''', (value,))
        result = self.cur.fetchone()
        return result[0] if result else None  # grabs the id
