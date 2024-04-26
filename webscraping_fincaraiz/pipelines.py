from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
import mysql.connector
import sqlite3
import psycopg2

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
        # price = adapter.get("Precio")
        # if price != "No info":
        #     value1 = price.replace('$','').replace('.','').replace(' ','')
        #     adapter["Precio"] = float(value1)

        # Habitaciones, baños, estudio, cuarto util, parqueaderos to int

        int_columns = ['Baños', 'Habitaciones', 'Parqueaderos', 'Estudio']

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

class SaveToMySQLPipeline:

    def __init__(self):
        self.conn = mysql.connector.connect(
            host = 'localhost',
            user = 'root',
            password = 'Copa2018!',
            database = 'webscraping'
        )

        self.cur = self.conn.cursor()

        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS proyectos(
            id int NOT NULL auto_increment, 
            nombre text,
            tipo text,
            ciudad text,
            barrio text,
            link VARCHAR(255),
            precio VARCHAR(255),
            area FLOAT,
            entrega VARCHAR(255),
            habitaciones VARCHAR(255),
            cuarto_util text,
            baños VARCHAR(255),
            parqueaderos VARCHAR(255),
            estudio VARCHAR(255),
            PRIMARY KEY (id)
        )
        """)

    def process_item(self, item, spider):

        ## Define insert statement
        self.cur.execute(""" insert into proyectos (
            nombre,
            tipo,
            ciudad,
            barrio,
            link,
            precio,
            area,
            entrega,
            habitaciones,
            cuarto_util,
            baños,
            parqueaderos,
            estudio
            ) values (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
                )""", (
            item["nombre"],
            item["tipo"],
            item["ciudad"],
            item["barrio"],
            item["link"],
            item["Precio"],
            item["Área"],
            item["Entrega"],
            item["Habitaciones"],
            item["cuarto_util"],
            item["Baños"],
            item["Parqueaderos"],
            item["Estudio"]
        ))

        ## Execute insert of data into database
        self.conn.commit()
        return item
    
    def close_spider(self, spider):

        ## Close cursor & connection to database 
        self.cur.close()
        self.conn.close()        



class SaveToSQLitePipeline:
    def __init__(self):
        # Connect to SQLite database (creates a new file if it doesn't exist)
        self.conn = sqlite3.connect('/Users/arro2/Documents/realestate_api_front/instance/informeinmobiliario.db')
        self.cur = self.conn.cursor()

        self.unique_ciudades = set()
        self.unique_barrios = set()
        
        self.conn.commit()

    def process_item(self, item, spider):
        # Check if the project already exists
        existing_project = self.cur.execute('''
            SELECT id FROM proyectos WHERE nombre = ? AND ciudad = ? AND barrio = ? AND area = ?
        ''', (item['nombre'], item['ciudad'], item['barrio'], item['Área'])).fetchone()

        if existing_project:
            # Project already exists, update the existing row
            print(f"Updating existing project: {item['nombre']}, {item['ciudad']}, {item['barrio']}, {item['Área']}")
            self.cur.execute('''
                UPDATE proyectos
                SET tipo = ?, link = ?, precio = ?, area = ?, entrega = ?,
                    habitaciones = ?, cuarto_util = ?, baños = ?, parqueaderos = ?, estudio = ?
                WHERE id = ?
            ''', (
                item['tipo'],
                item['link'],
                item['Precio'],
                item['Área'],
                item['Entrega'],
                item['Habitaciones'],
                item['cuarto_util'],
                item['Baños'],
                item['Parqueaderos'],
                item['Estudio'],
                existing_project[0]  # ID of the existing project
            ))
        else:
            # Insert data into the projects table
            self.cur.execute('''
                INSERT INTO proyectos (
                    nombre, tipo, ciudad, barrio, link, precio, area, entrega,
                    habitaciones, cuarto_util, baños, parqueaderos, estudio
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                item['nombre'],
                item['tipo'],
                item['ciudad'],
                item['barrio'],
                item['link'],
                item['Precio'],
                item['Área'],
                item['Entrega'],
                item['Habitaciones'],
                item['cuarto_util'],
                item['Baños'],
                item['Parqueaderos'],
                item['Estudio']
            ))

        self.unique_ciudades.add(item["ciudad"])
        self.unique_barrios.add(item["barrio"])

        self.conn.commit()
        return item

    def close_spider(self, spider):
        # Close the database connection when the spider is closed
        for ciudad_name in self.unique_ciudades:
            self.cur.execute('''
                INSERT OR IGNORE INTO ciudad (name) VALUES (?)
            ''', (ciudad_name,))

        for barrio_name in self.unique_barrios:
            if barrio_name:
                self.cur.execute('''
                    INSERT OR IGNORE INTO barrio (name) VALUES (?)
                ''', (barrio_name,))

        self.conn.commit()

        self.conn.close()

class SaveToPostgreSQLPipeline:
    def __init__(self):
        # Connect to PostgreSQL database
        self.conn = psycopg2.connect(
            database="real_estate_project",
            user="real_estate_project_user",
            password="0PL8AoPK4ao8qjIbHkdFH45AcrSRsPtr",
            host="dpg-clhrkbug1b2c73ah2ki0-a",
            port="5432"
        )
        self.cur = self.conn.cursor()

        self.unique_ciudades = set()
        self.unique_barrios = set()

        self.conn.commit()

    def process_item(self, item, spider):
        # Check if the project already exists
        existing_project = self.cur.execute('''
            SELECT id FROM proyectos WHERE nombre = %s AND ciudad = %s AND barrio = %s AND area = %s
        ''', (item['nombre'], item['ciudad'], item['barrio'], item['Área'])).fetchone()

        if existing_project:
            # Project already exists, update the existing row
            print(f"Updating existing project: {item['nombre']}, {item['ciudad']}, {item['barrio']}, {item['Área']}")
            self.cur.execute('''
                UPDATE proyectos
                SET tipo = %s, link = %s, precio = %s, area = %s, entrega = %s,
                    habitaciones = %s, cuarto_util = %s, baños = %s, parqueaderos = %s, estudio = %s
                WHERE id = %s
            ''', (
                item['tipo'],
                item['link'],
                item['Precio'],
                item['Área'],
                item['Entrega'],
                item['Habitaciones'],
                item['cuarto_util'],
                item['Baños'],
                item['Parqueaderos'],
                item['Estudio'],
                existing_project[0]  # ID of the existing project
            ))
        else:
            # Insert data into the projects table
            self.cur.execute('''
                INSERT INTO proyectos (
                    nombre, tipo, ciudad, barrio, link, precio, area, entrega,
                    habitaciones, cuarto_util, baños, parqueaderos, estudio
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                item['nombre'],
                item['tipo'],
                item['ciudad'],
                item['barrio'],
                item['link'],
                item['Precio'],
                item['Área'],
                item['Entrega'],
                item['Habitaciones'],
                item['cuarto_util'],
                item['Baños'],
                item['Parqueaderos'],
                item['Estudio']
            ))

        self.unique_ciudades.add(item["ciudad"])
        self.unique_barrios.add(item["barrio"])

        return item

    def close_spider(self, spider):
        # Close the database connection when the spider is closed
        for ciudad_name in self.unique_ciudades:
            self.cur.execute('''
                INSERT INTO ciudad (name) VALUES (%s)
                ON CONFLICT (name) DO NOTHING
            ''', (ciudad_name,))

        for barrio_name in self.unique_barrios:
            if barrio_name:
                self.cur.execute('''
                    INSERT INTO barrio (name) VALUES (%s)
                    ON CONFLICT (name) DO NOTHING
                ''', (barrio_name,))

        self.conn.commit()
        self.conn.close()



class SaveToSQLitePipelineUpdated:
    def __init__(self):
        # Connect to SQLite database (creates a new file if it doesn't exist)
        self.conn = sqlite3.connect('/Users/arro2/Documents/realestate_api_front/instance/informeinmobiliario.db')
        self.cur = self.conn.cursor()

        self.unique_ciudades = set()
        self.unique_barrios = set()

        self.conn.commit()

    def process_item(self, item, spider):
        # Get or create Ciudad and Barrio IDs
        ciudad_id = self.get_or_create_id('ciudad', 'name', item["ciudad"])
        barrio_id = self.get_or_create_id('barrio', 'name', item["barrio"])

        # Check if the project already exists
        existing_project = self.cur.execute('''
            SELECT id FROM proyectos WHERE nombre = ? AND ciudad_id = ? AND barrio_id = ? AND area = ?
        ''', (item['nombre'], ciudad_id, barrio_id, item['Área'])).fetchone()

        if existing_project:
            # Project already exists, update the existing row
            print(f"Updating existing project: {item['nombre']}, {item['ciudad']}, {item['barrio']}, {item['Área']}")
            self.cur.execute('''
                UPDATE proyectos
                SET tipo = ?, link = ?, precio = ?, area = ?, entrega = ?,
                    habitaciones = ?, cuarto_util = ?, baños = ?, parqueaderos = ?, estudio = ?
                WHERE id = ?
            ''', (
                item['tipo'],
                item['link'],
                item['Precio'],
                item['Área'],
                item['Entrega'],
                item['Habitaciones'],
                item['cuarto_util'],
                item['Baños'],
                item['Parqueaderos'],
                item['Estudio'],
                existing_project[0]  # ID of the existing project
            ))
        else:
            # Insert data into the projects table
            self.cur.execute('''
                INSERT INTO proyectos (
                    nombre, tipo, ciudad_id, barrio_id, link, precio, area, entrega,
                    habitaciones, cuarto_util, baños, parqueaderos, estudio
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                item['nombre'],
                item['tipo'],
                ciudad_id,
                barrio_id,
                item['link'],
                item['Precio'],
                item['Área'],
                item['Entrega'],
                item['Habitaciones'],
                item['cuarto_util'],
                item['Baños'],
                item['Parqueaderos'],
                item['Estudio']
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
        self.cur.execute(f'''
            INSERT OR IGNORE INTO {table} ({column}) VALUES (?)
        ''', (value,))
        self.cur.execute(f'''
            SELECT id FROM {table} WHERE {column} = ?
        ''', (value,))
        result = self.cur.fetchone()
        return result[0] if result else None
