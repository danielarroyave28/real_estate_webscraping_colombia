from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
#import mysql.connector
import sqlite3
import psycopg2
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
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
        if barrio == '' or barrio == ' ':
            adapter['barrio'] = 'Sin-Barrio'
    

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
            ciudad_id = self.get_or_create_id('ciudad', 'name', item["ciudad"], ciudad_id=None)
            barrio_id = self.get_or_create_id('barrio', 'name', item["barrio"], ciudad_id)

            current_datetime = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            price = float(item['Precio'].replace('$', '').replace('.', '').replace(' ', ''))

            self.cur.execute('''
                SELECT id FROM proyectos WHERE nombre = %s AND ciudad_id = %s AND barrio_id = %s AND area = %s
            ''', (item['nombre'], ciudad_id, barrio_id, item['Área']))
            
            existing_project_id = self.cur.fetchone()

            if existing_project_id is not None:
                # Project already exists, update the existing row
                print(f"Updating existing project: {item['nombre']}, {item['ciudad']}, {item['barrio']}, {item['Área']}")
                self.cur.execute('''
                    UPDATE proyectos
                    SET tipo = %s, propiedad = %s, link = %s, precio = %s, area = %s, entrega = %s,
                        habitaciones = %s, cuarto_util = %s, baños = %s, parqueaderos = %s, estudio = %s, last_updated = %s, website_updated = %s
                    WHERE id = %s
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
                    existing_project_id[0]  # ID of the existing project
                ))
            else:
                # Insert data into the projects table
                self.cur.execute('''
                    INSERT INTO proyectos (
                        nombre, tipo, propiedad, ciudad_id, barrio_id, link, precio, area, entrega,
                        habitaciones, cuarto_util, baños, parqueaderos, estudio, last_updated, website_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

    def get_or_create_id(self, table, column, value, ciudad_id):
        # Get or create ID for a given value in a table
        if table == 'barrio':
            self.cur.execute(f'''
            INSERT INTO {table} ({column}) VALUES (%s, %s) ON CONFLICT DO NOTHING
        ''', (value, ciudad_id))
            
        else: 
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
        ciudad_id = self.get_or_create_id('ciudad', 'name', item["ciudad"], ciudad_id=None)
 
        barrio_id = self.get_or_create_id('barrio', 'name', item["barrio"], ciudad_id)


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

    def get_or_create_id(self, table, column, value, ciudad_id):
        # Get or create ID for a given value in a table
        if value != '':
            if table == 'barrio':
                self.cur.execute(f'''
                INSERT INTO {table} ({column}, ciudad_id) VALUES (?, ?)
            ''', (value, ciudad_id))
                
            else: 
                self.cur.execute(f'''
                    INSERT OR IGNORE INTO {table} ({column}) VALUES (?)
                ''', (value,))  # inserts the barrio/ciudad name into its respective table

        self.cur.execute(f'''
            SELECT id FROM {table} WHERE {column} = ?
        ''', (value,))
        result = self.cur.fetchone()
        return result[0] if result else None  # grabs the id

from datetime import datetime, timezone
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, Date
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Ciudad(Base):
    __tablename__ = 'ciudad'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True)

    proyectos = relationship('Proyecto', back_populates='ciudad')
    barrio = relationship('Barrio', back_populates='ciudad')

    def to_dict(self):
        return {
            'id': self.id,
            'nombre': self.name
        }

class Barrio(Base):
    __tablename__ = 'barrio'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=True)

    ciudad_id = Column(Integer, ForeignKey('ciudad.id'), nullable=True)
    ciudad = relationship('Ciudad', back_populates='barrio')

    proyectos = relationship('Proyecto', back_populates='barrio')

class Proyecto(Base):
    __tablename__ = 'proyectos'
    id = Column(Integer, primary_key=True, autoincrement=True)
    nombre = Column(String)
    tipo = Column(String)
    propiedad = Column(String)
    link = Column(String)
    precio = Column(Float)
    area = Column(Float)
    entrega = Column(String)
    habitaciones = Column(Integer)
    cuarto_util = Column(String)
    baños = Column(Integer)
    parqueaderos = Column(Integer)
    estudio = Column(Integer)
    last_updated = Column(Date)
    website_updated = Column(Date)

    ciudad_id = Column(Integer, ForeignKey('ciudad.id'), nullable=True)
    ciudad = relationship('Ciudad', back_populates='proyectos')

    barrio_id = Column(Integer, ForeignKey('barrio.id'), nullable=True)
    barrio = relationship('Barrio', back_populates='proyectos')

class SaveToSQLitePipelineUpdatedALCHEMY:
    def __init__(self):
        #db_path = '/Users/Usuario/Documents/real_estate_webscraping_api/instance/informeinmobiliario.db'
        POSTGRES_URI = f"postgresql://postgres:{os.getenv('PASSWORD')}@{os.getenv('HOST')}/daniel"
        engine = create_engine(POSTGRES_URI)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def process_item(self, item, spider):
        ciudad_name = item["ciudad"]
        barrio_name = item["barrio"]

        
        
        try:
            ciudad = self.session.query(Ciudad).filter_by(name=ciudad_name).first()
            if not ciudad:
                ciudad = Ciudad(name=ciudad_name)
                self.session.add(ciudad)
                self.session.commit()

            barrio = self.session.query(Barrio).filter_by(name=barrio_name).first()
            if not barrio:
                barrio = Barrio(name=barrio_name, ciudad_id=ciudad.id)
                self.session.add(barrio)
                self.session.commit()

            current_datetime = datetime.now(timezone.utc)

            price = float(item['Precio'].replace('$', '').replace('.', '').replace(' ', ''))

            proyecto = self.session.query(Proyecto).filter_by(nombre=item['nombre'], ciudad_id=ciudad.id, barrio_id=barrio.id, area=item['Área']).first()

            if proyecto:
                # Project already exists, update the existing row
                print(f"Updating existing project: {item['nombre']}, {ciudad_name}, {barrio_name}, {item['Área']}")

                try:
                    website_update = datetime.strptime(item["website_updated"],'%Y-%m-%d').date()

                except:
                    website_update = None

                proyecto.tipo = item['tipo']
                proyecto.propiedad = item['propiedad']
                proyecto.link = item['link']
                proyecto.precio = price
                proyecto.area = item['Área']
                proyecto.entrega = item['Entrega']
                proyecto.habitaciones = item['Habitaciones']
                proyecto.cuarto_util = item['cuarto_util']
                proyecto.baños = item['Baños']
                proyecto.parqueaderos = item['Parqueaderos']
                proyecto.estudio = item['Estudio']
                proyecto.last_updated = current_datetime.date()
                proyecto.website_updated = website_update
            else:
                # Insert data into the projects table
                try:
                    website_update = datetime.strptime(item["website_updated"],'%Y-%m-%d').date()

                except:
                    website_update = None
                proyecto = Proyecto(
                    nombre=item['nombre'],
                    tipo=item['tipo'],
                    propiedad=item['propiedad'],
                    ciudad_id=ciudad.id,
                    barrio_id=barrio.id,
                    link=item['link'],
                    precio=price,
                    area=item['Área'],
                    entrega=item['Entrega'],
                    habitaciones=item['Habitaciones'],
                    cuarto_util=item['cuarto_util'],
                    baños=item['Baños'],
                    parqueaderos=item['Parqueaderos'],
                    estudio=item['Estudio'],
                    last_updated=current_datetime.date(),
                    website_updated=website_update
                )
                self.session.add(proyecto)

            self.session.commit()

        except Exception as e:
            self.session.rollback()
            print(f"error processing item {e}")
        
        return item

    def close_spider(self, spider):
        self.session.close()