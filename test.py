from datetime import date
from bs4 import BeautifulSoup
import requests
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os
import re
import json

def get_page_content(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    else:
        return None

def get_dolar_oficial():
    soup=BeautifulSoup(get_page_content("https://dolarhoy.com/cotizaciondolaroficial"), 'html.parser')
    return soup.select_one('div.tile.is-parent.is-8 > div.tile.is-child:nth-of-type(2) > div.value').text.strip().replace("$", "").replace(",", ".")

def get_dolar_blue():
    soup=BeautifulSoup(get_page_content("https://dolarhoy.com/cotizaciondolarblue"), 'html.parser')
    return soup.select_one('div.tile.is-parent.is-8 > div.tile.is-child:nth-of-type(2) > div.value').text.strip().replace("$", "").replace(",", ".")


def get_mexx_products(dolar_oficial, dolar_blue):
    mexx_categories = ["notebooks", "motherboards", "procesadores", "memorias-ram", "almacenamiento", "placas-de-video", "fuentes-de-poder", "gabinetes", "refrigeracion-pc", "teclados,-mouses-y-pads", "auriculares-y-microfonos", "camaras-web-e-ip", "monitores"]
    products = []

    for category in mexx_categories:
        category_url = "https://www.mexx.com.ar/" + "productos-rubro/" + category + "/?all=1"
        category_content = get_page_content(category_url)
        
        if category_content:
            # Crear el objeto BeautifulSoup a partir del contenido HTML
            soup = BeautifulSoup(category_content, 'html.parser')
            
            # Encontrar los productos de la categoría
            category_products = soup.find_all('div', class_='card card-ecommerce mt-0 ta-c')
            
            # Recorrer los productos y extraer la información
            for product in category_products:
                product_id = product.find('div', class_='card-body px-3 pb-0 pt-0').find('h4').find('a')['href'].split('/')[-1].split('-')[0]
                product_image = product.find('div', class_='view overlay px-20 mi-h-200').find('a').find('img')['src']
                product_title = product.find('div', class_='card-body px-3 pb-0 pt-0').find('h4').find('a').text.strip()
                product_price = product.find('div', class_='price').find('h4').find('b').text.strip().replace("$", "")
                product_date = date.today().strftime("%d/%m/%Y")
                product_dolar_oficial_price = round(int(product_price.replace('.', "")) / float(dolar_oficial), 2)
                product_dolar_blue_price = round(int(product_price.replace('.', "")) / float(dolar_blue), 2)

                try:
                    product.find('div', class_='enstocklistado')
                    product_stock = "Si"
                except:
                    product_stock = "No"

                products.append({"id":product_id, "image":product_image, "title": product_title, "category":category.replace("-", " "), "price":product_price, "stock":product_stock, "date":product_date, "dolar_oficial":dolar_oficial, "oficial_price":product_dolar_oficial_price, "dolar_blue":dolar_blue, "blue_price":product_dolar_blue_price})

    return products


def get_logg_hardstore_products(dolar_oficial, dolar_blue):
    logg_hardstore_categories = ["Notebooks", "Motherboards", "Procesadores", "MemoriaRAM", "Almacenamiento", "Placasdevideo", "Fuentesdealimentacion", "Gabinetes", "Refrigeracion", "Perifericos", "Audio", "Pantallasyvideo"]
    products = []

    for category in logg_hardstore_categories:
        category_url = "https://www.logg.com.ar/" + "Products?SelectedViewMode=Vertical&CategoryName=" + category + "&CategoriesSelected=&ManufacturersSelected=&CurrentPage=0&PageSize=100&Order=7&FilterText=&/"
        
        category_content = get_page_content(category_url)

        if category_content:
            # Crear el objeto BeautifulSoup a partir del contenido HTML
            soup = BeautifulSoup(category_content, 'html.parser')
            
            # Encontrar los productos de la categoría
            category_products = soup.find_all('a', class_='product-card')

            # Recorrer los productos y extraer la información
            for product in category_products:
                product_id = re.search(r"clickProduct\(.*?, '(\d+)',", product.get('onclick')).group(1)
                product_image = product.find('div', class_='card-img-container').find('img')['src']
                product_title = product.find('div', class_='card-body').find('p', class_='card-text').text.strip()
                product_price = product.find('div', class_='card-body').find('h5', class_='card-price').find('span').text.strip().replace("$", "")
                product_date = date.today().strftime("%d/%m/%Y")
                product_dolar_oficial_price = round(int(product_price.replace('.', "")) / float(dolar_oficial), 2)
                product_dolar_blue_price = round(int(product_price.replace('.', "")) / float(dolar_blue), 2)

                product_stock = "Si"

                products.append({"id":product_id,"image":product_image, "title":product_title, "category":category, "price":product_price, "stock":product_stock, "date":product_date, "dolar_oficial":dolar_oficial, "oficial_price":product_dolar_oficial_price, "dolar_blue": dolar_blue, "blue_price":product_dolar_blue_price})

    return products

def lambda_handler():
    uri = f"mongodb+srv://{os.environ.get('MONGO_DB_USERNAME')}:{os.environ.get('MONGO_DB_PASSWORD')}@hardware-data-analysis.9klxpr8.mongodb.net/?retryWrites=true&w=majority"

    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client['hardware-data-analysis']

    dolar_oficial = get_dolar_oficial()
    dolar_blue = get_dolar_blue()

    mexx_products = get_mexx_products(dolar_oficial, dolar_blue)
    logg_hardstore_products = get_logg_hardstore_products(dolar_oficial, dolar_blue)

    products_collection = db["mexx_products"]
    products_collection.insert_many(mexx_products)

    products_collection = db["logg_hardstore_products"]
    products_collection.insert_many(logg_hardstore_products)

    return {
        'statusCode': 200,
        'body': json.dumps('Code executed successfully!')
    }

lambda_handler()