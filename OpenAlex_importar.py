import json

with open('config.json', 'r') as file:
    config = json.load(file)
    url = config['DEFAULT']["Url_bd"]
    nombre = config['DEFAULT']["Nombre_bd"]
    print(f"{url}\n{nombre}")
