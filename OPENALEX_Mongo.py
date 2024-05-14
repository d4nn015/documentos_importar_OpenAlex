from pymongo import MongoClient
from datetime import datetime, timedelta
import os, logging

log_file = os.path.join(os.path.dirname(__file__), 'logs', 'logs_openAlex.log')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename=log_file,
                    filemode='a')


class MongoDB:

    def __init__(self, mongo_uri, db_name):
        self.mongo_uri = mongo_uri
        self.db_name = db_name

    def insertar(self, listaTrabajos):

        if not listaTrabajos:  # Verificar si la lista de documentos está vacía
            print("No hay documentos para guardar.")
            return
        try:
            client = MongoClient(self.mongo_uri)
            db = client[self.db_name]
            collection = db["prueba"]

            collection.insert_many(listaTrabajos)
            listaTrabajos.clear()

            client.close()

        except Exception as e:
            print("Error")

    def borrartodo(self, nombre_coleccion):
        client = MongoClient(self.mongo_uri)
        db = client[self.db_name]
        collection = db[nombre_coleccion]

        collection.delete_many({})
        logging.info("Datos borrados correctamente")

    def guardar_fechadescarga(self, idCliente, num_procesados, num_importados, num_actualizados):
        fecha_actual = datetime.now()

        client = MongoClient(self.mongo_uri)
        db = client[self.db_name]
        coleccion = db['fecha_descarga']

        coleccion.insert_one({'ClienteId': idCliente,
                              'fecha': fecha_actual,
                              'Documentos encontrados': num_procesados,
                              'Documentos importados': num_importados,
                              'Documentos actualizados': num_actualizados
                              })

    def obtener_orcid_autor(self):
        client = MongoClient(self.mongo_uri)
        db = client[self.db_name]
        coleccion = db['configuraciones']

        # Obtener el documento de configuraciones
        documento_conf = coleccion.find_one({})

        autores = {}
        contador = 0

        for autor in documento_conf.get("autores", []):
            ids = {"ORCID": '', "SCP": ''}  # Reiniciar el diccionario de identificadores para cada autor
            for identificador in autor.get("identificadores", []):
                if identificador.get("tipo") == "ORCID":
                    ids["ORCID"] = identificador.get("_id")
                else:
                    ids["SCP"] = identificador.get("_id")
            autores[contador] = ids  # Agregar los identificadores al diccionario de autores
            contador += 1

        return autores

    def isRepetido(self, diccionarioTrabajo, trabajosActualizados):
        cliente = MongoClient(self.mongo_uri)
        db = cliente[self.db_name]
        coleccion = db["prueba"]

        documentos = coleccion.find({"documento.id": diccionarioTrabajo['id']})

        for trabajo in documentos:
            print(self.compararRepetidosFecha(trabajo, diccionarioTrabajo))
            if not self.compararRepetidosFecha(trabajo, diccionarioTrabajo):

                nuevaVersion = trabajo["version"] + 1
                fechaModificacion = datetime.now()
                coleccion.update_one({"_id": trabajo["_id"]}, {
                    "$set": {"documento": diccionarioTrabajo, "fechaModi": fechaModificacion, "version": nuevaVersion}})
                logging.info(f'Trabajo actualizado con id: {trabajo["documento"]["id"]}')
                trabajosActualizados[0] += 1
                return True
            else:
                logging.info('Ya existe un trabajo identico')
                cliente.close()
                return True
        cliente.close()

        return False

    # Compara un objeto de la coleccion y un diccionario de un trabajo, con el mismo id, si el campo "updated_date" es distinto devuelve true y si no false
    def compararRepetidosFecha(self, trabajoColeccion, diccionarioTrabajo):
        if 'updated_date' in trabajoColeccion["documento"] and 'updated_date' in diccionarioTrabajo:
            return trabajoColeccion["documento"]['updated_date'] != diccionarioTrabajo['updated_date']

    def obtener_ids_clientes(self):
        clienteMongo = MongoClient(self.mongo_uri)
        db = clienteMongo[self.db_name]
        coleccion = db['configuraciones']
        listaId = []

        listaIdClientesFechasOrdenadas = self.eliminar_repetidos_listaClientes(self.idClientes_porFecha())
        listaIdClientes = [doc['clienteId'] for doc in coleccion.find()]

        for clienteId in listaIdClientes:
            if clienteId not in listaIdClientesFechasOrdenadas:
                cliente = coleccion.find_one({"clienteId": clienteId})
                if cliente["enabled"] is True:
                    listaId.append(cliente)

        for idCliente in listaIdClientesFechasOrdenadas:
            cliente = coleccion.find_one({"clienteId": idCliente})
            if cliente["enabled"] is True and self.comprobarFechaCliente(cliente):
                listaId.append(cliente)

        clienteMongo.close()

        return listaId

    def comprobarFechaCliente(self, cliente):
        clienteMongo = MongoClient(self.mongo_uri)
        bd = clienteMongo[self.db_name]
        coleccionFechas = bd["fecha_descarga"]
        fechaCliente = coleccionFechas.find_one({"clienteId": cliente["clienteId"]})
        if (datetime.now() - fechaCliente["fecha"]) > timedelta(days=cliente["periodicidad"]):
            return True

    def eliminar_repetidos_listaClientes(self, lista):
        # Creamos un diccionario para almacenar el ID y su posición más baja
        id_posiciones = {}
        for i, id in enumerate(lista):
            if id not in id_posiciones:
                # Si el ID no está en el diccionario, lo agregamos con su posición
                id_posiciones[id] = i
            else:
                # Si el ID ya está en el diccionario, actualizamos su posición si es menor
                id_posiciones[id] = min(id_posiciones[id], i)

        # Creamos una lista única usando las posiciones más bajas de cada ID
        lista_unica = [id for id, pos in sorted(id_posiciones.items(), key=lambda x: x[1])]
        return lista_unica

    def idClientes_porFecha(self):
        clienteMongo = MongoClient(self.mongo_uri)
        db = clienteMongo[self.db_name]
        coleccionFecha = db["fecha_descarga"]

        resultados = coleccionFecha.find().sort("fecha", 1)

        ids_ordenados = [doc['clienteId'] for doc in resultados]

        clienteMongo.close()

        return ids_ordenados

    def obtener_configuarion_cliente(self, idCliente):
        cliente = MongoClient(self.mongo_uri)
        db = cliente[self.db_name]
        coleccion = db['configuraciones']

        documento = coleccion.find_one({"clienteId": idCliente})

        afiliaciones = documento["affiliations"]
        autores = documento["autores"]

        return afiliaciones, autores