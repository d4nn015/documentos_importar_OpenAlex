import json
from pymongo import MongoClient
from datetime import datetime, timedelta
import logging.config

logging.config.fileConfig('logging.ini')
logger = logging.getLogger('documentos_importar_OpenAlex_mongo')

class MongoDB:

    """
    Inicializa la conexion con mongoDB
    """
    def __init__(self):
        with open('config.json', 'r') as file:
            config = json.load(file)
            self.mongo_uri = config['DEFAULT']["Url_bd"]
            self.db_name = config['DEFAULT']["Nombre_bd"]

    """
    Inserta una lista de trabajos en la base de datos.
    """
    def insertar(self, listaTrabajos):

        if not listaTrabajos:
            return
        try:
            client = MongoClient(self.mongo_uri)
            db = client[self.db_name]
            collection = db["documentos"]

            collection.insert_many(listaTrabajos)
            listaTrabajos.clear()

            client.close()

        except Exception as e:
            logger.error(f"Error insertando la lista : {e}")

    """
    Registra la fecha de descarga, el id de un cliente y su número de documentos procesados, importados y actualizados.
    """
    def guardar_fecha_descarga(self, configuracionId, idCliente, num_procesados, num_importados, num_actualizados, num_erroneos, estado, resultMessage, tiempo):
        fecha_actual = datetime.now()

        if estado == True:
            estado = 'SUCCESS'
        else:
            estado = 'ERROR'

        client = MongoClient(self.mongo_uri)
        db = client[self.db_name]
        coleccion = db['descargas']

        coleccion.insert_one({'fechaCrea': fecha_actual,
                              'configuracionId': configuracionId,
                              'clienteId': idCliente,
                              'documentosEncontrados': num_procesados,
                              'documentosImportados': num_importados,
                              'documentosActualizados': num_actualizados,
                              'documentosErroneos': num_erroneos,
                              'tiempo': tiempo,
                              'estado': estado,
                              'resultMessage': resultMessage,
                              })

    """
    Comprueba si un documento ya está presente en la base de datos. Si lo está y además no es idéntico (lo han actualizado en OpenAlex), se actualiza. 

    :param diccionarioTrabajo: Diccionario del trabajo a comprobar.
    :param clase_OpenAlex_documentos: Instancia de la clase OpenAlex.
    :return: True si el documento está repetido, False en caso contrario.
    """
    def isRepetido(self, diccionarioTrabajo, clase_OpenAlex_documentos):
        cliente = MongoClient(self.mongo_uri)
        db = cliente[self.db_name]
        coleccion = db["documentos"]

        documentos = coleccion.find({"documento.id": diccionarioTrabajo['id']})

        for trabajo in documentos:
            if not self._compararRepetidos_FechaActualizacion(trabajo, diccionarioTrabajo):

                nuevaVersion = trabajo["version"] + 1
                fechaModificacion = datetime.now()
                coleccion.update_one({"_id": trabajo["_id"]}, {
                    "$set": {"documento": diccionarioTrabajo, "fechaModi": fechaModificacion, "version": nuevaVersion}})
                logger.debug(f'Trabajo actualizado con id: {trabajo["documento"]["id"]}')
                clase_OpenAlex_documentos.trabajosActualizados += 1
                return True
            else:
                logger.debug('Ya existe un trabajo identico')
                cliente.close()
                return True
        cliente.close()

        return False

    """
    Compara las fechas de actualización de dos trabajos que son el mismo, para determinar si se ha actualizado.

    :param trabajoColeccion: Trabajo almacenado en la colección de MongoDB.
    :param diccionarioTrabajo: Trabajo a comparar.
    :return: True si las fechas de actualización son diferentes, False si son la misma.
    """
    def _compararRepetidos_FechaActualizacion(self, trabajoColeccion, diccionarioTrabajo):
        if 'updated_date' in trabajoColeccion["documento"] and 'updated_date' in diccionarioTrabajo:
            return trabajoColeccion["documento"]['updated_date'] != diccionarioTrabajo['updated_date']

    """
    Obtiene los IDs de los clientes registrados en la base de datos.

    :return: Lista de IDs de clientes.
    """
    def obtener_configuraciones(self):
        clienteMongo = MongoClient(self.mongo_uri)
        db = clienteMongo[self.db_name]
        coleccion = db['configuraciones']
        listaConfiguraciones = []

        listaIdClientesFechasOrdenadas = self._listaIdClientes_OrdenadosPorFecha()
        listaIdClientes = [doc['clienteId'] for doc in coleccion.find()]

        for clienteId in listaIdClientes:
            if clienteId not in listaIdClientesFechasOrdenadas:
                cliente = coleccion.find_one({"clienteId": clienteId})
                if cliente["enabled"] is True:
                    listaConfiguraciones.append(cliente)

        for idCliente in listaIdClientesFechasOrdenadas:
            cliente = coleccion.find_one({"clienteId": idCliente})
            if cliente["enabled"] is True and self._comprobar_FechaCliente(cliente):
                listaConfiguraciones.append(cliente)

        clienteMongo.close()

        return listaConfiguraciones

    """
    Comprueba si la fecha de descarga para un cliente ha superado su periodicidad establecida.

    :return: True si han pasado los dias establecidos
    """
    def _comprobar_FechaCliente(self, cliente):
        clienteMongo = MongoClient(self.mongo_uri)
        bd = clienteMongo[self.db_name]
        coleccionFechas = bd["descargas"]
        fechaCliente = coleccionFechas.find_one({"clienteId": cliente["clienteId"]})
        if (datetime.now() - fechaCliente["fecha"]) > timedelta(days=cliente["periodicidad"]):
            return True

    """
    Elimina elementos duplicados de una lista manteniendo el orden original.

    :return: Lista única sin elementos duplicados.
    """
    def _eliminar_repetidos_listaClientes(self, lista):
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

    """
    Obtiene los IDs de clientes ordenados por fecha de descarga.
    """
    def _listaIdClientes_OrdenadosPorFecha(self):
        clienteMongo = MongoClient(self.mongo_uri)
        db = clienteMongo[self.db_name]
        coleccionFecha = db["descargas"]

        resultados = coleccionFecha.find().sort("fecha", 1)

        ids_ordenados = [doc['clienteId'] for doc in resultados]

        clienteMongo.close()

        return self._eliminar_repetidos_listaClientes(ids_ordenados)

