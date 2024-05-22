import json, requests, math
from datetime import datetime
import time

from OpenAlex_mongo import MongoDB
import logging.config

from OpenAlex_acceso import OpenAlex_acceso

logging.config.fileConfig('logging.ini')
logger = logging.getLogger('documentos_importar_OpenAlex')

class OpenALex:
    """
    Inicializa la clase OpenAlex cargando la configuración desde un archivo JSON,
    estableciendo la conexión a MongoDB y definiendo los contadores, la lista de documentos y el ciclo de operaciones.
    """
    def __init__(self):
        self.mongo = MongoDB()
        self.cicloInserciones = 1000
        self.listaTrabajos = []
        self.trabajosEncontrados = 0
        self.trabajosActualizados = 0
        self.numeroTrabajosInsertados = 0
        self.trabajosErroneos = 0
        self.inicio = 0

    """
    Descarga los datos de todos los clientes registrados en la base de datos.
    Obtiene la lista de IDs de clientes y llama al método descargarCliente para cada uno.
    Y maneja y registra los errores de solicitud HTTP.
    """
    def descargar_todo(self):

        configuraciones = self.mongo.obtener_configuraciones()

        for configuracion in configuraciones:
            id = configuracion['clienteId']
            try:
                logger.info(f"Descargando cliente con id : {id}")
                self._descarga_configuracion(configuracion)
            except requests.exceptions.HTTPError as err:
                logger.error(f"Limite de peticiones alcanzado : {err}")
                tiempo = time.time()-self.inicio
                self.mongo.guardar_fecha_descarga(configuracion['_id'], id, self.trabajosEncontrados, self.numeroTrabajosInsertados, self.trabajosActualizados, self.trabajosErroneos, False, err, tiempo)
                self._limpiar_Contadores()
            except Exception as e:
                logger.error({e})
                tiempo = time.time()-self.inicio
                self.mongo.guardar_fecha_descarga(configuracion['_id'], id, self.trabajosEncontrados, self.numeroTrabajosInsertados, self.trabajosActualizados, self.trabajosErroneos, False, e, tiempo)
                self._limpiar_Contadores()
                continue


    """
    Descarga los documentos de un cliente específico basándose en sus afiliaciones y autores.
    Guarda la fecha de descarga y resetea los contadores.
    
    :param idCliente: ID del cliente a descargar.
    """
    def _descarga_configuracion(self, configuracion):
        self.inicio = time.time()

        afiliaciones = configuracion["affiliations"]
        autores = configuracion["autores"]

        for id in afiliaciones:
            self._descarga_por_institucion(id["affiliationId"])

        self._descarga_por_autores(autores)

        tiempo = time.time()-self.inicio

        self.mongo.guardar_fecha_descarga(configuracion['_id'], configuracion['clienteId'], self.trabajosEncontrados, self.numeroTrabajosInsertados, self.trabajosActualizados, self.trabajosErroneos, True, None, tiempo)

        self._limpiar_Contadores()

    def _limpiar_Contadores(self):
        self.trabajosEncontrados = 0
        self.trabajosActualizados = 0
        self.numeroTrabajosInsertados = 0
        self.trabajosErroneos = 0
        self.inicio = 0

    """
    Descarga documentos de una institución asociada al cliente.
    
    :param idInstitucion: Id de la institución.
    """
    def _descarga_por_institucion(self, idInstitucion):
        self._buscar_docs(idInstitucion, 0)

    """
    Descarga documentos de cada autor, asociados al cliente.
       
    :param autores: Lista de autores.
    """
    def _descarga_por_autores(self, autores):

        for autor in autores:
            orcid = None
            for identificador in autor.get('identificadores', []):
                if identificador.get('tipo') == "ORCID":
                    orcid = identificador.get('_id')
                    break
                elif identificador.get('tipo') == "SCP":
                    scopus_id = identificador.get('_id')
                    orcid = self._buscar_orcid_con_scopus(scopus_id)
                    break

            if orcid:
                self._buscar_docs(orcid, 1)

    """
    Busca el identificador ORCID de un autor utilizando su identificador Scopus.
    
    :param idScopus: Id de Scopus del autor.
    :return: ORCID del autor o None si no se encuentra.
    """
    def _buscar_orcid_con_scopus(self, idScopus):
        data = OpenAlex_acceso.url_BuscarAutor_Scopus(idScopus)
        for autor in data['results']:
            if autor is None:
                logger.debug(f"No se encontró ORCID para el codigo Scopus:{idScopus}")
                return None
            else:
                return autor['orcid']

    """
    Realiza la búsqueda y descarga de documentos de una institución o autor.
    Procesa los resultados e inserta los documentos en MongoDB.
    
    :param id: Id de la institución o autor para el registro en el log.
    :param tipo: Si es 0 sera el id de una institución, si es 1 el orcid de un autor
    """
    def _buscar_docs(self, id, tipo):

        totalTrabajosProcesados = 0

        if tipo == 0:
            data = OpenAlex_acceso.url_TrabajosInstitucion(id, 1)
        else:
            data = OpenAlex_acceso.url_TrabajosAutor(id, 1)

        numeroTotalPaginas = self._numero_total_paginas(data)

        encontrados = json.loads(json.dumps(data['meta']['count']))
        self.trabajosEncontrados += encontrados

        for numeroPagina in range(1, numeroTotalPaginas + 1):
            try:
                if tipo == 0:
                    dataTrabajos = OpenAlex_acceso.url_TrabajosInstitucion(id, numeroPagina)
                else:
                    dataTrabajos = OpenAlex_acceso.url_TrabajosAutor(id, numeroPagina)
                resultados_pagina = json.loads(json.dumps(dataTrabajos['results']))
                totalTrabajosProcesados += self._comprobar_insertar_trabajosPorPagina(resultados_pagina)
            except requests.exceptions.RequestException as e:
                logger.error(f"Error en la configuracion con id {id} al realizar la solicitud de la pagina {numeroPagina} : {e}")
                self.trabajosErroneos += 25
                continue
        if len(self.listaTrabajos) > 0:
            self.numeroTrabajosInsertados += len(self.listaTrabajos)
            self.mongo.insertar(self.listaTrabajos)
        logger.info(f"Trabajos procesados con id {id} : {totalTrabajosProcesados}/{encontrados}")

    """
    Procesa e inserta los documentos de cada página en MongoDB.
    Comprueba si el documento es repetido, es decir, si esta ya insertado en MongoDB.
    
    :param diccionario: Diccionario con los documentos a procesar.
    :return: Número de trabajos procesados en la página.
    """
    def _comprobar_insertar_trabajosPorPagina(self, diccionario):
        contadorTrabajosProcesados = 0
        self._cambiar_resumen(diccionario)

        for trabajo in diccionario:
            contadorTrabajosProcesados += 1
            if not self.mongo.isRepetido(trabajo, self):
                diccionarioFinal = {"documento": trabajo, "fechaCrea": datetime.now(),
                                    "fechaModi": datetime.now(), "version": 0}
                self.listaTrabajos.append(diccionarioFinal)
                logger.debug(f'Trabajo añadido con id: {trabajo["id"]}')
                if len(self.listaTrabajos) >= self.cicloInserciones:
                    self.numeroTrabajosInsertados += len(self.listaTrabajos)
                    self.mongo.insertar(self.listaTrabajos)
        return contadorTrabajosProcesados

    """
   Calcula el número total de páginas de resultados para una búsqueda.
   
   :param data: resultado de una peticion a la API de OpenAlex.
   :return: Número total de páginas.
   """
    def _numero_total_paginas(self, data):

        totalTrabajos = int(data['meta']['count'])

        numeroPaginas = math.ceil(totalTrabajos / 25)
        return numeroPaginas

    """
    Cambia el abstract_inverted_index de los documentos por un texto reconstruido.
    
    :param documentos: Lista de documentos con índices invertidos en sus resúmenes.
    """
    def _cambiar_resumen(self, documentos):
        for documento in documentos:
            texto = documento.get("abstract_inverted_index")
            texto_reconstruido = self._reconstruir_abstract(texto)
            documento["abstract_inverted_index"] = texto_reconstruido

    """
    Reconstruye el abstract_inverted_index en un texto completo.
    
    :param resumen: Abstract_inverted_index del documento.
    :return: Texto completo reconstruido.
    """
    def _reconstruir_abstract(self, resumen):
        # Crear una lista vacía para almacenar las palabras reconstruidas
        texto_reconstruido = []

        if resumen is not None:
            # Iterar sobre las claves y valores del diccionario
            for palabra, posicion in resumen.items():
                # Iterar sobre las posiciones de cada palabra
                for position in posicion:
                    # Si la posición está dentro del rango actual de la lista reconstruida,
                    # inserta la palabra en esa posición
                    if position < len(texto_reconstruido):
                        texto_reconstruido[position] = palabra
                    # Si la posición está fuera del rango actual de la lista reconstruida,
                    # extiende la lista con espacios en blanco hasta la posición correcta
                    else:
                        texto_reconstruido.extend([''] * (position - len(texto_reconstruido)))
                        texto_reconstruido.append(palabra)

        # Unir las palabras reconstruidas en una cadena de texto
        texto_reconstruido = ' '.join(texto_reconstruido)

        return texto_reconstruido


if __name__ == "__main__":
    op = OpenALex()
    op.descargar_todo()
