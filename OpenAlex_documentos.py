import json, os, logging, requests, math
from datetime import datetime
from OPENALEX_Mongo import MongoDB

log_file = os.path.join(os.path.dirname(__file__), 'logs', 'logs_openAlex_documentos.log')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename=log_file,
                    filemode='a')
logger = logging.getLogger('importar_documentos_OpenAlex_documentos')
loggerMongo = logging.getLogger('pymongo')
loggerMongo.setLevel(logging.WARNING)


class OpenALex:
    """
    Inicializa la clase OpenAlex cargando la configuración desde un archivo JSON,
    estableciendo la conexión a MongoDB y definiendo los contadores, la lista de documentos y el ciclo de operaciones.
    """
    def __init__(self):
        with open('config.json', 'r') as file:
            config = json.load(file)
            mongo_uri = config['DEFAULT']["Url_bd"]
            db_name = config['DEFAULT']["Nombre_bd"]
        self.mongo = MongoDB(mongo_uri, db_name)
        self.cicloInserciones = 1000
        self.listaTrabajos = []
        self.trabajosEncontrados = 0
        self.trabajosActualizados = 0
        self.numeroTrabajosInsertados = 0

    """
    Descarga los datos de todos los clientes registrados en la base de datos.
    Obtiene la lista de IDs de clientes y llama al método descargarCliente para cada uno.
    Y maneja y registra los errores de solicitud HTTP.
    """
    def descargar_todo(self):

        clientes_ids = self.mongo.obtener_ids_clientes()

        try:
            for id in clientes_ids:
                logger.info(f"Descargando cliente con id : {id}")
                self.descarga_cliente(id)
        except requests.exceptions.HTTPError as err:
            logger.error(f"Limite de peticiones alcanzado : {err}")

    """
    Descarga los documentos de un cliente específico basándose en sus afiliaciones y autores.
    Guarda la fecha de descarga y resetea los contadores.
    
    :param idCliente: ID del cliente a descargar.
    """
    def descarga_cliente(self, idCliente):
        resultado = self.mongo.obtener_configuracion_cliente(idCliente)

        afiliaciones, autores = resultado

        for id in afiliaciones:
            self.descarga_por_institucion(id["affiliationId"])

        self.descarga_por_autores(autores)
        self.mongo.guardar_fechadescarga(idCliente, self.trabajosEncontrados, self.numeroTrabajosInsertados, self.trabajosActualizados)

        self.trabajosEncontrados = 0
        self.trabajosActualizados = 0
        self.numeroTrabajosInsertados = 0

    """
    Descarga documentos de una institución asociada al cliente.
    
    :param idInstitucion: Id de la institución.
    """
    def descarga_por_institucion(self, idInstitucion):
        url = "https://api.openalex.org/works?filter=institutions.id:" + idInstitucion + "&page={}"
        self.buscar_docs(url, idInstitucion)

    """
    Descarga documentos de cada autor, asociados al cliente.
       
    :param autores: Lista de autores.
    """
    def descarga_por_autores(self, autores):

        for autor in autores:
            orcid = None
            for identificador in autor.get('identificadores', []):
                if identificador.get('tipo') == "ORCID":
                    orcid = identificador.get('_id')
                    break
                elif identificador.get('tipo') == "SCP":
                    scopus_id = identificador.get('_id')
                    orcid = self.buscar_orcid_con_scopus(scopus_id)
                    break

            if orcid:
                url = "https://api.openalex.org/works?filter=author.orcid:" + orcid + "&page={}"
                self.buscar_docs(url, orcid)

    """
    Busca el identificador ORCID de un autor utilizando su identificador Scopus.
    
    :param idScopus: Id de Scopus del autor.
    :return: ORCID del autor o None si no se encuentra.
    """
    def buscar_orcid_con_scopus(self, idScopus):
        url = f'https://api.openalex.org/authors?filter=scopus:{idScopus}'
        data = requests.get(url).json()
        for autor in data['results']:
            if autor is None:
                logger.debug(f"No se encontró ORCID para el codigo Scopus:{idScopus}")
                return None
            else:
                return autor['orcid']

    """
    Realiza la búsqueda y descarga de documentos a partir de una URL paginada.
    Procesa los resultados e inserta los documentos en MongoDB.
    
    :param urlpagina: URL para la búsqueda de documentos con paginación.
    :param id: Id de la institución o autor para el registro en el log.
    """
    def buscar_docs(self, urlpagina, id):
        totalTrabajosProcesados = 0

        numeroTotalPaginas = self.numero_total_paginas(urlpagina.format(1))

        urlEncontrados = urlpagina.format(1)
        dataEncontrados = requests.get(urlEncontrados).json()
        encontrados = json.loads(json.dumps(dataEncontrados['meta']['count']))
        self.trabajosEncontrados += encontrados

        for numeroPagina in range(1, numeroTotalPaginas + 1):
            try:
                url = urlpagina.format(numeroPagina)
                data = requests.get(url).json()
                resultados_pagina = json.loads(json.dumps(data['results']))
                totalTrabajosProcesados += self.comprobar_insertar_trabajosPorPagina(resultados_pagina)
            except requests.exceptions.RequestException as e:
                logger.error(f"Error al realizar la solicitud de la pagina {numeroPagina} : {e}")
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
    def comprobar_insertar_trabajosPorPagina(self, diccionario):
        contadorTrabajosProcesados = 0
        self.cambiar_resumen(diccionario)

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
   
   :param url: URL para la búsqueda.
   :return: Número total de páginas.
   """
    def numero_total_paginas(self, url):
        data = requests.get(url).json()

        totalTrabajos = int(data['meta']['count'])

        numeroPaginas = math.ceil(totalTrabajos / 25)
        return numeroPaginas

    """
    Cambia el abstract_inverted_index de los documentos por un texto reconstruido.
    
    :param documentos: Lista de documentos con índices invertidos en sus resúmenes.
    """
    def cambiar_resumen(self, documentos):
        for documento in documentos:
            texto = documento.get("abstract_inverted_index")
            texto_reconstruido = self.reconstruir_abstract(texto)
            documento["abstract_inverted_index"] = texto_reconstruido

    """
    Reconstruye el abstract_inverted_index en un texto completo.
    
    :param resumen: Abstract_inverted_index del documento.
    :return: Texto completo reconstruido.
    """
    def reconstruir_abstract(self, resumen):
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
