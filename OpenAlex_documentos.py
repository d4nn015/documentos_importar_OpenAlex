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

    def __init__(self):
        with open('config.json', 'r') as file:
            config = json.load(file)
            mongo_uri = config['DEFAULT']["Url_bd"]
            db_name = config['DEFAULT']["Nombre_bd"]
        self.mongo = MongoDB(mongo_uri, db_name)
        self.cicloInserciones = 2
        self.listaTrabajos = []
        self.trabajosEncontrados = 0
        self.trabajosActualizados = 0
        self.numeroTrabajosInsertados = 0

    def descargarTodo(self):

        clientes_ids = self.mongo.obtener_ids_clientes()

        try:
            for id in clientes_ids:
                logger.info(f"Descargando cliente con id : {id}")
                self.descargarCliente(id)
        except requests.exceptions.HTTPError as err:
            logger.error(f"Limite de peticiones alcanzado : {err}")


    def descargarCliente(self, idCliente):
        resultado = self.mongo.obtener_configuarion_cliente(idCliente)

        afiliaciones, autores = resultado

        for id in afiliaciones:
            self.descarga_por_institucion(id["affiliationId"])

        self.descarga_por_autores(autores)
        self.mongo.guardar_fechadescarga(idCliente, self.trabajosEncontrados, self.numeroTrabajosInsertados, self.trabajosActualizados)

        self.trabajosEncontrados = 0
        self.trabajosActualizados = 0
        self.numeroTrabajosInsertados = 0

    def descarga_por_institucion(self, idInstitucion):
        url = "https://api.openalex.org/works?filter=institutions.id:" + idInstitucion + "&page={}"
        self.buscar_docs(url, idInstitucion)

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

    def buscar_orcid_con_scopus(self, idScopus):
        url = f'https://api.openalex.org/authors?filter=scopus:{idScopus}'
        data = requests.get(url).json()
        for autor in data['results']:
            if autor is None:
                logger.debug(f"No se encontró ORCID para el codigo Scopus:{idScopus}")
                return None
            else:
                return autor['orcid']

    def buscar_docs(self, urlpagina, id):
        totalTrabajosProcesados = 0

        numeroTotalPaginas = self.numero_Total_Paginas(urlpagina.format(1))

        urlEncontrados = urlpagina.format(1)
        dataEncontrados = requests.get(urlEncontrados).json()
        encontrados = json.loads(json.dumps(dataEncontrados['meta']['count']))
        self.trabajosEncontrados += encontrados

        for numeroPagina in range(1, numeroTotalPaginas + 1):
            try:
                url = urlpagina.format(numeroPagina)
                data = requests.get(url).json()
                resultados_pagina = json.loads(json.dumps(data['results']))
                totalTrabajosProcesados += self.recorrerInsertarTrabajos_porPagina(resultados_pagina)
            except requests.exceptions.RequestException as e:
                logger.error(f"Error al realizar la solicitud de la pagina {numeroPagina} : {e}")
                continue
        if len(self.listaTrabajos) > 0:
            self.numeroTrabajosInsertados += len(self.listaTrabajos)
            self.mongo.insertar(self.listaTrabajos)
        logger.info(f"Trabajos procesados con id {id} : {totalTrabajosProcesados}/{encontrados}")

    def recorrerInsertarTrabajos_porPagina(self, diccionario):
        contadorTrabajosProcesados = 0
        self.cambiar_resumen(diccionario)

        for trabajo in diccionario:
            contadorTrabajosProcesados += 1
            if not self.mongo.isRepetido(trabajo, self.trabajosActualizados):
                diccionarioFinal = {"documento": trabajo, "fechaCrea": datetime.now(),
                                    "fechaModi": datetime.now(), "version": 0}
                self.listaTrabajos.append(diccionarioFinal)
                logger.debug(f'Trabajo añadido con id: {trabajo["id"]}')
                if len(self.listaTrabajos) >= self.cicloInserciones:
                    self.numeroTrabajosInsertados += len(self.listaTrabajos)
                    self.mongo.insertar(self.listaTrabajos)
        return contadorTrabajosProcesados

    def numero_Total_Paginas(self, url):
        data = requests.get(url).json()

        totalTrabajos = int(data['meta']['count'])

        numeroPaginas = math.ceil(totalTrabajos / 25)
        return numeroPaginas

    def cambiar_resumen(self, documentos):
        for documento in documentos:
            texto = documento.get("abstract_inverted_index")
            texto_reconstruido = self.reconstruir_abstract(texto)
            documento["abstract_inverted_index"] = texto_reconstruido

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
    op.descargarTodo()
