import requests


class OpenAlex_acceso:

    """
    Retorna los datos de trabajos asociados a una institución específica.

    :param idInstitucion: ID de la institución.
    :param pag: Número de página para la paginación de resultados.
    :return: Documentos asociados a la institución en formato JSON.
    """
    @staticmethod
    def url_TrabajosInstitucion(idInstitucion, pag):
        url = f'https://api.openalex.org/works?filter=institutions.id:{idInstitucion}&page={pag}'
        response = requests.get(url)
        data = response.json()
        return data

    """
    Retorna los datos de trabajos asociados a un autor específico.

    :param orcid: Identificador ORCID del autor.
    :param pag: Número de página para la paginación de resultados.
    :return: Documentos asociados al autor en formato JSON.
    """
    @staticmethod
    def url_TrabajosAutor(orcid, pag):
        url = f'https://api.openalex.org/works?filter=author.orcid:{orcid}&page={pag}'
        response = requests.get(url)
        data = response.json()
        return data

    """
    Busca un autor utilizando su identificador Scopus.

    :param idScopus: Identificador Scopus del autor.
    :return: Datos del autor en formato JSON.
    """
    @staticmethod
    def url_BuscarAutor_Scopus(idScopus):
        url = f'https://api.openalex.org/authors?filter=scopus:{idScopus}'
        response = requests.get(url)
        data = response.json()
        return data
