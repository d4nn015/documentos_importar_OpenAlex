import requests


class OpenAlex_acceso:

    @staticmethod
    def url_TrabajosInstitucion(idInstitucion, pag):
        url = f'https://api.openalex.org/works?filter=institutions.id:{idInstitucion}&page={pag}'
        response = requests.get(url)
        data = response.json()
        return data

    @staticmethod
    def url_TrabajosAutor(orcid, pag):
        url = f'https://api.openalex.org/works?filter=author.orcid:{orcid}&page={pag}'
        response = requests.get(url)
        data = response.json()
        return data

    @staticmethod
    def url_BuscarAutor_Scopus(idScopus):
        url = f'https://api.openalex.org/authors?filter=scopus:{idScopus}'
        response = requests.get(url)
        data = response.json()
        return data
