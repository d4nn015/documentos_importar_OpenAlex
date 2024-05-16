# Documentos importar OpenAlex

## OpenAlex_documentos

Implementa una clase, diseñado para gestionar la descarga y almacenamiento de documentos de OpenAlex en MongoDB. La clase `OpenAlex` facilita la interacción con la API de OpenAlex para obtener los documentos sobre las afiliaciones y autores de cada cliente que luego almacena en MongoDB. En esta clase, se maneja la descarga de datos, el procesamiento y la inserción en la base de datos, además de registrar eventos y errores para el seguimiento y la resolución de problemas.

### Método `descargar_todo`

Inicia el proceso de descarga de documentos para todos los clientes registrados en la colección "configuraciones" en MongoDB. Obtiene una lista de IDs de los clientes cuya ultima fecha de descarga es mayor al periodo definido en su configuración y estén habilitados. Itera sobre cada id de cliente y llama al método `descargaCliente`.


### Método `_descarga_cliente`

Maneja la descarga de documentos de un cliente en específico. Primero obtiene la configuración del cliente, códigos de las instituciones y autores, desde MongoDB y llama a los métodos de `descarga_por_institucion` y `descarga_por_autores`. Y por último, guarda la fecha de descarga del cliente y resetea los contadores.


###  Método `_descarga_por_institucion`

Descarga los documentos asociados a la institución pasada como parámetro "idInstitucion". Construye la url especifica para la institución utilizando su id y llama al método de `buscar_docs`.


### Método `_descarga_por_autores`

Descarga los documentos asociados a un cliente específico mediante el ORCID. De MongoDB, obtiene la lista de identificadores de los autores. Un autor puede tener ORCID o no. Itera sobre la lista, y si el autor tiene ORCID, construye la url y llama al método de `buscar_docs`. Si no tiene, intenta obtenerlo desde OpenAlex utilizando el identificador de Scopus, mediante el método `buscar_orcid_con_scopus`.


### Método `_buscar_orcid_con_scopus`

Obtiene el identificador ORCID de un autor utilizando su identificador de Scopus. Construye la url y realiza una solicitud a la API de OpenAlex. Procesa la respuesta y devuelve el ORCID si se encuentra, o `None` si no se encuentra.


### Método `_buscar_docs`

Realiza la búsqueda y descarga de documentos a partir de una url paginada. Primero calcula el número total de páginas de resultados utilizando el método `numero_Total_Paginas`. Luego, itera sobre el rango de páginas y realiza solicitudes para cada una. Obtiene el json y procesa los resultados llamando al método `recorrerInsertarTrabajos_porPagina`.Y por último, si queda documentos que no se hayan insertado, en el método `recorrerInsertarTrabajos_porPagina`, los inserta a MongoDB.


### Método `_comprobar_insertar_trabajosPorPagina`

Procesa los documentos pasados por el método de `buscar_docs` como un diccionario y los inserta en MongoDB. Primero llama al método de `cambiar_resumen`. Luego itera sobre el diccionario y comprueba si el documento esta repetido, llamando  al método `isRepetido` de la clase OpenAlex_Mongo. Si no esta repetido, encapsula los datos para la nueva estructura y lo añade a la lista de trabajos a insertar. Inserta los trabajos en MongoDB cuando la lista alcanza el tamaño del ciclo de inserciones definido.

### Método `_numero_total_paginas`

Calcula el número total de páginas de resultados para una búsqueda dada.


### Método `_cambiar_resumen`

Cambia el abstract_inverted_index de los documentos por un texto reconstruido. Itera sobre la lista de documentos y llama al método `reconstruir_abstract`.


### Método `_reconstruir_abstract`

Reconstruye el índice invertido de un resumen en un texto completo. Primero, crea una lista vacía para almacenar las palabras reconstruidas. Luego, itera sobre las claves y valores del índice invertido del resumen e inserta cada palabra en su posición correcta en la lista. Extiende la lista con espacios en blanco si la posición de la palabra está fuera del rango actual. Une las palabras reconstruidas en una cadena de texto. Y devuelve el texto reconstruido.

---
## OpenAlex_mongo

Contiene la clase **MongoDB** que se encarga de las funciones que tienen que ver con la base de datos, en el constructor se le pasan la url de conexión y el nombre de la base de datos.


### Método `insertar`
Inserta los documentos de una lista en la colección *documentos*.


### Método `guardar_fecha_descarga`

Inserta un objeto en la colección *fecha_descarga* con la fecha actual, el id de un cliente y el número de sus documentos encontrados, insertados y actualizados.


### Método `isRepetido`

Comprueba si un documento existe en la colección mediante su id de OpenAlex, si existe y ha sido modificado en OpenAlex se actualiza.


### Método `obtener_configuracion_cliente`

Obtiene las afiliaciones y los autores de un cliente.


### Método `obtener_ids_clientes`

Obtiene una lista con ids de clientes que se pueden descargar, basándose en los campos *enabled* y *periodicidad* de los clientes. Los primeros de la lista serán los clientes nuevos y después en orden de los que mas llevan sin descargarse a los que menos.

---
## OpenAlex_acceso

### Método `url_TrabajosInstitucion`

Método estático que retorna los datos de los documentos asociados a una institución específica.

### Método `url_TrabajosAutor`

Método estático que retorna los datos de los documentos asociados a un autor específico.


### Método `url_BuscarAutor_Scopus`

Método estático que retorna los datos de un autor basado en su identificador Scopus.

