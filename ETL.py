import re
from pathlib import Path
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from tqdm import tqdm
from PyPDF2 import PdfReader


class LakeHouse:
    def __init__(self):
        self._ruta = Path.cwd().joinpath('LakeHouse')

        self._ruta_bronze = self._ruta.joinpath('Bronze')
        self._ruta_silver = self._ruta.joinpath('Silver')
        self._ruta_gold = self._ruta.joinpath('Gold')

        self._set_lakehouse()

    def _set_lakehouse(self):
        if not self._ruta.exists():
            self._ruta.mkdir()

        if not self._ruta_bronze.exists():
            self._ruta_bronze.mkdir()

        if not self._ruta_silver.exists():
            self._ruta_silver.mkdir()

        if not self._ruta_gold.exists():
            self._ruta_gold.mkdir()

    def a_lakehouse(self):
        return self._ruta_bronze

    def bronce(self):
        return pd.read_parquet(self._ruta_bronze.joinpath('data_resoluciones_sunat.parquet'))

    def plata(self, data_bruta_resoluciones):
        """
            PROCESAR LAS RESOLUCIONES
        """
        tabla_delta_plata_res = data_bruta_resoluciones.copy()

        # Solo obtener el numero de la resolucion
        tabla_delta_plata_res['Resolucion'] = tabla_delta_plata_res['Resolucion'].str.extract(r'(\d+-\d+)')

        # Convertir el tipo de dato de la fecha
        tabla_delta_plata_res['Fecha_Peruano'] = pd.to_datetime(tabla_delta_plata_res['Fecha_Peruano'],
                                                                format='%d/%m/%Y',
                                                                errors='coerce')

        # Crear el link del documento
        tabla_delta_plata_res['URL_Documento'] = tabla_delta_plata_res.apply(LakeHouse._crear_link_documento, axis=1)

        # Eliminar data que no es necesaria para optimizar recursos
        tabla_delta_plata_res.drop(['Enlace_Documento'], axis=1, inplace=True)

        tabla_delta_plata_res.to_parquet(self._ruta_silver.joinpath('tabla_delta_plata_resoluciones_sunat.parquet'),
                                         engine='pyarrow',
                                         compression='snappy')

        """-----------------------------------------------------------------------------------------------------"""
        """
            PROCESAR EL CONTENIDO DE LOS DOCUMENTOS
        """
        tabla_parrafos = self._ruta_silver.joinpath('tabla_parrafos.txt')
        LakeHouse._eliminar_archivo_temp(tabla_parrafos)
        with open(tabla_parrafos, 'w', encoding='utf-8') as f:
            f.write('Parrafo_documento|Indice|Nombre_documento\n')

        for ruta_doc in tqdm(self._ruta_bronze.iterdir()):
            extension = ruta_doc.suffix
            if extension != '.pdf':
                continue

            reader = PdfReader(str(ruta_doc))
            contenido = ''  # Extraer el contenido de todas las paginas
            for pagina in reader.pages:
                extract = pagina.extract_text()
                if extract:
                    contenido = contenido + " " + extract

            p = LakeHouse._identificar_parrafos(contenido)
            paragraph_split = p.split("|")
            paragraph_reshape = np.array(paragraph_split).reshape(-1, 1)

            parrafos_documento = pd.DataFrame(data=paragraph_reshape, columns=["Parrafo_documento"])
            parrafos_documento['Parrafo_documento'] = parrafos_documento['Parrafo_documento'].replace('', np.nan)
            parrafos_documento.dropna(subset=['Parrafo_documento'], inplace=True)  # Elimina nulos
            parrafos_documento.reset_index(drop=True, inplace=True)

            parrafos_documento['Indice'] = parrafos_documento.index + 1
            parrafos_documento['Nombre_documento'] = ruta_doc.stem

            parrafos_documento.to_csv(tabla_parrafos, sep="|", index=False, header=False, mode="a", encoding='utf-8')

        t = pd.read_csv(tabla_parrafos, sep='|', encoding='utf-8')
        t.to_parquet(self._ruta_silver.joinpath('tabla_delta_plata_contenido_doc_res_sunat.parquet'),
                     engine='pyarrow',
                     compression='snappy')
        LakeHouse._eliminar_archivo_temp(tabla_parrafos)
        print("\t[Silver] Procesamiento de las resoluciones: OK\n")
        print("\t[Silver] Procesamiento del contenido de los documentos: OK\n")


    def oro(self):
        tabla_delta_resoluciones_sunat = pd.read_parquet(
            self._ruta_silver.joinpath('tabla_delta_plata_resoluciones_sunat.parquet'))
        tabla_delta_contenido_doc_res_sunat = pd.read_parquet(
            self._ruta_silver.joinpath('tabla_delta_plata_contenido_doc_res_sunat.parquet'))

        tabla_delta_consolidado = pd.merge(tabla_delta_resoluciones_sunat,
                                           tabla_delta_contenido_doc_res_sunat,
                                           how='inner',
                                           left_on='Resolucion',
                                           right_on='Nombre_documento')
        tabla_delta_consolidado.to_parquet(self._ruta_gold.joinpath('tabla_delta_oro_dw.parquet'),
                                           engine='pyarrow',
                                           compression='snappy')
        print("[Gold] Consolidar las resoluciones y contenido de los documentos")

    @staticmethod
    def _eliminar_archivo_temp(tabla_parrafos):
        if tabla_parrafos.exists():
            tabla_parrafos.unlink()

    @staticmethod
    def _crear_link_documento(row):
        link_principal = 'https://www.sunat.gob.pe/legislacion/superin/'
        anio = row['Resolucion'][-4:]
        nombre_documento = re.search(r'\d+-\d+\.pdf', row['Enlace_Documento']).group()
        return f'{link_principal}{anio}/{nombre_documento}'

    @staticmethod
    def _identificar_parrafos(text):
        lines = text.splitlines()  # Convierte el texto plano en array formado por las lineas

        parag = ""
        for line in lines:
            line = line.replace("|", "")
            line = line.replace("é", "ó")
            if (
                    len(line) == 0
                    or line == " "
                    or line == "  "
                    or line == "   "
                    or line == "    "
                    or line == "     "
                    or line == "      "
                    or line == "       "
            ):  # '' y ' '
                parag = parag + "|"
            else:
                parag = parag + line + " "

        return parag


class SUNAT:
    def __init__(self, anio='2024'):
        self._anio = anio
        self._url_principal = 'https://www.sunat.gob.pe/legislacion/superin/'
        self._url = f'{self._url_principal}{anio}/indices/indcor.htm'

    def ingesta_datos(self, ruta_lake_house):
        try:
            # Ingresar a la pagina de resoluciones SUNAT
            response = requests.get(self._url)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')

            tabla = soup.find('table')

            datos = []

            print("2. Ingesta de datos: Resoluciones y documentacion de SUNAT a un Lake House")
            # Iterar y extraer las resoluciones de la tabla
            for fila in tqdm(tabla.find_all('tr')[2:]):
                columnas = fila.find_all('td')
                if len(columnas) >= 3:
                    numero_resolucion = columnas[0].text.strip()
                    sumilla = columnas[1].text.strip()
                    fecha_peruano = columnas[2].text.strip()

                    link = None
                    if columnas[0].find('a'):
                        link = columnas[0].find('a')['href']

                        self._descargar_documentos(link, ruta_lake_house)

                    datos.append({
                        'Resolucion': numero_resolucion,
                        'Sumilla': sumilla,
                        'Fecha_Peruano': fecha_peruano,
                        'Enlace_Documento': link
                    })

            # Almacenar la data en un archivo parquet en el Lakehouse
            df = pd.DataFrame(datos)
            df.to_parquet(ruta_lake_house.joinpath('data_resoluciones_sunat.parquet'),
                          engine='pyarrow',
                          compression='snappy')
        except:
            pass

    def _descargar_documentos(self, nombre_documento, ruta_lake_house):
        try:
            documento = re.search(r'\d+-\d+\.pdf', nombre_documento).group()
            url_sunat_documento = f'{self._url_principal}{self._anio}/{documento}'

            response = requests.get(url_sunat_documento, stream=True)
            response.raise_for_status()

            if 'application/pdf' in response.headers.get('Content-Type', ''):
                ruta_documento = ruta_lake_house.joinpath(documento)
                with open(ruta_documento, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
        except Exception as ex:
            print(ex)


if __name__ == '__main__':

    lk = LakeHouse()

    print("1. Fuente de datos: SUNAT\n")
    sunat = SUNAT()
    sunat.ingesta_datos(lk.a_lakehouse())

    print("3. Lake House")
    print("\t[Bronze] Identificar las resoluciones y documentos en el Lake House")
    data_bruta_resoluciones = lk.bronce()
    lk.plata(data_bruta_resoluciones)
    lk.oro()
