# Antecedentes

KPMG Perú ofrece servicios de auditoría, asesoría tributaria y legal, y consultoría empresarial. Esto incluye auditorías financieras, planeamiento fiscal, cumplimiento legal, y estrategias para la gestión de riesgos, optimización operativa y transacciones empresariales, atendiendo a clientes locales e internacionales con más de 50 años de experiencia en el mercado​

Dentro de KPMG Perú, la línea de negocio **Tax Advisory** se especializa en revisar las declaraciones de impuestos emitidas por las empresas, asegurando su corrección y cumplimiento de obligaciones fiscales. Su objetivo principal es preparar a las organizaciones ante posibles fiscalizaciones de la SUNAT y mitigar riesgos, como sanciones o multas, brindando un soporte integral en temas tributarios.

# Problematica

El equipo de **Tax Advisory** enfrenta un desafío al gestionar un gran volumen de información normativa, la cual se encuentra dispersa en diversas fuentes, como sitios web oficiales. Este acceso disperso de informacion no solo dificulta la consulta rapida y precisa de las normativas, sino que también aumenta el riesgo de omitir detalles críticos para garantizar el cumplimiento fiscal de las empresas. 

Este proceso manual de búsqueda puede ser lento e ineficiente, afectando la capacidad del equipo para reaccionar rápidamente ante cambios normativos o fiscalizaciones inesperadas. Centralizar y sistematizar esta información es esencial para reducir tiempos, garantizar precisión y minimizar riesgos asociados con multas o sanciones fiscales.

# Solucion 

La solución permite centralizar y automatizar la extracción de normativas desde fuentes oficiales, procesando grandes volúmenes de información y utilizando inteligencia artificial para estructurar y analizar los datos. Esto responde a la necesidad de los profesionales de contar con una plataforma unificada que facilite la búsqueda rápida y precisa de normas, mejorando la eficiencia, reduciendo tiempos de respuesta y asegurando un respaldo sólido frente a fiscalizaciones.

# Tecnologias

La solución propuesta, basada en la arquitectura mostrada, aborda la problemática de centralizar y facilitar el acceso a la información normativa dispersa utilizando las siguientes tecnologías:

1. **Ingesta de datos con Azure Data Factory**: Se recolectan datos desde fuentes externas, como resoluciones del Tribunal Fiscal y la SUNAT, asegurando la extracción eficiente de información normativa clave desde diferentes sitios web.

2. **Almacenamiento estructurado en Data Lakehouse**: Los datos recolectados se organizan en un modelo *Bronze-Silver-Gold* dentro de **Azure Data Lake Storage**:
   - **Bronze**: Contiene los datos crudos extraídos.
   - **Silver**: Datos procesados y limpiados para asegurar calidad.
   - **Gold**: Datos listos para ser consumidos en análisis o aplicaciones.

3. **Procesamiento y orquestación con Databricks**: Se procesan los datos en diferentes capas del Data Lakehouse, aplicando transformaciones como estandarizacion de tipos de datos, eliminación de duplicados, datos irrelevantes y adecuación de datos. Databricks también gestiona las orquestaciones para asegurar un flujo continuo.

4. **Entrenamiento de modelos con Azure OpenAI Service**: Se aprovecha esta tecnología para entrenar modelos de inteligencia artificial a raiz del gran volumen normativo tanto como datos y documentos.

5. **Consumo mediante API y App**: Los datos procesados y centralizados se exponen a través de una API, lo que facilita que el equipo de Tax Advisory acceda a la información consolidada desde una aplicación personalizada para realizar consultas rápidas y sustentaciones ante fiscalizaciones

6. **Gobernanza y seguridad**: Tecnologías como **Azure Entra ID**, **Azure Key Vault** y **Azure Monitor** garantizan el control de accesos, la protección de datos sensibles y el monitoreo continuo de la solución.

# Codigo fuente

Se ha ejemplificado en local el proceso ETL en la nube siguiendo la arquitectura medallón 
* El flujo inicia con la extracción de datos desde la página de resoluciones de la SUNAT, almacenándolos en la carpeta **Bronze**, donde se guardan tanto los datos normativos como los documentos en su formato bruto. 
* Luego, en la carpeta **Silver**, se realiza el procesamiento, eliminando columnas innecesarias y limpiando los datos; además, en el caso de los documentos, su contenido se estructura convirtiendo cada párrafo en una fila dentro de una tabla. 
* Finalmente, en la carpeta **Gold**, se integran las entidades procesadas, combinando la información normativa y el contenido de los documentos en una única tabla consolidada, lista para su análisis y uso.

Consideraciones
* Se usa el archivo parquet para almacenar datos
* Python 3.8: 
  * BeatifulSoup4 y requests (ingesta)
  * Pandas (procesamiento e integracion) 