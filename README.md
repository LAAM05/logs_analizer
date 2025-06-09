# logs_analizer
script en Python que te permita procesar múltiples archivos de logs Squid con el formato " *<timestamp> <client_ip> - - [<fecha:hora>] "<método URL protocolo>" <código_HTTP> <tamaño_respuesta> <acción> <destino> <cliente> <tipo_contenido> - <método TCP_DENIED/HIER_NONE ...>* ", y generar reportes

✅ Objetivos del script
Identificar la cantidad de usuarios únicos
Calcular el tráfico total (en bytes)
Generar el Top 500 sitios más visitados por mes

🧠 Requisitos técnicos
Python 3.x
Librerías necesarias:
os / re / datetime → estándar
pandas → para manejo de datos
collections.Counter → conteo eficiente
