import os
import re
from collections import defaultdict

def parse_logs(log_dir):
    usuarios = set()
    total_bytes = 0
    sitios_contador = defaultdict(int)
    
    # Expresión regular ajustada para el formato exacto de los logs
    log_pattern = re.compile(
        r'^"[^"]+ - (\S+) \[[^\]]+\] \\\"(?:CONNECT|GET|POST|PUT|DELETE|HEAD|OPTIONS) ([^ ]+) HTTP/[^\"]+\\" \d+ (\d+|-)'
    )
    
    print(f"Buscando archivos en: {log_dir}")
    for filename in os.listdir(log_dir):
        print(f"Procesando archivo: {filename}")
        if not filename.endswith('.log'):
            print(f"Saltando archivo {filename} - no es un archivo .log")
            continue
            
        filepath = os.path.join(log_dir, filename)
        if os.path.isfile(filepath):
            try:
                print(f"Abriendo archivo: {filepath}")
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    line_count = 0
                    match_count = 0
                    for line in f:
                        line = line.strip()
                        line_count += 1
                        if not line:
                            continue
                            
                        match = log_pattern.match(line)
                        if match:
                            match_count += 1
                            user = match.group(1)
                            raw_url = match.group(2)
                            bytes_str = match.group(3)
                            
                            # Registrar usuario
                            if user != '-' and user != 'unknown':
                                usuarios.add(user)
                            
                            # Sumar tráfico (manejar '-' como 0)
                            if bytes_str != '-':
                                total_bytes += int(bytes_str)
                            
                            # Normalizar URL
                            sitio = normalize_url(raw_url)
                            if sitio:
                                sitios_contador[sitio] += 1
                    print(f"Archivo {filename}: {line_count} líneas procesadas, {match_count} coincidencias encontradas")
            except Exception as e:
                print(f"Error procesando {filename}: {str(e)}")
    
    print(f"\nResumen de procesamiento:")
    print(f"Usuarios encontrados: {len(usuarios)}")
    print(f"Total de bytes: {total_bytes}")
    print(f"Sitios únicos: {len(sitios_contador)}")
    
    return usuarios, total_bytes, sitios_contador

def normalize_url(raw_url):
    """Normaliza la URL para obtener el dominio principal"""
    # Remover protocolo si existe
    if '://' in raw_url:
        url = raw_url.split('://', 1)[1]
    else:
        url = raw_url
    
    # Remover puerto, parámetros y rutas
    dominio = url.split('/')[0].split(':')[0].split('?')[0]
    
    # Remover www si existe
    if dominio.startswith('www.'):
        dominio = dominio[4:]
    
    return dominio.lower()

def main():
    log_dir = "logs"  # Corregido a minúsculas
    output_dir = "output"
    
    # Crear directorio de salida si no existe
    os.makedirs(output_dir, exist_ok=True)
    
    if not os.path.exists(log_dir):
        print(f"Error: No existe la carpeta '{log_dir}'")
        return
    
    usuarios, total_bytes, sitios_contador = parse_logs(log_dir)
    
    # Calcular totales
    total_usuarios = len(usuarios)
    total_mb = total_bytes / (1024 * 1024)
    
    # Guardar resultados en archivos
    with open(os.path.join(output_dir, 'usuarios.txt'), 'w') as f:
        f.write(f"Total de usuarios únicos: {total_usuarios}\n")
        f.write("\n".join(sorted(usuarios)))
    
    with open(os.path.join(output_dir, 'trafico_total.txt'), 'w') as f:
        f.write(f"Total de tráfico: {total_mb:.2f} MB\n")
    
    # Top 500 sitios
    top_500 = sorted(sitios_contador.items(), key=lambda x: x[1], reverse=True)[:500]
    with open(os.path.join(output_dir, 'top_500_global.csv'), 'w') as f:
        f.write("Sitio,Visitas\n")
        for sitio, count in top_500:
            f.write(f"{sitio},{count}\n")
    
    # Mostrar resumen en consola
    print(f"\n{'='*50}")
    print(f"Total de usuarios únicos: {total_usuarios}")
    print(f"Total de tráfico: {total_mb:.2f} MB")
    print(f"\nTop 10 sitios más visitados:")
    print(f"{'='*50}")
    for i, (sitio, count) in enumerate(top_500[:10], 1):
        print(f"{i:>4}. {sitio}: {count} visitas")
    print(f"\nResultados completos guardados en la carpeta '{output_dir}'")

if __name__ == "__main__":
    main()