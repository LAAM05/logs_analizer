import os
import re
from datetime import datetime, timezone
from collections import defaultdict, Counter
import pandas as pd

LOG_DIR = 'logs'
OUTPUT_DIR = 'output'

# Patrones regex para los formatos de log observados
LOG_PATTERNS = [
    # Formato 1: [timestamp] [duración] [ip] [cache]/[status] [size] [método] [url]
    re.compile(
        r'^(?P<timestamp>\d+\.\d+)\s+'
        r'(?P<duration>\d+)\s+'
        r'(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+'
        r'(?P<cache_status>\S+)/(?P<status>\d+)\s+'
        r'(?P<size>\d+)\s+'
        r'(?P<method>\S+)\s+'
        r'(?P<url>\S+)\s+'
    ),
    # Formato 2: [timestamp] [ip] - [usuario] [fecha] "[método] [url] HTTP/..." [status] [size]
    re.compile(
        r'^(?P<timestamp>\d+\.\d+)\s+'
        r'(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+'
        r'-\s+'
        r'(?P<user>\S+)\s+'
        r'\[(?P<date>.+?)\]\s+'
        r'"(?P<method>\S+)\s+'
        r'(?P<url>\S+)\s+'
        r'HTTP/\d\.\d"\s+'
        r'(?P<status>\d+)\s+'
        r'(?P<size>\d+)\s+'
    ),
    # Formato 3: Líneas de error
    re.compile(
        r'^(?P<timestamp>\d+\.\d+)\s+'
        r'(?P<duration>\d+)\s+'
        r'(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+'
        r'(?P<cache_status>\S+)/(?P<status>\d+)\s+'
        r'(?P<size>\d+)\s+'
        r'-'
    )
]

def parse_log_file(file_path):
    domains_counter = Counter()
    users = set()
    traffic_per_ip = defaultdict(int)

    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            data = None
            for pattern in LOG_PATTERNS:
                match = pattern.match(line)
                if match:
                    data = match.groupdict()
                    break
            
            if not data:
                continue

            # Obtener IP y tamaño
            ip = data['ip']
            try:
                size = int(data.get('size', '0'))
            except ValueError:
                size = 0
            traffic_per_ip[ip] += size
            users.add(ip)

            # Determinar fecha
            dt = None
            if 'date' in data:
                try:
                    dt = datetime.strptime(data['date'], "%d/%b/%Y:%H:%M:%S %z")
                except ValueError:
                    pass
            if not dt and 'timestamp' in data:
                try:
                    ts = float(data['timestamp'])
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                except ValueError:
                    continue
            if not dt:
                continue

            # Extraer dominio de la URL
            url = data.get('url', '')
            if not url:
                continue

            # Manejar URLs de tipo CONNECT: "dominio:puerto"
            domain = url.split(':')[0] if data.get('method') == 'CONNECT' else url
            
            # Extraer dominio limpio (sin protocolo ni puerto)
            domain_match = re.search(r'(?:https?://)?([^/:]+)', domain)
            if domain_match:
                domain = domain_match.group(1).lower()
            else:
                domain = domain.lower()

            # Agrupar por año y mes
            year_month = f"{dt.year}_{dt.strftime('%b').lower()}"
            domains_counter[(year_month, domain)] += 1

    return {
        'domains': domains_counter,
        'users': users,
        'traffic': traffic_per_ip
    }

def process_all_logs():
    all_domains = Counter()
    all_users = set()
    monthly_domains = defaultdict(Counter)
    total_traffic = 0

    for filename in os.listdir(LOG_DIR):
        if filename.startswith('.'):
            continue
        file_path = os.path.join(LOG_DIR, filename)
        print(f"[+] Procesando: {filename}")
        try:
            parsed = parse_log_file(file_path)
            all_users.update(parsed['users'])
            
            for (month_domain, count) in parsed['domains'].items():
                month, domain = month_domain
                all_domains[domain] += count
                monthly_domains[month][domain] += count
            
            total_traffic += sum(parsed['traffic'].values())
        except Exception as e:
            print(f"  [!] Error en {filename}: {str(e)}")

    # Guardar resultados
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 1. Usuarios únicos
    with open(os.path.join(OUTPUT_DIR, 'usuarios.txt'), 'w') as f:
        f.write(f"Usuarios únicos: {len(all_users)}\n")
        f.write("Lista:\n" + "\n".join(f"- {ip}" for ip in sorted(all_users)))
    
    # 2. Tráfico total
    with open(os.path.join(OUTPUT_DIR, 'trafico_total.txt'), 'w') as f:
        total_mb = total_traffic / (1024 ** 2)
        f.write(f"Tráfico total: {total_traffic} bytes\n")
        f.write(f"Tráfico total aproximado: {total_mb:.2f} MB\n")
    
    # 3. Top 500 por mes
    top_mes_dir = os.path.join(OUTPUT_DIR, 'top_500_por_mes')
    os.makedirs(top_mes_dir, exist_ok=True)
    for month, counter in monthly_domains.items():
        top_domains = counter.most_common(500)
        df = pd.DataFrame(top_domains, columns=['Dominio', 'Accesos'])
        df.to_csv(os.path.join(top_mes_dir, f'{month}.csv'), index=False)
    
    # Top 500 global
    df_global = pd.DataFrame(all_domains.most_common(500), columns=['Dominio', 'Accesos'])
    df_global.to_csv(os.path.join(OUTPUT_DIR, 'top_500_global.csv'), index=False)

    print("\n✅ Análisis completado:")
    print(f"- Usuarios únicos: {len(all_users)}")
    print(f"- Tráfico total: {total_traffic / (1024 ** 2):.2f} MB")
    print(f"- Resultados en: {OUTPUT_DIR}")

if __name__ == '__main__':
    process_all_logs()