import os, re
from datetime import datetime
from collections import defaultdict,Counter
import pandas as pd

LOG_DIR = 'logs'
OUTPUT_DIR = 'output'

# Expresión regular para parsear cada línea del log
LOG_REGEX = re.compile(
    r'^(?P<timestamp>\d+\.\d+)\s+'
    r'(?P<ip>\d+\.\d+\.\d+\.\d+)\s+'
    r'-\s+-\s+$$'
    r'(?P<date>[^$$]+)'
    r'$$\s+"'
    r'(?P<method>\w+)\s+'
    r'(?P<url>(?:https?://)?[^ ]+)'
    r'\s+HTTP/\d\.\d"\s+'
    r'(?P<status>\d+)\s+'
    r'(?P<size>\d+)'
)

def parse_log_file(file_path):
    domains_counter = Counter()
    traffic_data = defaultdict(int)
    users = set()

    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            match = LOG_REGEX.match(line.strip())
            if not match:
                continue

            data = match.groupdict()

            # Parsear fecha y hora
            date_str = data['date']
            try:
                dt = datetime.strptime(date_str, "%d/%b/%Y:%H:%M:%S %z")
            except ValueError:
                continue

            ip = data['ip']
            size = int(data['size'])
            url = data['url']

            # Extraer dominio
            domain_match = re.search(r'(?:https?://)?([^/]+)', url)
            domain = domain_match.group(1) if domain_match else url

            # Agrupar por año y mes
            year_month = f"{dt.year}_{dt.strftime('%b').lower()}"

            # Contadores
            domains_counter[(year_month, domain)] += 1
            traffic_data[ip] += size
            users.add(ip)

    return {
        'domains': domains_counter,
        'traffic': traffic_data,
        'users': users
    }

def process_all_logs():
    all_domains = Counter()
    total_traffic = 0
    all_users = set()
    monthly_domains = defaultdict(Counter)

    for filename in os.listdir(LOG_DIR):
        file_path = os.path.join(LOG_DIR, filename)
        print(f"[+] Procesando archivo: {filename}")
        parsed = parse_log_file(file_path)

        # Usuarios
        all_users.update(parsed['users'])

        # Tráfico total
        total_traffic += sum(parsed['traffic'].values())

        # Dominios por mes
        for (month_domain, count) in parsed['domains'].items():
            month, domain = month_domain
            all_domains[domain] += count
            monthly_domains[month][domain] += count

    # Guardar resultados
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Usuarios
    with open(os.path.join(OUTPUT_DIR, 'usuarios.txt'), 'w') as f:
        f.write(f"Usuarios únicos: {len(all_users)}\n")
        f.write("Lista:\n")
        for user in sorted(all_users):
            f.write(f"- {user}\n")

    # 2. Tráfico total
    with open(os.path.join(OUTPUT_DIR, 'trafico_total.txt'), 'w') as f:
        f.write(f"Tráfico total: {total_traffic} bytes\n")
        f.write(f"Tráfico total aproximado: {round(total_traffic / (1024 ** 2), 2)} MB\n")

    # 3. Top 500 por mes
    os.makedirs(os.path.join(OUTPUT_DIR, 'top_500_por_mes'), exist_ok=True)
    for month, counter in monthly_domains.items():
        top_list = counter.most_common(500)
        df = pd.DataFrame(top_list, columns=['Dominio', 'Accesos'])
        df.to_csv(os.path.join(OUTPUT_DIR, 'top_500_por_mes', f'{month}.csv'), index=False)

    # Top global (opcional)
    df_global = pd.DataFrame(all_domains.most_common(500), columns=['Dominio', 'Accesos'])
    df_global.to_csv(os.path.join(OUTPUT_DIR, 'top_500_global.csv'), index=False)

    print("\n✅ Análisis completado:")
    print(f"- Usuarios únicos: {len(all_users)}")
    print(f"- Tráfico total: {round(total_traffic / (1024 ** 2), 2)} MB")
    print(f"- Archivos guardados en '{OUTPUT_DIR}'")

if __name__ == '__main__':
    process_all_logs()