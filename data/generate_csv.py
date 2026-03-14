import argparse
import csv
import random
import datetime
import os

def generate_csv(rows, output_path):
    start_date = datetime.date(2026, 1, 1)
    end_date = datetime.date(2026, 3, 31)
    delta_days = (end_date - start_date).days

    print(f"Generando {rows} filas en {output_path}...")
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['date', 'product_id', 'quantity', 'price'])
        
        for i in range(rows):
            # Fecha aleatoria
            random_days = random.randint(0, delta_days)
            date = start_date + datetime.timedelta(days=random_days)
            
            product_id = random.randint(1001, 1050)
            quantity = random.randint(1, 100)
            price = round(random.uniform(1.00, 999.99), 2)
            
            writer.writerow([date, product_id, quantity, price])
            
            if (i + 1) % 100_000 == 0:
                print(f"Generadas {i + 1} filas...")

    print(f"¡Completado! Archivo generado: {output_path}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generador de CSV de ventas masivas para pruebas de estrés.')
    parser.add_argument('--rows', type=int, default=1000, help='Número de filas a generar')
    parser.add_argument('--output', type=str, default='data/big_test.csv', help='Ruta del archivo de salida')
    
    args = parser.parse_args()
    
    # Asegurar que el directorio existe
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    
    generate_csv(args.rows, args.output)
