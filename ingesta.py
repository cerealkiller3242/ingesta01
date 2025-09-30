import boto3
import csv
import mysql.connector

# Configuración de MySQL
db_config = {
    "host": "mysql_host",      # cambia por host real
    "user": "usuario",         # cambia por usuario real
    "password": "password",    # cambia por password real
    "database": "basedatos"    # cambia por nombre de BD
}

# Configuración de S3
ficheroUpload = "data.csv"
nombreBucket = "gcr-output-01"

def extraer_datos():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tu_tabla")  # cambia 'tu_tabla'
    rows = cursor.fetchall()

    columnas = [i[0] for i in cursor.description]

    with open(ficheroUpload, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columnas)   # cabeceras
        writer.writerows(rows)

    cursor.close()
    conn.close()
    print("Datos exportados a", ficheroUpload)

def subir_a_s3():
    s3 = boto3.client("s3")
    s3.upload_file(ficheroUpload, nombreBucket, ficheroUpload)
    print("Archivo subido a S3:", ficheroUpload)

if __name__ == "__main__":
    extraer_datos()
    subir_a_s3()
    print("Ingesta completada")
