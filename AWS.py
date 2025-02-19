import pandas as pd
import boto3
from io import StringIO
import pandas as pd
import json

# Conexion con EBX
class AWS():

    # Defino el metodo constructor y las variables por defecto
    def __init__(self) -> None:
        self.password = self.get_secret()
        self.s3 = boto3.client('s3')
        

    def guardar_dataframe_en_s3(self, dataframe, nombre_archivo, bucket_name):
        # Convertir el DataFrame a formato CSV en memoria
        csv_buffer = StringIO()
        dataframe.to_csv(csv_buffer, index=False)
    
        # Especificar el directorio dentro del bucket
        full_key = f'api/catalogo-autos/carga-continua/entidad-insert/{nombre_archivo}'
    
        try:
            self.s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=full_key)
            print(f'Se insertó el archivo {nombre_archivo} en el bucket {bucket_name} en el directorio ')
            return True
        except Exception as e:
            print(f'Error al insertar el archivo {nombre_archivo} en el bucket {bucket_name} en el directorio : {e}')
            return False
    
    def traer_dataframe_desde_s3(self, nombre_archivo, bucket_name, separador=","):
        # Especificar el directorio dentro del bucket
        full_key =  f'api/catalogo-autos/{nombre_archivo}'
        # Conectar con S3 y obtener el archivo CSV
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=full_key)
            csv_data = response['Body'].read().decode('utf-8')
            dataframe = pd.read_csv(StringIO(csv_data), sep=separador)
            print(f'Se ha cargado el archivo {nombre_archivo} desde el bucket {bucket_name} en el directorio ')
            return dataframe
        except Exception as e:
            print(f'Error al cargar el archivo {nombre_archivo} desde el bucket {bucket_name} en el directorio : {e}')
            return None

    def traer_dataframe_control_desde_s3(self, nombre_archivo, bucket_name):
        # Especificar el directorio dentro del bucket
        #full_key =  f'api/catalogo-autos/carga-continua/archivos-excel/{nombre_archivo}'
        full_key =  f'api/catalogo-autos/{nombre_archivo}'
        # Conectar con S3 y obtener el archivo CSV
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=full_key)
            csv_data = response['Body'].read().decode('utf-8')
            dataframe = pd.read_csv(StringIO(csv_data))
            print(f'Se ha cargado el archivo {nombre_archivo} desde el bucket {bucket_name} en el directorio')
            return dataframe
        except Exception as e:
            print(f'Error al cargar el archivo {nombre_archivo} desde el bucket {bucket_name} en el directorio: {e}')
            return None
    
    def guardar_dataframe_control_en_s3(self, dataframe, nombre_archivo, bucket_name):
        # Convertir el DataFrame a formato CSV en memoria
        csv_buffer = StringIO()
        dataframe.to_csv(csv_buffer, index=False)
    
        # Especificar el directorio dentro del bucket
        full_key = f'api/catalogo-autos/carga-continua/archivos-excel/{nombre_archivo}'
    
        # Conectar con S3 y subir el archivo
        try:
            self.s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=full_key)
            print(f'Se insertó el archivo {nombre_archivo} en el bucket {bucket_name} en el directorio ')
            return True
        except Exception as e:
            print(f'Error al insertar el archivo {nombre_archivo} en el bucket {bucket_name} en el directorio: {e}')
            return False
            
    def get_secret(self):
        secret_name = "secreto-autocatalogo"
        region_name = "us-east-1"
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name=region_name)
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response['SecretString'])
        secret = secret["secret-autocatalogo"]
        return secret
        
    
    def guardar_dataframe_vista_en_s3(self, dataframe, nombre_archivo, bucket_name):
        # Convertir el DataFrame a formato CSV en memoria
        csv_buffer = StringIO()
        dataframe.to_csv(csv_buffer, index=False)
    
        # Especificar el directorio dentro del bucket
        full_key = f'api/catalogo-autos/vista-safe/{nombre_archivo}'
    
        # Conectar con S3 y subir el archivo
        s3 = boto3.client('s3')
        try:
            s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=full_key)
            print(f'Se insertó el archivo {nombre_archivo} en el bucket {bucket_name} en el directorio ')
            return True
        except Exception as e:
            print(f'Error al insertar el archivo {nombre_archivo} en el bucket {bucket_name} en el directorio: {e}')
            return False
    
    