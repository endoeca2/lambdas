import pandas as pd
import requests
import os
import boto3
from io import StringIO
import json
import pandas as pd
import requests
from datetime import datetime, timedelta, timezone
import time

# Conexion con EBX
class EBX():

    # Defino el metodo constructor y las variables por defecto
    def __init__(self,ruta_modelo_datos) -> None:

        self.select_url = os.getenv("EBX_base_url") + 'data/v1/' +  os.getenv("ruta_modelo_datos")
        self.insert_update_url = os.getenv("EBX_base_url") + 'form-data/v1/' + os.getenv("ruta_modelo_datos")
        self.token_url = os.getenv("EBX_base_url") + 'auth/v1/token:create'
        self.password = self.get_secret()
        self.secrets_client = boto3.client('secretsmanager')
        self.token = None
        self.bucket_name = os.getenv("s3_bucket_name")
    
    #Método para obtener la contraseña del secret manager AWS
    def get_secret(self):
        secret_name = "secreto-autocatalogo"
        region_name = "us-east-1"
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name=region_name)
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response['SecretString'])
        secret = secret["secret-autocatalogo"]
        return secret
        
    # Metodo para obtener el token
    #   Regresa una bandera con True si obtubo el token, y false si fallo. 
    def get_token(self):
        control_key = "api/catalogo-autos/ebx-token-control.txt"
        
        try:
            # Verificar si el token ya está en Secrets Manager y es reciente
            token_data = self.retrieve_token_from_secrets()
            if token_data and 'last_updated' in token_data:
                last_updated = datetime.fromisoformat(token_data['last_updated']).replace(tzinfo=timezone.utc)
                now = datetime.now(timezone.utc)
                if now - last_updated < timedelta(minutes=29):
                    print("Token vigente encontrado, regresando accessToken.")
                    return True, token_data['accessToken']

            # Esperar si otra Lambda está solicitando un nuevo token
            s3 = boto3.client('s3')
            backoff = 1
            max_backoff = 16
            total_wait_time = 0

            while True:
                try:
                    s3.head_object(Bucket=self.bucket_name, Key=control_key)
                    print("Esperando a que otra Lambda termine de solicitar un nuevo token...")
                    time.sleep(backoff)
                    total_wait_time += backoff
                    backoff = min(backoff * 2, max_backoff)

                    if total_wait_time >= 60:
                        raise TimeoutError("Timeout mientras se esperaba un nuevo token.")
                except s3.exceptions.ClientError:
                    break

            # Colocar el archivo de control para indicar que se está solicitando un nuevo token
            s3.put_object(Bucket=self.bucket_name, Key=control_key, Body="En progreso")

            # Solicitar un nuevo token
            new_token = self.request_new_token()
            if new_token:
                self.store_token_in_secrets(new_token)
                print("Nuevo token obtenido y almacenado.")
                
                # Eliminar archivo de control y agregar una pausa
                s3.delete_object(Bucket=self.bucket_name, Key=control_key)
                time.sleep(3)  # Pausa adicional para sincronización
                return True, new_token['accessToken']
            else:
                print("Error al obtener el token.")
                return False, None

        except Exception as e:
            # Asegurarse de que el archivo de control se elimine en caso de error
            try:
                s3.delete_object(Bucket=self.bucket_name, Key=control_key)
            except Exception as cleanup_error:
                print("Error al limpiar el archivo de control del token:", cleanup_error)
            raise e

    # Metodo para solicitar un nuevo token
    def request_new_token(self):
        data = {'login': os.getenv("usuario"), 'password': self.password}
        response = requests.post(self.token_url, json=data, verify=False)
        if response.status_code == 200:
            return response.json()
        else:
            print("Error al obtener el token", response.json())
            return None

    # Metodo para almacenar el token en Secrets Manager con 'last_updated'
    def store_token_in_secrets(self, token_data):
        # Añadimos la fecha de actualización al token
        token_data['last_updated'] = datetime.now().isoformat()
        # Convertimos el token a JSON y lo guardamos en Secrets Manager
        self.secrets_client.put_secret_value(
            SecretId="qa-syp-secret-ebx-catautos",
            SecretString=json.dumps(token_data)
        )

    # Metodo para recuperar el token de Secrets Manager
    def retrieve_token_from_secrets(self):
        try:
            # Recupera el secreto de Secrets Manager
            secret = self.secrets_client.get_secret_value(SecretId="qa-syp-secret-ebx-catautos")
            # Convierte el contenido JSON a un diccionario Python
            return json.loads(secret['SecretString'])
        except self.secrets_client.exceptions.ResourceNotFoundException:
            print("El token no se ha encontrado en Secrets Manager.")
            return None

    # Metodo para obtener el contenido de una tabla
    def select_tabla_content(self, table_name):

        # Intento obtener el token
        Flag_Token,Respuesta = self.get_token()
        if Flag_Token:
            token = Respuesta

            # Defino los parametros de la consulta y realizo la consulta
            url = self.select_url + table_name +':select'
            params = {'includeHistory': True,
                    'includeContent': True,
                    'includeLabel': 'no',                    
                    'includeDetails': False,
                    'includeMeta': False,
                    'pageSize': 10000,
                      }
            headers = {'Authorization': f'EBX {token}','Content-Type': 'application/json'}
            
            # Realizo la llamada al Endpoint
            response = requests.post(url, params=params, headers=headers,verify=False)
            if response.status_code == 200:

                # Guardo los primeros datos y creo el diccionario de informacion
                Registros = response.json()['rows']

                while response.json()['pagination']['nextPage']!=None:

                    url = response.json()['pagination']['nextPage']
                    response = requests.post(url, headers=headers,verify=False)
                    Registros = Registros + response.json()['rows']

                # Ortganizo los registros recibidos y creo un DF
                Total = []
                for num in range(len(Registros)):
                    reg = {}
                    for campo in Registros[num]['content'].keys():
                        reg[campo] = Registros[num]['content'][campo]['content']
                    Total.append(reg)
                return pd.DataFrame(Total)  

            else:
                print(f"Error al seleccionar datos de la tabla: {response.status_code} {response.reason}")
                return {}
        else:
            return Respuesta
            
    # Metodo para obtener el contenido de la vista de una tabla
    def select_vista_content(self, table_name, view_name):

        # Intento obtener el token
        Flag_Token,Respuesta = self.get_token()
        if Flag_Token:
            token = Respuesta

            # Defino los parametros de la consulta y realizo la consulta
            url = self.select_url + table_name +':select'
            params = {'includeHistory': True,
                    'includeContent': True,
                    'includeLabel': 'no',                    
                    'includeDetails': False,
                    'includeMeta': False,
                    'pageSize': 10000,
                    'viewPublication' : view_name
                      }
            headers = {'Authorization': f'EBX {token}','Content-Type': 'application/json'}
            
            # Realizo la llamada al Endpoint
            response = requests.post(url, params=params, headers=headers,verify=False)
            if response.status_code == 200:

                # Guardo los primeros datos y creo el diccionario de informacion
                Registros = response.json()['rows']

                while response.json()['pagination']['nextPage']!=None:

                    url = response.json()['pagination']['nextPage']
                    response = requests.post(url, headers=headers,verify=False)
                    Registros = Registros + response.json()['rows']

                # Ortganizo los registros recibidos y creo un DF
                Total = []
                for num in range(len(Registros)):
                    reg = {}
                    for campo in Registros[num]['content'].keys():
                        reg[campo] = Registros[num]['content'][campo]['content']
                    Total.append(reg)
                return pd.DataFrame(Total)  

            else:
                print(f"Error al seleccionar datos de la tabla: {response.status_code} {response.reason}")
                return {}
        else:
            return Respuesta
    
    # Este metodo INSERTA un nuevo registro y marca error si el registro ya existe en la tabla
    def insert_register(self,table_name,registro,tipo_datos):

        # Intento obtener el token
        Flag_Token,Respuesta = self.get_token()
        if Flag_Token:
            token = Respuesta

            # Defino los parametros de la consulta y realizo la consulta
            url = self.insert_update_url + table_name
            params = {
                        'includeDetails': False,
                        'includeForeignKey': False,
                        'includeLabel': 'no',
                        'updateOrInsert': False,
                    }
            headers = {'Authorization': f'EBX {token}','Content-Type': 'application/json'}            

            # Construimos el insert en el formato de entrada

            Aux = {}
            for camp in tipo_datos.keys():

                if tipo_datos[camp] == 'str':
                    Aux[camp] = {"content": str(registro[camp])}
                if tipo_datos[camp] == 'int':
                    Aux[camp] = {"content": int(registro[camp])}
                if tipo_datos[camp] == 'bool':
                    Aux[camp] = {"content": bool(registro[camp])}
                if tipo_datos[camp] == 'float':
                    Aux[camp] = {"content": float(registro[camp])}

            data = {}
            data['content']=Aux
            
            # Realizamos la llamada al endpoint 
            response = requests.post(url, json=data, params=params, headers=headers,verify=False)

            if str(response.status_code)[0] == '2':
                return True,response.json()
            else:
                print(f"Error: {response.status_code} {response.reason}")
                print("Error al insertar el elemento \n",registro)
                return False, response.json()
        else:
            return False, Respuesta


    # Este metodo actualiza el valor de un registro (No puede insertar nuevos)
    def update_register(self,table_name,registro,tipo_datos,llaves):

        # Intento obtener el token
        Flag_Token,Respuesta = self.get_token()
        if Flag_Token:
            token = Respuesta

            # Defino los parametros de la consulta y realizo la consulta
            url = self.insert_update_url + table_name
            params = {
                                    'blockingConstraintsDisabled': True,
                                }
            headers = {'Authorization': f'EBX {token}','Content-Type': 'application/json'}            

            # Construimos el insert en el formato de entrada
            Aux = {}
            prim_key = []
            for camp in tipo_datos.keys():
                # La informacion a actualizar no puede ser parte de los keys de la tabla
                if not (camp in llaves.keys()):
                    if tipo_datos[camp] == 'str':
                        Aux[camp] = {"content": str(registro[camp])}
                    if tipo_datos[camp] == 'int':
                        Aux[camp] = {"content": int(registro[camp])}
                    if tipo_datos[camp] == 'bool':
                        Aux[camp] = {"content": bool(registro[camp])}
                    if tipo_datos[camp] == 'float':
                        Aux[camp] = {"content": float(registro[camp])}
                # Identifico todas las llaves del registro 
                else:
                    prim_key += (str(registro[camp])).split('|')
            data = {}
            data['content']=Aux  

            # Reconstruyo la llave y actualizamos la url
            prim_key = '%7C'.join(prim_key)
            url = url +'/'+prim_key
    
            # Realizamos la llamada al endpoint
            response = requests.put(url, json=data, params=params, headers=headers,verify=False)

            if str(response.status_code)[0] == '2':
                return True,response.json()
            else:
                print(f"Error: {response.status_code} {response.reason}")
                print("Error al insertar el elemento \n",registro)
                return False, response.json()
        else:
            return False, Respuesta


    # Este metodo INSERTA un nuevo registro y marca error si el registro ya existe en la tabla
    def in_up_register(self,table_name,registro,tipo_datos):

        # Intento obtener el token
        Flag_Token,Respuesta = self.get_token()
        if Flag_Token:
            token = Respuesta

            # Defino los parametros de la consulta y realizo la consulta
            url = self.insert_update_url + table_name
            params = {
                        'includeDetails': False,
                        'includeForeignKey': False,
                        'includeLabel': 'no',
                        'updateOrInsert': True,
                    }
            headers = {'Authorization': f'EBX {token}','Content-Type': 'application/json'}            

            # Construimos el insert en el formato de entrada

            Aux = {}
            for camp in tipo_datos.keys():

                if tipo_datos[camp] == 'str':
                    Aux[camp] = {"content": str(registro[camp])}
                if tipo_datos[camp] == 'int':
                    Aux[camp] = {"content": int(registro[camp])}
                if tipo_datos[camp] == 'bool':
                    Aux[camp] = {"content": bool(registro[camp])}
                if tipo_datos[camp] == 'float':
                    Aux[camp] = {"content": float(registro[camp])}

            data = {}
            data['content']=Aux
            
            # Realizamos la llamada al endpoint 
            response = requests.post(url, json=data, params=params, headers=headers,verify=False)

            if str(response.status_code)[0] == '2':
                return True,response.json()
            else:
                print(f"Error: {response.status_code} {response.reason}")
                print("Error al insertar el elemento \n",registro)
                return False, response.json()
        else:
            return False, Respuesta
            
            
    def select_register_df(self, table_name, keys_df):
        # Intento obtener el token
        Flag_Token, Respuesta = self.get_token()
        if Flag_Token:
            token = Respuesta
    
            # Defino los parametros de la consulta y realizo la consulta
            url_base = self.select_url + table_name
            headers = {'Authorization': f'EBX {token}', 'Content-Type': 'application/json'}
            
            # Lista para almacenar los resultados
            results = []
    
            # Recorrer cada registro en el DataFrame de llaves
            for idx, row in keys_df.iterrows():
                # Tomo la columna 'CODCAT_CODAPARTADO_CODVEHICULO_ANOVEH' y separo por '|'
                keys = row['CODCAT_CODAPARTADO_CODVEHICULO_CODAPARTADO_CODTIPOVEH'].split('|')
    
                if len(keys) < 3:
                    print(f"Error: el registro en la fila {idx} no contiene suficientes llaves separadas por '|'.")
                    continue
    
                primary_keys = keys
    
                # Reconstruyo la llave y actualizo la URL
                prim_key = '%7C'.join(primary_keys)
                url = url_base + '/' + prim_key
    
                # Realizamos la llamada al endpoint
                response = requests.get(url, headers=headers, verify=False)
    
                if str(response.status_code)[0] == '2':
                    # Proceso y organizo los registros recibidos
                    Registros = response.json().get('content', {})
                    if not Registros:
                        continue
    
                    # Organizo el registro en un diccionario
                    reg = {}
                    for campo in Registros.keys():
                        reg[campo] = Registros[campo]['content']
    
                    # Agrego el registro a la lista de resultados
                    results.append(reg)
                else:
                    print(f"Error: {response.status_code} {response.reason}")
                    print(response.text)
                    print("Error al seleccionar el elemento con las llaves: \n", row.to_dict())
    
            # Devuelvo todos los registros seleccionados como un DataFrame
            return pd.DataFrame(results)
        else:
            return pd.DataFrame()



    # Este metodo inserta/actualiza 100 registros en una tabla
    def Insert_Update_100(self,dataframe,tipo_datos,table_name,tipo_in_up):

        # Intento obtener el token
        Flag_Token,Respuesta = self.get_token()
        if Flag_Token:
            token = Respuesta

            # Convierto el DataFrame al formato de insersion
            data = {}
            rows = []
            for num_reg in range(len(dataframe)):
                content = {"content":{}}
                for key in dataframe.keys():
                    info_key = {}
                    if tipo_datos[key] == 'str':
                        info_key["content"] = str(dataframe[key].iloc[num_reg])
                    elif tipo_datos[key] == 'int':
                        info_key["content"] = int(dataframe[key].iloc[num_reg])
                    elif tipo_datos[key] == 'bool':
                        info_key["content"] = bool(dataframe[key].iloc[num_reg])
                    elif tipo_datos[key] == 'float':
                        info_key["content"] = float(dataframe[key].iloc[num_reg])
                    content["content"][key] = info_key
                rows.append(content)
            data['rows'] = rows

            # Defino los parametros de la consulta y realizo la consulta
            url = self.insert_update_url + table_name
            # identifico si se trata de una operacion de insert o de update
            updateOrInsert = False
            if tipo_in_up == 'insert':updateOrInsert = False
            elif tipo_in_up == 'update': updateOrInsert =True
            params = {
                            'includeDetails': False,
                            'includeForeignKey': False,
                            'includeLabel': 'no',
                            'updateOrInsert': updateOrInsert,
                        }
            
            headers = {'Authorization': f'EBX {token}','Content-Type': 'application/json'}            
            response = requests.post(url, json=data, params=params, headers=headers,verify=False)
    
            return True,response.json()

        else:
            return False, Respuesta