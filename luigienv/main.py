import luigi
import psycopg2
from dbfread2 import DBF
from io import StringIO
import csv
import time # No se usa actualmente, pero lo mantengo por si lo necesitas después
import os
import glob

# --- Constantes y Configuración (Opcional, pero buena práctica) ---
# Podrías moverlos a un archivo de configuración luigi.cfg o definirlos aquí
DEFAULT_DBF_FOLDER = 'DATA'
DEFAULT_DB_PARAMS = {
    'dbname': 'bytsscom_unmsm',
    'user': 'postgres',
    'password': 'postgres', # Considera usar variables de entorno o secretos para la contraseña
    'host': 'localhost',
    'port': '5432'
}
DEFAULT_DBF_ENCODING = 'cp1252'
CHAR_DECODE_ERRORS = 'replace'

# --- Tareas Base Mejoradas ---

class CleanMarkers(luigi.Task):
    """
    Elimina los marcadores .ready y .done para forzar re-ejecución.
    Esta tarea ahora se ejecuta una vez al inicio del pipeline.
    """
    dbf_folder = luigi.Parameter(default=DEFAULT_DBF_FOLDER)
    # Parámetro para generar un marcador único para esta ejecución de limpieza
    # Esto evita que se ejecute múltiples veces si es requerido por varias tareas
    # (aunque en la nueva estructura, solo Pipeline lo requiere directamente).
    timestamp = luigi.Parameter(default=time.strftime("%Y%m%d%H%M%S"))

    def output(self):
        # Marcador para esta instancia específica de limpieza.
        # Se coloca en la carpeta raíz de los DBF para indicar que la limpieza general se hizo.
        return luigi.LocalTarget(os.path.join(self.dbf_folder, f'_pipeline_cleaned_{self.timestamp}.marker'))

    def run(self):
        # Eliminar archivos .ready y .done de la carpeta dbf_folder
        patterns_to_remove = [
            os.path.join(self.dbf_folder, '*.ready'),
            os.path.join(self.dbf_folder, '*.done'),
            os.path.join(self.dbf_folder, '*_pipeline_cleaned_*.marker') # Limpia marcadores de limpiezas anteriores
        ]
        
        files_removed_count = 0
        for pattern in patterns_to_remove:
            for f_path in glob.glob(pattern):
                # Evita que se borre a sí mismo si el patrón es muy general y el timestamp es el mismo
                if os.path.basename(f_path) == os.path.basename(self.output().path) and pattern.endswith('.marker'):
                    continue
                try:
                    os.remove(f_path)
                    files_removed_count += 1
                    print(f"Removed marker: {f_path}")
                except OSError as e:
                    print(f"Error removing file {f_path}: {e}")
        
        print(f"Total markers removed: {files_removed_count}")
        # Generar marcador de que la limpieza para esta corrida se completó.
        with self.output().open('w') as f:
            f.write(f'Cleaned at {time.asctime()}')

# ... (resto del código sin cambios) ...

class ExtractDBF(luigi.Task):
    """
    Valida que un archivo DBF específico es accesible y legible.
    """
    dbf_path = luigi.Parameter()
    dbf_encoding = luigi.Parameter(default=DEFAULT_DBF_ENCODING)
    char_decode_errors = luigi.Parameter(default=CHAR_DECODE_ERRORS)

    # No requiere CleanMarkers directamente, Pipeline se encarga de eso.

    def output(self):
        # Marcador que indica que este DBF específico está listo para ser procesado.
        return luigi.LocalTarget(f"{self.dbf_path}.ready")

    def run(self):
        try:
            # Usar un iterador para verificar la legibilidad.
            # Leer solo el primer registro es suficiente para una validación básica.
            dbf_iterator = DBF(self.dbf_path,
                               record_factory=dict, # Devuelve registros como diccionarios
                               encoding=self.dbf_encoding,
                               char_decode_errors=self.char_decode_errors)
                               # SE HA QUITADO EL PARÁMETRO load=False DE AQUÍ
            
            # Intentar leer el primer registro para asegurar la integridad.
            first_record = next(iter(dbf_iterator), None)
            
            # Si el DBF está vacío pero tiene una estructura válida, dbf_iterator.header.numrecords será 0
            # y first_record será None. Esto es un caso válido.
            # El problema sería si numrecords > 0 y no podemos leer el primer registro.
            if first_record is None and dbf_iterator.header.numrecords > 0:
                # Esta condición podría indicar un archivo corrupto si se esperan registros.
                # Para una simple verificación de "se puede abrir y leer", el next(iter()) es suficiente.
                # Si quieres ser más estricto, puedes dejar esta comprobación.
                print(f"Warning: DBF {self.dbf_path} header indicates {dbf_iterator.header.numrecords} records, but couldn't read the first one (might be empty or an issue).")
            
            print(f"DBF check successful for {self.dbf_path}")

        except Exception as e:
            # Propagar la excepción para que Luigi marque la tarea como fallida.
            raise RuntimeError(f"DBF check failed for {self.dbf_path}: {e}") from e
        
        # Si llegamos aquí, el DBF es legible; escribir el marcador .ready.
        with self.output().open('w') as f:
            f.write('ok')


class BulkLoadTask(luigi.Task):
    """
    Tarea base para cargar datos de un archivo DBF a una tabla PostgreSQL.
    """
    table = luigi.Parameter() # Nombre de la tabla destino (ej: schema.tablename)
    columns = luigi.ListParameter() # Lista de columnas en la tabla PostgreSQL
    dbf_path = luigi.Parameter() # Ruta completa al archivo .dbf
    
    # Parámetros con valores por defecto
    field_map = luigi.DictParameter(default={}) # Mapeo de nombres de campo DBF a nombres de columna SQL
    truncate = luigi.BoolParameter(default=True) # Si es True, trunca la tabla antes de cargar
    connection_params = luigi.DictParameter(default=DEFAULT_DB_PARAMS)
    dbf_encoding = luigi.Parameter(default=DEFAULT_DBF_ENCODING)
    char_decode_errors = luigi.Parameter(default=CHAR_DECODE_ERRORS)

    # El filtro ahora es un método que las subclases pueden sobreescribir.
    # Por defecto, no filtra nada.
    def filter_record(self, record):
        """
        Determina si un registro del DBF debe ser incluido.
        Sobrescribir en las subclases para lógica de filtrado específica.
        """
        return True

    def requires(self):
        # Esta tarea depende de que el DBF específico haya sido validado.
        # CleanMarkers ya no es una dependencia directa aquí, Pipeline se encarga.
        return ExtractDBF(
            dbf_path=self.dbf_path,
            dbf_encoding=self.dbf_encoding,
            char_decode_errors=self.char_decode_errors
        )

    def output(self):
        # Marcador que indica que la carga para esta tabla y DBF se completó.
        # El nombre del marcador incluye el nombre de la tabla para evitar colisiones si múltiples
        # tablas se cargan desde el mismo DBF (aunque no es el caso aquí).
        base_name = os.path.basename(self.dbf_path)
        table_suffix = self.table.replace('.', '_') # ej. bytsscom_bytsiaf_certificado
        return luigi.LocalTarget(os.path.join(os.path.dirname(self.dbf_path), f"{base_name}.{table_suffix}.done"))

    def run(self):
        conn = None # Inicializar fuera del try para asegurar que exista en el finally
        try:
            conn = psycopg2.connect(**self.connection_params)
            conn.autocommit = False # Usar transacciones explícitas para el bloque DDL y COPY
            
            with conn.cursor() as cur:
                if self.truncate:
                    print(f"Truncating table {self.table} and disabling triggers...")
                    cur.execute(f"TRUNCATE TABLE {self.table};")
                    # Es más seguro deshabilitar e habilitar triggers a nivel de tabla
                    cur.execute(f"ALTER TABLE {self.table} DISABLE TRIGGER ALL;")
                
                buffer = StringIO()
                # Usar quote_minimal para solo entrecomillar campos que lo necesiten (ej: contienen comas, nuevas líneas)
                writer = csv.writer(buffer, lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
                
                # Escribir encabezado en el buffer CSV (columnas de la tabla SQL)
                writer.writerow(self.columns)
                
                record_count = 0
                loaded_count = 0

                # Iterar sobre los registros del DBF
                # Abrir el DBF aquí de nuevo; ExtractDBF solo lo validó.
                dbf_records = DBF(self.dbf_path,
                                  record_factory=dict,
                                  encoding=self.dbf_encoding,
                                  char_decode_errors=self.char_decode_errors)
                
                for rec in dbf_records:
                    record_count += 1
                    if not self.filter_record(rec): # Usar el método de filtro
                        continue
                    
                    # Construir la fila para el CSV usando el field_map
                    # Si una columna no está en field_map, se usa el mismo nombre.
                    row_data = [rec.get(self.field_map.get(col, col)) for col in self.columns]
                    writer.writerow(row_data)
                    loaded_count += 1
                
                print(f"Processed {record_count} records from {self.dbf_path}. Will attempt to load {loaded_count} records.")

                if loaded_count > 0:
                    buffer.seek(0) # Regresar al inicio del buffer para la lectura con COPY
                    cols_sql = ', '.join(self.columns) # Entrecomillar por si hay mayúsculas/minúsculas o caracteres especiales
                    
                    # Usar COPY FROM STDIN, es la forma más eficiente.
                    # HEADER true indica que la primera línea del buffer es el encabezado.
                    copy_sql = f"COPY {self.table} ({cols_sql}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
                    print(f"Executing COPY: {copy_sql[:100]}...") # Imprimir solo una parte del SQL por brevedad
                    cur.copy_expert(sql=copy_sql, file=buffer)
                else:
                    print(f"No records to load into {self.table} after filtering.")

                if self.truncate:
                    print(f"Enabling triggers for table {self.table}...")
                    cur.execute(f"ALTER TABLE {self.table} ENABLE TRIGGER ALL;")
                
                conn.commit() # Confirmar la transacción
                print(f"Successfully loaded {loaded_count} records into {self.table}.")

        except Exception as e:
            if conn:
                conn.rollback() # Revertir cambios en caso de error
            raise RuntimeError(f"Failed to load data into {self.table} from {self.dbf_path}: {e}") from e
        finally:
            if conn:
                conn.close()

        # Escribir el marcador .done si todo fue exitoso
        with self.output().open('w') as f:
            f.write(f"{loaded_count} records loaded into {self.table} at {time.asctime()}")

# --- Tareas de Carga Específicas ---
# Ahora heredan de la BulkLoadTask mejorada y solo definen sus particularidades.

class LoadCertificado(BulkLoadTask):
    table = 'bytsscom_bytsiaf.certificado'
    columns = ['ANO_EJE','CERTIFICADO','SEC_EJEC','TIPO_CERTIFICADO','ESTADO_REGISTRO','COD_ERROR','COD_MENSA','ESTADO_ENVIO']
    field_map = {'CERTIFICADO':'CERTIFICAD','TIPO_CERTIFICADO':'TIPO_CERTI','ESTADO_REGISTRO':'ESTADO_REG','ESTADO_ENVIO':'ESTADO_ENV'}

    def filter_record(self, rec): # Sobrescribir el método de filtro
        return rec.get('TIPO_CERTI') == '2' and \
               rec.get('ESTADO_REG') == 'A' and \
               rec.get('ANO_EJE') in ['2024','2025'] # Suponiendo que ANO_EJE es string en el DBF

class LoadCertificadoFase(BulkLoadTask):
    table = 'bytsscom_bytsiaf.certificado_fase'
    columns = [
        'ANO_EJE','SEC_EJEC','CERTIFICADO','SECUENCIA','SECUENCIA_PADRE','FUENTE_FINANC','ETAPA',
        'TIPO_ID','RUC','ES_COMPROMISO','MONTO','MONTO_COMPROMETIDO','MONTO_NACIONAL','GLOSA',
        'ESTADO_REGISTRO','COD_ERROR','COD_MENSA','ESTADO_ENVIO','SALDO_NACIONAL',
        'IND_ANULACION','TIPO_FINANCIAMIENTO','TIPO_OPERACION','SEC_AREA'
    ]
    field_map = {
        'CERTIFICADO':'CERTIFICAD','SECUENCIA_PADRE':'SECUENCIA_','FUENTE_FINANC':'FUENTE_FIN',
        'ES_COMPROMISO':'ES_COMPROM','MONTO_COMPROMETIDO':'MONTO_COMP','MONTO_NACIONAL':'MONTO_NACI',
        'ESTADO_REGISTRO':'ESTADO_REG','ESTADO_ENVIO':'ESTADO_ENV','SALDO_NACIONAL':'SALDO_NACI',
        'IND_ANULACION':'IND_ANULAC','TIPO_FINANCIAMIENTO':'TIPO_FINAN','TIPO_OPERACION':'TIPO_OPERA'
    }
    def filter_record(self, rec):
        return rec.get('ANO_EJE') in ['2024','2025']

class LoadCertificadoSecuencia(BulkLoadTask):
    table = 'bytsscom_bytsiaf.certificado_secuencia'
    columns = [
        'ANO_EJE','SEC_EJEC','CERTIFICADO','SECUENCIA','CORRELATIVO','COD_DOC','NUM_DOC','FECHA_DOC',
        'ESTADO_REGISTRO','ESTADO_ENVIO','IND_CERTIFICACION','ESTADO_REGISTRO2','ESTADO_ENVIO2',
        'MONTO','MONTO_COMPROMETIDO','MONTO_NACIONAL','MONEDA','TIPO_CAMBIO','COD_ERROR','COD_MENSA',
        'TIPO_REGISTRO','FECHA_BD_ORACLE','ESTADO_CTB','SECUENCIA_SOLICITUD','FECHA_CREACION_CLT',
        'FECHA_MODIFICACION_CLT','FLG_INTERFASE'
    ]
    field_map = {
        'CERTIFICADO':'CERTIFICAD','CORRELATIVO':'CORRELATIV','MONTO_COMPROMETIDO':'MONTO_COMP',
        'MONTO_NACIONAL':'MONTO_NACI','ESTADO_REGISTRO':'ESTADO_REG','ESTADO_ENVIO':'ESTADO_ENV',
        'IND_CERTIFICACION':'IND_CERTIF','TIPO_CAMBIO':'TIPO_CAMBI','TIPO_REGISTRO':'TIPO_REGIS',
        'FECHA_BD_ORACLE':'FECHA_BD_O','FECHA_CREACION_CLT':'FECHA_CREA','FECHA_MODIFICACION_CLT':'FECHA_MODI'
    }
    def filter_record(self, rec):
        return rec.get('ANO_EJE') in ['2024','2025']

class LoadCertificadoMeta(BulkLoadTask):
    table = 'bytsscom_bytsiaf.certificado_meta'
    columns = [
        'ANO_EJE','SEC_EJEC','CERTIFICADO','SECUENCIA','CORRELATIVO','ID_CLASIFICADOR','SEC_FUNC',
        'MONTO','MONTO_COMPROMETIDO','MONTO_NACIONAL','ESTADO_REGISTRO','COD_ERROR','COD_MENSA',
        'ESTADO_ENVIO','MONTO_NACIONAL_AJUSTE','SYS_COD_CLASIF','SYS_ID_CLASIFICADOR'
    ]
    field_map = {
        'CERTIFICADO':'CERTIFICAD','CORRELATIVO':'CORRELATIV','MONTO_COMPROMETIDO':'MONTO_COMP',
        'MONTO_NACIONAL':'MONTO_NACI','ESTADO_REGISTRO':'ESTADO_REG','ESTADO_ENVIO':'ESTADO_ENV',
        'ID_CLASIFICADOR':'ID_CLASIFI','MONTO_NACIONAL_AJUSTE':'MONTO_NAC2'
    }
    def filter_record(self, rec):
        return rec.get('ANO_EJE') in ['2024','2025']

class LoadExpedienteFase(BulkLoadTask):
    table = 'bytsscom_bytsiaf.expediente_fase'
    columns = [
        'ANO_EJE','SEC_EJEC','EXPEDIENTE','CICLO','FASE','SECUENCIA','SECUENCIA_PADRE','SECUENCIA_ANTERIOR',
        'MES_CTB','MONTO_NACIONAL','MONTO_SALDO','ORIGEN','FUENTE_FINANC','MEJOR_FECHA','TIPO_ID','RUC',
        'TIPO_PAGO','TIPO_RECURSO','TIPO_COMPROMISO','ORGANISMO','PROYECTO','ESTADO','ESTADO_ENVIO',
        'ARCHIVO','TIPO_GIRO','TIPO_FINANCIAMIENTO','COD_DOC_REF','FECHA_DOC_REF','NUM_DOC_REF',
        'CERTIFICADO','CERTIFICADO_SECUENCIA','SEC_EJEC_RUC'
    ]
    field_map = {
        'SECUENCIA_PADRE':'SECUENCIA2','SECUENCIA_ANTERIOR':'SECUENCIA_','MONTO_NACIONAL':'MONTO_NACI',
        'MONTO_SALDO':'MONTO_SALD','FUENTE_FINANC':'FUENTE_FIN','MEJOR_FECHA':'MEJOR_FECH',
        'TIPO_RECURSO':'TIPO_RECUR','TIPO_COMPROMISO':'TIPO_COMPR','TIPO_FINANCIAMIENTO':'TIPO_FINAN',
        'COD_DOC_REF':'COD_DOC_RE','FECHA_DOC_REF':'FECHA_DOC_','NUM_DOC_REF':'NUM_DOC_RE',
        'CERTIFICADO':'CERTIFICAD','CERTIFICADO_SECUENCIA':'CERTIFICA2','SEC_EJEC_RUC':'SEC_EJEC_R'
    }
    def filter_record(self, rec):
        return rec.get('ANO_EJE') in ['2024','2025']


# --- Pipeline Principal ---

class MainPipeline(luigi.WrapperTask):
    """
    Pipeline principal que orquesta la limpieza y la carga de todas las tablas.
    """
    # Parámetros del pipeline que se pueden pasar desde la línea de comandos o luigi.cfg
    dbf_folder = luigi.Parameter(default=DEFAULT_DBF_FOLDER)
    # Usar los parámetros de conexión por defecto, pero permitir que se sobrescriban
    connection_params = luigi.DictParameter(default=DEFAULT_DB_PARAMS)
    # Parámetro para forzar la limpieza. Útil si los marcadores de limpieza persisten.
    force_clean = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)


    def requires(self):
        # Define las tareas a ejecutar.
        # Primero, siempre se intenta ejecutar CleanMarkers.
        # El timestamp asegura que CleanMarkers se ejecute una vez por "instancia" de pipeline
        # si se pasa el mismo timestamp, o una vez por corrida si se genera dinámicamente.
        # Al hacerlo depender del pipeline, se ejecuta antes que las cargas.
        
        # Generamos un timestamp para esta ejecución del pipeline,
        # así CleanMarkers sabe si ya limpió para esta "sesión".
        # Si force_clean es True, se usa un nuevo timestamp para forzar la limpieza.
        # Si no, se usa un timestamp fijo para que no limpie en cada sub-tarea si hubiera dependencias múltiples.
        # Sin embargo, con la estructura actual, CleanMarkers solo es dependencia directa de Pipeline.
        clean_timestamp = time.strftime("%Y%m%d%H%M%S") if self.force_clean else "static_pipeline_clean"
        
        # La tarea de limpieza es la primera dependencia.
        # Luigi se encargará de que se ejecute solo una vez si es requerida por múltiples tareas,
        # pero aquí la hacemos explícitamente una dependencia del WrapperTask.
        yield CleanMarkers(dbf_folder=self.dbf_folder, timestamp=clean_timestamp)

        # Lista de configuraciones de tareas de carga.
        # Esto hace más fácil añadir o quitar tablas del pipeline.
        table_load_configs = [
            {'task_class': LoadCertificado, 'dbf_name': 'certificado.dbf'},
            {'task_class': LoadCertificadoFase, 'dbf_name': 'certificado_fase.dbf'},
            {'task_class': LoadCertificadoSecuencia, 'dbf_name': 'certificado_secuencia.dbf'},
            {'task_class': LoadCertificadoMeta, 'dbf_name': 'certificado_meta.dbf'},
            {'task_class': LoadExpedienteFase, 'dbf_name': 'expediente_fase.dbf'},
        ]

        for config in table_load_configs:
            dbf_full_path = os.path.join(self.dbf_folder, config['dbf_name'])
            # Los parámetros de la tarea de carga se pasan aquí.
            # Las tareas BulkLoadTask requerirán internamente su ExtractDBF correspondiente.
            yield config['task_class'](
                dbf_path=dbf_full_path,
                connection_params=self.connection_params
                # Otros parámetros como field_map, columns, table, filter_fn ya están definidos
                # en las clases específicas (LoadCertificado, etc.)
            )

if __name__ == '__main__':
    # Para ejecutar desde la línea de comandos:
    # python tu_script.py MainPipeline --local-scheduler
    # Para forzar limpieza:
    # python tu_script.py MainPipeline --local-scheduler --force-clean
    luigi.build([MainPipeline()], local_scheduler=True)
    # O si tienes un scheduler central:
    # luigi.run()