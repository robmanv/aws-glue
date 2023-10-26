import boto3

# Substitua pelos seus valores
database_name = "database_robson"
table_name = "tabela_robson"
bucket_name = "bucket-robson"
s3_path = f"s3://{bucket_name}/databases/"

# Substitua pela região desejada (por exemplo, "us-east-1")
region = "us-east-1"

# Inicialize o cliente S3 com a região especificada
s3 = boto3.client('s3', region_name=region)

# Verifique se o bucket já existe
try:
    s3.head_bucket(Bucket=bucket_name)
    print(f'O bucket {bucket_name} já existe.')
except Exception as e:
    if e.response['Error']['Code'] == '404':
        print(f'O bucket {bucket_name} não existe e será criado.')
        try:
            s3.create_bucket(Bucket=bucket_name)
            print(f'O bucket {bucket_name} foi criado na região {region}.')
        except Exception as e:
            print(f'Ocorreu um erro ao criar o bucket: {e}')
    else:
        print(f'Ocorreu um erro ao verificar o bucket: {e}')

# Se o bucket não existir, crie-o


# Inicialize o cliente AWS Glue
glue = boto3.client('glue')

# Verifique se o banco de dados já existe
try:
    response = glue.get_database(Name=database_name)
    print(f'O banco de dados {database_name} já existe.')
except glue.exceptions.EntityNotFoundException:
    print(f'O banco de dados {database_name} não existe.')
    # Crie o banco de dados
    glue.create_database(DatabaseInput={'Name': database_name})
except Exception as e:
    print(f'Ocorreu um erro ao verificar o banco de dados: {e}')
    

# Defina o esquema da tabela
table_input = {
    'DatabaseName': database_name,
    'TableInput': {
        'Name': table_name,
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'Name', 'Type': 'string'},
                {'Name': 'Age', 'Type': 'int'}
            ],
            'Location': s3_path,
            'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'Compressed': False,
            'NumberOfBuckets': -1,
            'SerdeInfo': {
                'Name': table_name,
                'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                'Parameters': {}
            }
        },
        'TableType': 'EXTERNAL_TABLE'
    }
}

# Verifique se a tabela já existe
try:
    response = glue.get_table(DatabaseName=database_name, Name=table_name)
    print(f'A tabela {table_name} no banco de dados {database_name} já existe.')
except glue.exceptions.EntityNotFoundException:
    print(f'A tabela {table_name} no banco de dados {database_name} não existe e será criado.')
    # Crie a tabela
    glue.create_table(**table_input)
except Exception as e:
    print(f'Ocorreu um erro ao verificar a tabela: {e}')

# Se quiser, atualize o Data Catalog para reconhecer as novas tabelas
glue.get_table(DatabaseName=database_name, Name=table_name)

from pyspark.sql import SparkSession
from awsglue.context import GlueContext

# Inicialize o contexto Spark
spark = SparkSession.builder.getOrCreate()
glueContext = GlueContext(spark)

# Defina os dados e o esquema da tabela
data = [("John Doe", 28), ("Jane Smith", 32), ("Jim Brown", 45)]
columns = ["Name", "Age"]

# Crie um DataFrame
df = spark.createDataFrame(data, columns)

# Escreva o DataFrame como um arquivo Parquet
df.write.mode("overwrite").parquet(path="s3://bucket-robson/databases/")

# Registre o DataFrame como uma tabela no AWS Glue Data Catalog
df.write.format("parquet").mode("overwrite").option("path", "s3://bucket-robson/databases/").saveAsTable(f"{database_name}.{table_name}")

# Execute uma consulta SQL para selecionar todos os dados da tabela
df = spark.sql(f"SELECT * FROM {database_name}.{table_name}")

# Mostre os dados
df.show()