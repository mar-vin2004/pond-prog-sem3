from minio import Minio
from minio.error import S3Error

def test_minio():
    # Configuração do cliente MinIO
    client = Minio(
        "localhost:9000",  # Endereço do MinIO
        access_key="minioadmin",  # Chave de acesso
        secret_key="minioadmin",  # Chave secreta
        secure=False  # Use True se estiver usando HTTPS
    )

    bucket_name = "test-bucket"
    object_name = "test.txt"
    file_content = "Este é um arquivo de teste."

    try:
        # Criar bucket se não existir
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' criado com sucesso.")
        else:
            print(f"Bucket '{bucket_name}' já existe.")

        # Criar um arquivo de teste
        with open(object_name, "w") as file:
            file.write(file_content)

        # Fazer upload do arquivo para o MinIO
        client.fput_object(bucket_name, object_name, object_name)
        print(f"Arquivo '{object_name}' enviado com sucesso para o bucket '{bucket_name}'.")

        # Fazer download do arquivo do MinIO
        client.fget_object(bucket_name, object_name, f"downloaded_{object_name}")
        print(f"Arquivo '{object_name}' baixado com sucesso do bucket '{bucket_name}'.")

        # Ler o conteúdo do arquivo baixado
        with open(f"downloaded_{object_name}", "r") as file:
            downloaded_content = file.read()
            print(f"Conteúdo do arquivo baixado: {downloaded_content}")

    except S3Error as e:
        print(f"Erro ao interagir com o MinIO: {e}")

if __name__ == "__main__":
    test_minio()