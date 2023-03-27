import psycopg2
import os

# Define os parâmetros necessários para a conexão com o banco de dados. Valores retirados do arquivo "docker-compose.yml" fornecido
DB_NAME = "dvdrental"
DB_HOST = "127.0.0.1"
DB_USER = "postgres"
DB_PASSWORD = "password"

TRANSACTIONAL_DB_PORT = os.getenv("TRANSACTIONAL_DB_PORT", "5432")
ANALYTICS_DB_PORT = os.getenv("ANALYTICS_DB_PORT", "5440")

# Salva em uma variável o comando SQL para listar as tabelas de um banco de dados
LIST_ALL_TABLES_QUERY = """SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"""


# Abre a conexão com o banco de dados desejado (transactional ou analytics) com base no nome e porta do db
def create_db_connection(db_name, port):
    return psycopg2.connect(
        database=db_name,
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=port,
    )


# Cria um novo banco de dados na instância do postgres analytics com o mesmo nome do db transacional para fazer a importação do dump.
def create_new_analytics_database():
    connection_analytics = create_db_connection("postgres", ANALYTICS_DB_PORT)
    connection_analytics.autocommit = True
    cursor_analytics = connection_analytics.cursor()
    cursor_analytics.execute(f"""CREATE DATABASE {DB_NAME}""")
    cursor_analytics.close()


# Função para criar um arquivo chamado "dumpfile" para onde as tabelas do banco de dados transacional são copiadas.
# Foi utilizado o método os.system para rodar o comando do PostgreSQL "pg_dump" em um subshell.
def create_db_dump():
    os.system(f"""PGPASSWORD="{DB_PASSWORD}" pg_dump -h {DB_HOST} -p {TRANSACTIONAL_DB_PORT} --username={DB_USER} {DB_NAME} > dumpfile""")


# Função para transferir as tabelas salvas em "dumpfile" para o banco de dados de mesmo nome (dvdrental) criado no db analytics.
# Foi utilizado o método os.system para rodar o comando do PostgreSQL "psql" em um subshell
def restore_dump_to_analytics_db():
    os.system(f"""PGPASSWORD="{DB_PASSWORD}" psql -h {DB_HOST} -p {ANALYTICS_DB_PORT} --username={DB_USER} {DB_NAME} < dumpfile""")


# Função para listar as tabelas do banco de dados desejado a fim de garantir que a operação foi bem sucedida.
def list_db_tables(database, db_name, db_port):
    connection = create_db_connection(db_name, db_port)
    cursor = connection.cursor()

    cursor.execute(LIST_ALL_TABLES_QUERY)
    print(f"\nListing tables for {database} db: \n")
    for table in cursor.fetchall():
        print(table[0])
    connection.close()


if __name__ == "__main__":
    print(f"Creating {DB_NAME} database at analytics db")
    create_new_analytics_database()
    # Lista as tabelas dos DB transactional e analytics para mostrar que o primeiro está populado e o segundo não.
    list_db_tables("transactional", DB_NAME, TRANSACTIONAL_DB_PORT)
    list_db_tables("analytics", DB_NAME, ANALYTICS_DB_PORT)

    print("Creating transactional db dump")
    create_db_dump()
    print("Restoring transactional db dump to analytics db")
    restore_dump_to_analytics_db()

    # Lista novamento as tabelas dos DB transactional e analytics para mostrar que ambos estão populados.
    list_db_tables("transactional", DB_NAME, TRANSACTIONAL_DB_PORT)
    list_db_tables("analytics", DB_NAME, ANALYTICS_DB_PORT)
    print("\n\nDone")
