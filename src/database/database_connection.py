import psycopg2


def database_connection():
    # Definine as informações de conexão com o banco de dados
    conn = psycopg2.connect(
        host="localhost",
        database="dados_lolzinho",
        user="postgres",
        password="SUASENHAAQUI"
    )

    return conn
