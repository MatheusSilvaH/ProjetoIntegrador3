import psycopg2


def database_connection():
    '''
     Essa função retorna os dados de conexão com o banco
    '''
    conn = psycopg2.connect(
        host="localhost",
        database="dados_lolzinho",
        user="postgres",
        password="SUASENHAAQUI"
    )

    return conn
