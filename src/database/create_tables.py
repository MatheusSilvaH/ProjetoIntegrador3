import psycopg2

from src.database.database_connection import database_connection

'''
Aqui são criadas as tabelas Fato e Dimensão usadas no projeto caso 
ainda não existam no banco, os dados de conexão com o banco ficam em database_connection
'''
def create_dim_champions():
    try:
        conn = database_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS d_campeoes (
                id INTEGER PRIMARY KEY,
                nome VARCHAR(50)
            );
        """)

        conn.commit()
        cur.close()
        conn.close()
        print('Tabela "d_campeoes" criada com sucesso!')
    except Exception as erro:
        print(f'Erro ao criar a tabela d_campeoes. {erro}')


def create_dim_matches():
    try:
        conn = database_connection()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS d_partidas (
                gameCreation DECIMAL,
                gameDuration DECIMAL,
                gameId BIGINT PRIMARY KEY,
                gameMode VARCHAR(255),
                gameVersion VARCHAR(255),
                mapId INTEGER,
                platformId VARCHAR(255),
                queueId INTEGER,
                seasonId INTEGER
            )
        """)

        conn.commit()
        cur.close()
        conn.close()
        print('Tabela "d_partidas" criada com sucesso!')
    except Exception as erro:
        print(f'Erro ao criar a tabela d_partidas. {erro}')


def create_fact_winner():
    try:
        conn = database_connection()
        cur = conn.cursor()

        cur.execute('''CREATE TABLE IF NOT EXISTS f_vitorias
                       (teamId INT,
                        win VARCHAR(10),
                        firstBlood BOOLEAN,
                        firstTower BOOLEAN,
                        firstInhibitor BOOLEAN,
                        firstBaron BOOLEAN,
                        firstDragon BOOLEAN,
                        firstRiftHerald BOOLEAN,
                        towerKills INT,
                        inhibitorKills INT,
                        baronKills INT,
                        dragonKills INT,
                        vilemawKills INT,
                        riftHeraldKills INT,
                        dominionVictoryScore INT,
                        gameId BIGINT REFERENCES d_partidas(gameid),
                        ban_championIds VARCHAR(200));''')

        conn.commit()
        cur.close()
        conn.close()
        print('Tabela "f_vitorias" criada com sucesso!')
    except Exception as erro:
        print(f'Erro ao criar a tabela f_vitorias. {erro}')


def create_fact_loser():
    try:
        conn = database_connection()
        cur = conn.cursor()

        cur.execute('''CREATE TABLE IF NOT EXISTS f_derrotas
                       (teamId INT,
                        win VARCHAR(10),
                        firstBlood BOOLEAN,
                        firstTower BOOLEAN,
                        firstInhibitor BOOLEAN,
                        firstBaron BOOLEAN,
                        firstDragon BOOLEAN,
                        firstRiftHerald BOOLEAN,
                        towerKills INT,
                        inhibitorKills INT,
                        baronKills INT,
                        dragonKills INT,
                        vilemawKills INT,
                        riftHeraldKills INT,
                        dominionVictoryScore INT,
                        gameId BIGINT REFERENCES d_partidas(gameid),
                        ban_championIds VARCHAR(200));''')

        conn.commit()
        cur.close()
        conn.close()
        print('Tabela "f_derrotas" criada com sucesso!')
    except Exception as erro:
        print(f'Erro ao criar a tabela f_derrotas. {erro}')


def create_dim_bans():
    try:
        conn = database_connection()
        cur = conn.cursor()

        cur.execute('''
            CREATE TABLE IF NOT EXISTS d_bans (
                idban SERIAL PRIMARY KEY,
                win VARCHAR(10),
                gameId INTEGER REFERENCES d_partidas(gameId),
                ban0 INTEGER REFERENCES d_campeoes(id),
                ban1 INTEGER REFERENCES d_campeoes(id),
                ban2 INTEGER REFERENCES d_campeoes(id),
                ban3 INTEGER REFERENCES d_campeoes(id),
                ban4 INTEGER REFERENCES d_campeoes(id)
            );
        ''')

        conn.commit()
        cur.close()
        conn.close()
        print('Tabela "d_bans" criada com sucesso!')
    except Exception as erro:
        print(f'Erro ao criar a tabela d_bans. {erro}')


def create_all_tables():
    create_dim_champions()
    create_dim_matches()
    create_dim_bans()
    create_fact_winner()
    create_fact_loser()
