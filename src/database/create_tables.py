import psycopg2

from src.database.database_connection import database_connection


def create_dim_champions():
    conn = database_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE d_campeoes (
            id INTEGER PRIMARY KEY,
            nome VARCHAR(50)
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print('Tabela "d_campeoes" criada com sucesso!')


def create_dim_matches():
    conn = database_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE d_partidas (
            gameCreation real,
            gameDuration real,
            gameId BIGINT PRIMARY KEY,
            gameMode VARCHAR(255),
            gameType VARCHAR(255),
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


def create_fact_winner():
    conn = database_connection()
    cur = conn.cursor()

    cur.execute('''CREATE TABLE f_vitorias
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
                    ban_championIds VARCHAR(200) REFERENCES d_campeoes(nome));''')

    conn.commit()
    cur.close()
    conn.close()
    print('Tabela "f_vitorias" criada com sucesso!')


def create_fact_bans():
    conn = database_connection()
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE f_bans (
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
    print('Tabela "f_bans" criada com sucesso!')
create_dim_champions()
create_dim_matches()
create_fact_bans()
