from src.database.database_connection import database_connection
from src.dataframes.get_dataframes import get_match_data, get_match_winners, get_match_losers, get_champions, get_bans_winners, get_bans_losers


def insert_champions_data():
    conn = database_connection()
    cur = conn.cursor()
    for index, row in get_champions().iterrows():
        sql = """
                INSERT INTO d_campeoes (
                    id, nome
                ) VALUES (
                %s, %s
                )     
            """
        data = (
            row['id'], row['nome']
        )

        cur.execute(sql, data)

    conn.commit()

    conn.close()


def insert_dim_matches_data():
    conn = database_connection()
    cur = conn.cursor()

    for index, row in get_match_data().iterrows():
        try:
            sql = """
                INSERT INTO d_partidas (
                    gameCreation, gameDuration, gameId, gameMode, gameType,
                    gameVersion,mapId, platformId, queueId, seasonId
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )      
            """
            data = (
                row['gameCreation'], row['gameDuration'], row['gameId'], row['gameMode'], row['gameType'],
                row['gameVersion'], row['mapId'], row['platformId'], row['queueId'], row['seasonId']
            )
            cur.execute(sql, data)
        except Exception as e:
            print(f"Error inserting row {index}: {e}")

    conn.commit()

    conn.close()


def insert_fact_winner():
    conn = database_connection()
    cur = conn.cursor()

    for index, row in get_match_winners().iterrows():
        try:
            sql = """
                INSERT INTO f_vitorias (
                    team_id, win, first_blood, first_tower, first_inhibitor,
                    first_baron, first_dragon, first_rift_herald, tower_kills,
                    inhibitor_kills, baron_kills, dragon_kills, vilemaw_kills,
                    rift_herald_kills, dominion_victory_score, game_id, ban_champion_ids
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            data = (
                row['teamId'], row['win'], row['firstBlood'], row['firstTower'], row['firstInhibitor'],
                row['firstBaron'], row['firstDragon'], row['firstRiftHerald'], row['towerKills'],
                row['inhibitorKills'], row['baronKills'], row['dragonKills'], row['vilemawKills'],
                row['riftHeraldKills'], row['dominionVictoryScore'], row['gameId'], row['ban_championIds']
            )

            cur.execute(sql, data)
        except Exception as erro:
            print(f'Erro ao inserir a linha {index}. Erro: {erro}')

    conn.commit()

    conn.close()


def insert_fact_bans_winners():
    conn = database_connection()
    cur = conn.cursor()

    for index, row in get_bans_winners().iterrows():
        try:
            sql = """
                INSERT INTO f_bans (
                win, gameId, ban0, ban1, ban2, ban3, ban4
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s
                )
            """

            data = (
                row['win'], row['gameId'], row['ban0'], row['ban1'], row['ban2'], row['ban3'], row['ban4']
            )

            cur.execute(sql, data)
        except Exception as erro:
            print(f'Erro ao inserir a linha {index}. Erro: {erro}')

    conn.commit()

    conn.close()


def insert_fact_losers():
    conn = database_connection()
    cur = conn.cursor()

    for index, row in get_match_losers().iterrows():
        try:
            sql = """
                INSERT INTO f_derrotas (
                    team_id, win, first_blood, first_tower, first_inhibitor,
                    first_baron, first_dragon, first_rift_herald, tower_kills,
                    inhibitor_kills, baron_kills, dragon_kills, vilemaw_kills,
                    rift_herald_kills, dominion_victory_score, game_id, ban_champion_ids
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            data = (
                row['teamId'], row['win'], row['firstBlood'], row['firstTower'], row['firstInhibitor'],
                row['firstBaron'], row['firstDragon'], row['firstRiftHerald'], row['towerKills'],
                row['inhibitorKills'], row['baronKills'], row['dragonKills'], row['vilemawKills'],
                row['riftHeraldKills'], row['dominionVictoryScore'], row['gameId'], row['ban_championIds']
            )

            cur.execute(sql, data)
        except Exception as erro:
            print(f'Erro ao inserir a linha {index}. Erro: {erro}')

    conn.commit()

    conn.close()


def insert_fact_bans_losers():
    conn = database_connection()
    cur = conn.cursor()

    for index, row in get_bans_losers().iterrows():
        try:
            sql = """
                INSERT INTO f_bans (
                win, gameId, ban0, ban1, ban2, ban3, ban4
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s
                )
            """

            data = (
                row['win'], row['gameId'], row['ban0'], row['ban1'], row['ban2'], row['ban3'], row['ban4']
            )

            cur.execute(sql, data)
        except Exception as erro:
            print(f'Erro ao inserir a linha {index}. Erro: {erro}')

    conn.commit()

    conn.close()

