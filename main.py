from src.database.create_tables import create_all_tables
from src.database.insert_data import insert_dim_champions_data, insert_dim_matches_data, insert_fact_winner, \
    insert_dim_bans_winners, insert_fact_losers, insert_fact_bans_losers


def main():
    # Cria todas as tabelas caso não existam no banco
    create_all_tables()

    #TEnta inserir os DataFrames por tabela
    insert_dim_champions_data()

    insert_dim_matches_data()

    insert_fact_winner()

    insert_dim_bans_winners()

    insert_fact_losers()

    insert_fact_bans_losers()

if __name__ == '__main__':
    main()
