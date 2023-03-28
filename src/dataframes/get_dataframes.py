import pandas as pd

def get_match_data():
    match_df = pd.read_pickle(r'D:\Faesa\5P\projeto_integrador\lolzinho_api\dataset\match_data_version1.pickle')

    match_df.drop(['participantIdentities', 'participants', 'status.message', 'status.status_code'], axis=1, inplace=True)

    return match_df


def get_match_winners():
    winners_df = pd.read_pickle(r'D:\Faesa\5P\projeto_integrador\lolzinho_api\dataset\match_winner_data_version1.pickle')

    winners_df['ban_championIds'] = winners_df['bans'].apply(extract_champion_ids)

    winners_df.drop('bans', axis=1, inplace=True)

    return winners_df


def get_bans_winners():
    new_df = get_match_winners().apply(separate_champions, axis=1)
    bans_winners_df = pd.concat([get_match_winners()['win'], get_match_winners()['gameId'], new_df], axis=1)
    bans_winners_df.rename(columns={'0': 'ban0', '1': 'ban1', '2': 'ban2', '3': 'ban3', '4': 'ban4'}, inplace=True)

    return bans_winners_df


def get_match_losers():
    losers_df = pd.read_pickle(r'D:\Faesa\5P\projeto_integrador\lolzinho_api\dataset\match_loser_data_version1.pickle')

    losers_df['ban_championIds'] = losers_df['bans'].apply(extract_champion_ids)

    losers_df.drop(losers_df.bans, axis=1, inplace=True)

    return losers_df


def get_bans_losers():
    new_df = get_match_losers().apply(separate_champions, axis=1)
    bans_losers_df = pd.concat([get_match_losers()['win'], get_match_losers()['gameId'], new_df], axis=1)
    bans_losers_df.rename(columns={'0': 'ban0', '1': 'ban1', '2': 'ban2', '3': 'ban3', '4': 'ban4'}, inplace=True)

    return bans_losers_df


def get_champions():
    champions_df = pd.read_csv(r'D:\Faesa\5P\projeto_integrador\lolzinho_api\dataset\champions.csv', sep=';')
    champions_df['id'] = champions_df['id'].astype(int)

    return champions_df


def extract_champion_ids(bans_list):
    return [d['championId'] for d in bans_list]


def separate_champions(row):
    return pd.Series(row['ban_championIds'])
