import pandas as pd


def add_players_to_games(just_add):
    if just_add:
        old_games = pd.read_csv(f"../data/games_with_players.csv", low_memory=False)

        print(old_games.columns.values)
        print(old_games.shape)

        existing_games_ids = list(old_games['gameid'].unique())
        write_folder = 'added_data'
    else:
        existing_games_ids = []
        write_folder = 'data'

    player_data = pd.read_csv(f"{write_folder}\\player_data.csv", low_memory=False)
    print(player_data['position'].unique())

    roles = list(player_data['position'].unique())

    all_games = pd.read_csv(f"{write_folder}\\all_games.csv", low_memory=False)
    all_games = all_games[~all_games['gameid'].isin(existing_games_ids)]
    print("all games shape:", all_games.shape)

    # p = all_games[(all_games['league'] == 'CBLOL') & (all_games['patch'] == 13.01)]
    # for idx, row in p.iterrows():
    #     print(row['blue_teamname'], row['red_teamname'], row['date'])
    # print(p.shape)
    # input()

    # for role in roles:
    #     all_games['blue_' + role] = ""
    #
    # for role in roles:
    #     all_games['red_' + role] = ""

    # golddiffat10,xpdiffat10,csdiffat10, golddiffat15,xpdiffat15,csdiffat15,kills,deaths,assists

    for idx, game in all_games.iterrows():

        if idx % 5000 == 0:
            print("processed:", idx)

        players = player_data[player_data['gameid'].values == game['gameid']]

        for role in roles:

            blue_player = players[(players['position'].values == role) & (players['side'].values == 'Blue')]
            red_player = players[(players['position'].values == role) & (players['side'].values == 'Red')]

            if blue_player.shape[0] == 1:
                all_games.at[idx, 'blue_' + role] = blue_player['playername'].item()

                all_games.at[idx, 'blue_' + role + '_golddiffat10'] = blue_player['golddiffat10'].item()
                all_games.at[idx, 'blue_' + role + '_xpdiffat10'] = blue_player['xpdiffat10'].item()
                all_games.at[idx, 'blue_' + role + '_csdiffat10'] = blue_player['csdiffat10'].item()
                all_games.at[idx, 'blue_' + role + '_golddiffat15'] = blue_player['golddiffat15'].item()
                all_games.at[idx, 'blue_' + role + '_xpdiffat15'] = blue_player['xpdiffat15'].item()
                all_games.at[idx, 'blue_' + role + '_csdiffat15'] = blue_player['csdiffat15'].item()

                all_games.at[idx, 'blue_' + role + '_goldat10'] = blue_player['goldat10'].item()
                all_games.at[idx, 'blue_' + role + '_xpat10'] = blue_player['xpat10'].item()
                all_games.at[idx, 'blue_' + role + '_csat10'] = blue_player['csat10'].item()
                all_games.at[idx, 'blue_' + role + '_goldat15'] = blue_player['goldat15'].item()
                all_games.at[idx, 'blue_' + role + '_xpat15'] = blue_player['xpat15'].item()
                all_games.at[idx, 'blue_' + role + '_csat15'] = blue_player['csat15'].item()

                all_games.at[idx, 'blue_' + role + '_kills'] = blue_player['kills'].item()
                all_games.at[idx, 'blue_' + role + '_deaths'] = blue_player['deaths'].item()
                all_games.at[idx, 'blue_' + role + '_assists'] = blue_player['assists'].item()
            else:
                print("ANOMALY Blue", idx, game['gameid'], blue_player)

            if red_player.shape[0] == 1:
                all_games.at[idx, 'red_' + role] = red_player['playername'].item()

                all_games.at[idx, 'red_' + role + '_golddiffat10'] = red_player['golddiffat10'].item()
                all_games.at[idx, 'red_' + role + '_xpdiffat10'] = red_player['xpdiffat10'].item()
                all_games.at[idx, 'red_' + role + '_csdiffat10'] = red_player['csdiffat10'].item()
                all_games.at[idx, 'red_' + role + '_golddiffat15'] = red_player['golddiffat15'].item()
                all_games.at[idx, 'red_' + role + '_xpdiffat15'] = red_player['xpdiffat15'].item()
                all_games.at[idx, 'red_' + role + '_csdiffat15'] = red_player['csdiffat15'].item()

                all_games.at[idx, 'red_' + role + '_goldat10'] = red_player['goldat10'].item()
                all_games.at[idx, 'red_' + role + '_xpat10'] = red_player['xpat10'].item()
                all_games.at[idx, 'red_' + role + '_csat10'] = red_player['csat10'].item()
                all_games.at[idx, 'red_' + role + '_goldat15'] = red_player['goldat15'].item()
                all_games.at[idx, 'red_' + role + '_xpat15'] = red_player['xpat15'].item()
                all_games.at[idx, 'red_' + role + '_csat15'] = red_player['csat15'].item()

                all_games.at[idx, 'red_' + role + '_kills'] = red_player['kills'].item()
                all_games.at[idx, 'red_' + role + '_deaths'] = red_player['deaths'].item()
                all_games.at[idx, 'red_' + role + '_assists'] = red_player['assists'].item()
            else:
                print("ANOMALY", idx, game['gameid'], red_player)

    if just_add:
        all_games = pd.concat([old_games, all_games], axis=0, ignore_index=True)
        all_games = all_games.drop_duplicates(['gameid'])
        all_games.reset_index(inplace=True)
        all_games = all_games.drop(['index', 'Unnamed: 0'], axis=1)
        print("final shape all_games:", all_games.shape)
        all_games.to_csv(f"added_data\\games_with_players.csv")

    else:
        all_games.to_csv(f"{write_folder}\\games_with_players.csv")

    print("games_with_players Done!")
