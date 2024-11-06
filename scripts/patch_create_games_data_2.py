import pandas as pd


def create_games(just_add):
    if just_add:
        write_folder = 'added_data'
    else:
        write_folder = 'data'

    useful_columns = ['gameid', 'league', 'playoffs', 'game', 'patch', 'teamname', 'side', 'date', 'gamelength',
                      'result', 'teamkills', 'teamdeaths', 'firstblood', 'firstdragon', 'firstherald', 'firstbaron',
                      'firsttower', 'dragons', 'opp_dragons', 'heralds', 'opp_heralds', 'towers', 'opp_towers',
                      'inhibitors', 'opp_inhibitors', 'barons', 'opp_barons', 'totalgold', 'golddiffat10',
                      'xpdiffat10', 'golddiffat15', 'xpdiffat15',
                      'goldat10',
                      'xpat10', 'goldat15', 'xpat15',
                      ]

    common_columns = ['gameid', 'league', 'playoffs', 'game', 'patch', 'date', 'gamelength', 'result']

    team_columns = [x for x in useful_columns if x not in common_columns]
    print("team columns:", team_columns)

    final_columns = common_columns + \
                    ['blue_' + x for x in useful_columns if x not in common_columns] + \
                    ['red_' + x for x in useful_columns if x not in common_columns]

    df = pd.read_csv(f"{write_folder}\\team_data.csv", low_memory=False)
    df = df.fillna(0)

    if just_add:
        old_games = pd.read_csv(f"../data/all_games.csv", low_memory=False)
        old_games = old_games.drop(['Unnamed: 0'], axis=1)
        old_games_ids = list(old_games['gameid'].unique())

        #print(list(old_games.columns.values))

        gameids = list(df['gameid'].unique())
        gameids = [x for x in gameids if x not in old_games_ids]
    else:
        gameids = list(df['gameid'].unique())

    print(df['side'].unique())
    print(len(gameids))

    combined_data = []

    total_df = pd.DataFrame(columns=final_columns)
    counter = 0

    for c, id in enumerate(gameids):
        if c % 5000 == 0:
            print("processed:", c, "gathered:", counter)

        game_stats = df[df['gameid'].values == id]
        blue_stats = game_stats[game_stats['side'].values == 'Blue']
        red_stats = game_stats[game_stats['side'].values == 'Red']

        if blue_stats.shape[0] != 1:
            print("many games with id", id)
            print(blue_stats[['date', 'teamname', 'gameid']])

            continue

        if red_stats.shape[0] != 1:
            print("many games with id", id)
            print(red_stats[['date', 'teamname', 'gameid']])

            continue

        for com_col in common_columns:
            total_df.at[counter, com_col] = blue_stats[com_col].item()

        # total_df.at[counter, 'blue_teamname'] = blue_stats['teamname'].item()
        # total_df.at[counter, 'red_teamname'] = red_stats['teamname'].item()

        for team_col in team_columns:
            total_df.at[counter, 'blue_' + team_col] = blue_stats[team_col].item()
            total_df.at[counter, 'red_' + team_col] = red_stats[team_col].item()

        counter += 1

    if just_add:
        total_df = total_df[list(old_games.columns.values)]
        all_games = pd.concat([old_games, total_df], axis=0)
        all_games.reset_index(inplace=True, drop=True)
        all_games.to_csv(f"added_data\\all_games.csv")
        print("start shape:", old_games.shape[0], "final shape:", all_games.shape[0])

    else:
        total_df.to_csv(f"{write_folder}\\all_games.csv")




