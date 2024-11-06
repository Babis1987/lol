import pandas as pd
import os


cols = ['gameid', 'datacompleteness', 'url', 'league', 'year', 'split', 'playoffs', 'date', 'game', 'patch',
        'participantid', 'side', 'position', 'playername', 'playerid', 'teamname', 'teamid', 'champion',
        'ban1', 'ban2', 'ban3', 'ban4', 'ban5',
        'gamelength', 'result', 'kills', 'deaths', 'assists', 'teamkills', 'teamdeaths', 'doublekills', 'triplekills',
        'quadrakills', 'pentakills', 'firstblood', 'firstbloodkill', 'firstbloodassist', 'firstbloodvictim', 'team kpm',
        'ckpm', 'firstdragon', 'dragons', 'opp_dragons', 'elementaldrakes', 'opp_elementaldrakes', 'infernals',
        'mountains', 'clouds', 'oceans', 'chemtechs', 'hextechs', 'dragons (type unknown)', 'elders', 'opp_elders',
        'firstherald', 'heralds', 'opp_heralds', 'firstbaron', 'barons', 'opp_barons', 'firsttower', 'towers',
        'opp_towers', 'firstmidtower', 'firsttothreetowers', 'turretplates', 'opp_turretplates', 'inhibitors',
        'opp_inhibitors', 'damagetochampions', 'dpm', 'damageshare', 'damagetakenperminute', 'damagemitigatedperminute',
        'wardsplaced', 'wpm', 'wardskilled', 'wcpm', 'controlwardsbought', 'visionscore', 'vspm', 'totalgold',
        'earnedgold', 'earned gpm', 'earnedgoldshare', 'goldspent', 'gspd', 'total cs', 'minionkills', 'monsterkills',
        'monsterkillsownjungle', 'monsterkillsenemyjungle', 'cspm', 'goldat10', 'xpat10', 'csat10', 'opp_goldat10',
        'opp_xpat10', 'opp_csat10', 'golddiffat10', 'xpdiffat10', 'csdiffat10', 'killsat10', 'assistsat10', 'deathsat10',
        'opp_killsat10', 'opp_assistsat10', 'opp_deathsat10', 'goldat15', 'xpat15', 'csat15', 'opp_goldat15', 'opp_xpat15',
        'opp_csat15', 'golddiffat15', 'xpdiffat15', 'csdiffat15', 'killsat15', 'assistsat15', 'deathsat15',
        'opp_killsat15', 'opp_assistsat15', 'opp_deathsat15']


def filter_cols(path):
    df = pd.read_csv(f"..\\pre_process_data\\raw_data/{path}", low_memory=False)

    df = df[(df['position'] == 'team') & (df['datacompleteness'] == 'complete')]
    df = df[df['teamname'].notna()]

    useful_columns = ['gameid', 'league', 'playoffs', 'game', 'patch', 'side', 'teamname', 'date', 'gamelength',
                      'result', 'teamkills', 'teamdeaths', 'firstblood',  'firstdragon', 'firstherald', 'firstbaron',
                      'firsttower', 'dragons', 'opp_dragons', 'heralds', 'opp_heralds', 'towers', 'opp_towers',
                      'inhibitors', 'opp_inhibitors', 'barons', 'opp_barons', 'totalgold', 'golddiffat10',
                      'xpdiffat10', 'golddiffat15', 'xpdiffat15',
                      'goldat10',
                      'xpat10', 'goldat15', 'xpat15'
                  ]

    df = df[useful_columns]

    return df


def gather_team_data():
    read_folder_name = '..\\pre_process_data\\raw_data\\'
    print(read_folder_name)

    paths = os.listdir(read_folder_name)
    print(paths)
    dfs = [filter_cols(p) for p in paths]

    final_df = pd.concat(dfs, axis=0)

    print(list(final_df.columns.values))
    print(final_df.shape)

    final_df.reset_index(inplace=True)
    final_df.to_csv(f"data\\team_data.csv")
    final_df.to_csv(f"added_data\\team_data.csv")
    print("team data done")
