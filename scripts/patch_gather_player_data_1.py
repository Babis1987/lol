import pandas as pd
import os.path


def filter_cols(path):
    df = pd.read_csv(f"../pre_process_data/raw_data/{path}", low_memory=False)
    df = df[df['position'] != 'team']

    return df


def gather_player_data():
    folder_name = '..\\pre_process_data\\raw_data\\'
    print(folder_name)

    paths = os.listdir(folder_name)
    print(paths)

    dfs = [filter_cols(p) for p in paths]

    final_df = pd.concat(dfs, axis=0)
    final_df = final_df[final_df['datacompleteness'] == 'complete']

    final_df.reset_index(inplace=True)

    file_name = f"../data/player_data.csv"
    final_df.to_csv(file_name)

    file_name = f"../added_data/player_data.csv"
    final_df.to_csv(file_name)
    print("done", file_name)
