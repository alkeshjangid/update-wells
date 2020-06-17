import pandas as pd
import numpy as np
import os
import re
import time
import shutil
import tkinter as tk
from tkinter import filedialog


def match_logs_to_trajectories(df_surveys, las_dir):

    pd.set_option('display.width', 175)
    pd.set_option('max_columns',20)
    pd.set_option('display.max_colwidth', 70)

    root = tk.Tk()
    root.withdraw()

    stop_depth_dict = {}
    all_files = os.listdir(las_dir)
    for i, las_file in enumerate(all_files):
        if i == 0:
            start = time.time()
        stop_depth = -9999

        cnt = 0
        data = []
        with open(os.path.join(las_dir, las_file), 'r') as file:
            # data = file.read()
            while True:
                line = file.readline()
                cnt += 1
                data.append(line)
                if cnt > 40:
                    break
        data = ' '.join(data)
        td_findall = re.findall(r'STOP.ft\s+(\d+)', data, re.I)
        if td_findall:
            stop_depth = td_findall[0]
        stop_depth_dict[las_file[:10]] = int(stop_depth)
        if i > 0 and i % 100 == 0:
            stop = time.time()
            seconds_to_complete = (stop - start) * ((len(all_files) - i) / i)
            if seconds_to_complete > 60:
                time_remaining_string = f'{seconds_to_complete / 60:.2f} minutes'
            else:
                time_remaining_string = f'{seconds_to_complete:.1f} seconds'
            print(f'Extracted stop depth from {i}/{len(all_files)} LAS files   ---   Estimated time remaining: : {time_remaining_string}')

    df_las = pd.DataFrame(columns=['API10', 'Stop_Depth'], data=stop_depth_dict.items())
    df_las['Stop_Depth'] = pd.to_numeric(df_las['Stop_Depth'])

    df_las = df_las.dropna()
    last_10 = df_surveys.groupby('API14').tail(10)
    last_10_inc = last_10.groupby('API14')['Inclination'].mean().reset_index()
    idx = df_surveys.groupby(['API14'])['MeasuredDepth'].transform(max) == df_surveys['MeasuredDepth']
    del df_surveys['Inclination']
    last_1 = df_surveys[idx]
    last_1 = last_1.drop_duplicates()

    last_10_inc = last_10_inc.merge(last_1, how='left', on=['API14'])
    last_10_inc['Vertical'] = last_10_inc['Inclination'].apply(lambda x: 1 if x < 70 else 0)
    last_10_inc['Horizontal'] = 1 - last_10_inc['Vertical']
    last_10_inc['API10'] = last_10_inc['API14'].str.slice(0, 10)
    vh_counts = last_10_inc.groupby('API10')[['Vertical', 'Horizontal']].sum().reset_index().rename(columns={'Vertical': 'Num_Vert', 'Horizontal': 'Num_Horiz'})

    # match survey df to las df
    mrg = df_las.merge(last_10_inc, how='left', on='API10')
    mrg = mrg.merge(vh_counts, how='left', on='API10')
    # anything without a survey is flagged (and given 0000)
    # anything with 1 survey is given that survey's 14 digit
    mrg['dist_diff'] = mrg['Stop_Depth'] - mrg['MeasuredDepth']
    mrg['flag'] = 0
    mrg.loc[mrg['Num_Vert'] + mrg['Num_Horiz'] == 1, 'flag'] = 1
    mrg.loc[mrg['MeasuredDepth'].isnull(), 'flag'] = 1
    # if it has a well spot, it didn't have a TD. Also likely is that we don't have the well spot in the project.

    # multiple 14 digit for LAS:
    # see if any match within 200'

    mult = mrg[mrg['Num_Vert'] + mrg['Num_Horiz'] > 1]
    mrg['multiple_API14'] = 0
    mrg.loc[mrg['Num_Vert'] + mrg['Num_Horiz'] > 1, 'multiple_API14'] = 1
    count_within_dist = mult[np.abs(mult['dist_diff']) < 200].groupby('API10')['API14'].count().reset_index().rename(
        columns={'API14': 'num_within_dist'})
    mult = mult.merge(count_within_dist, how='left', on='API10')
    # if only one within 200ft, assign to that
    mult.loc[(mult['num_within_dist'] == 1) & (np.abs(mult['dist_diff']) < 200), 'flag'] = 1
    # if there is only one vertical, assign to that
    mult2 = mult[mult['num_within_dist'] != 1].copy()
    mult2.loc[(mult2['Vertical'] == 1) & (mult2['Num_Vert'] == 1), 'flag'] = 1
    mult3 = mult2[~(mult2['Num_Vert'] == 1)]
    # if there are multiple verticals, assign to the lowest numbered vertical
    mult4 = mult3[mult3['Num_Vert'] > 1].sort_values('API14').groupby('API10').head(1)
    mult4['flag'] = 1
    mult5 = mult3[~mult3['API10'].isin(mult4['API10'].values)]
    # if there are multiple horizontals (and no verticals), assign to the lowest numbered horizontal
    mult5 = mult5[mult5['Num_Horiz'] > 1].sort_values('API14').groupby('API10').head(1)
    mult5['flag'] = 1

    # produce excel sheet showing what was assigned where
    # copy LAS with new name (open, lookup new 14 digit, write out)
    accepted_mult = pd.DataFrame()
    for temp_df in [mult, mult2, mult3, mult4, mult5]:
        accepted_mult = accepted_mult.append(temp_df[temp_df['flag'] == 1])
    if mult.API10.nunique() != len(accepted_mult):
        print('Did not assign all LAS. This wasnt supposed to happen. Holla atcha boy to fix this.')

    mrg.loc[mrg['API14'].isin(accepted_mult['API14']), 'flag'] = 1

    if mrg.flag.sum() != mrg.API10.nunique():
        print('Did not assign all LAS. This wasnt supposed to happen. Holla atcha boy to fix this.')

    # add fake api14 if no header data
    temp = mrg[mrg['API14'].isnull()].copy()
    temp['API14'] = temp['API10'] + '0000'
    mrg = mrg[~mrg['API14'].isnull()]
    mrg = mrg.append(temp)
    mrg['assigned_other_than_0000'] = 0
    flag1 = mrg[mrg['flag'] == 1]
    mrg.loc[mrg['API10'].isin(flag1[flag1['API14'].str.slice(-4) != '0000']['API10']), 'assigned_other_than_0000'] = 1
    #
    os.makedirs(os.path.join(os.getcwd(), 'output', 'las'), exist_ok=True)
    print('Copying LAS...')
    for orig_las_file in os.listdir(las_dir):
        if orig_las_file[-4:].lower() == '.las':
            orig_api = orig_las_file[:10]
            new_api = mrg.loc[mrg['API10'] == orig_api, 'API14'].values[0]
            # open the file, swap out its API number
            with open(os.path.join(las_dir, orig_las_file), 'r') as file:
                data = file.read()
            new_data = re.sub(r'(API.\s+)(\d+)', rf'\g<1>{new_api}', data)
            with open(os.path.join(os.getcwd(), 'output', 'las', f'{new_api}.las'), 'w') as file_write:
                file_write.write(new_data)
                file_write.close()

    # write out report of what happened
    print('Generating report...')
    writer = pd.ExcelWriter(os.path.join(os.getcwd(), 'output', 'las', f'API14 report.xlsx'), engine='xlsxwriter')
    mrg.to_excel(writer, index=False, freeze_panes=(1,0))
    worksheet = writer.sheets['Sheet1']

    # Iterate through each column and set the width == the max length in that column.
    # A padding length of 1 is also added.
    for i, col in enumerate(mrg.columns):
        # find length of column i
        column_len = mrg[col].astype(str).str.len().max()
        # Setting the length if the column header is larger
        # than the max column value length
        column_len = max(column_len, len(col)) + 1
        # set the column length
        worksheet.set_column(i, i, column_len)
    writer.save()

    print(f'Done. Check the folder: {os.path.join(os.getcwd(), "output", "las")}')

    return
