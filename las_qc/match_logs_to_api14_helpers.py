import pandas as pd
import numpy as np
import os
import time
import re


def extract_stop_depths_from_las(las_directory):
    stop_depth_dict = {}
    all_files = os.listdir(las_directory)
    for i, las_file in enumerate(all_files):
        if i == 0:
            start = time.time()
        stop_depth = -9999

        cnt = 0
        data = []
        with open(os.path.join(las_directory, las_file), 'r') as file:
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
            print(
                f'Extracted stop depth from {i}/{len(all_files)} LAS files   ---   Estimated time remaining: : {time_remaining_string}')

    df_las = pd.DataFrame(columns=['API10', 'Stop_Depth'], data=stop_depth_dict.items())
    df_las['Stop_Depth'] = pd.to_numeric(df_las['Stop_Depth'])

    df_las = df_las.dropna()

    return df_las
