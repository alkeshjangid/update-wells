import pandas as pd
import numpy as np
import re
import os
import pyperclip


def find_project_basin(list_of_valid_basins):
    cur_play = input('Please enter the name of the basin you would like to gather data for\n').upper()
    while True:
            if cur_play in list_of_valid_basins:
                break
            else:
                cur_play = input('Invalid basin name was entered. Please enter one of the following items:\n {}\n'
                                 .format(list_of_valid_basins)).upper()

    return cur_play


def survey_to_set(file_path):
    # wellId is an API
    cNames = ['Well_ID', 'DX-ft', 'DY-ft', 'TVD-ft', 'MD-ft', 'Azimuth-TrueNorth-Deg', 'Azimuth-GridNorth-Deg',
              'Inclination_Deg']
    df = pd.read_csv(file_path, skiprows=4, names=cNames)
    projAPIs = set(df['Well_ID'])
    fin = set()
    for i in projAPIs:
        fin.add(str(i))
    return fin  # , df


def query_to_set(conn, query, play):
    query = query.format(play)
    cursor = conn.cursor()
    cursor.execute(query)
    ds9set = set()
    for row in cursor.fetchall():
        ds9set.add(row[0])
    return ds9set


def query_to_dict(conn, query, play):
    query = query.format(play)
    cursor = conn.cursor()
    cursor.execute(query)
    ds9dict = {}
    for row in cursor.fetchall():
        ds9dict[row[0]] = row[1]
    return ds9dict


def get_difference(ds9set, projectset):
    fin = ds9set.difference(projectset)
    # fin = projectset.difference(ds9set)
    fin_str = set()
    for i in fin:
        fin_str.add(str(i))
    return fin_str


def remove_duplicate_well_numbers(row):
    if row['WellName'] and row['WellNumber']:
        name_end = re.search(r'\d+[a-zA-Z]*?\s*?$', row['WellName'])
        number = row['WellNumber']
        if name_end:
            if name_end[0].strip() == number:
                row['WellNumber'] = np.nan
    return row


def wrap_column_values_for_xlsx(df):

    columns = list(df.columns)
    for c in columns:
        df['{}'.format(c)] = df['{}'.format(c)].apply(lambda x: str(x) + " ")

    return df


def remove_errors(df_errors, df_qc, apiLevel):

    err_to_remove_here = ['Inclination is greater than 130',
                          'Inclination skips 45 deg or more',
                          'Azimuth delta is at least 30',
                          'MD skips 1000 ft or more']
    df_err_to_remove = df_errors[df_errors['Error'].isin(err_to_remove_here)]

    if apiLevel == 10:
        print(f'Removing {df_err_to_remove.API10.nunique()} APIs with bad surveys...'
              '\nThese are logged to a file named "Survey errors"')
    elif apiLevel == 12:
        print(f'Removing {df_err_to_remove.API12.nunique()} APIs with bad surveys...'
              '\nThese are logged to a file named "Survey errors"')
    elif apiLevel == 14:
        print(f'Removing {df_err_to_remove.API14.nunique()} APIs with bad surveys...'
              '\nThese are logged to a file named "Survey errors"')
    df_qc = df_qc[~df_qc['API{}'.format(apiLevel)].isin(df_err_to_remove['API{}'.format(apiLevel)])]
    # make unique path name

    return df_qc


def is_outlier(points, thresh=3.5):
    """
    Credit:
    https://stackoverflow.com/questions/22354094/pythonic-way-of-detecting-outliers-in-one-dimensional-observation-data

    Returns a boolean array with True if points are outliers and False
    otherwise.

    Parameters:
    -----------
        points : An numobservations by numdimensions array of observations
        thresh : The modified z-score to use as a threshold. Observations with
            a modified z-score (based on the median absolute deviation) greater
            than this value will be classified as outliers.

    Returns:
    --------
        mask : A numobservations-length boolean array.

    References:
    ----------
        Boris Iglewicz and David Hoaglin (1993), "Volume 16: How to Detect and
        Handle Outliers", The ASQC Basic References in Quality Control:
        Statistical Techniques, Edward F. Mykytka, Ph.D., Editor.
    """
    if len(points.shape) == 1:
        points = points[:, None]
    median = np.nanmedian(points, axis=0)
    diff = np.sum((points - median) ** 2, axis=-1)
    diff = np.sqrt(diff)
    med_abs_deviation = np.nanmedian(diff)

    modified_z_score = 0.6745 * diff / med_abs_deviation

    return modified_z_score > thresh


def haversine_distance(lat1, lon1, lat2, lon2):
    '''
    Calculates the haversine distance between two points.
    https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula

    Returns the distance in feet as a float.
    '''

    from math import cos, asin, sqrt

    p = 0.017453292519943295  # Pi/180
    a = 0.5 - cos((lat2 - lat1) * p) / 2 + cos(lat1 * p) * cos(lat2 * p) * (1 - cos((lon2 - lon1) * p)) / 2

    return (12756.2 * asin(sqrt(a))) * 3280.84  # 2*R*asin(sqrt(a)), convert to feet


def format_df_columns(df):
    df = df.rename(columns={'api10': 'API10', 'api12': 'API12', 'wellid': 'API14', 'kb_elevation_(ft)': 'proj_kb'})
    # df = df.astype({'API10': 'object', 'API12': 'object', 'API14': 'object'})
    return df


def update_data_type(df, column, out_dtype):
    in_dtype = df[column].dtypes
    if in_dtype != out_dtype:
        df = df.astype({column: out_dtype})

    return df


def filter_apis(filter_q, file_path, ds9apis):
    active = True
    while active:
        if filter_q.lower() == 'n':
            apis = ds9apis
            break
        elif filter_q.lower() == 'y':
            projapis = survey_to_set(file_path)
            apis = get_difference(ds9apis, projapis)
            break

        else:
            filter_q = input('You must input a valid option[Y/N].\n')
            continue
    return apis


def get_api_list_from_folder(folder_path):
    apis = set()
    for dirName, subdirList, fileList in os.walk(folder_path):
        for fName in fileList:
            apis.add(fName[:10])

    return apis


def get_api_level_from_list(api_list):
    level = len(api_list[0])

    return level


def get_columns_to_drop(df, keep_list):
    to_drop = [x for x in list(df.columns) if x not in keep_list]

    return to_drop


def multi_input(prompt):
    text = ""
    stopword = ""
    print('{}'.format(prompt))
    while True:
        line = input()
        if line.strip() == stopword:
            break
        text += "%s\n" % line
    u_list = text.split("\n")
    return u_list

