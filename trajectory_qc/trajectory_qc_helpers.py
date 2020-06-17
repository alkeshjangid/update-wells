import pandas as pd
import os
import numpy as np
from pandas import ExcelWriter


pd.options.mode.chained_assignment = None


def give_info_statements(max_inc):
    print('\nQC\'ing surveys... (Expect up to 20 seconds per 100,000 rows)\n')
    print('Criteria:\n'
          'Horizontal survey: First 5 rows inclination < ' + str(max_inc) + ', last 10 rows inclination >= 70\n'
          'Horizontal section: First 5 rows inclination > ' + str(
           max_inc) + ', last 10 rows inclination >= 70\n'
          'Vertical section/survey: First 5 rows inclination > ' + str(
           max_inc) + ', last 10 rows inclination < 70\n'
          'Pilot: Vertical survey tied to the same api as a horizontal wellbore\n'
          '"Problems with MD/TVD values": Either MD < TVD or the MD was not increasing')
    return


def reorderCols(xdf):
    cols = xdf.columns.tolist()
    cols[0], cols[1] = cols[1], cols[0]
    return xdf[cols]

# apiLevel = 12


def save_xls(list_dfs, xls_path):
    writer = ExcelWriter(xls_path)
    for df in list_dfs:
        df.to_excel(writer, sheet_name=df.name, index=False)
    writer.save()

    return


def remove_multiple_apis_per_trajectory(df, apiLevel, df_dict):

    if apiLevel == 10:
        mult_api = df.reset_index().groupby('TrajectoryID')['API{}'.format(apiLevel)].nunique().reset_index()
        mult_api = mult_api[mult_api.API10 > 1]
    elif apiLevel == 12:
        mult_api = df.reset_index().groupby('TrajectoryID')['API{}'.format(apiLevel)].nunique().reset_index()
        mult_api = mult_api[mult_api.API12 > 1]
    # if the apiLevel is 14 then check for duplicates at a 12 digit level still
    elif apiLevel == 14:
        mult_api = df.reset_index().groupby('TrajectoryID')['API{}'.format(12)].nunique().reset_index()
        mult_api = mult_api[mult_api.API12 > 1]
    else:
        mult_api = 'APILevel parameter is invalid.'
        print(mult_api)
    if len(mult_api > 0):
        print('\nWarning: Dropped surveys assigned to multiple APIs:\n')
        mislabeled = df[df.TrajectoryID.isin(mult_api.TrajectoryID)]
        print(mislabeled.groupby(['TrajectoryID', 'API{}'.format(apiLevel)]).size())
    df = df[~df.TrajectoryID.isin(mult_api.TrajectoryID)]

    df_dict['df'] = df

    return df_dict


def add_avg_max_columns(df_dict, apiLevel):
    # build dfs of avg inc (top 5 rows, bottom 10 rows), avg tvdss (bottom 10 rows), max md, for each survey
    # return merged dfs, joined back to input
    # input is df from remove_multiple_apis_per_trajectory

    df = df_dict['df']

    # dataframe with top 5 rows for each TrajectoryID
    df_head_5 = df.groupby('TrajectoryID').head(5)

    # mean inclination for first 10 rows of each survey
    inc_head_avg = df_head_5.groupby(
        ['API{}'.format(apiLevel), 'TrajectoryID']).Inclination.mean()

    # max MD of each survey
    md_max = df.groupby(['API{}'.format(apiLevel), 'TrajectoryID']).MeasuredDepth.max()

    # dataframe with last 10 rows for each TrajectoryID
    df_tail_10 = df.groupby('TrajectoryID').tail(10)

    # mean inclination for last 10 rows of each survey
    inc_tail_avg = df_tail_10.groupby(
        ['API{}'.format(apiLevel), 'TrajectoryID']).Inclination.mean()

    # average TVDSS for last 10 rows of each survey
    tvd_tail_avg = df_tail_10.groupby(
        ['API{}'.format(apiLevel), 'TrajectoryID']).TrueVerticalDepth.mean()

    # convert from series to df's

    # create a column of the TrajectoryID (was an index), leaves API+apiLevel as the index. series to df.
    tvd_tail_avg = tvd_tail_avg.reset_index('TrajectoryID')
    tvd_tail_avg = tvd_tail_avg.rename(columns={'TrueVerticalDepth': 'Avg_TVD_last_10'})

    inc_head_avg = inc_head_avg.reset_index('TrajectoryID')
    inc_head_avg = inc_head_avg.rename(columns={'Inclination': 'Avg_inc_first_5'})

    inc_tail_avg = inc_tail_avg.reset_index('TrajectoryID')
    inc_tail_avg = inc_tail_avg.rename(columns={'Inclination': 'Avg_inc_last_10'})

    md_max = md_max.reset_index('TrajectoryID')
    md_max = md_max.rename(columns={'MeasuredDepth': 'Max_MD'})

    # merge all these attributes into one df
    # TODO: create list of merge attributes ,place this in loop
    attrs = [tvd_tail_avg, inc_head_avg, inc_tail_avg, md_max]

    df2 = df.merge(tvd_tail_avg, how='outer', on='TrajectoryID')
    df2 = df2.merge(inc_head_avg, how='outer', on='TrajectoryID')
    df2 = df2.merge(inc_tail_avg, how='outer', on='TrajectoryID')
    df2 = df2.merge(md_max, how='outer', on='TrajectoryID')

    df_dict['df2'] = df2

    return df_dict


def add_survey_counts_column(df_dict, apiLevel, max_inc):
    # input df is df2 from add_avg_max_columns

    df = df_dict['df2']

    result = df.reset_index().groupby('API{}'.format(apiLevel))['TrajectoryID'].nunique().reset_index()
    result = result.rename(columns={'TrajectoryID': 'Num_Surveys'})
    mqc2 = pd.merge(df, result, how='inner', on='API{}'.format(apiLevel))

    full_horiz = mqc2[(mqc2.Avg_inc_first_5 < max_inc) & (mqc2.Avg_inc_last_10 >= 70)]
    result = full_horiz.reset_index().groupby('API{}'.format(apiLevel))['TrajectoryID'].nunique().reset_index()
    result = result.rename(columns={'TrajectoryID': 'Num_HZ_Surveys'})
    count_hz = pd.merge(mqc2, result, how='left', on='API{}'.format(apiLevel)).fillna(0)

    df_dict['count_hz'] = count_hz

    return df_dict


def find_length_of_horizontal_section(df):

    hz_section = df[df['Inclination'] > 80].copy()
    hz_section = hz_section.drop_duplicates(subset='TrajectoryID', keep='first')
    hz_section['HZ_length'] = hz_section['Max_MD'] - hz_section['MeasuredDepth']
    df = df.merge(hz_section[['TrajectoryID', 'HZ_length']], how='left', on='TrajectoryID')

    return df


def compare_md_and_tvd(df_dict):
    """input count_hz (output of add_survey_counts_column)
    This function check if MD is greater than TVD. If false, drop the survey"""

    df = df_dict['count_hz']

    count_hz = df.set_index('TrajectoryID')
    print(count_hz[count_hz.MeasuredDepth < count_hz.TrueVerticalDepth].index)
    bad_rows_mdtvd = count_hz[count_hz.MeasuredDepth < count_hz.TrueVerticalDepth]
    mdtvd = count_hz.drop(bad_rows_mdtvd.index)  # , axis=1)

    # reset the index from TrajectoryID back to 0, 1, 2, 3... etc.
    count_hz, mdtvd = count_hz.reset_index(), mdtvd.reset_index()

    # keep only the rows from multiple_qc that are NOT in mdtvd (df of whole dropped surveys)
    mdtvd_bad = count_hz[~count_hz.TrajectoryID.isin(mdtvd.TrajectoryID)]

    df_dict['count_hz'], df_dict['mdtvd'] = count_hz, mdtvd

    return df_dict  # , mdtvd_bad, bad_rows_mdtvd


def check_for_increasing_md(df_dict):
    """input: mdtvd
    info dict is used for later documentation of counts and tracking dropped rows"""
    # verify MD is always increasing. if not, check if db exists from md>tvd and add to it or create that db

    df = df_dict['mdtvd']

    does_md_increase = df.groupby('TrajectoryID').apply(lambda x: x.MeasuredDepth.is_monotonic)
    # is_monotonic checks if all the values are increasing for each trajectoryID
    mdtvd = df.set_index('TrajectoryID')
    md_good = mdtvd.loc[does_md_increase[does_md_increase == True].index.tolist()].reset_index().copy()
    md_bad = mdtvd.loc[does_md_increase[does_md_increase == False].index.tolist()].reset_index()
    mdtvd = md_good.reset_index()  # changed from mdtvd to md_good
    df_dict['mdtvd'], df_dict['md_good'] = mdtvd, md_good

    return df_dict


def filter_for_horizontals(max_inc, df_dict):
    # Keep full horizontal wellbores if there's only one TrajectoryID for the API
    """input: md_good (output of check_for_increasing_md)"""

    df = df_dict['md_good']

    multiple_qc = df[(df.Num_HZ_Surveys == 1)
                     & (df.Avg_inc_first_5 < max_inc)
                     & (df.Avg_inc_last_10 >= 70)]
    df_dict['multiple_qc'] = multiple_qc

    return df_dict

# no additional processing on multiple_qc, the next time it is used is to generate df1


def build_subsets_for_reference(apiLevel, max_inc, df_dict):
    # Keep good deviated/near-straight wellbores if there's only one TrajectoryID for the API
    # CAUTION: COULD BE MISSING HORIZONTAL SECTION

    md_good = df_dict['md_good']
    count_hz = df_dict['count_hz']

    one_dev = md_good[(md_good.Avg_inc_first_5 < max_inc)
                      & (md_good.Avg_inc_last_10 < 70)
                      & (md_good.Num_Surveys == 1)
                      & ((md_good.Avg_inc_first_5 > 0.1) | (md_good.Avg_inc_last_10 > 0.1))]
    df_dict['one_dev'] = one_dev

    # sometimes theres a full survey for a pilot hole and a full survey for a directional
    pilot_w_horiz = count_hz[(count_hz.Num_HZ_Surveys >= 1)
                             & (count_hz.Num_HZ_Surveys < count_hz.Num_Surveys)]
    df_dict['pilot_w_horiz'] = pilot_w_horiz

    horiz_w_pilot = pilot_w_horiz[(pilot_w_horiz.Avg_inc_first_5 < max_inc)
                                  & (pilot_w_horiz.Avg_inc_last_10 >= 70)]
    df_dict['horiz_w_pilot'] = horiz_w_pilot

    pilot_of_horiz = pilot_w_horiz[(pilot_w_horiz.Avg_inc_first_5 < max_inc)
                                   & (pilot_w_horiz.Avg_inc_last_10 < 70)]
    df_dict['pilot_of_horiz'] = pilot_of_horiz

    # organize
    mult_full_horiz = count_hz[(count_hz.Num_HZ_Surveys > 1)
                               & (count_hz.Avg_inc_first_5 < max_inc)
                               & (count_hz.Avg_inc_last_10 >= 70)]

    mult_full_horiz = mult_full_horiz.reset_index(drop=True)  # reset to 0, 1, 2...
    df_dict['mult_full_horiz'] = mult_full_horiz

    # Keep the longer TD as the "accepted survey",
    # but it is extremely that there are good surveys in the ones with shorter TDs
    max_md_horiz = mult_full_horiz.iloc[mult_full_horiz.groupby('API{}'.format(apiLevel))['Max_MD'].idxmax()]
    df_dict['max_md_horiz'] = max_md_horiz

    accepted_md_horiz = mult_full_horiz[mult_full_horiz['TrajectoryID'].isin(max_md_horiz.TrajectoryID)]
    df_dict['accepted_md_horiz'] = accepted_md_horiz

    # probably good surveys, shorter MD
    unused_md_horiz = mult_full_horiz[~mult_full_horiz['TrajectoryID'].isin(max_md_horiz.TrajectoryID)]
    df_dict['unused_md_horiz'] = unused_md_horiz

    # Organize potential split horizontal surveys (has to be Num_Surveys > 1 at all times)
    all_mult = md_good[md_good.Num_Surveys > 1]
    df_dict['all_mult'] = all_mult
    # if there's a vertical and horizontal section for the same API, these could be stitched together
    vert = all_mult[(all_mult.Avg_inc_first_5 < max_inc)
                    & (all_mult.Avg_inc_last_10 < 70)
                    & (all_mult.Num_HZ_Surveys == 0)]  # only keeping vertical sections
    horiz = all_mult[(all_mult.Avg_inc_first_5 > max_inc)
                     & (all_mult.Avg_inc_last_10 >= 70)
                     & (all_mult.Num_HZ_Surveys == 0)]  # only keeping horizontal sections

    # extract APIs from the horizontal-only trajectories, filter vertical-only trajectories with the same API
    if apiLevel == 10:
        vert_sep = vert[vert['API{}'.format(apiLevel)].isin(horiz.API10.unique())]
        horiz_sep = horiz[horiz['API{}'.format(apiLevel)].isin(vert.API10.unique())]
    elif apiLevel == 12:
        vert_sep = vert[vert['API{}'.format(apiLevel)].isin(horiz.API12.unique())]
        horiz_sep = horiz[horiz['API{}'.format(apiLevel)].isin(vert.API12.unique())]
    elif apiLevel == 14:
        vert_sep = vert[vert['API{}'.format(apiLevel)].isin(horiz.API14.unique())]
        horiz_sep = horiz[horiz['API{}'.format(apiLevel)].isin(vert.API14.unique())]
    # combine all surveys, sorting by API and then average inc to hopefully put the vertical survey on top
    split = pd.concat([vert_sep, horiz_sep]).sort_values(['API{}'.format(apiLevel), 'Avg_inc_first_5'])
    df_dict['split'] = split
    df_dict['vert'] = vert
    df_dict['horiz'] = horiz
    df_dict['vert_sep'] = vert_sep
    df_dict['horiz_sep'] = horiz_sep

    return df_dict


def get_subset_counts(df_dict, apiLevel):

    # reorder_li = [df_dict['multiple_qc'], df_dict['one_dev'], df_dict['split'], df_dict['accepted_md_horiz'],
    #               df_dict['unused_md_horiz'], df_dict['pilot_of_horiz'], df_dict['drop6']]
    #
    # for df in reorder_li:
    #     reorderCols(df)
    drop1 = df_dict['count_hz'][~df_dict['count_hz'].TrajectoryID.isin(df_dict['multiple_qc'].TrajectoryID)]
    drop2 = drop1[~drop1.TrajectoryID.isin(df_dict['one_dev'].TrajectoryID)]
    drop3 = drop2[~drop2.TrajectoryID.isin(df_dict['split'].TrajectoryID)]
    drop4 = drop3[~drop3.TrajectoryID.isin(df_dict['accepted_md_horiz'].TrajectoryID)]
    drop5 = drop4[~drop4.TrajectoryID.isin(df_dict['unused_md_horiz'].TrajectoryID)]
    drop6 = drop5[~drop5.TrajectoryID.isin(df_dict['pilot_of_horiz'].TrajectoryID)]
    df_dict['drop5'] = drop5
    df_dict['drop6'] = drop6
    multiple_qc = reorderCols(df_dict['multiple_qc'])
    df_dict['multiple_qc'] = multiple_qc
    one_dev = reorderCols(df_dict['one_dev'])
    df_dict['one_dev'] = one_dev
    split = reorderCols(df_dict['split'])
    df_dict['split'] = split
    accepted_md_horiz = reorderCols(df_dict['accepted_md_horiz'])
    df_dict['accepted_md_horiz'] = accepted_md_horiz
    unused_md_horiz = reorderCols(df_dict['unused_md_horiz'])
    df_dict['unused_md_horiz'] = unused_md_horiz
    pilot_of_horiz = reorderCols(df_dict['pilot_of_horiz'])
    df_dict['pilot_of_horiz'] = pilot_of_horiz
    drop6 = reorderCols(df_dict['drop6'])
    df_dict['drop6'] = drop6


    n_starting = len(df_dict['df'].TrajectoryID.value_counts())  # count number of starting surveys
    # count how many surveys there are now; get numbers of interest
    n_mdtvd = n_starting - len(df_dict['mdtvd'].TrajectoryID.value_counts())
    n_md_good = n_starting - len(df_dict['md_good'].TrajectoryID.value_counts())
    n_multiple_qc = len(df_dict['multiple_qc'].TrajectoryID.value_counts())
    n_horiz_w_pilot = len(df_dict['horiz_w_pilot'].TrajectoryID.value_counts())
    n_one_dev = len(one_dev.TrajectoryID.value_counts())

    if apiLevel == 10:
        n_split = len(split.API10.value_counts())
        n_all_mult_api = len(df_dict['all_mult'].API10.value_counts())
    elif apiLevel == 12:
        n_split = len(split.API12.value_counts())
        n_all_mult_api = len(df_dict['all_mult'].API12.value_counts())
    elif apiLevel == 14:
        n_split = len(split.API14.value_counts())
        n_all_mult_api = len(df_dict['all_mult'].API14.value_counts())

    n_accepted_md_horiz = len(accepted_md_horiz.TrajectoryID.value_counts())
    n_unused_md_horiz = len(unused_md_horiz.TrajectoryID.value_counts())
    n_pilot_of_horiz = len(pilot_of_horiz.TrajectoryID.value_counts())
    n_all_mult = len(df_dict['all_mult'].TrajectoryID.value_counts())
    n_vert = len(df_dict['vert'].TrajectoryID.value_counts())
    n_horiz = len(df_dict['horiz'].TrajectoryID.value_counts())
    n_split_comp = len(df_dict['horiz_sep'].TrajectoryID.value_counts()) \
        + len(df_dict['vert_sep'].TrajectoryID.value_counts())
    n_badsurveys = len(df_dict['drop5'].TrajectoryID.value_counts())

    print('\nSurvey HZ_survey_counts:\n' + str(n_starting) + ' total surveys examined\n'
          + str(n_multiple_qc) + ' APIs with one full horizontal wellbore survey (Excel: One Horizontal)\n'
          + str(n_horiz_w_pilot) + ' of those ' + str(n_multiple_qc) + ' horizontals had a pilot hole\n'
          + 'There were ' + str(n_pilot_of_horiz) + ' pilot hole surveys between those ' + str(
        n_horiz_w_pilot) + ' horizontals (Excel: Pilot)\n'
          + str(n_one_dev) + ' APIs with one vertical or directional wellbore survey (Excel: One Directional)\n\n'
          + 'There were ' + str(n_all_mult_api) + ' APIs with multiple surveys (which shared ' + str(
        n_all_mult) + ' total surveys)\n'
          + 'Of those ' + str(n_all_mult) + ' multiple surveys...\n'
          + '  ' + str(n_vert) + ' were vertical survey sections and ' + str(
        n_horiz) + ' were horizontal survey sections,\n'
          + '  ' + str(n_split) + ' APIs had ' + str(
        n_split_comp) + ' vertical and horizontal sections of (likely) full surveys (Excel: Split Surveys)\n'
          + '  ' + str(
        n_accepted_md_horiz) + ' horizontal surveys had the longest MD per API (Excel: Mult Horizontals - Longest MD)\n'
          + '  ' + str(
        n_unused_md_horiz) + ' horizontal surveys had shorter MDs per API (Excel: Mult Horizontals - Short MDs)\n\n'
          + str(n_mdtvd + n_md_good) + ' surveys had problems with MD/TVD values\n'
          + str(n_badsurveys) + ' surveys did not fit any category (Excel: No fit)\n\n')

    return df_dict


def write_subsets_to_file(df_dict):
    multiple_qc, one_dev, split, accepted_md_horiz, \
    unused_md_horiz, pilot_of_horiz, drop6 = df_dict['multiple_qc'], df_dict['one_dev'], df_dict['split'], \
                                             df_dict['accepted_md_horiz'], df_dict['unused_md_horiz'], \
                                             df_dict['pilot_of_horiz'], df_dict['drop6']

    list_dfs = [multiple_qc, one_dev, split, accepted_md_horiz, unused_md_horiz, pilot_of_horiz,
                drop6]  # dfs we want to output to excel
    multiple_qc.name = 'One Horizontal'
    one_dev.name = 'One Directional'
    split.name = 'Split Surveys'
    accepted_md_horiz.name = 'Mult Horizontals - Longest MD'
    unused_md_horiz.name = 'Mult Horizontals - Short MDs'
    pilot_of_horiz.name = 'Pilot'
    drop6.name = 'No fit'

    # log & remove bad surveys

    choice = int(input('''Would you like to output the data subsets? 
    This is an excel file that may take a few minutes to write.
    1) Yes
    2) No
    Enter choice here: '''))

    if choice == 1:
        # csv if max df length is > 1,000,000, else output to excel
        if len(max(list_dfs, key=len)) < 1000000:  # excel is the preferred output, but can only handle 1 million rows

            # make unique path name
            number = 1
            while True:
                newFile = r'output/trajectory_data/QCd_Surveys' + '_' + str(number) + '.xlsx'
                if not os.path.exists(newFile):
                    break
                number = number + 1

            print('\nNow writing an excel file (' + newFile + ') to the Python CWD...')
            save_xls(list_dfs, newFile)

            print('\nDone.')
        else:
            print('\nNow writing multiple .csv files to the Python CWD...')
            for i in range(len(list_dfs)):
                list_dfs[i].to_csv(r'output/trajectory_data/' +list_dfs[i].name + '.csv', index=False)
            print('\nDone.')
    else:
        print('\nDone.')
    return


def concat_dfs_for_output(df_dict):

    csvconcat = pd.concat([df_dict['multiple_qc'], df_dict['accepted_md_horiz'],
                           df_dict['pilot_of_horiz'], df_dict['one_dev']])
    # get the difference between rows, restarting at each survey and excluding the first row
    print(csvconcat.columns)
    csvconcat = csvconcat.reset_index(drop=True)
    csvconcat['diff'] = csvconcat.groupby('TrajectoryID')['MeasuredDepth'].diff()

    csvconcat['diff_shift'] = csvconcat['diff'].shift()
    csvconcat.loc[csvconcat['diff_shift'].isnull(), 'diff'] = np.nan
    del csvconcat['diff_shift']

    # remove 0s from Completion_TD
    csvconcat['Completion_TD'] = csvconcat['Completion_TD'].replace(0, np.nan)

    return csvconcat

