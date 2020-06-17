from trajectory_qc.trajectory_qc_helpers import *
import logging


def initial_qc(df, max_inc, apiLevel, df_dict={}):
    give_info_statements(max_inc)

    df_dict = remove_multiple_apis_per_trajectory(df, apiLevel, df_dict)
    df_dict = add_avg_max_columns(df_dict, apiLevel)
    df_dict = add_avg_max_columns(df_dict, apiLevel)
    df_dict = add_survey_counts_column(df_dict, apiLevel, max_inc)
    df_dict = compare_md_and_tvd(df_dict)
    df_dict = check_for_increasing_md(df_dict)
    df_dict = filter_for_horizontals(max_inc, df_dict)
    df_dict = build_subsets_for_reference(apiLevel, max_inc, df_dict)
    df_dict = get_subset_counts(df_dict, apiLevel)
    write_subsets_to_file(df_dict)
    df_qc = concat_dfs_for_output(df_dict)
    df_qc = find_length_of_horizontal_section(df_qc)
    return df_qc


def additional_survey_qc(df_surveys, df_errors, hz, apiLevel):
    if apiLevel == 14:
        apiLevel = 12
    info_cols = ['API{}'.format(apiLevel), 'TrajectoryID', 'Error']

    # need to flag wells where the MD_diff is greater than a certain cutoff
    print('adding MD flag')
    apis_md_diff = df_surveys.loc[df_surveys['diff'] >= 1000, 'API{}'.format(apiLevel)].unique()
    skip_1k = df_surveys[df_surveys['API{}'.format(apiLevel)].isin(apis_md_diff)].copy()
    if len(skip_1k) > 0:
        print('adding flagged MD to errors df')
        skip_1k['Error'] = 'MD skips 1000 ft or more'
        err = skip_1k.loc[skip_1k['Num_HZ_Surveys'] >= hz, info_cols].drop_duplicates()
        logging.info(err.to_string(index=False))
        df_errors = df_errors.append(err)

    print('adding inclination diff flag')
    # Inclination diff > 45 degrees
    df_surveys['incl_diff'] = df_surveys.groupby('API{}'.format(apiLevel))['Inclination'].diff()
    df_surveys['incl_diff_shift'] = df_surveys['incl_diff'].shift()
    # took out the next line to include the surveys missing a heel AND vertical section
    # df_surveys.loc[df_surveys['incl_diff_shift'].isnull(), 'incl_diff'] = np.nan
    del df_surveys['incl_diff_shift']

    incl_change = df_surveys[df_surveys['incl_diff'].abs() >= 45]
    # for every API in that list, add a warning
    if len(incl_change) > 0:
        print('adding flagged inclinations to errors df >=45')
        incl_change['Error'] = 'Inclination skips 45 deg or more'
        err = incl_change.loc[incl_change['Num_HZ_Surveys'] >=hz, info_cols].drop_duplicates()
        logging.info(err.to_string(index=False))
        df_errors = df_errors.append(err)

    inclination_gt_130 = df_surveys[df_surveys['Inclination'] >= 130]
    if len(inclination_gt_130) > 0:
        print('adding flagged inclinations to errors df >=130')
        inclination_gt_130['Error'] = 'Inclination is greater than 130'
        err = inclination_gt_130.loc[inclination_gt_130['Num_HZ_Surveys'] >= hz, info_cols].drop_duplicates()
        logging.info(err.to_string(index=False))
        df_errors = df_errors.append(err)

    # only get the heel section or greater. shouldn't be changing quickly here
    # select where incl > 40, reverse the order, group by API,
    # take the cum product (True=1), reorder, slice
    print('adding flag for rapidly increasing inclination at the heel')
    high_incl = df_surveys[df_surveys.Inclination.gt(40)[::-1]
        .groupby(df_surveys['API{}'.format(apiLevel)])
        .cumprod()
        .reindex_like(df_surveys)]
    # next lines deal with the scenario where a north-trending well goes from 359 to 1 degree(s)
    # actual difference in that case is 2 degrees, not 358 degrees
    # subtract 360 if over 180, then take the minimum of the two diff columns
    if len(high_incl) > 0:
        high_incl['az_minus_360'] = high_incl['Azimuth'].apply(lambda x: x - 360 if x > 180 else x)
        high_incl['az_diff'] = high_incl.groupby('API{}'.format(apiLevel))['Azimuth'].diff()
        high_incl['az_diff_360'] = high_incl.groupby('API{}'.format(apiLevel))['az_minus_360'].diff()
        high_incl['min_az_diff'] = high_incl[['az_diff', 'az_diff_360']].abs().min(axis=1)

        # flag if azimuth changes faster than 30 deg
        az_diff = high_incl[np.abs(high_incl['min_az_diff']) >= 30]
        if len(az_diff) > 0:
            print('azimuth flags')
            az_diff['Error'] = 'Azimuth delta is at least 30'
            err = az_diff.loc[az_diff['Num_HZ_Surveys'] >= hz, info_cols].drop_duplicates()
            logging.info(err.to_string(index=False))
            df_errors = df_errors.append(err)
    else:
        print('No horizontal sections in existing surveys.')

    print('Count of additional survey errors: ', len(df_errors))

    return df_errors
