# local imports
from utils.functions import *
from utils.get_data import get_well_header_data, get_trajectories, ds9conn, ds9query, get_ds9_prodeng, \
    basins, state_query, wells_with_trajectories_query
from trajectory_qc.trajectory_qc import *
from well_header_qc.well_header_qc import add_header_flags, well_headers_auto_drop
import datetime
from las_qc.match_logs_to_api14_new import *

print(os.getcwd())


def get_files():
    """
    Get all CSV files for importing into a Transform PA
    :param play: BasinConformed name
    :return: CSV files for all required imports
    """

    try:
        active = True
        while active:
            process = input('\nEnter the process you would like to perform:\n'
                            '1) Gather and QC trajectories and well header data\n'
                            '2) Get production/engineering data\n'
                            '3) Find project trajectories that are missing from DS9.\n'
                            '4) Match logs to API14\n')

            # get well header data frame

            if str(process) == '1':
                p_trajectories = True
                while p_trajectories:
                    q_method = input('Choose what method of data pull you would like to perform: \n'
                                     '1) All wells for a given basin conformed \n'
                                     '2) Wells from a list of APIs \n'
                                     '3) Wells with LAS (takes folder of LAS files as input) \n')

                    # get apis to run processing on

                    if str(q_method) == '1':
                        play = find_project_basin(basins)
                        state_inq = input('Would you like to separate the basin by state[Y/N]?\n')
                        if state_inq.lower() == 'y':
                            states = query_to_dict(ds9conn, state_query, play)
                            state_input = input('Please enter the stateAPI(s) for the states you would '
                                                'like to gather for, separated by comma: \n {}'.format(states))
                            clause = 'AND APIstate IN ({})'.format(state_input)
                            headers_name = 'output/header_data/header_data_to_import_{basin}_{st}.xlsx'.format(
                                basin=play.lower(), st=state_input)
                            export_name = 'output/trajectory_data/surveys_for_import_{basin}_{st}.csv'.format(
                                basin=play.lower(), st=state_input)
                            errors_name = r'output/trajectory_data/survey_errors_{basin}_{st}.csv'.format(
                                basin=play.lower(), st=state_input)
                        elif state_inq.lower() == 'n':
                            clause = ''
                            headers_name = r'output/header_data/header_data_to_import_{basin}.xlsx'\
                                .format(basin=play.lower())
                            export_name = r'output/trajectory_data/surveys_for_import_{basin}.csv'\
                                .format(basin=play.lower())
                            errors_name = r'output/trajectory_data/survey_errors_{basin}.csv'.format(
                                basin=play.lower())
                        else:
                            print('You must input a valid option[Y/N].')
                            continue
                        export_name = export_name.replace(", ", "_")
                        apiLevel = input('What API level would you like to qc these trajectories on [10/12]?\n')
                        apiLevel = int(apiLevel)
                        ds9apis_with_trajectories = query_to_set(ds9conn, ds9query, play)
                        # get APIs for surveys
                        print('\nGetting APIs for surveys...')
                        # file_path = input('\nPlease copy/paste the file path for your deviated surveys.\n ')
                        file_path = filedialog.askopenfilename(title='Select your deviated surveys file')

                        filter_q = input('\nWould you like to remove the project APIs from the list of wells to gather '
                                         'surveys for? Not recommended if starting a new project. [Y/N]\n')
                        apis = filter_apis(filter_q, file_path, ds9apis_with_trajectories)
                        clause = 'AND API14 IN {apis}'.format(apis=tuple(apis)) + clause

                    elif str(q_method) == '2':
                        apis = multi_input('Paste the list of APIs you would like to gather data for.'
                                           ' Press ENTER when done.')
                        apiLevel = input('What API level would you like to qc these wells on [10/12]?\n')
                        apiLevel = int(apiLevel)
                        q_level = get_api_level_from_list(apis)
                        print('assuming all the APIs you entered are {} digits.'.format(q_level))
                        clause = 'AND w.API{q_level} IN {apis}'.format(q_level=q_level, apis=tuple(apis))
                        play = '%%'
                        basin_name = input('Please enter your project name (to be used for naming export files) \n')
                        headers_name = 'output/header_data/header_data_to_import_{basin}.xlsx'.format(
                            basin=basin_name.lower())
                        export_name = 'output/trajectory_data/surveys_for_import_{basin}.csv'.format(
                            basin=basin_name.lower())
                        errors_name = r'output/trajectory_data/survey_errors_{basin}.csv'.format(
                            basin=basin_name.lower())

                    elif str(q_method) == '3':
                        las_folder = filedialog.askdirectory(title='Select the folder of all your LAS')
                        apis = get_api_list_from_folder(las_folder)
                        apiLevel = input('What API level would you like to qc these wells on [10/12]?\n')
                        apiLevel = int(apiLevel)
                        clause = 'AND w.API10 IN {apis}'.format(apis=tuple(apis))
                        play = '%%'
                        basin_name = input('Please enter your project name (to be used for naming export files) \n')
                        headers_name = 'output/header_data/header_data_to_import_{basin}.xlsx'.format(
                            basin=basin_name.lower())
                        export_name = 'output/trajectory_data/surveys_for_import_{basin}.csv'.format(
                            basin=basin_name.lower())
                        errors_name = r'output/trajectory_data/survey_errors_{basin}.csv'.format(
                            basin=basin_name.lower())

                    else:
                        print('Invalid input. PLease enter 1, 2 or 3')
                        continue

                    print('Gathering trajectories for {} APIs'.format(len(apis)))
                    # get data frame of new trajectories for project
                    df_traj = get_trajectories(clause)

                    # QC the trajectories
                    print('\nBeginning the QC of the trajectory data...')
                    df_qc = initial_qc(df_traj, max_inc=5, apiLevel=apiLevel, df_dict={})
                    print('\nAdditional QC being applied...')
                    df_qc2 = additional_survey_qc(df_qc, pd.DataFrame(), 1, apiLevel)
                    if len(df_qc2) > 0:
                        df_fin = remove_errors(df_qc2, df_qc, apiLevel)
                        df_qc2.to_csv(errors_name)
                    else:
                        df_fin = df_qc
                    print('Writing final surveys that passed QC to current working directory...')

                    df_fin.to_csv(export_name)

                    print("Beginning DS9 well header query...")

                    height = input('\nWhat is the max height above ground you would like for your KBs? Everything over'
                                   ' this value will be flagged.\n')
                    df_headers = get_well_header_data(play, clause, height_above_ground=height)
                    header_cols = list(df_headers.columns)
                    df_headers = df_headers.apply(lambda row: remove_duplicate_well_numbers(row), axis=1)
                    print('Length of data after apply function:', len(df_headers.dropna(subset=['WellNumber'])))

                    # test variables
                    df_trajectories = df_fin
                    # proj_headers = input('\nPlease copy/paste the file path of your project headers.\n')
                    proj_headers = filedialog.askopenfilename(title='Select your project headers file')

                    df_proj_headers = pd.read_csv(proj_headers,
                                                  encoding='latin1',
                                                  dtype={'api10': 'str', 'api12': 'str', 'wellid': 'str'})
                    # las_inv = input('\nPlease copy/paste the file path of your project LAS inventory.\n')
                    las_inv = filedialog.askopenfilename(title='Select your LAS inventory file')

                    print('beginning well head qc...')
                    kb_cutoff = input('\nWhat difference between project KB and DS9 KB would you like to flag?\n')
                    td_cutoff = input('\nWhat difference between completion Td and survey Td would you like to flag?\n')
                    dist_flag = input('\nWhat max distance between project and DS9 well points would you like?\n')
                    df_headers = add_header_flags(df_headers, df_trajectories, df_proj_headers, las_inv,
                                                  kb_cutoff=int(kb_cutoff), td_cutoff=int(td_cutoff),
                                                  dist_flag=int(dist_flag), apiLevel=apiLevel, q_method=q_method)

                    drop = input('\n\nWould you like to automatically drop wells that are as missing '
                                 'all of the following from your well headers output:\n'
                                 '-DS9 trajectory \n'
                                 '-DS9 LAS \n'
                                 '-project LAS\n'
                                 '[Y/N]\n')
                    if str(drop).lower() == 'y':
                        df_headers = well_headers_auto_drop(df_headers)
                    elif str(drop).lower() == 'n':
                        df_headers = df_headers
                    else:
                        print('You must input a valid option[Y/N].')
                        continue

                    df_headers.to_excel(headers_name, index=False)
                    print('\nWriting header data to current working directory...')
                    while True:
                        again = input("\nWould you like to get trajectory data and well headers "
                                      "for a different basin[Y/N]?\n")
                        if again.lower() == 'y':
                            break
                        elif again.lower() == 'n':
                            p_trajectories = False
                            break
                        else:
                            print('You must input a valid option[Y/N].')
                            continue

            elif str(process) == '2':
                p_prod_eng = True
                while p_prod_eng:
                    play = find_project_basin(basins)
                    print('Beginning process...')
                    # get production and engineering data
                    df_prodeng = get_ds9_prodeng(play)
                    # df_prodeng = wrap_column_values_for_xlsx(df_prodeng)
                    print('Writing production and engineering data to current working directory...')
                    df_prodeng.to_excel('output/production_engineering_data/prodeng_{}.xlsx'.format(play.lower()),
                                        index=False)
                    while True:
                        again = input("Would you get production/engineering data for a different basin[Y/N]?\n")
                        if again.lower() == 'y':
                            break
                        elif again.lower() == 'n':
                            p_prod_eng = False
                            break
                        else:
                            print('You must input a valid option[Y/N].')
                            continue
            elif str(process) == '3':
                p_compare_apis = True
                while p_compare_apis:
                    # old_surveys = input('\nPlease copy/paste the file path of your deviated surveys that are '
                    #                     'currently in production.\n')
                    old_surveys = filedialog.askopenfilename(title='Select the file of your deviated surveys that are '
                                                                   'currently in production')

                    proj_name = input('\nPlease enter your project name (to be used for output file name).')
                    print('Beginning to query DS9...')
                    ds9_apis = query_to_set(ds9conn, wells_with_trajectories_query, play='')
                    survey_apis = survey_to_set(old_surveys)
                    survey_apis = {x[:12] for x in survey_apis}
                    print('Searching for missing surveys...')
                    diff = survey_apis.difference(ds9_apis)
                    diff= [x for x in diff]
                    df_to_remove = pd.DataFrame(diff, columns=['API12'])
                    d = r'output\trajectory_data'
                    f_name = r'\missing_trajectories_{project}.xlsx'.format(project=proj_name)
                    file = d + f_name
                    if os.path.isfile(os.getcwd()  + r'\\' + file):
                        f_name = r'\missing_trajectories_{project}_{time}.xlsx'.format(project=proj_name,
                                                                                       time=datetime.datetime.now().strftime('%H-%M'))
                    file = d + f_name
                    file = file.replace(r'\\', '/')
                    print('Writing APIs to remove to .xlsx')
                    df_to_remove.to_excel(file)

                    while True:
                        again = input("Would you like to find missing trajectories for a different project? [Y/N]?\n")
                        if again.lower() == 'y':
                            break
                        elif again.lower() == 'n':
                            p_compare_apis = False
                            break
                        else:
                            print('You must input a valid option[Y/N].\n')
                            continue

            elif str(process) == '4':
                p_match_las = True
                while p_match_las:
                    las_dir = filedialog.askdirectory(title='Select the folder of all your LAS')
                    apis = get_api_list_from_folder(las_dir)
                    clause = 'AND w.API10 IN {}'.format(tuple(apis))
                    traj_df = get_trajectories(clause=clause)
                    to_drop = get_columns_to_drop(traj_df, keep_list=['API14', 'MeasuredDepth', 'Inclination'])
                    traj_df = traj_df.drop(columns=to_drop)
                    print('query complete')
                    match_logs_to_trajectories(traj_df, las_dir)
                    while True:
                        again = input('Would you like to match LAS for a different data set? [Y/N]\n')
                        if again.lower() == 'y':
                            break
                        elif again.lower() == 'n':
                            p_match_las = False
                            break
                        else:
                            print('You must input a valid option[Y/N].\n')
                            continue
            else:
                print('\nInvalid input, please enter 1, 2, 3, or 4')
            # determine whether to run the program again or exit
            while True:
                again = input("Would you like to run a different process[Y/N]?\n")
                if again.lower() == 'y':
                    break
                elif again.lower() == 'n':
                    active = False
                    break
                else:
                    print('You must input a valid option[Y/N]!\n')
                    continue

    except Exception as ex:
        print('\nException in get_files\n')
        print(str(ex))


if __name__ == '__main__':

    try:
        get_files()

    except Exception as ex:
        print('\nException in get_files\n')
        print(str(ex))
