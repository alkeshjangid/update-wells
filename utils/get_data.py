# package imports
import pyodbc
import pandas as pd


def get_well_header_data(play, clause, height_above_ground):

    sql = '''select distinct 
CASE WHEN AlternateElevationValue is NULL
THEN 1 
END as Needs_KB,
CASE WHEN (AlternateElevationValue-GroundElevation) < 0
THEN 1
END as KB_Not_Above_Ground,
CASE WHEN (AlternateElevationValue-GroundElevation) > {height_above_ground}
THEN 1
END as KB_Far_Above_Ground,
CASE WHEN TotalDepth IS NULL
THEN 1
END as No_TD,
CASE WHEN WellName IS NULL
THEN 1
END as No_WellName,
CASE WHEN h.API12 IS NULL
THEN 1
END as No_Trajectory_DS9,
CASE WHEN HasWellLogLas = 0
THEN 1
END as No_LAS_DS9,
w.API14, 
    w.API12, 
    w.API10, 
    SurfaceLongitude, 
    SurfaceLatitude, 
    WellName,
    WellNumber,
    TotalDepth, 
    Field, 
    CountyParishName, 
    StateProvinceName, 
    CountryName, 
    WellStatus, 
    ReportedOperator, 
    CurrentOperator,
    GroundElevation, 
    AlternateElevationValue,    
    AlternateElevationType, 
    SpudDate,
    ProductType
    from ds9.pres.vw_well_30 w
LEFT JOIN [DS9].[ds9].[Trajectory] h
ON h.API12 = w.API12
where AlternateElevationType like '%KB%'
    and BasinConformed like '%{basin}%'
    and w.isDeleted = 0
    AND RIGHT(w.api14, 2) = 00
    {clause}
    '''.format(height_above_ground=height_above_ground, basin=play, clause=clause)

    cnxn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};'
                          'SERVER=AUS2-DS9-PRL01;'
                          'DATABASE=DS9;'
                          'TRUSTED_CONNECTION=yes')

    print('\nQuerying to build a dataframe of DS9 well header data...')

    df = pd.DataFrame()
    i = 1

    chunksize = 10000
    df_iterator = pd.read_sql(sql, cnxn, chunksize=chunksize)
    for df_chunk in df_iterator:
        print('Writing {} rows as chunk no. {}...'.format(chunksize, i))
        i += 1
        df = df.append(df_chunk)
    cols = ['WellName', 'WellNumber', 'ReportedOperator', 'Field']
    # drop columns that don't need special chars removed
    df[cols] = df[cols].replace({'\'': '',
                                 '\"': '',
                                 '\,': '',
                                 '\;': '',
                                 '\*': '',
                                 '\:': ' ',
                                 '\-': ' ',
                                 '\.': '',
                                 '\/': '',
                                 '\_': ' ',
                                 '\#': '',
                                 '\+': ''}, regex=True)
    return df


def get_trajectories(clause):

    sql = '''SELECT DISTINCT h.API10
    ,h.API12
,w.API14
,d.TrajectoryId as 'TrajectoryID'
,d.Azimuth
,d.Inclination
,d.MeasuredDepth
,d.TrueVerticalDepth
,w.TotalDepth as Completion_TD
FROM [DS9].[ds9].[Trajectory] h
JOIN [DS9].[ds9].[TrajectoryDetail] d 
on h.TrajectoryId = d.TrajectoryId
JOIN [DS9].[pres].[vw_well_30] w 
on w.WellboreId = h.wellboreId
WHERE h.IsDeleted = 0 
AND RIGHT(w.api14, 2) = 00
{clause}
ORDER BY w.API14, d.TrajectoryID, d.MeasuredDepth'''.format(clause=clause)

    cnxn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};'
                          'SERVER=AUS2-DS9-PRL01;'
                          'DATABASE=DS9;'
                          'TRUSTED_CONNECTION=yes')

    print('\nQuerying to build a dataframe of digitized trajectory data...')

    df = pd.DataFrame()
    i = 1

    chunksize = 10000
    df_iterator = pd.read_sql(sql, cnxn, chunksize=chunksize)
    for df_chunk in df_iterator:
        print('Writing {} rows as chunk no. {}...'.format(chunksize, i))
        i += 1
        df = df.append(df_chunk)

    return df


def get_ds9_prodeng(play):
    """
    get ds9 production and engineering data
    :param play:
    :return:
    """
    sql_query = '''
    select 
    distinct API14, 
    API10, API12, 
    AlternateElevationValue, CompletionDate, GrossPerforationInterval, FirstProductionDate, 
    First6Liq, First6Gas, First6Wtr, First12Liq, 
    First12Gas, First12Wtr, First24Liq, First24Gas, First24Wtr, 
    cum3MonthsGas, cum3MonthsOil, cum3MonthsWater, First60Liq, 
    First60Gas, First60Wtr, cumulativeGasOilRatio, 
    LiqCum, GasCum, WtrCum, first3MonthsGasOilRatio, FirstGas, FirstOil, 
    ShallowestPerf, DeepestPerf, PeakGas, PeakLiq, 
    EUROil, EURGas, LiqGrav, Reservoir, PeakBOE, 
    FirstJobTotalFluid, FirstJobTotalProppant, FootageInZone
    FROM ds9.pres.vw_Well_30
    WHERE BasinConformed like '%{}%'
    and AlternateElevationType like '%KB%'
    '''.format(play)

    cnxn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};'
                          'SERVER=AUS2-DS9-PRL01;'
                          'DATABASE=DS9;'
                          'TRUSTED_CONNECTION=yes')

    print('\nQuerying to build a dataframe of DS9 production/engineering data...')

    df = pd.DataFrame()
    i = 1

    chunksize = 10000
    df_iterator = pd.read_sql(sql_query, cnxn, chunksize=chunksize)
    for df_chunk in df_iterator:
        print('Writing {} rows as chunk no. {}...'.format(chunksize, i))
        i += 1
        df = df.append(df_chunk)

    return df


def get_completion_tds():
    # input = well_header_apis
    sql_query = """
    SELECT DISTINCT w.uwi
    ,w.TotalDepth as Completion_TD
    FROM div1_daily.dbo.tblWell w
    JOIN div1_daily.dbo.tbltotalDepthType dtype
    ON w.totalDepthTypeID = dtype.totaldepthTypeId
    WHERE dtype.typeLong like 'Completion'
    """
    div1_cnxn = pyodbc.connect(r'DRIVER={SQL Server Native Client 11.0};'
                               r'SERVER=gis-mssql.prod.aus\gis;'
                               'DATABASE=div1_daily;'
                               'TRUSTED_CONNECTION=yes')

    df = pd.DataFrame()
    i = 1
    chunksize = 10000
    df_iterator = pd.read_sql(sql_query, div1_cnxn, chunksize=chunksize)
    for df_chunk in df_iterator:
        # print('Writing {} rows as chunk no. {}', i)
        i += 1
        df = df.append(df_chunk)

    # df = data_set.drop('index', 1)

    z = list(zip(*map(df.get, df)))

    completion_tds = {}
    for api, td in z:
        if api in completion_tds.keys():
            completion_tds[api].append(td)  # append value to existing list
        else:
            completion_tds[api] = [td]

    return completion_tds


# variable names to import
basins = ['ANADARKO', 'APPALACHIAN', 'ARKLA', 'ARKOMA', 'BURGOS - RIO GRANDE', 'CENTRAL BASIN PLATFORM',
          'CHEROKEE PLATFORM', 'DELAWARE', 'DENVER-JULESBURG', 'EAST TEXAS', 'EAST TEXAS COASTAL', 'EASTERN SHELF',
          'FORT WORTH', 'GREEN RIVER - OVERTHRUST', 'GULF COAST CENTRAL', 'GULF COAST EAST', 'GULF COAST WEST',
          'LOUISIANA COASTAL', 'MIDLAND', 'NORTH PARK', 'NORTHWEST SHELF', 'PALO DURO', 'PARADOX', 'PICEANCE',
          'POWDER RIVER', 'SAN JUAN', 'UINTA', 'VAL VERDE', 'WILLISTON', 'WIND RIVER']

ds9conn = pyodbc.connect(
    r'DRIVER={SQL Server Native Client 'r'11.0};SERVER=aus2-ds9-prl01;DATABASE=DS9;TRUSTED_CONNECTION=yes'
)

ds9query = """SELECT DISTINCT tw.[API14]
      ,tw.[API12]
      ,tw.[API10]
      ,tw.WellId
      ,[HasDigitizedTrajectory]
      ,[BasinConformed]
      ,tt.UpdatedDate
      ,tt.CreatedDate
      ,tt.DeletedDate
      ,tt.TrajectoryGrade
  FROM [DS9].[pres].[vw_well_30] tw
  LEFT JOIN [DS9].[ds9].[Trajectory] tt
  on tw.wellid = tt.wellid
  WHERE tt.IsDeleted = 0 and BasinConformed = '{}'
  AND tt.UpdatedDate > '10-01-2018'
  AND RIGHT(tw.api14, 2) = 00"""


state_query = """SELECT DISTINCT
     tw.StateNameConformed
     , APIState
     FROM[DS9].[pres].[vw_well_30] tw
     LEFT JOIN [DS9].[ds9].[Trajectory] tt
     on tw.API12 = tt.API12
     WHERE
     BasinConformed = '{}'
     AND tt.UpdatedDate > '10-01-2018'
     and tt.IsDeleted = 0"""

wells_with_trajectories_query = """SELECT DISTINCT
    w.API12
    from ds9.pres.vw_well_30 w
LEFT JOIN [DS9].[ds9].[Trajectory] h
ON h.API12 = w.API12
where h.API12 IS NOT NULL
and w.IsDeleted = 0"""