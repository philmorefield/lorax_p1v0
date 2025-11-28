import os
import sqlite3

import pandas as pd

pd.set_option("display.max_columns", None) # show all cols

BASE_FOLDER = 'D:\\projects\\ICLUS_v3\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'
if os.path.isdir('D:\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'

DATABASE_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\databases')
MIGRATION_DB = os.path.join(DATABASE_FOLDER, 'migration.sqlite')
ACS_DB = os.path.join(DATABASE_FOLDER, 'acs.sqlite')
ACS_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\ACS')
ACS_AGE_GROUP_MAP = {1: '1_TO_4',
                     2: '5_TO_17',
                     3: '18_TO_19',
                     4: '20_TO_24',
                     5: '25_TO_29',
                     6: '30_TO_34',
                     7: '35_TO_39',
                     8: '40_TO_44',
                     9: '45_TO_49',
                     10: '50_TO_54',
                     11: '55_TO_59',
                     12: '60_TO_64',
                     13: '65_TO_69',
                     14: '70_TO_74',
                     15: '75_AND_OVER'}


def make_fips_changes(df):
    con =sqlite3.connect(MIGRATION_DB)
    query = 'SELECT OLD_FIPS AS COFIPS, NEW_FIPS \
             FROM fips_or_name_changes'
    df_fips = pd.read_sql_query(sql=query, con=con)
    con.close()

    df = df.merge(right=df_fips,
                how='left',
                left_on='ORIGIN_FIPS',
                right_on='COFIPS')
    df.loc[~df.NEW_FIPS.isnull(), 'ORIGIN_FIPS'] = df['NEW_FIPS']
    df = df.drop(columns=['NEW_FIPS', 'COFIPS'])
    df = df.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'AGE_GROUP'], as_index=False).sum()

    df = df.merge(right=df_fips,
                how='left',
                left_on='DESTINATION_FIPS',
                right_on='COFIPS')
    df.loc[~df.NEW_FIPS.isnull(), 'DESTINATION_FIPS'] = df['NEW_FIPS']
    df = df.drop(columns=['NEW_FIPS', 'COFIPS'])
    df = (df.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'AGE_GROUP'],
                     as_index=False).agg(FLOW=('FLOW', 'sum'),
                                         ORIGIN_POPULATION=('ORIGIN_POPULATION', 'min')))
    df = df.query('ORIGIN_FIPS != DESTINATION_FIPS').copy()
    df['ORIGIN_POPULATION'] = df['ORIGIN_POPULATION'].astype(int)

    assert not df.isna().any().any(), "NaN values present after FIPS changes"

    return df


def get_acs_2011_2015_migration():
    xl_filename = 'county-to-county-by-age-2011-2015-current-residence-sort.xlsx'

    columns = ('D_STFIPS', 'D_COFIPS', 'O_STFIPS', 'O_COFIPS', 'AGE_GROUP',
               'D_STATE', 'D_COUNTY', 'D_POP', 'D_POP_MOE', 'D_NONMOVERS',
               'D_NONMOVERS_MOE', 'D_MOVERS', 'D_MOVERS_MOE',
               'D_MOVERS_SAME_CY', 'D_MOVERS_SAME_CY_MOE',
               'D_MOVERS_FROM_DIFF_CY_SAME_ST',
               'D_MOVERS_FROM_DIFF_CY_SAME_ST_MOE', 'D_MOVERS_FROM_DIFF_ST',
               'D_MOVERS_DIFF_ST_MOE', 'D_MOVERS_FROM_ABROAD',
               'D_MOVERS_FROM_ABROAD_MOE', 'O_STATE', 'O_COUNTY',
               'ORIGIN_POPULATION', 'O_POP_MOE', 'O_NONMOVERS',
               'O_NOMMOVERS_MOE', 'O_MOVERS', 'O_MOVERS_MOE',
               'O_MOVERS_SAME_CY', 'O_MOVERS_SAME_CY_MOE',
               'O_MOVERS_FROM_DIFF_CY_SAME_ST',
               'O_MOVERS_FROM_DIFF_CY_SAME_ST_MOE', 'O_MOVERS_FROM_DIFF_ST',
               'O_MOVERS_DIFF_ST_MOE', 'O_MOVERS_PUERTO_RICO',
               'O_MOVERS_PUERTO_RICO_MOE', 'FLOW', 'TOTAL_FLOW_MOE')

    xls = pd.ExcelFile(os.path.join(ACS_FOLDER, '2011_2015', 'migration', xl_filename))
    df = pd.concat([xls.parse(sheet_name=name, header=None, names=columns, skiprows=4, skipfooter=8) for name in xls.sheet_names if name != 'Puerto Rico'])

    df = df[~df.O_STFIPS.str.contains('XXX')]
    foreign = ('EUR', 'ASI', 'SAM', 'ISL', 'NAM', 'CAM', 'CAR', 'AFR', 'OCE')
    df = df.loc[~df.O_STFIPS.isin(foreign), ['D_STFIPS', 'D_COFIPS', 'O_STFIPS', 'O_COFIPS', 'AGE_GROUP', 'ORIGIN_POPULATION', 'FLOW']]

    df['D_STFIPS'] = df.D_STFIPS.astype(int).astype(str).str.zfill(2)
    df['D_COFIPS'] = df.D_COFIPS.astype(int).astype(str).str.zfill(3)
    df['DESTINATION_FIPS'] = df.D_STFIPS + df.D_COFIPS

    df['O_STFIPS'] = df.O_STFIPS.astype(int).astype(str).str.zfill(2)
    df['O_COFIPS'] = df.O_COFIPS.astype(int).astype(str).str.zfill(3)
    df['ORIGIN_FIPS'] = df.O_STFIPS + df.O_COFIPS

    df['AGE_GROUP'] = df['AGE_GROUP'].replace(to_replace=ACS_AGE_GROUP_MAP)
    df = df[['ORIGIN_FIPS', 'DESTINATION_FIPS', 'AGE_GROUP', 'FLOW', 'ORIGIN_POPULATION']]

    # make FIPS changes and consolidate migration flows
    df = make_fips_changes(df)
    df = df.sort_values(by=['ORIGIN_FIPS', 'AGE_GROUP', 'DESTINATION_FIPS'])

    assert not df.isna().any().any(), "NaN values present after cleaning"

    return df


def calculate_flow_percentages(df):
    # assume migration rates for 1-year olds are the same as 0-year olds
    df.loc[df.AGE_GROUP == '1_TO_4', 'AGE_GROUP'] = '0_TO_4'

    # ignoring migration to/from Puerto Rico for now
    df = df.loc[~df.ORIGIN_FIPS.str.startswith('72')]
    df = df.loc[~df.DESTINATION_FIPS.str.startswith('72')]

    df['MIGRATION_RATE'] = df['FLOW'].div(df['ORIGIN_POPULATION'])

    assert not df.isna().any().any(), "NaN values present after calculating migration rates"

    return df[['ORIGIN_FIPS', 'DESTINATION_FIPS', 'AGE_GROUP', 'MIGRATION_RATE']]


def get_gross_migration_ratios_by_age():
    df = get_acs_2011_2015_migration()
    df = calculate_flow_percentages(df)

    # con = sqlite3.connect(ACS_DB)
    # df.to_sql(name='acs_gross_migration_ratios_2011_2015_age',
    #           con=con,
    #           if_exists='replace',
    #           index=False)
    # con.close()

    df.to_csv(os.path.join(DATABASE_FOLDER, 'acs_gross_migration_ratios_2011_2015_age.csv'),
              index=False)

def main():
    get_gross_migration_ratios_by_age()

if __name__ == '__main__':
    main()
