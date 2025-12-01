import os

import pandas as pd


BASE_FOLDER = 'D:\\OneDrive\\lorax_p1v0\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'
PROCESSED_FILES = os.path.join(BASE_FOLDER, 'inputs\\processed_files')
ACS_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\ACS')
ACS_AGE_GROUP_MAP = {1: '0_TO_4',
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



def get_acs_2011_2015_population_by_age():
    csv_name = 'ACSDP5Y2015.DP05-Data.csv'
    csv = os.path.join(ACS_FOLDER, '2011_2015', 'population', csv_name)
    usecols = ['GEOID'] + [f'DP05_00{str(i).zfill(2)}E' for i in range(4, 17)]
    df = pd.read_csv(filepath_or_buffer=csv,
                     usecols=usecols,
                     skiprows=[1],
                     encoding='latin-1')

    cols = ['COFIPS', '0_TO_4', '5_TO_9', '10_TO_14', '15_TO_19', '20_TO_24',
            '25_TO_34', '35_TO_44', '45_TO_54', '55_TO_59', '60_TO_64',
            '65_TO_74', '75_TO_84', '85_AND_OVER']
    df.columns = cols
    df['COFIPS'] = df['COFIPS'].str[-5:]
    df = df.melt(id_vars='COFIPS', var_name='AGE_GROUP', value_name='ORIGIN_POPULATION')
    df['ORIGIN_FIPS'] = df['COFIPS'].str[:2]

    # oldest migration age group is 75+, so combine 75-84 and 85+
    df.loc[df.AGE_GROUP == '75_TO_84', 'AGE_GROUP'] = '75_AND_OVER'
    df.loc[df.AGE_GROUP == '85_AND_OVER', 'AGE_GROUP'] = '75_AND_OVER'

    df = df[['ORIGIN_FIPS', 'AGE_GROUP', 'ORIGIN_POPULATION']].groupby(by=['ORIGIN_FIPS', 'AGE_GROUP'], as_index=False).sum()

    # migration files use 5-17 and 18-19 age groups; adjust accordingly
    df.loc[df.AGE_GROUP == '5_TO_9', 'AGE_GROUP'] = '5_TO_17'
    df.loc[df.AGE_GROUP == '10_TO_14', 'AGE_GROUP'] = '5_TO_17'
    df = df.groupby(by=['ORIGIN_FIPS', 'AGE_GROUP'], as_index=False).sum()
    age15to19 = df[(df.AGE_GROUP == '15_TO_19')].copy()
    df = df.query('AGE_GROUP != "15_TO_19"')

    # add 3/5 of the 15-19 age group to the 5-17 age group
    add5to17 = age15to19.copy()
    add5to17['AGE_GROUP'] = '5_TO_17'
    add5to17['ORIGIN_POPULATION'] = add5to17['ORIGIN_POPULATION'] * (3/5)
    add5to17 = add5to17.rename(columns={'ORIGIN_POPULATION': 'ADD_POPULATION'})
    df = df.merge(add5to17[['ORIGIN_FIPS', 'AGE_GROUP', 'ADD_POPULATION']],
                  on=['ORIGIN_FIPS', 'AGE_GROUP'],
                  how='left')
    df['ADD_POPULATION'] = df['ADD_POPULATION'].fillna(0)
    df['ORIGIN_POPULATION'] = df['ORIGIN_POPULATION'] + df['ADD_POPULATION']
    df = df.drop(columns=['ADD_POPULATION'])

    # now create the 18-19 age group, equal to 2/5 of the 15-19 age group
    add18to19 = age15to19.copy()
    add18to19['AGE_GROUP'] = '18_TO_19'
    add18to19['ORIGIN_POPULATION'] = add18to19['ORIGIN_POPULATION'] * (2/5)
    df = pd.concat([df, add18to19[['ORIGIN_FIPS', 'AGE_GROUP', 'ORIGIN_POPULATION']]], ignore_index=True)

    df = df.sort_values(by=['ORIGIN_FIPS', 'AGE_GROUP']).reset_index(drop=True)
    assert not df.isna().any().any(), "NaN values present after cleaning"

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

    df['DESTINATION_FIPS'] = df.D_STFIPS.astype(int).astype(str).str.zfill(2)
    df['ORIGIN_FIPS'] = df.O_STFIPS.astype(int).astype(str).str.zfill(2)
    df['AGE_GROUP'] = df['AGE_GROUP'].replace(to_replace=ACS_AGE_GROUP_MAP)
    df = df[['ORIGIN_FIPS', 'DESTINATION_FIPS', 'AGE_GROUP', 'FLOW']].query('ORIGIN_FIPS != DESTINATION_FIPS')

    # ACS population uses 25_TO_34 age group
    df.loc[df.AGE_GROUP == '25_TO_29', 'AGE_GROUP'] = '25_TO_34'
    df.loc[df.AGE_GROUP == '30_TO_34', 'AGE_GROUP'] = '25_TO_34'

    # ACS population uses 35_TO_44 age group
    df.loc[df.AGE_GROUP == '35_TO_39', 'AGE_GROUP'] = '35_TO_44'
    df.loc[df.AGE_GROUP == '40_TO_44', 'AGE_GROUP'] = '35_TO_44'

    # ACS population uses 45_TO_54 age group
    df.loc[df.AGE_GROUP == '45_TO_49', 'AGE_GROUP'] = '45_TO_54'
    df.loc[df.AGE_GROUP == '50_TO_54', 'AGE_GROUP'] = '45_TO_54'

    # ACS population uses 65_TO_74 age group
    df.loc[df.AGE_GROUP == '65_TO_69', 'AGE_GROUP'] = '65_TO_74'
    df.loc[df.AGE_GROUP == '70_TO_74', 'AGE_GROUP'] = '65_TO_74'

    # consolidate migration flows
    df = df.sort_values(by=['ORIGIN_FIPS', 'AGE_GROUP', 'DESTINATION_FIPS'])
    df = df.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'AGE_GROUP'], as_index=False).sum()

    assert not df.isna().any().any(), "NaN values present after cleaning"

    return df


def calculate_flow_percentages(migration, population):

    df = migration.merge(population,
                         on=['ORIGIN_FIPS', 'AGE_GROUP'],
                         how='left')

    # ignoring migration to/from Puerto Rico for now
    df = df.loc[~df.ORIGIN_FIPS.str.startswith('72')]
    df = df.loc[~df.DESTINATION_FIPS.str.startswith('72')]

    df['MIGRATION_RATE'] = df['FLOW'].div(df['ORIGIN_POPULATION'])

    assert not df.isna().any().any(), "NaN values present after calculating migration rates"

    return df[['ORIGIN_FIPS', 'DESTINATION_FIPS', 'AGE_GROUP', 'MIGRATION_RATE']]


def get_gross_migration_ratios_by_age():
    population = get_acs_2011_2015_population_by_age()
    migration = get_acs_2011_2015_migration()
    df = calculate_flow_percentages(migration, population)

    df.to_csv(os.path.join(PROCESSED_FILES, 'migration', 'state_acs_gross_migration_ratios_2011_2015_age.csv'),
              index=False)

def main():
    get_gross_migration_ratios_by_age()

if __name__ == '__main__':
    main()
