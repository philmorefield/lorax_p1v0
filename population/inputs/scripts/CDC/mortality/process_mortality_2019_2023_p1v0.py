'''
Revised: 2025-03-27


'''
import glob
import os
import sqlite3

from itertools import product

import pandas as pd
from polars import dataframe

na_values = ['Suppressed', 'Not Applicable', 'None', 'Missing', 'Not Available', 'Unreliable']

AGE_GROUP_SORT_MAP = {'0-4': 0,
                      '5-9': 1,
                      '10-14': 2,
                      '15-19': 3,
                      '20-24': 4,
                      '25-29': 5,
                      '30-34': 6,
                      '35-39': 7,
                      '40-44': 8,
                      '45-49': 9,
                      '50-54': 10,
                      '55-59': 11,
                      '60-64': 12,
                      '65-69': 13,
                      '70-74': 14,
                      '75-79': 15,
                      '80-84': 16,
                      '85+': 17}

if os.path.exists('D:\\OneDrive\\ICLUS_v3'):
    ICLUS_FOLDER = 'D:\\OneDrive\\ICLUS_v3'
else:
    ICLUS_FOLDER = 'D:\\projects\\ICLUS_v3'

CSV_FILES = os.path.join(ICLUS_FOLDER, 'population\\inputs\\raw_files\\CDC\\age')
DATABASE_FOLDER = os.path.join(ICLUS_FOLDER, 'population\\inputs\\databases')
MIGRATION_DB = os.path.join(DATABASE_FOLDER, 'migration.sqlite')


def get_cofips_and_state():
    query = 'SELECT COFIPS, STUSPS AS STABBR \
             FROM fips_to_urb20_bea10_hhs'
    con = sqlite3.connect(MIGRATION_DB)
    df = pd.read_sql_query(sql=query, con=con)
    con.close()

    return df


def create_template():
    cofips_all = get_cofips_and_state()

    ages = ['1',
            '1-4',
            '5-9',
            '10-14',
            '15-19',
            '20-24',
            '25-29',
            '30-34',
            '35-39',
            '40-44',
            '45-49',
            '50-54',
            '55-59',
            '60-64',
            '65-69',
            '70-74',
            '75-79',
            '80-84',
            '85+']

    cofips = list(cofips_all.COFIPS.values)
    genders = ['MALE', 'FEMALE']

    df = pd.DataFrame(list(product(cofips, ages, genders)),
                      columns=['COFIPS', 'AGE_GROUP', 'SEX'])

    return df


def apply_county_level_mortality(df):

    dataframes = []

    # first apply county level mortality; not all cohorts will have values
    for csv in glob.glob(os.path.join(CSV_FILES, 'Underlying*.csv')):
        temp = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
        if '85+' in os.path.basename(csv):
            continue
        if 'county' not in os.path.basename(csv):
            continue
        if 'Sex' not in temp.columns:
            if 'female' in os.path.basename(csv).lower():
                temp['Sex'] = 'Female'
            else:
                temp['Sex'] = 'Male'
        temp = temp[['County Code', 'Sex', 'Five-Year Age Groups Code', 'Crude Rate']]
        temp.columns = ['COFIPS', 'SEX', 'AGE_GROUP', 'MORTALITY']
        temp.dropna(how='any', inplace=True)
        temp['COFIPS'] = temp['COFIPS'].astype(int).astype(str).str.zfill(5)

        dataframes.append(temp)

    mort = pd.concat(objs=dataframes, ignore_index=True)
    mort['SEX'] = mort['SEX'].str.upper()

    df = df.merge(right=mort, how='left', on=['COFIPS', 'AGE_GROUP', 'SEX'])

    return df

def apply_state_level_mortality(df):

    dataframes = []

    # process age groups <85 first
    for csv in glob.glob(os.path.join(CSV_FILES, 'Underlying*.csv')):
        temp = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
        if 'state' not in os.path.basename(csv):
            continue
        if '85+' in os.path.basename(csv):
            continue
        if 'Sex' not in temp.columns:
            if 'female' in os.path.basename(csv).lower():
                temp['Sex'] = 'Female'
            else:
                temp['Sex'] = 'Male'

        temp = temp[['State Code', 'Sex', 'Five-Year Age Groups Code', 'Crude Rate']]
        temp.columns = ['STFIPS', 'SEX', 'AGE_GROUP', 'STATE_MORTALITY']
        temp.dropna(how='any', inplace=True)
        temp['STFIPS'] = temp['STFIPS'].astype(int).astype(str).str.zfill(5)

        dataframes.append(temp)

    # process the 85+ age group
    for csv in glob.glob(os.path.join(CSV_FILES, 'Underlying*.csv')):
        temp = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
        if '85+' not in os.path.basename(csv):
            continue
        if 'state' not in os.path.basename(csv):
            continue
        if 'Sex' not in temp.columns:
            if 'female' in os.path.basename(csv).lower():
                temp['Sex'] = 'Female'
            else:
                temp['Sex'] = 'Male'

        temp['Ten-Year Age Groups Code'] = '85+'
        temp = temp[['State Code', 'Sex', 'Ten-Year Age Groups Code', 'Crude Rate']]
        temp.columns = ['STFIPS', 'SEX', 'AGE_GROUP', 'STATE_MORTALITY']
        temp.dropna(how='any', inplace=True)
        temp['STFIPS'] = temp['STFIPS'].astype(int).astype(str).str.zfill(5)

        dataframes.append(temp)

    mort = pd.concat(objs=dataframes, ignore_index=True)
    mort['STFIPS'] = mort['STFIPS'].astype(int).astype(str).str.zfill(2)
    mort['SEX'] = mort['SEX'].str.upper()

    df['STFIPS'] = df['COFIPS'].str[:2]
    df = df.merge(right=mort, how='left', on=['STFIPS', 'SEX', 'AGE_GROUP'])
    df.loc[df.MORTALITY.isnull(), 'MORTALITY'] = df['STATE_MORTALITY']

    df = df.drop(columns=['STFIPS', 'STATE_MORTALITY'])

    return df


def apply_hhs_level_mortality(df):
    query = 'SELECT COFIPS, HHS AS HHS_REGION \
             FROM fips_to_urb20_bea10_hhs'
    con = sqlite3.connect(MIGRATION_DB)
    hhs = pd.read_sql_query(sql=query, con=con)
    con.close()

    dataframes = []

    for sex in ['male', 'female']:
        fn = f'Underlying Cause of Death, 2019-2023, {sex}, HHS.csv'
        csv = os.path.join(CSV_FILES, fn)
        temp = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
        if 'Sex' not in temp.columns:
            if sex == 'female':
                temp['Sex'] = 'Female'
            else:
                temp['Sex'] = 'Male'
        temp = temp[['HHS Region Code', 'Sex', 'Five-Year Age Groups Code', 'Crude Rate']]
        temp.columns = ['HHS_REGION', 'SEX', 'AGE_GROUP', 'HHS_MORTALITY']
        temp.dropna(how='any', inplace=True)

        dataframes.append(temp)

    # add 85+ age group to the dataframe
    csv = os.path.join(CSV_FILES, 'Underlying Cause of Death, 2019-2023, 85+, HHS.csv')
    temp = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
    temp['Five-Year Age Groups Code'] = '85+'
    temp = temp[['HHS Region Code', 'Sex', 'Five-Year Age Groups Code', 'Crude Rate']]
    temp.columns = ['HHS_REGION', 'SEX', 'AGE_GROUP', 'HHS_MORTALITY']
    temp.dropna(how='any', inplace=True)

    dataframes.append(temp)

    mort = pd.concat(objs=dataframes, ignore_index=True)
    mort['HHS_REGION'] = mort['HHS_REGION'].str.replace('HHS', '').astype(int)
    mort['SEX'] = mort['SEX'].str.upper()

    # identify the HHS region for each county
    df = df.merge(right=hhs, how='left', on='COFIPS')

    # join HHS-level mortality rates
    df['HHS_REGION'] = df['HHS_REGION'].astype(int)
    df = df.merge(right=mort, how='left', on=['HHS_REGION', 'SEX', 'AGE_GROUP'])
    df.loc[df.MORTALITY.isnull(), 'MORTALITY'] = df['HHS_MORTALITY']

    df = df.drop(columns=['HHS_REGION', 'HHS_MORTALITY'])

    return df


def combine_under_5_age_groups(df):
    # combine <1 and 1-4 age groups using a weighted average
    weight_map = {'1': 0.2,
                  '1-4': 0.8}

    young = df.copy().query('AGE_GROUP == "1" | AGE_GROUP == "1-4"')
    young['WEIGHT'] = young['AGE_GROUP'].map(weight_map)
    young['MORT_x_WEIGHT'] = young.eval('MORTALITY * WEIGHT')
    young['NUMERATOR'] = young.groupby(by=['COFIPS', 'SEX'])['MORT_x_WEIGHT'].transform('sum')
    young['DENOMENATOR'] = young.groupby(by=['COFIPS', 'SEX'])['WEIGHT'].transform('sum')
    young['MORTALITY'] = young.eval('NUMERATOR / DENOMENATOR')
    young['AGE_GROUP'] = '0-4'
    young = young.drop(columns=['WEIGHT', 'NUMERATOR', 'DENOMENATOR', 'MORT_x_WEIGHT'])
    young = young.drop_duplicates()

    df = df.query('AGE_GROUP != "1" & AGE_GROUP != "1-4"')
    df = pd.concat(objs=[df, young], ignore_index=True, verify_integrity=True)

    return df


def make_fips_changes(df):
    con =sqlite3.connect(MIGRATION_DB)
    query = 'SELECT OLD_FIPS AS COFIPS, NEW_FIPS \
             FROM fips_or_name_changes'
    df_fips = pd.read_sql_query(sql=query, con=con)
    con.close()

    df = df.merge(right=df_fips,
                  how='left',
                  on='COFIPS')

    df.loc[~df.NEW_FIPS.isnull(), 'COFIPS'] = df['NEW_FIPS']
    df = df.drop(columns='NEW_FIPS')

    # TODO: this mean should be weighted by population, technically
    df = df.groupby(by=['COFIPS', 'AGE_GROUP', 'SEX'], as_index=False).mean()

    return df


def convert_age_group_to_list(s):
    if s == '85+':
        result = [85] # highest age group is 85+
    else:
        s = s.split('-')
        result = list(range(int(s[0]), int(s[1]) + 1))

    return result


def main():
    # create the template Dataframe that hold all county/race/age combinations
    # and start merging information
    df = create_template()
    df = apply_county_level_mortality(df)
    df = apply_state_level_mortality(df)
    df = apply_hhs_level_mortality(df)
    df = combine_under_5_age_groups(df)
    df = make_fips_changes(df)

    df = df.sort_values(by=['AGE_GROUP', 'COFIPS'], key=lambda x: x.map(AGE_GROUP_SORT_MAP))
    df = df.rename(columns={'MORTALITY': 'MORTALITY_RATE_100K',
                            'COFIPS': 'GEOID'})

    # expand age groups
    df['AGE_GROUP'] = df['AGE_GROUP'].apply(lambda x: convert_age_group_to_list(x))
    df = df.explode('AGE_GROUP', ignore_index=True).rename(columns={'AGE_GROUP': 'AGE'})

    # con = sqlite3.connect(os.path.join(DATABASE_FOLDER, 'cdc.sqlite'))
    # df.to_sql(name='mortality_2019_2023_county',
    #           con=con,
    #           if_exists='replace',
    #           index=False)
    # con.close()

    df.to_csv(os.path.join(DATABASE_FOLDER, 'mortality_2019_2023_county.csv'), index=False)

    print("Finished!")


if __name__ == '__main__':
    main()
