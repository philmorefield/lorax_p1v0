import glob
import os

from itertools import product

import pandas as pd


BASE_FOLDER = 'D:\\OneDrive\\lorax_p1v0\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'
PROCESSED_FILES = os.path.join(BASE_FOLDER, 'inputs\\processed_files')
CDC_FILES = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\CDC\\age')

NA_VALUES = ['Suppressed', 'Not Applicable', 'None', 'Missing', 'Not Available', 'Unreliable']
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






def get_stfips():
    csv = os.path.join(BASE_FOLDER, 'inputs', 'fips_to_urb20_bea10_hhs.csv')
    df = pd.read_csv(filepath_or_buffer=csv)
    df['STFIPS'] = df['COFIPS'].astype(str).str.zfill(5).str[:2]

    return df[['STFIPS', 'POPULATION20', 'HHS']].drop_duplicates()


def create_template():
    stfips_all = get_stfips()

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

    stfips = list(stfips_all.STFIPS.values)
    genders = ['MALE', 'FEMALE']

    df = pd.DataFrame(list(product(stfips, ages, genders)),
                      columns=['STFIPS', 'AGE_GROUP', 'SEX'])

    return df


def apply_state_level_mortality(df):

    dataframes = []

    # process age groups <85 first
    for csv in glob.glob(os.path.join(CDC_FILES, 'Underlying*.csv')):
        temp = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=NA_VALUES)
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
    for csv in glob.glob(os.path.join(CDC_FILES, 'Underlying*.csv')):
        temp = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=NA_VALUES)
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

    df = df.merge(right=mort, how='left', on=['STFIPS', 'SEX', 'AGE_GROUP'])
    df = df.rename(columns={'STATE_MORTALITY': 'MORTALITY'})

    return df


def apply_hhs_level_mortality(df):
    hhs = get_stfips()[['STFIPS', 'HHS']].drop_duplicates()
    hhs = hhs.rename(columns={'HHS': 'HHS_REGION'})

    dataframes = []

    for sex in ['male', 'female']:
        fn = f'Underlying Cause of Death, 2019-2023, {sex}, HHS.csv'
        csv = os.path.join(CDC_FILES, fn)
        temp = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=NA_VALUES)
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
    csv = os.path.join(CDC_FILES, 'Underlying Cause of Death, 2019-2023, 85+, HHS.csv')
    temp = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=NA_VALUES)
    temp['Five-Year Age Groups Code'] = '85+'
    temp = temp[['HHS Region Code', 'Sex', 'Five-Year Age Groups Code', 'Crude Rate']]
    temp.columns = ['HHS_REGION', 'SEX', 'AGE_GROUP', 'HHS_MORTALITY']
    temp.dropna(how='any', inplace=True)

    dataframes.append(temp)

    mort = pd.concat(objs=dataframes, ignore_index=True)
    mort['HHS_REGION'] = mort['HHS_REGION'].str.replace('HHS', '').astype(int)
    mort['SEX'] = mort['SEX'].str.upper()

    # identify the HHS region for each county
    df = df.merge(right=hhs, how='left', on='STFIPS')

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
    young['NUMERATOR'] = young.groupby(by=['STFIPS', 'SEX'])['MORT_x_WEIGHT'].transform('sum')
    young['DENOMENATOR'] = young.groupby(by=['STFIPS', 'SEX'])['WEIGHT'].transform('sum')
    young['MORTALITY'] = young.eval('NUMERATOR / DENOMENATOR')
    young['AGE_GROUP'] = '0-4'
    young = young.drop(columns=['WEIGHT', 'NUMERATOR', 'DENOMENATOR', 'MORT_x_WEIGHT'])
    young = young.drop_duplicates()

    df = df.query('AGE_GROUP != "1" & AGE_GROUP != "1-4"')
    df = pd.concat(objs=[df, young], ignore_index=True, verify_integrity=True)

    return df


def main():
    # create the template Dataframe that hold all county/race/age combinations
    # and start merging information
    df = create_template()

    df = apply_state_level_mortality(df)
    df = apply_hhs_level_mortality(df)
    assert df.MORTALITY.isnull().sum() == 0, "Some mortality rates are still null!"

    df = combine_under_5_age_groups(df)
    df = df.reset_index().groupby(by=['STFIPS', 'AGE_GROUP', 'SEX'], as_index=False).mean()

    df = df.sort_values(by=['AGE_GROUP', 'STFIPS'], key=lambda x: x.map(AGE_GROUP_SORT_MAP))
    df = df.rename(columns={'MORTALITY': 'MORTALITY_RATE_100K',
                            'STFIPS': 'GEOID'})

    df.to_csv(os.path.join(PROCESSED_FILES, 'mortality', 'cdc_mortality_2019_2023_p1v0.csv'), index=False)

    print("Finished!")


if __name__ == '__main__':
    main()
