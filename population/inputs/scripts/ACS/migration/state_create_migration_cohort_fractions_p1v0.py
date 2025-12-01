import os

import pandas as pd


BASE_FOLDER = 'D:\\OneDrive\\lorax_p1v0\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'
PROCESSED_FILES = os.path.join(BASE_FOLDER, 'inputs\\processed_files')


def retrieve_sex_ratios():
    print("Processing sex ratios...")

    csv = os.path.join(PROCESSED_FILES, 'migration', 'state_acs_gross_migration_ratios_2011_2015_sex.csv')
    df = pd.read_csv(csv)

    df = df.sort_values(by=['ORIGIN_FIPS', 'DESTINATION_FIPS'])
    df = df.set_index(keys=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'SEX'])
    df = df.rename(columns={'MIGRATION_PROPORTION': 'VALUE'})

    return df


def retrieve_age_ratios():
    print("Processing age ratios...")

    csv = os.path.join(PROCESSED_FILES, 'migration', 'state_acs_gross_migration_ratios_2011_2015_age.csv')
    df = pd.read_csv(csv)

    df = df.sort_values(by=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'AGE_GROUP'])
    df = df.set_index(keys=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'AGE_GROUP'])
    df = df.rename(columns={'MIGRATION_RATE': 'VALUE'})

    return df


def main():
    sex = retrieve_sex_ratios()
    age = retrieve_age_ratios()

    df = sex.mul(other=age, axis='index').reset_index()
    df = df.rename(columns={'VALUE': 'MIGRATION_RATE'})
    df['AGE_GROUP'] = df['AGE_GROUP'].str.replace('_TO_', '-').str.replace('_AND_OVER', '+')

    # the 15-19 rates will be a weighted average of 5-17 and 18-19 rates
    df15_19 = df[(df.AGE_GROUP == '5-17') | (df.AGE_GROUP == '18-19')].copy()
    df15_19.loc[df.AGE_GROUP == '5-17', 'WEIGHT'] = (3/5)
    df15_19.loc[df.AGE_GROUP == '18-19', 'WEIGHT'] = (2/5)
    df15_19['WEIGHTxRATE'] = df15_19['MIGRATION_RATE'] * df15_19['WEIGHT']
    df15_19['SUM_WEIGHTxRATE'] = df15_19.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'SEX'])['WEIGHTxRATE'].transform('sum')
    df15_19['SUM_WEIGHTS'] = df15_19.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'SEX'])['WEIGHT'].transform('sum')
    df15_19['MIGRATION_RATE'] = df15_19['SUM_WEIGHTxRATE'] / df15_19['SUM_WEIGHTS']
    df15_19 = df15_19[['ORIGIN_FIPS', 'DESTINATION_FIPS', 'SEX', 'MIGRATION_RATE']].drop_duplicates()
    df15_19['AGE_GROUP'] = '15-19'

    df = df[df.AGE_GROUP != '18-19']
    df.loc[df.AGE_GROUP == '5-17', 'AGE_GROUP'] = '5-9'
    df10_14 = df[df.AGE_GROUP == '5-9'].copy()
    df10_14['AGE_GROUP'] = '10-14'

    df = df.sort_values(by=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'AGE_GROUP']).reset_index(drop=True)

    # split the 25-34 age group
    df.loc[df.AGE_GROUP == '25-34', 'AGE_GROUP'] = '25-29'
    df25_29 = df[df.AGE_GROUP == '25-29'].copy()
    df30_34 = df25_29.copy()
    df30_34['AGE_GROUP'] = '30-34'

    # split the 35-44 age group
    df.loc[df.AGE_GROUP == '35-44', 'AGE_GROUP'] = '35-39'
    df35_39 = df[df.AGE_GROUP == '35-39'].copy()
    df40_44 = df35_39.copy()
    df40_44['AGE_GROUP'] = '40-44'

    # split the 45-54 age group
    df.loc[df.AGE_GROUP == '45-54', 'AGE_GROUP'] = '45-49'
    df45_49 = df[df.AGE_GROUP == '45-49'].copy()
    df50_54 = df45_49.copy()
    df50_54['AGE_GROUP'] = '50-54'

    # split the 65-74 age group
    df.loc[df.AGE_GROUP == '65-74', 'AGE_GROUP'] = '65-69'
    df65_69 = df[df.AGE_GROUP == '65-69'].copy()
    df70_74 = df65_69.copy()
    df70_74['AGE_GROUP'] = '70-74'

    # create the 75-79 age group from the 75+ group
    df.loc[df.AGE_GROUP == '75+', 'AGE_GROUP'] = '75-79'
    df75_79 = df[df.AGE_GROUP == '75-79'].copy()
    df80_84 = df75_79.copy()
    df80_84['AGE_GROUP'] = '80-84'
    df85_plus = df75_79.copy()
    df85_plus['AGE_GROUP'] = '85+'


    df = pd.concat([df, df10_14, df15_19, df30_34, df40_44, df50_54, df70_74, df75_79, df80_84, df85_plus], ignore_index=True)


    df.to_csv(os.path.join(PROCESSED_FILES, 'migration', 'state_acs_gross_migration_age_sex_fractions_2011_2015.csv'),
              index=False)

    print("Finished!")


if __name__ == '__main__':
    main()
