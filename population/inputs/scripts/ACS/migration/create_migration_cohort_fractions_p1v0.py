import os
import sqlite3

import pandas as pd


BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'
DATABASES = os.path.join(BASE_FOLDER, 'inputs', 'databases')
ACS_DB = os.path.join(DATABASES, 'acs.sqlite')


def retrieve_sex_ratios():
    print("Processing sex ratios...")

    csv = os.path.join(DATABASES, 'acs_gross_migration_ratios_2011_2015_sex.csv')
    df = pd.read_csv(csv)

    df = df.sort_values(by=['ORIGIN_FIPS', 'DESTINATION_FIPS'])
    df = df.set_index(keys=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'SEX'])
    df = df.rename(columns={'MIGRATION_PROPORTION': 'VALUE'})

    return df


def retrieve_age_ratios():
    print("Processing age ratios...")

    csv = os.path.join(DATABASES, 'acs_gross_migration_ratios_2011_2015_age.csv')
    df = pd.read_csv(csv)

    df = df.sort_values(by=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'AGE_GROUP'])
    df = df.set_index(keys=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'AGE_GROUP'])
    df = df.rename(columns={'MIGRATION_RATE': 'VALUE'})

    return df


def convert_age_group_to_list(s):
    if s == '75+':
        result = list(range(75, 86)) # highest age group is 85+
    else:
        s = s.split('-')
        result = list(range(int(s[0]), int(s[1]) + 1))

    return result


def main():
    sex = retrieve_sex_ratios()
    age = retrieve_age_ratios()

    df = sex.mul(other=age, axis='index').reset_index()
    # There seems to be three dyads with NaN values after multiplying; these
    # pairs are in the "by sex" table but not in the "by age" table, probably
    # for privacy reasons (small counts). We will drop these.
    assert df[df.isna().any(axis=1)].shape == (3, 5), "Too many NaN values present after calculating fractions"
    df = df.dropna()

    df = df.rename(columns={'VALUE': 'MIGRATION_RATE'})
    df['AGE_GROUP'] = df['AGE_GROUP'].str.replace('_TO_', '-').str.replace('_AND_OVER', '+')

    # expand age groups
    df['AGE_GROUP'] = df['AGE_GROUP'].apply(lambda x: convert_age_group_to_list(x))
    df = df.explode('AGE_GROUP', ignore_index=True).rename(columns={'AGE_GROUP': 'AGE'})

    con = sqlite3.connect(ACS_DB)
    df.to_sql(name='acs_gross_migration_age_sex_fractions_2011_2015',
              con=con,
              if_exists='replace',
              index=False)
    con.close()

    df.to_csv(os.path.join(DATABASES, 'acs_gross_migration_age_sex_fractions_2011_2015.csv'),
              index=False)

    print("Finished!")


if __name__ == '__main__':
    main()
