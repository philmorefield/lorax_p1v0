import os

import polars as pl


BASE_FOLDER = 'D:\\OneDrive\\lorax_p1v0\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'
PROCESSED_FILES = os.path.join(BASE_FOLDER, 'inputs\\processed_files')

CBO_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\CBO')
CSV_FOLDER = '57059-2025-09-Demographic-Projections//CSV files'
CSV_FILE = 'grossMigration_byYearAgeSexStatusFlow.csv'


def add_age_group(df):
    df = df.with_columns(pl.when(pl.col('AGE') <= 4).then(pl.lit('0-4'))
                           .when(pl.col('AGE') <= 9).then(pl.lit('5-9'))
                           .when(pl.col('AGE') <= 14).then(pl.lit('10-14'))
                           .when(pl.col('AGE') <= 19).then(pl.lit('15-19'))
                           .when(pl.col('AGE') <= 24).then(pl.lit('20-24'))
                           .when(pl.col('AGE') <= 29).then(pl.lit('25-29'))
                           .when(pl.col('AGE') <= 34).then(pl.lit('30-34'))
                           .when(pl.col('AGE') <= 39).then(pl.lit('35-39'))
                           .when(pl.col('AGE') <= 44).then(pl.lit('40-44'))
                           .when(pl.col('AGE') <= 49).then(pl.lit('45-49'))
                           .when(pl.col('AGE') <= 54).then(pl.lit('50-54'))
                           .when(pl.col('AGE') <= 59).then(pl.lit('55-59'))
                           .when(pl.col('AGE') <= 64).then(pl.lit('60-64'))
                           .when(pl.col('AGE') <= 69).then(pl.lit('65-69'))
                           .when(pl.col('AGE') <= 74).then(pl.lit('70-74'))
                           .when(pl.col('AGE') <= 79).then(pl.lit('75-79'))
                           .when(pl.col('AGE') <= 84).then(pl.lit('80-84'))
                           .otherwise(pl.lit('85+'))
                           .alias('AGE_GROUP'))

    return df

def group_years(df):
    df = df.with_columns(pl.when((pl.col('YEAR') >= 2022) & (pl.col('YEAR') <= 2024)).then(pl.lit('2022-2024'))
                           .when((pl.col('YEAR') >= 2025) & (pl.col('YEAR') <= 2029)).then(pl.lit('2025-2029'))
                           .when((pl.col('YEAR') >= 2030) & (pl.col('YEAR') <= 2034)).then(pl.lit('2030-2034'))
                           .when((pl.col('YEAR') >= 2035) & (pl.col('YEAR') <= 2039)).then(pl.lit('2035-2039'))
                           .when((pl.col('YEAR') >= 2040) & (pl.col('YEAR') <= 2044)).then(pl.lit('2040-2044'))
                           .when((pl.col('YEAR') >= 2045) & (pl.col('YEAR') <= 2049)).then(pl.lit('2045-2049'))
                           .when((pl.col('YEAR') >= 2050) & (pl.col('YEAR') <= 2054)).then(pl.lit('2050-2054'))
                           .when((pl.col('YEAR') >= 2055) & (pl.col('YEAR') <= 2059)).then(pl.lit('2055-2059'))
                           .when((pl.col('YEAR') >= 2060) & (pl.col('YEAR') <= 2064)).then(pl.lit('2060-2064'))
                           .when((pl.col('YEAR') >= 2065) & (pl.col('YEAR') <= 2069)).then(pl.lit('2065-2069'))
                           .when((pl.col('YEAR') >= 2070) & (pl.col('YEAR') <= 2074)).then(pl.lit('2070-2074'))
                           .when((pl.col('YEAR') >= 2075) & (pl.col('YEAR') <= 2079)).then(pl.lit('2075-2079'))
                           .when((pl.col('YEAR') >= 2080) & (pl.col('YEAR') <= 2084)).then(pl.lit('2080-2084'))
                           .when((pl.col('YEAR') >= 2085) & (pl.col('YEAR') <= 2089)).then(pl.lit('2085-2089'))
                           .when((pl.col('YEAR') >= 2090) & (pl.col('YEAR') <= 2094)).then(pl.lit('2090-2094'))
                           .when((pl.col('YEAR') >= 2095) & (pl.col('YEAR') <= 2098)).then(pl.lit('2095-2099'))
                           .otherwise(pl.lit(None))
                           .alias('TIME_STEP'))

    return df


def main():
    df = pl.read_csv(source=os.path.join(CBO_FOLDER, CSV_FOLDER, CSV_FILE),
                     infer_schema_length=10000)
    df = df.rename({'year': 'YEAR',
                    'age': 'AGE', 'sex': 'SEX', 'immigration_status': 'STATUS',
                    'migration_flow': 'TYPE', 'number_of_people': 'FLOW'})
    df = df.drop('STATUS')
    df = df.with_columns([pl.col('AGE').str.replace('+', '', literal=True).str.replace('-1', '0').cast(pl.Int32),
                          pl.col('SEX').str.to_uppercase(),
                          pl.col('TYPE').str.to_uppercase(),
                          pl.col('FLOW').cast(pl.Int32)])

    # aggregate by age group and pivot
    df = df.group_by(['YEAR', 'AGE', 'SEX', 'TYPE']).sum()
    df = df.pivot(on='TYPE',
                  index=['YEAR', 'AGE', 'SEX'],
                  values='FLOW').fill_null(0)
    df = df.with_columns((pl.col('IMMIGRATION') - pl.col('EMIGRATION')).alias('NET_IMMIGRATION'))

    df = add_age_group(df)
    df = group_years(df)
    df = df.select(['TIME_STEP', 'AGE_GROUP', 'SEX', 'NET_IMMIGRATION'])

    df.write_csv(os.path.join(PROCESSED_FILES, 'immigration', 'national_cbo_net_migration_by_year_age_sex.csv'))


if __name__ == '__main__':
    main()
