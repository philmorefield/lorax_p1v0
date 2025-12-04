import os

from itertools import product

import geopandas as gpd
import pandas as pd
import polars as pl

from matplotlib import pyplot as plt


BASE_FOLDER = 'D:\\OneDrive\\lorax_p1v0\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'
CENSUS_CSV_PATH = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\Census')
PROCESSED_FILES = os.path.join(BASE_FOLDER, 'inputs\\processed_files')
CDC_FILES = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\CDC\\age')
FIGURES = os.path.join(BASE_FOLDER, 'figures', 'p1v1')
GEOSPATIAL = 'D:\\OneDrive\\ICLUS_v3\\geospatial'

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

    stfips = list(set(stfips_all.GEOID.values))
    genders = ['MALE', 'FEMALE']

    df = pd.DataFrame(list(product(stfips, ages, genders)),
                      columns=['GEOID', 'AGE_GROUP', 'SEX'])

    return df


def read_county_shapefile():
    f = 'county_2020_dissolved_proj.shp'
    gdf = gpd.read_file(filename=os.path.join(GEOSPATIAL, f))
    gdf.rename(columns={'NEW_FIPS': 'GEOID'}, inplace=True)
    gdf = gdf.to_crs("EPSG:5070")

    return gdf


def read_state_shapefile():
    f = 'state_2020.shp'
    gdf = gpd.read_file(filename=os.path.join(GEOSPATIAL, f))
    gdf = gdf.to_crs("EPSG:5070")

    return gdf


def create_maps(df):

    # bins = (0.5, 1, 5, 10, 25, 100, 1000)
    # labels = ('<0.5', '<1', '<5', '<10', '<25', '>=200')

    # df['BINS'] = pd.cut(x=df['PERCENT_OF_AGE_SEX_COHORT'] * 10000,
    #                     bins=bins,
    #                     right=True,
    #                     labels=labels,
    #                     include_lowest=True)
    gdf = read_county_shapefile()[['GEOID', 'geometry']]
    gdf.GEOID = gdf.GEOID.astype(str).str.zfill(5)
    states = read_state_shapefile()
    for age in df.AGE.unique():
        cohort = df.query('AGE == @age')
        temp = gdf.merge(right=cohort, how='right', on='GEOID')

        temp.plot(column='MORTALITY',
                    scheme='FisherJenks',
                    k=5,
                    cmap='plasma',
                    legend=True,
                    legend_kwds={'bbox_to_anchor': (0.18, 0.3),#(1, 0.375),
                                'fontsize': 'x-small',
                                'fancybox': True},
                    missing_kwds={'color': 'black'})
        states.boundary.plot(ax=plt.gca(), edgecolor='lightgray', linewidth=0.2)
        plt.gca().set_xlim(-2371000, 2278000)
        plt.gca().set_ylim(246000, 3186000)
        plt.gca().axis('off')
        plt.title(label=f"2011-2015 Births per 1,000 women: Age {age}")
        plt.tight_layout()

        # simplify the legend labels
        try:
            for label in plt.gca().get_legend().get_texts():
                upper_bound = label.get_text().split(',')[-1]
                label.set_text(f'< {upper_bound}')
        except AttributeError:
            pass

        # plt.show()
        fn = f'county_mortality_rates_{age}.png'
        plt.savefig(os.path.join(FIGURES, 'mortality', 'rates', fn), dpi=300)
        plt.clf()
        plt.close()


def get_stfips():
    csv = os.path.join(BASE_FOLDER, 'inputs', 'fips_to_urb20_bea10_hhs.csv')
    df = pd.read_csv(filepath_or_buffer=csv)
    df['GEOID'] = df['COFIPS'].astype(str).str.zfill(5).str[:2]

    return df[['GEOID', 'POPULATION20', 'HHS']].drop_duplicates()


def age_to_age_group(age):
    """Convert single year age to five-year age group."""
    if age >= 85:
        return '85+'
    else:
        group_start = (age // 5) * 5
        return f"{group_start}-{group_start + 4}"


def get_census_deaths_2023():
    '''
    Get historical deaths by state from the Census Bureau. We'll use this to
    adjust the CDC mortality rates.
    '''
    columns = ['SUMLEV', 'STATE', 'DEATHS2023']
    csv = os.path.join(CENSUS_CSV_PATH, '2024', 'intercensal', 'co-est2024-alldata.csv')
    df = pd.read_csv(csv, encoding='latin-1')
    df = df[columns]
    df = df.query('SUMLEV == 40').drop(columns='SUMLEV').reset_index(drop=True)
    df.columns = ['GEOID', 'DEATHS']
    df['GEOID'] = df['GEOID'].astype(str).str.zfill(2)

    return df

def get_cbo_population():
    cols = ['AGE',
            'TOTAL_POPULATION',
            'TOTAL_MALE',
            'TOTAL_MALE_SINGLE',
            'TOTAL_MALE_MARRIED',
            'TOTAL_MALE_WIDOWED',
            'TOTAL_MALE_DIVORCED',
            'TOTAL_FEMALE',
            'TOTAL_FEMALE_SINGLE',
            'TOTAL_FEMALE_MARRIED',
            'TOTAL_FEMALE_WIDOWED',
            'TOTAL_FEMALE_DIVORCED']

    csv_folder = os.path.join(BASE_FOLDER, 'inputs', 'raw_files', 'CBO', '57059-2025-09-Demographic-Projections')
    csv_fn = '57059-2025-09-Demographic-Projections.xlsx'
    df = pl.read_excel(source=os.path.join(csv_folder, csv_fn),
                       sheet_name='2. Pop by age, sex, marital',
                       read_options={'skip_rows': 9})

    # Set column names and drop null columns/rows
    df.columns = cols
    df = df.drop_nulls()

    # Select and clean the required columns
    df = df.select(['AGE', 'TOTAL_MALE', 'TOTAL_FEMALE']).drop_nulls()
    # df = df.filter(pl.col('AGE') != 'Age')

    # Convert to appropriate types
    df = df.with_columns([pl.col('TOTAL_MALE').cast(pl.Int32),
                          pl.col('TOTAL_FEMALE').cast(pl.Int32)])

    # Split into chunks of 101 rows and add year
    n = 100
    df_list = []
    for i in range(0, df.height, n):
        chunk = df.slice(i, n).with_columns(pl.lit(2022 + i // n).alias('YEAR'))
        df_list.append(chunk)

    df = pl.concat(df_list)
    df = df.filter(pl.col('YEAR') == 2024).drop('YEAR').rename({'TOTAL_MALE': 'MALE', 'TOTAL_FEMALE': 'FEMALE'})
    df = df.unpivot(index='AGE', variable_name='SEX', value_name='POPULATION_CBO')

    # Convert ages to age groups
    df = df.with_columns(pl.col('AGE').map_elements(age_to_age_group).alias('AGE_GROUP'))
    df = df.group_by(['AGE_GROUP', 'SEX']).agg(pl.col('POPULATION_CBO').sum())

    assert df.shape == (36, 3)

    return df


def get_starting_population_2023():
    '''
    2024 launch population is taken from U.S. Census Intercensal Population
    Estimates.
    '''
    census_sya_input_folder = os.path.join(CENSUS_CSV_PATH, '2024', 'intercensal', 'syasex')

    df_list = []
    for csv in os.listdir(census_sya_input_folder):
        if csv.endswith('.csv'):
            temp = pl.read_csv(source=os.path.join(census_sya_input_folder, csv),
                                  encoding='latin1').filter(pl.col('YEAR') == 5)
            temp = temp.with_columns((pl.col('STATE').cast(pl.String).str.zfill(2)).alias('GEOID')).rename({'TOT_MALE': 'MALE', 'TOT_FEMALE': 'FEMALE'})
            temp = temp.select(['GEOID', 'AGE', 'MALE', 'FEMALE'])
            # Convert ages to age groups
            temp = temp.with_columns(pl.col('AGE').map_elements(age_to_age_group).alias('AGE_GROUP')).drop('AGE')
            temp = temp.unpivot(index=['GEOID', 'AGE_GROUP'], variable_name='SEX', value_name='POPULATION')
            # Aggregate by age groups
            temp = temp.group_by(['GEOID', 'AGE_GROUP', 'SEX']).agg(pl.col('POPULATION').sum())

            df_list.append(temp)
    df = pl.concat(items=df_list, how='vertical')

    df = df.sort(['GEOID', 'AGE_GROUP', 'SEX'])

    # Group by age group and sex, calculate percentages
    df = df.with_columns((pl.col('POPULATION') / pl.col('POPULATION').sum().over(['AGE_GROUP', 'SEX']) * 100)
                           .alias('POPULATION') )

    cbo_2024_pop = get_cbo_population()

    df = df.join(other=cbo_2024_pop,
                 on=['AGE_GROUP', 'SEX'],
                 how='left',
                 coalesce=True)

    df = df.with_columns((pl.col('POPULATION_CBO') * (pl.col('POPULATION') / 100.0)).alias('POPULATION'))
    df = df.select(['GEOID', 'AGE_GROUP', 'SEX', 'POPULATION'])

    return df.to_pandas()


def main():
    '''
    Calculate deaths by state for using CDC mortality rates (2023)
    and the 2025 starting population from Census
    '''
    # create the template Dataframe that hold all county/age group combinations
    # and start merging information
    csv = (os.path.join(PROCESSED_FILES, 'mortality', 'state_cdc_mortality_2023_p1v0.csv'))
    mortality = pd.read_csv(csv)
    mortality['GEOID'] = mortality['GEOID'].astype(str).str.zfill(2)

    # get the 2023 population from Census
    starting_pop_2023 = get_starting_population_2023()

    # retreive the estimated state-level deaths from Census
    census_deaths_2023 = get_census_deaths_2023()

    # calculate deaths by state using CDC mortality rates and Census population
    df = starting_pop_2023.merge(right=mortality,
                                 how='left',
                                 on=['GEOID', 'AGE_GROUP', 'SEX'])
    assert df.isnull().sum().sum() == 0, "Missing values after merging population and mortality rates"
    df['DEATHS_CALC'] = (df['MORTALITY_RATE_100K'] / 100000) * df['POPULATION']
    df = df[['GEOID', 'DEATHS_CALC']].groupby(by='GEOID', as_index=False).sum()

    df = df.merge(right=census_deaths_2023,
                  how='left',
                  on='GEOID')
    df['MORTALITY_ADJUSTMENT'] = df['DEATHS'] / df['DEATHS_CALC']
    df = mortality.merge(right=df[['GEOID', 'MORTALITY_ADJUSTMENT']],
                         how='left',
                         on='GEOID')

    df['ADJUSTED_MORTALITY'] = df['MORTALITY_ADJUSTMENT'] * df['MORTALITY_RATE_100K']
    df = df[['GEOID', 'AGE_GROUP', 'SEX', 'ADJUSTED_MORTALITY']].rename(columns={'ADJUSTED_MORTALITY': 'MORTALITY_RATE_100K'})

    # create_maps(df)
    df.to_csv(path_or_buf=os.path.join(PROCESSED_FILES, 'mortality', 'state_adjusted_cdc_mortality_2023_p1v0.csv'), index=False)

    print("Finished!")


if __name__ == '__main__':
    main()
