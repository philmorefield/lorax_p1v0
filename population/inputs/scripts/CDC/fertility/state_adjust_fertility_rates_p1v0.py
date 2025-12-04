'''
Revised: 2025-12-2

This script processes fertility rates for 2018-2022 from the CDC. County level
fertility rates are assigned to each county using the following logic:

1) If there is a rate for the county/race/age group, use that rate.
2) If the fertility rate for a county/race/age group is still undefined,
   use the "Unidentified Counties" rate from the county level data.
3) If the fertility rate for a county is still undefined, use the state-wide
   rate for that race/age cohort.
4) If the county and state rates are missing, use the HHS region rate.
5) The NHPI rates for some counties are obviously spurious (e.g., >4,000 births
   per 1,000 females). Through personal communication with CDC staff, I
   confirmed that these artifacts are due to unusually low population estimates
   from the Census. Based on histograms of fertility rates for all race/age
   cohorts, all NHPI fertility rates >300 are replaced with the HHS region
   rate.
'''

import os

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

        temp.plot(column='FERTILITY',
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
        fn = f'county_fertility_rates_{age}.png'
        plt.savefig(os.path.join(FIGURES, 'fertility', 'rates', fn), dpi=300)
        plt.clf()
        plt.close()


def get_stfips():
    csv = os.path.join(BASE_FOLDER, 'inputs', 'fips_to_urb20_bea10_hhs.csv')
    df = pd.read_csv(filepath_or_buffer=csv)
    df['GEOID'] = df['COFIPS'].astype(str).str.zfill(5).str[:2]

    return df[['GEOID', 'POPULATION20', 'HHS']].drop_duplicates()


def get_state_level_fertility_cdc():
    na_values = ['Suppressed', 'Not Applicable', 'None', 'Missing', 'Not Available']
    csv = os.path.join(CDC_FILES, 'Natality, 2024, State.csv')
    df = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
    df = df[['State of Residence Code', 'Age of Mother 9 Code', 'Births', 'Female Population', 'Fertility Rate']]
    df.columns = ['GEOID', 'AGE_GROUP', 'BIRTHS', 'FEMALE_POPULATION', 'FERTILITY']
    df.dropna(how='any', inplace=True)
    df['GEOID'] = df['GEOID'].astype(int).astype(str).str.zfill(2)

    return df[['GEOID', 'AGE_GROUP', 'BIRTHS', 'FERTILITY']]


def age_to_age_group(age):
    """Convert single year age to five-year age group."""
    if age >= 85:
        return '85+'
    else:
        group_start = (age // 5) * 5
        return f"{group_start}-{group_start + 4}"


def get_census_births_2024():
    '''
    Get historical births by state from the Census Bureau. We'll use this to
    adjust the CDC fertility rates.
    '''
    columns = ['SUMLEV', 'STATE', 'BIRTHS2024']
    csv = os.path.join(CENSUS_CSV_PATH, '2024', 'intercensal', 'co-est2024-alldata.csv')
    df = pd.read_csv(csv, encoding='latin-1')
    df = df[columns]
    df = df.query('SUMLEV == 40').drop(columns='SUMLEV').reset_index(drop=True)
    df.columns = ['GEOID', 'BIRTHS']
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


def get_starting_population_2024():
    '''
    2024 launch population is taken from U.S. Census Intercensal Population
    Estimates.
    '''
    census_sya_input_folder = os.path.join(CENSUS_CSV_PATH, '2024', 'intercensal', 'syasex')

    df_list = []
    for csv in os.listdir(census_sya_input_folder):
        if csv.endswith('.csv'):
            temp = pl.read_csv(source=os.path.join(census_sya_input_folder, csv),
                                  encoding='latin1').filter(pl.col('YEAR') == 6)
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
    df = df.filter((pl.col('AGE_GROUP').is_in(['15-19', '20-24', '25-29', '30-34', '35-39', '40-44'])) & (pl.col('SEX') == 'FEMALE'))

    return df.to_pandas()


def main():
    '''
    Calculate births by state using CDC fertility rates (2024)
    and the 2025 starting population from Census
    '''
    # get the 2024 CDC fertility rates by state
    fertility = get_state_level_fertility_cdc()

    # get the 2024 starting population from Census
    starting_pop_2024 = get_starting_population_2024()

    # retreive the estimated state-level births from Census
    # census_births_2024 = get_census_births_2024()

    # calculate births by state using CDC fertility rates and Census population
    df = starting_pop_2024.merge(right=fertility,
                                 how='left',
                                 on=['GEOID', 'AGE_GROUP'])
    assert df.isnull().sum().sum() == 0, "Missing values after merging population and fertility rates"
    df['BIRTHS_CALC'] = (df['FERTILITY'] / 1000) * df['POPULATION']
    df['ADJUSTED_FERTILITY'] = (df['BIRTHS'] / df['BIRTHS_CALC']) * df['FERTILITY']

    df['ADJUSTED_BIRTHS_CALC'] = (df['ADJUSTED_FERTILITY'] / 1000) * df['POPULATION']
    assert df['ADJUSTED_BIRTHS_CALC'].round().sum() == df['BIRTHS'].round().sum(), "Adjusted births do not match CDC estimates"

    df = df[['GEOID', 'AGE_GROUP', 'ADJUSTED_FERTILITY']].rename(columns={'ADJUSTED_FERTILITY': 'FERTILITY'})

    # create_maps(df)
    df.to_csv(path_or_buf=os.path.join(PROCESSED_FILES, 'fertility', 'state_adjusted_cdc_fertility_2024_p1v0.csv'), index=False)

    print("Finished!")


if __name__ == '__main__':
    main()
