import os

import geopandas as gpd
import polars as pl

from matplotlib import pyplot as plt


BASE_FOLDER = 'D:\\OneDrive\\lorax_p1v0\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'
PROCESSED_FILES = os.path.join(BASE_FOLDER, 'inputs\\processed_files')
FIGURES = os.path.join(BASE_FOLDER, 'figures', 'p1v0', 'immigration', 'state_fractions')
GEOSPATIAL = 'D:\\OneDrive\\lorax_p1v0\\geospatial'

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


def retrieve_sex_weights():
    print("Processing sex weights...")

    csv_file = os.path.join(PROCESSED_FILES, 'immigration', 'state_acs_immigration_weights_sex_2011_2015.csv')
    df = pl.read_csv(csv_file)

    # Convert to long format (melt equivalent)
    df = df.unpivot(index='DESTINATION_FIPS', variable_name='SEX', value_name='VALUE')

    return df


def retrieve_age_weights():
    print("Processing age weights...")

    csv_file = os.path.join(PROCESSED_FILES, 'immigration', 'state_acs_immigration_weights_age_2011_2015.csv')
    df = pl.read_csv(csv_file)

    # Convert to long format (melt equivalent)
    df = df.unpivot(index='DESTINATION_FIPS', variable_name='AGE', value_name='VALUE')

    return df


def read_state_shapefile():
    f = 'state_2020.shp'
    gdf = gpd.read_file(filename=os.path.join(GEOSPATIAL, f))
    gdf = gdf.to_crs("EPSG:5070")
    # Ensure STFIPS column exists and is properly formatted
    if 'STFIPS' not in gdf.columns and 'STATEFP' in gdf.columns:
        gdf = gdf.rename(columns={'STATEFP': 'STFIPS'})
    elif 'STFIPS' not in gdf.columns and 'GEOID' in gdf.columns:
        gdf = gdf.rename(columns={'GEOID': 'STFIPS'})

    return gdf


def create_maps(df):
    # Convert Polars DataFrame to pandas for geopandas compatibility
    df_pandas = df.to_pandas()

    gdf = read_state_shapefile()[['STFIPS', 'geometry']]
    gdf['STFIPS'] = gdf['STFIPS'].astype(str).str.zfill(2)
    df_pandas['PERCENT_OF_AGE_SEX_COHORT'] = round(df_pandas['PERCENT_OF_AGE_SEX_COHORT'] * 1000, 1)

    for age_group in df_pandas['AGE_GROUP'].unique():
        for sex in df_pandas['SEX'].unique():
            cohort = df_pandas.query('AGE_GROUP == @age_group & SEX == @sex')
            temp = gdf.merge(right=cohort, how='right', left_on='STFIPS', right_on='GEOID')

            temp.plot(column='PERCENT_OF_AGE_SEX_COHORT',
                      scheme='FisherJenks',
                      k=5,
                      cmap='plasma',
                      legend=True,
                      legend_kwds={'bbox_to_anchor': (0.18, 0.3),
                                   'fontsize': 'x-small',
                                   'fancybox': True,
                                   'title': 'x 10^3'},
                      missing_kwds={'color': 'black'})
            plt.gca().set_xlim(-2371000, 2278000)
            plt.gca().set_ylim(246000, 3186000)
            plt.gca().axis('off')
            plt.title(label=f"2011-2015 immigration fractions:\n {sex}, {age_group}")
            plt.tight_layout()

            # simplify the legend labels
            try:
                for label in plt.gca().get_legend().get_texts():
                    upper_bound = label.get_text().split(',')[-1]
                    label.set_text(f'< {upper_bound}')
            except AttributeError:
                pass

            # plt.show()
            fn = f'state_immigration_fractions_{sex}_{age_group}.png'
            plt.savefig(os.path.join(FIGURES, fn), dpi=300)
            plt.clf()
            plt.close()


def create_and_save_dataframe():
    sex = retrieve_sex_weights()
    age = retrieve_age_weights()

    # Join the dataframes on DESTINATION_FIPS
    df = sex.join(age, on='DESTINATION_FIPS', suffix='_age')

    # Multiply the values (equivalent to pandas mul operation)
    df = df.with_columns([(pl.col('VALUE') * pl.col('VALUE_age')).alias('VALUE')]).drop('VALUE_age')

    # Convert AGE to integer
    df = df.with_columns([pl.col('AGE').cast(pl.Int32)])

    # Calculate age-sex sum (equivalent to groupby transform)
    df = df.with_columns([pl.col('VALUE').sum().over(['AGE', 'SEX']).alias('AGE_SEX_SUM')])

    # Calculate percentage of age-sex cohort
    df = df.with_columns([(pl.col('VALUE') / pl.col('AGE_SEX_SUM')).alias('PERCENT_OF_AGE_SEX_COHORT')])

    # Rename and select columns
    df = df.with_columns([pl.col('DESTINATION_FIPS').alias('GEOID')]).select(['GEOID', 'AGE', 'SEX', 'PERCENT_OF_AGE_SEX_COHORT'])

    # Format GEOID as 2-digit string for states
    df = df.with_columns([pl.col('GEOID').cast(pl.String).str.zfill(2)])

    # Add AGE_GROUP column and aggregate to that level
    df = add_age_group(df).drop('AGE').group_by(['GEOID', 'AGE_GROUP', 'SEX']).agg(pl.col('PERCENT_OF_AGE_SEX_COHORT').sum().alias('PERCENT_OF_AGE_SEX_COHORT'))

    # Recalculate percentages to ensure they sum to 1 within each age-sex cohort
    df = df.with_columns([pl.col('PERCENT_OF_AGE_SEX_COHORT').sum().over(['AGE_GROUP', 'SEX']).alias('TOTAL_PERCENT')])
    df = df.with_columns([(pl.col('PERCENT_OF_AGE_SEX_COHORT') / pl.col('TOTAL_PERCENT')).alias('PERCENT_OF_AGE_SEX_COHORT')]).drop('TOTAL_PERCENT')

    # Write to CSV
    df.write_csv(os.path.join(PROCESSED_FILES, 'immigration', 'state_acs_immigration_age_sex_fractions_2011_2015.csv'))
    print("Dataframe saved as a CSV....")

    return df


def main():
    df = create_and_save_dataframe()
    create_maps(df)

if __name__ == '__main__':
    main()
