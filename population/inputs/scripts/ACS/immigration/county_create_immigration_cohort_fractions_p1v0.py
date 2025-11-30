import os

import geopandas as gpd
import pandas as pd

from matplotlib import pyplot as plt


BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'
DATABASES = os.path.join(BASE_FOLDER, 'inputs\\databases')
FIGURES = os.path.join(BASE_FOLDER, 'figures', 'p1v01', 'immigration', 'county_fractions')
GEOSPATIAL = 'D:\\OneDrive\\ICLUS_v3\\geospatial'


def retrieve_sex_weights():
    print("Processing sex weights...")

    # con = sqlite3.connect(ACS_DB)
    # query = 'SELECT * FROM acs_immigration_weights_sex_2011_2015'
    # df = pd.read_sql(sql=query, con=con)
    # con.close()
    csv_file = os.path.join(DATABASES, 'acs_immigration_weights_sex_2011_2015.csv')
    df = pd.read_csv(csv_file)

    df = df.melt(id_vars=['DESTINATION_FIPS'], var_name='SEX', value_name='VALUE')
    df = df.set_index(keys=['DESTINATION_FIPS', 'SEX'])

    return df


def retrieve_age_weights():
    print("Processing age weights...")

    # con = sqlite3.connect(ACS_DB)
    # query = 'SELECT * FROM acs_immigration_weights_age_2011_2015'
    # df = pd.read_sql(sql=query, con=con)
    # con.close()
    csv_file = os.path.join(DATABASES, 'acs_immigration_weights_age_2011_2015.csv')
    df = pd.read_csv(csv_file)

    df = (df.melt(id_vars=['DESTINATION_FIPS'],
                 var_name='AGE',
                 value_name='VALUE')
            .set_index(keys=['DESTINATION_FIPS', 'AGE']))

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
    df['PERCENT_OF_AGE_SEX_COHORT'] = round(df['PERCENT_OF_AGE_SEX_COHORT'] * 1000, 1)
    states = read_state_shapefile()
    for age in df.AGE.unique():
        for sex in df.SEX.unique():
            cohort = df.query('AGE == @age & SEX == @sex')
            temp = gdf.merge(right=cohort, how='right', on='GEOID')

            temp.plot(column='PERCENT_OF_AGE_SEX_COHORT',
                      scheme='FisherJenks',
                      k=5,
                      cmap='plasma',
                      legend=True,
                      legend_kwds={'bbox_to_anchor': (0.18, 0.3),#(1, 0.375),
                                   'fontsize': 'x-small',
                                   'fancybox': True,
                                   'title': 'x 10^3'},
                      missing_kwds={'color': 'black'})
            states.boundary.plot(ax=plt.gca(), edgecolor='lightgray', linewidth=0.2)
            plt.gca().set_xlim(-2371000, 2278000)
            plt.gca().set_ylim(246000, 3186000)
            plt.gca().axis('off')
            plt.title(label=f"2011-2015 immigration fractions:\n {sex}, {age}")
            plt.tight_layout()

            # simplify the legend labels
            try:
                for label in plt.gca().get_legend().get_texts():
                    upper_bound = label.get_text().split(',')[-1]
                    label.set_text(f'< {upper_bound}')
            except AttributeError:
                pass

            # plt.show()
            fn = f'county_immigration_fractions_{sex}_{age}.png'
            plt.savefig(os.path.join(FIGURES, fn), dpi=300)
            plt.clf()


def create_and_save_dataframe():
    sex = retrieve_sex_weights()
    age = retrieve_age_weights()

    df = sex.mul(other=age, axis='index').reset_index()
    df['AGE'] = df.AGE.astype(int)

    df['AGE_SEX_SUM'] = df.groupby(by=['AGE', 'SEX'], as_index=False)['VALUE'].transform('sum')
    df['PERCENT_OF_AGE_SEX_COHORT'] = df['VALUE'] / df['AGE_SEX_SUM']

    df = df.rename(columns={'DESTINATION_FIPS': 'GEOID'})
    df = df[['GEOID', 'AGE', 'SEX', 'PERCENT_OF_AGE_SEX_COHORT']]
    df.GEOID = df.GEOID.astype(str).str.zfill(5)

    # con = sqlite3.connect(ACS_DB)
    # df.to_sql(name='acs_immigration_age_sex_fractions_2011_2015',
    #           con=con,
    #           if_exists='replace',
    #           index=False)
    # con.close()

    df.to_csv(path_or_buf=os.path.join(DATABASES, 'acs_immigration_age_sex_fractions_2011_2015.csv'),
              index=False)
    print("Dataframe saved as a CSV....")

    return df


def main():
    df = create_and_save_dataframe()
    # create_maps(df)

if __name__ == '__main__':
    main()
