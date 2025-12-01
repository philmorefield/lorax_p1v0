'''
Revised: 2025-03-26

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

from itertools import product

import geopandas as gpd
import pandas as pd

from matplotlib import pyplot as plt


BASE_FOLDER = 'D:\\OneDrive\\lorax_p1v0\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'
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
    df['STFIPS'] = df['COFIPS'].astype(str).str.zfill(5).str[:2]

    return df[['STFIPS', 'POPULATION20', 'HHS']].drop_duplicates()


def apply_state_level_fertility(df):
    # first apply county level fertility; not all cohorts will have values
    na_values = ['Suppressed', 'Not Applicable', 'None', 'Missing', 'Not Available']
    csv = os.path.join(CDC_FILES, 'Natality, 2020-2024, state.csv')
    fert = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
    fert = fert[['State of Residence Code', 'Age of Mother 9 Code', 'Births', 'Female Population', 'Fertility Rate']]
    fert.columns = ['STFIPS', 'AGE_GROUP', 'BIRTHS', 'FEMALE_POPULATION', 'FERTILITY']
    fert.dropna(how='any', inplace=True)

    fert['STFIPS'] = fert['STFIPS'].astype(int).astype(str).str.zfill(2)

    df = df.merge(right=fert[['STFIPS', 'AGE_GROUP', 'FERTILITY']],
                  how='left',
                  on=['STFIPS', 'AGE_GROUP']).drop_duplicates()

    return df.sort_values(by=['STFIPS', 'AGE_GROUP'])


def create_template():
    stfips_all = get_stfips()

    ages = ['15-19',
            '20-24',
            '25-29',
            '30-34',
            '35-39',
            '40-44']

    stfips = list(stfips_all.STFIPS.values)

    df = pd.DataFrame(list(product(stfips, ages)),
                      columns=['STFIPS', 'AGE_GROUP'])

    return df


def main():
    '''
    Not all race/gender/age combinations are available at the county level. Use
    state and then HHS Region rates as needed.
    '''
    # create the template Dataframe that hold all state/age combinations
    df = create_template()

    # state level fertility from CDC
    df = apply_state_level_fertility(df)

    assert not df.isnull().any().any()

    df = df.rename(columns={'STFIPS': 'GEOID'})

    # create_maps(df)
    df.to_csv(path_or_buf=os.path.join(PROCESSED_FILES, 'fertility', 'state_cdc_fertility_2020_2024.csv'), index=False)

    print("Finished!")


if __name__ == '__main__':
    main()
