import datetime
import os

import pandas as pd
import seaborn as sns

from matplotlib import pyplot as plt

BASE_FOLDER = 'D:\\OneDrive\\lorax_p1v0\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\lorax_p1v0\\population'
CENSUS_CSV_PATH = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\Census')
PROJECTIONS_FOLDER = os.path.join(BASE_FOLDER, 'outputs')

FIGURES_FOLDER = os.path.join(BASE_FOLDER, 'figures', 'p1v0', 'components', 'state')

SCENARIO = 'CBO'
YEAR_MIN = 2010
YEAR_MAX = 2050


def get_st_name_to_fips():
    """
    Retrieve a dataframe mapping state names to FIPS codes.

    Returns:
        dataframe: A dataframe with columns for STNAME and STATE (FIPS code).
    """
    csv_folder = os.path.join(CENSUS_CSV_PATH, '2020', 'intercensal')
    csv_fn = 'co-est2020-alldata.csv'
    df = pd.read_csv(os.path.join(csv_folder, csv_fn), encoding='latin-1')
    df = df[['STATE', 'STNAME']].drop_duplicates().reset_index(drop=True)
    df['STATE'] = df['STATE'].astype(str).str.zfill(2)

    return df

def get_pre2020_pop():
    """
    Retrieve the 2010-2020 intercensal population estimate. This is the only
    piece that uses the co-est2020int-pop excel file so it is a standalone
    function.

    Returns:
        dataframe: A 2010-2020 annual time series of total population.
        string: County name.
        string: State name.
    """
    csv_folder = os.path.join(CENSUS_CSV_PATH, '2020', 'intercensal')
    csv_fn = 'co-est2020int-pop.xlsx'
    df = pd.read_excel(os.path.join(csv_folder, csv_fn),
                       skipfooter=6,
                       skiprows=5,
                       names=['COUNTY_STATE', '2010_base'] + [year for year in range(2010, 2021)])
    df[['CTYNAME', 'STNAME']] = df['COUNTY_STATE'].str.split(',', expand=True)
    df['STNAME'] = df['STNAME'].str.strip()
    df = df.drop(columns=['2010_base', 'CTYNAME', 'COUNTY_STATE'])
    df = df.groupby(by='STNAME').sum().reset_index()

    return df


def get_components_of_change_2010_2020():
    """
    Retrieve the components of change for 2010-2020 from the population database.

    Returns:
        dataframe: A dataframe with columns for GEOID, YEAR, BIRTHS, DEATHS,
            INTERNATIONALMIG, DOMESTICMIG and POPULATION.
    """
    # historical population, 2010-2020
    csv_folder = os.path.join(CENSUS_CSV_PATH, '2020', 'intercensal')
    csv_fn = 'co-est2020-alldata.csv'
    df = pd.read_csv(os.path.join(csv_folder, csv_fn), encoding='latin-1')
    df = df.query('COUNTY == 0')  # state-level rows
    df['GEOID'] = df['STATE'].astype(str).str.zfill(2)

    return df


def get_components_of_change_2020_2024():
    """
    Retrieve the components of change for 2020-2024 from the population database.

    Returns:
        dataframe: A dataframe with columns for GEOID, YEAR, BIRTHS, DEATHS,
            INTERNATIONALMIG, DOMESTICMIG and POPULATION.
    """

    # historical population, 2020-2024
    csv_folder = os.path.join(CENSUS_CSV_PATH, '2024', 'intercensal')
    csv_fn = 'co-est2024-alldata.csv'
    df = pd.read_csv(os.path.join(csv_folder, csv_fn), encoding='latin-1')
    df = df.query('COUNTY == 0')  # state-level rows
    df['GEOID'] = df['STATE'].astype(str).str.zfill(2)

    return df


def get_projected_population():
    """
    Retrieve the projected population from the projections database.

    Returns:
        dataframe: A dataframe with columns for GEOID, YEAR, and POPULATION.
    """
    all_proj_pop = pd.read_csv(os.path.join(PROJECTIONS_FOLDER, f'population_by_age_group_sex_{SCENARIO}.csv'))
    all_proj_pop['GEOID'] = all_proj_pop['GEOID'].astype(str).str.zfill(2)

    return all_proj_pop


def get_projected_births():
    """
    Retrieve the projected births from the projections database.

    Returns:
        dataframe: A dataframe with columns for GEOID, YEAR, and BIRTHS.
    """
    all_proj_births = pd.read_csv(os.path.join(PROJECTIONS_FOLDER, f'births_by_age_group_sex_{SCENARIO}.csv'))
    all_proj_births['GEOID'] = all_proj_births['GEOID'].astype(str).str.zfill(2)

    return all_proj_births


def get_projected_migration():
    """
    Retrieve the projected migration from the projections database.

    Returns:
        dataframe: A dataframe with columns for GEOID, YEAR, and MIGRATION.
    """
    all_proj_migration = pd.read_csv(os.path.join(PROJECTIONS_FOLDER, f'migration_by_age_group_sex_{SCENARIO}.csv'))
    all_proj_migration['GEOID'] = all_proj_migration['GEOID'].astype(str).str.zfill(2)

    return all_proj_migration


def get_projected_deaths():
    """
    Retrieve the projected deaths from the projections database.

    Returns:
        dataframe: A dataframe with columns for GEOID, YEAR, and DEATHS.
    """
    all_proj_deaths = pd.read_csv(os.path.join(PROJECTIONS_FOLDER, f'deaths_by_age_group_sex_{SCENARIO}.csv'))
    all_proj_deaths['GEOID'] = all_proj_deaths['GEOID'].astype(str).str.zfill(2)

    return all_proj_deaths


def get_projected_immigration():
    """
    Retrieve the projected immigration from the projections database.
    Returns:
        dataframe: A dataframe with columns for GEOID, YEAR, and IMMIGRATION.
    """
    all_proj_immigration = pd.read_csv(os.path.join(PROJECTIONS_FOLDER, f'immigration_by_age_group_sex_{SCENARIO}.csv'))
    all_proj_immigration['GEOID'] = all_proj_immigration['GEOID'].astype(str).str.zfill(2)

    return all_proj_immigration


def main():

    st_name_to_fips = get_st_name_to_fips()
    all_pre2020pop = get_pre2020_pop()
    all_comp_change_2010_2020 = get_components_of_change_2010_2020()
    all_comp_change_2020_2024 = get_components_of_change_2020_2024()
    all_proj_pop = get_projected_population()
    all_proj_births = get_projected_births()
    all_proj_migration = get_projected_migration()
    all_proj_deaths = get_projected_deaths()
    all_proj_immigration = get_projected_immigration()

    for geoid, stname in st_name_to_fips.values:
        fig = plt.figure(constrained_layout=True)
        gs = fig.add_gridspec(3, 2)

        ######################
        ## TOTAL POPULATION ##
        ######################

        ax_pop = fig.add_subplot(gs[0, :1])

        # historical population, 2010-2020
        pre2020pop = all_pre2020pop.query(f'STNAME == "{stname}"').drop(columns=['STNAME'])
        pre2020pop = pre2020pop.T.reset_index()
        pre2020pop.columns = ['YEAR', 'POPULATION']
        pre2020pop['YEAR'] = pre2020pop['YEAR'].astype(int)

        # historical population, 2020-2024
        columns = ['ESTIMATESBASE2020'] + [f'POPESTIMATE{year}' for year in range(2021, 2025)]
        post2020pop = all_comp_change_2020_2024.query(f'GEOID == "{geoid}"')
        post2020pop = post2020pop[columns].rename(columns={'ESTIMATESBASE2020': 'POPESTIMATE2020'})
        post2020pop = post2020pop.T.reset_index()
        post2020pop.columns = ['YEAR', 'POPULATION']
        post2020pop['YEAR'] = post2020pop['YEAR'].str[-4:].astype(int)
        post2020pop['POPULATION'] = post2020pop['POPULATION']

        histpop = pd.concat([pre2020pop, post2020pop], ignore_index=True)

        # future population
        proj_pop = all_proj_pop.query('GEOID == @geoid').drop(columns=['SEX', 'AGE_GROUP'])
        proj_pop = proj_pop.groupby(by='GEOID').sum().reset_index(drop=True).T.reset_index()
        proj_pop.columns = ['YEAR', 'POPULATION']
        proj_pop['YEAR'] = proj_pop['YEAR'].astype(int)
        proj_pop['POPULATION'] = proj_pop['POPULATION']

        sns.lineplot(x='YEAR', y='POPULATION', data=histpop, linewidth=2, color='gray', legend=False, ax=ax_pop, label='U.S. Census\n(intercensal estimate)')
        sns.lineplot(x='YEAR', y='POPULATION', data=proj_pop, linewidth=2, color='orange', legend=False, ax=ax_pop, label='p1v0 projection')

        plt.title('TOTAL POPULATION')
        ax_pop.set_xticklabels([])
        plt.gca().set_xlabel("")
        plt.gca().set_ylabel("")
        plt.gca().set_xlim(xmin=YEAR_MIN, xmax=YEAR_MAX)
        fig.legend(bbox_to_anchor=(0.925, 0.825))

        ############
        ## BIRTHS ##
        ############

        # historical births
        ax_births = fig.add_subplot(gs[1, :1])

        columns = ['BIRTHS' + str(year) for year in range(2010, 2021)]
        hist_births = all_comp_change_2010_2020.loc[all_comp_change_2010_2020.GEOID == geoid, columns]
        hist_births = hist_births.T.reset_index()
        hist_births.columns = ['YEAR', 'BIRTHS']
        hist_births['YEAR'] = hist_births['YEAR'].str[-4:].astype(int)
        hist_births.loc[hist_births['YEAR'] == 2010, 'BIRTHS'] *= 4
        hist_births['BIRTHS'] = hist_births['BIRTHS']

        # historical births, 2020-2024
        columns = ['BIRTHS' + str(year) for year in range(2020, 2025)]
        post2020_births = all_comp_change_2020_2024.loc[all_comp_change_2020_2024.GEOID == geoid, columns]
        post2020_births = post2020_births.T.reset_index()
        post2020_births.columns = ['YEAR', 'BIRTHS']
        post2020_births['YEAR'] = post2020_births['YEAR'].str[-4:].astype(int)
        post2020_births.loc[post2020_births['YEAR'] == 2020, 'BIRTHS'] *= 4
        post2020_births['BIRTHS'] = post2020_births['BIRTHS']

        # future births
        proj_births = all_proj_births.query('GEOID == @geoid').drop(columns=['SEX', 'AGE_GROUP'])
        proj_births = proj_births.groupby(by='GEOID').sum().reset_index(drop=True).T.reset_index()
        proj_births.columns = ['YEAR', 'BIRTHS']
        proj_births['YEAR'] = proj_births['YEAR'].astype(int)
        proj_births['BIRTHS'] = proj_births['BIRTHS'] / 5

        sns.lineplot(x='YEAR', y='BIRTHS', data=hist_births, linewidth=2, color='gray', legend=False, ax=ax_births)
        sns.lineplot(x='YEAR', y='BIRTHS', data=proj_births, linewidth=2, color='orange', legend=False, ax=ax_births)
        sns.lineplot(x='YEAR', y='BIRTHS', data=post2020_births, linewidth=2, color='gray', legend=False, ax=ax_births)

        plt.title('BIRTHS')
        ax_births.set_xticklabels([])
        ax_births.set_xlabel("")
        ax_births.set_ylabel("")
        plt.gca().set_xlim(xmin=YEAR_MIN, xmax=YEAR_MAX)

        ############################
        ## NET DOMESTIC MIGRATION ##
        ############################

        # historical migration
        ax_migration = fig.add_subplot(gs[1, 1:])

        columns = ['DOMESTICMIG' + str(year) for year in range(2010, 2021)]
        hist_migration = all_comp_change_2010_2020.loc[all_comp_change_2010_2020.GEOID == geoid, columns]
        hist_migration = hist_migration.T.reset_index()
        hist_migration.columns = ['YEAR', 'MIGRATION']
        hist_migration['YEAR'] = hist_migration['YEAR'].str[-4:].astype(int)
        hist_migration.loc[hist_migration['YEAR'] == 2010, 'MIGRATION'] *= 4
        hist_migration['MIGRATION'] = hist_migration['MIGRATION']

        # historical migration, 2020-2024
        columns = ['DOMESTICMIG' + str(year) for year in range(2020, 2025)]
        post2020_migration = all_comp_change_2020_2024.loc[all_comp_change_2020_2024.GEOID == geoid, columns]
        post2020_migration = post2020_migration.T.reset_index()
        post2020_migration.columns = ['YEAR', 'MIGRATION']
        post2020_migration['YEAR'] = post2020_migration['YEAR'].str[-4:].astype(int)
        post2020_migration.loc[post2020_migration['YEAR'] == 2020, 'MIGRATION'] *= 4
        post2020_migration['MIGRATION'] = post2020_migration['MIGRATION']

        # future migration
        columns = [f'NETMIG{year}' for year in range(2029, 2094 + 1, 5)]
        proj_migration = all_proj_migration.loc[all_proj_migration.GEOID == geoid, columns]
        proj_migration = proj_migration.sum().reset_index()
        proj_migration.columns = ['YEAR', 'MIGRATION']
        proj_migration['YEAR'] = proj_migration['YEAR'].str[-4:].astype(int)

        sns.lineplot(x='YEAR', y='MIGRATION', data=hist_migration, linewidth=2, color='gray', legend=False, ax=ax_migration)
        sns.lineplot(x='YEAR', y='MIGRATION', data=proj_migration, linewidth=2, color='orange', legend=False, ax=ax_migration)
        sns.lineplot(x='YEAR', y='MIGRATION', data=post2020_migration, linewidth=2, color='gray', legend=False, ax=ax_migration)

        plt.title('MIGRATION')
        ax_migration.set_xticklabels([])
        ax_migration.set_xlabel("")
        ax_migration.set_ylabel("")
        plt.gca().set_xlim(xmin=YEAR_MIN, xmax=YEAR_MAX)

        ############
        ## DEATHS ##
        ############

        ax_deaths = fig.add_subplot(gs[2, :1])

        # historical deaths, 2010-2020
        columns = ['DEATHS' + str(year) for year in range(2010, 2021)]
        hist_deaths = all_comp_change_2010_2020.loc[all_comp_change_2010_2020.GEOID == geoid, columns]
        hist_deaths = hist_deaths.T.reset_index()
        hist_deaths.columns = ['YEAR', 'DEATHS']
        hist_deaths['YEAR'] = hist_deaths['YEAR'].str[-4:].astype(int)
        hist_deaths.loc[hist_deaths['YEAR'] == 2010, 'DEATHS'] *= 4
        hist_deaths['DEATHS'] = hist_deaths['DEATHS']

        # historical deaths, 2020-2024
        columns = ['DEATHS' + str(year) for year in range(2020, 2025)]
        post2020_deaths = all_comp_change_2020_2024.loc[all_comp_change_2020_2024.GEOID == geoid, columns]
        post2020_deaths = post2020_deaths.T.reset_index()
        post2020_deaths.columns = ['YEAR', 'DEATHS']
        post2020_deaths['YEAR'] = post2020_deaths['YEAR'].str[-4:].astype(int)
        post2020_deaths.loc[post2020_deaths['YEAR'] == 2020, 'DEATHS'] *= 4
        post2020_deaths['DEATHS'] = post2020_deaths['DEATHS']

        # future deaths
        proj_deaths = all_proj_deaths.query('GEOID == @geoid').drop(columns=['AGE_GROUP', 'SEX'])
        proj_deaths = proj_deaths.groupby(by='GEOID').sum().reset_index(drop=True).T.reset_index()
        proj_deaths.columns = ['YEAR', 'DEATHS']
        proj_deaths['YEAR'] = proj_deaths['YEAR'].astype(int)
        proj_deaths['DEATHS'] = proj_deaths['DEATHS'] / 5

        sns.lineplot(x='YEAR', y='DEATHS', data=hist_deaths, linewidth=2, color='gray', legend=False, ax=ax_deaths)
        sns.lineplot(x='YEAR', y='DEATHS', data=proj_deaths, linewidth=2, color='orange', legend=False, ax=ax_deaths)
        sns.lineplot(x='YEAR', y='DEATHS', data=post2020_deaths, linewidth=2, color='gray', legend=False, ax=ax_deaths)

        plt.title('DEATHS')
        ax_deaths.set_xlabel('')
        ax_deaths.set_ylabel('')
        plt.gca().set_xlim(xmin=YEAR_MIN, xmax=YEAR_MAX)

        #####################
        ## NET IMMIGRATION ##
        #####################

        ax_immig = fig.add_subplot(gs[2, 1:])

        # historical immigration, 2010-2020
        columns = ['INTERNATIONALMIG' + str(year) for year in range(2010, 2021)]
        hist_immig = all_comp_change_2010_2020.loc[all_comp_change_2010_2020.GEOID == geoid, columns]
        hist_immig = hist_immig.T.reset_index()
        hist_immig.columns = ['YEAR', 'IMMIGRATION']
        hist_immig['YEAR'] = hist_immig['YEAR'].str[-4:].astype(int)
        hist_immig.loc[hist_immig['YEAR'] == 2010, 'IMMIGRATION'] *= 4
        hist_immig['IMMIGRATION'] = hist_immig['IMMIGRATION']

        # historical immigration, 2020-2024
        columns = ['INTERNATIONALMIG' + str(year) for year in range(2020, 2025)]
        post2020_immig = all_comp_change_2020_2024.loc[all_comp_change_2020_2024.GEOID == geoid, columns]
        post2020_immig = post2020_immig.T.reset_index()
        post2020_immig.columns = ['YEAR', 'IMMIGRATION']
        post2020_immig['YEAR'] = post2020_immig['YEAR'].str[-4:].astype(int)
        post2020_immig.loc[post2020_immig['YEAR'] == 2020, 'IMMIGRATION'] *= 4
        post2020_immig['IMMIGRATION'] = post2020_immig['IMMIGRATION']

        # future immigration
        proj_immig = all_proj_immigration.query('GEOID == @geoid').drop(columns=['AGE_GROUP', 'SEX'])
        proj_immig = proj_immig.groupby(by='GEOID').sum().reset_index(drop=True).T.reset_index()
        proj_immig.columns = ['YEAR', 'IMMIGRATION']
        proj_immig['YEAR'] = proj_immig['YEAR'].astype(int)
        proj_immig['IMMIGRATION'] = proj_immig['IMMIGRATION']

        sns.lineplot(x='YEAR', y='IMMIGRATION', data=hist_immig, linewidth=2, color='gray', legend=False, ax=ax_immig)
        sns.lineplot(x='YEAR', y='IMMIGRATION', data=proj_immig, linewidth=2, color='orange', legend=False, ax=ax_immig)
        sns.lineplot(x='YEAR', y='IMMIGRATION', data=post2020_immig, linewidth=2, color='gray', legend=False, ax=ax_immig)

        plt.title('IMMIGRATION')
        ax_immig.set_xlabel("")
        ax_immig.set_ylabel("")
        plt.gca().set_xlim(xmin=YEAR_MIN, xmax=YEAR_MAX)

        # plt.tight_layout()
        plt.suptitle(t=f'{stname}')
        month = datetime.date.today().month
        day = datetime.date.today().day
        year = datetime.date.today().year
        plt.figtext(x=0.85, y=0.95, s=f'Created: {month}/{day}/{year}', size=5)

        out_fn = f'state_components_of_change_{stname}_p1v0.png'
        plt.savefig(os.path.join(FIGURES_FOLDER, out_fn), dpi=300)

        # plt.show()
        # plt.clf()
        # del fig
        # del gs



if __name__ == '__main__':
    main()
