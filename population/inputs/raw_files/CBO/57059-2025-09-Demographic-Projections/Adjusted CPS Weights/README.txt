# Adjustments to Current Population Survey (CPS) weights to reflect CBO's 'Census Through 2020 + CBO Projection' of the Civilian Noninstitutionalized Population from 2000 to 2025

This data supplements CBO's September 2025 report An Update to the Demographic Outlook, 2025 to 2055.
https://www.cbo.gov/publication/61390

## Contents
-----------------------------------------------------------------------------------------------------
-- FILENAME --				        	-- DESCRIPTION --
-----------------------------------------------------------------------------------------------------
README.txt						This file
Census_Through_2020_Plus_CBO_Projection_Wtfinl.csv	CPS weight adjustment factors (CSV Format)
Census_Through_2020_Plus_CBO_Projection_Wtfinl.dta	CPS weight adjustment factors (DTA Format)
example_code.do						Stata do-file showing how to calculate the adjusted weights

To open the files without being restricted to read-only access, save the zipped archive to a 
local file directory and extract the files to a new directory.

## General Notes ##

All data files are in comma-separated values format.

Column headings are in row 1; data start in row 2.

## How to use ##
Adjusted CPS weights can be calculated by the following steps:

1. Download a CPS extract from IPUMS
Sarah Flood, Miriam King, Renae Rodgers, Steven Ruggles, J. Robert Warren, Daniel Backman, 
Annie Chen, Grace Cooper, Stephanie Richards, Megan Schouweiler, and Michael Westberry. 
IPUMS CPS: Version 12.0 [dataset]. Minneapolis, MN: IPUMS, 2024. https://doi.org/10.18128/D030.V12.0

That extract must contain the variables necessary to merge the weight adjustments: year, age, sex, race, hispan, and nativity

2. Create a 'native_born' indicator variable:
	native born (nativity != 5)
	foreign born (nativity == 5)

4. Merge in the 'Census_Through_2020_Plus_CBO_Projection_Wtfinl' data file by year, age, sex, race, hispan, native_born

5. Multiply the CPS weights (wtfinl) by the wtfinl_delta adjustment factor
