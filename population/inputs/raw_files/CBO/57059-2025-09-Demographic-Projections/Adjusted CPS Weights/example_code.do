clear all

cd "DIRECTORY" // Replace with data directory here

use  "DIRECTORY"  // CPS Extract from IPUMS


* NOTE: native_born includes the following values of the 'nativity' variable:
* 'unknown'
* 'native-born: both parents native-born'
* 'native-born: father foreign, mother native'
* 'native-born: mother foreign, father native'
* 'native-born: both parents foreign'
assert nativity != .
gen native_born = 1 if nativity != 5 // 
replace native_born = 0 if nativity == 5 // nativity == 'foreign born'

* Merge in adjusted CPS weights
merge m:1 year age sex race hispan native_born using "DIRECTORY\Census_Through_2020_Plus_CBO_Projection_Wtfinl.dta"

gen adj_wtfinl = wtfinl * wtfinl_delta
