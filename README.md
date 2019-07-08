# Google Analytics (Bioregistra)

## Ids
* Identification used to query a the google analytics board

## Metrics
* Aggregates all user activities on a webpage
* When no dimension is set, returned data is an aggregate
* When dimensions are set, returned data can be grouped for clearer insights 
* You must have at least one metric included.
* Only a maximum of 10 metrics can be supplied per query. 
* Majority of metrics can be combined in a query, provided there isn’t any dimension 
* You can combine a metric with another metric that also has specified dimensions, provided that the combination is valid

## Dimensions
* Dimensions are optional
* Groups metrics by common criteria 
* Only a maximum of 7 dimensions can be supplied per query. 
* When sending a query, you can’t include only dimensions.
* You must have at least one metric included.
* Only certain related dimensions can be queried, unrelated dimensions can’t be queried together

## Sort
* Arranges queried data in ascending or descending order based using either metrics or dimensions
* Metric/Dimensions used for sorting, must be same used when querying else errors will occur

## Filters
* Used to constraint the queried data using a metric or dimension

## Start Date and End Date
* Must be specified to enable google analytics return the results for a specified period of time
* Start date format should be either in yyyy-mm-dd or NDaysAgo
* End date format should be either in yyyy-mm-dd, NDaysAgo, yesterday, today

## Relevant Google Analytical Data
* User
* Sessions
* Traffic Sources
* Adwords
* Goal Conversions
* Platform or Devices
* Geo Network
* System
* Page Tracking
* Site Speed
* App Tracking

**Google Analytics Service Account Email: ga-etl-bioregistra@ga-bioregistra-etl.iam.gserviceaccount.com** 






