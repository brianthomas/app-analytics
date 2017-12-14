# Applications Program Analytics Project

## About
This project is an investigation of NASA Applications using data harvested from ITSEC-EDW.
All analysis code sits in notebooks directory. 

## Install

> Install virtual environment (suggest using 'virtualenv') and install the requirements.

The project requires the use of postgresql database (v9.6+). You will need to have that
up and running before doing anything else. Once it is up, create the 'bigfix' database using

> createdb bigfix
 
Gather up harvested csv data files (from itsec-edw bigfix db site) and run  

> python db/create_db.py -f <csv files> 

This should create the base schema and load the data into these tables. Afterwards, run 
the following 

> psql -d bigfix -f create_view.psql create_center_results.psql create_os_results.psql create_aces_results.psql

Then do all of the remaining files..
> psql -d bigfix -f *.psql

Now you are ready for running the analysis. Open the <foobar> notebook using jupyter

> jupyter notebook notebooks/<foobar>.ipynb 

