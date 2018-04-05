# Applications Program Analytics Project - Software Installed 

## About
This project is an investigation of NASA Applications using data harvested from Bigfix database using Web reports interface.

All analysis code sits in notebooks directory. 

## Install

> Install virtual environment (suggest using 'virtualenv') and install the requirements.

The project requires the use of postgresql database (v10.1+). You will need to have that
up and running before doing anything else. Once it is up, create the 'apps' database using

```bash
> createdb apps

# Then change to the 'db' directory of this project and run
> psql -d apps -f base_schema.psql
 
# Next, gather up harvested csv data files (from itsec-edw bigfix db site) and run  
> python bin/load_db.py -f <csv file> 

# This should load the data into the db. Afterwards, run the following 
> psql -d apps -f create_software_views.psql 

# Now you are ready for running the analysis. Open the <foobar> notebook using jupyter
> jupyter notebook notebooks/App-Analysis-IBM.ipynb 
```

