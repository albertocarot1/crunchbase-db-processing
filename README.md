# Crunchbase processing test

The objective of this test to see how you do writing clean, tested code as part of a production data pipeline.
This test is based on Crunchbase's famous 2013 dump of their then database.
This dump comes in the shape of CSV files found here: https://drive.google.com/file/d/10Vwc24zkGMwvmFKRgw_tcEHDacoQ98AC/view?usp=sharing.
However, the data needs to be on JSON lines that can be more easily loaded into Machine Learning frameworks.

The challenge here is to build a 'pipeline' that will convert this dump into a stream of company data, combining data from various different files.
The output should be a python script that outputs a json-line file, where each JSON line represents a company:

```json
{
  "company": {},
  "people" : [],
  "funding_rounds": []
}
```

Only acceptable companies should be included, ensuring that they are:
* Companies (obviously)
* The main company, not a child of another entity

The script should take as input:
* the number of companies to output
* acceptable `category_code`s
* minimum amount of total $ investment in a company (as the sum of the disclosed amounts in `funding_rounds`)

## Getting Started

Firstly you need to get the data and extract it to the right place. 
Download https://drive.google.com/file/d/10Vwc24zkGMwvmFKRgw_tcEHDacoQ98AC/view?usp=sharing and extract it, 
making sure that you now have a `data/` folder in your root directory.

## Deliverable

When evaluating the submission, there have to be 2 or 3 commands to run be able to see what you have accomplished:
```shell script
docker-compose up # This is optional, just in case you want to use some infrastructure, but bonus points if you don't
pipenv run tests # Hopefully there'll be more tests than the ones already written ;)
pipenv run dump_companies # This you'll need to create
```

## Outcome

The following steps can be quickly launched to see the program working as described above:
```shell script
pipenv run tests 
pipenv run dump_companies
```
For the other scenarios, launch the following command to see the script's interface:
```shell script
pipenv run dump_companies_options
```

The script can be used with the full options inside the virtual env.
In order to do that run the commands:
```shell script
pipenv install  # This will install the virtual env and activate it
python dump_companies.py  # Add the desired options, described by pipenv run dump_companies_options, or just by adding --help as an option.
```
