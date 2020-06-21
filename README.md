# InReach Ventures' Technical Interview

Thanks for taking the time to interview with us. 
We're very excited that you're interested in joining us and we hope you find this test challenging in a good way!
Time management and communication are as important as writing code, 
so we expect you to give us a good idea of how long you took to complete this, 
and to ask questions and communicate with us while you work on it.
How long you take is up to you, but our guideline would be 2 hours.

## The Challenge

The objective of this test to see how you do writing clean, tested code as part of a production data pipeline.
We have based this on Crunchbase's famous 2013 dump of their then database.
This dump comes in the shape of CSV files found here: https://drive.google.com/file/d/10Vwc24zkGMwvmFKRgw_tcEHDacoQ98AC/view?usp=sharing.
However, the InReach Machine Learning pipeline is not based on CSV batches, but rather on JSON lines that can be more easily loaded into Machine Learning frameworks.

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

As you can see, you can currently run:
```shell script
pipenv run tests # 1 test case with 2 tests currently pass
pipenv run dump_company 1 # will output simplified JSON for company '1'
```

Make sure these work to ensure you have your `pipenv` environment correctly setup.

## Deliverable

When evaluating your submission, we want to just have to run 2 or 3 commands to be able to see what you have accomplished:
```shell script
docker-compose up # This is optional, just in case you want to use some infrastructure, but bonus points if you don't
pipenv run tests # Hopefully there'll be more tests than the ones already written ;)
pipenv run dump_companies # This you'll need to create
```

Good luck! Please remember to communicate with us while you're working on the project. 
We'll endeavour to be responsive, but remember that working remotely is all about time-management and asynchronous communication.