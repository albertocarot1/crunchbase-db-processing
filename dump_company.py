import os


from dump.CrunchbaseReader import CrunchbaseReader

company_reader = CrunchbaseReader(f"{os.path.dirname(__file__)}/data/", "objects.csv","funding_rounds.csv", "ipos.csv", "people.csv", "relationships.csv", 100)

company_reader.convert_companies("companies.json")

# company = company_reader.company(sys.argv[1])
# if company is not None:
#     print(json.dumps(company))
#     sys.exit(0)
# sys.exit(1)
