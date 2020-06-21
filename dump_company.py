import json
import os
import sys

from dump.CompanyReader import CompanyReader

company_reader = CompanyReader('{}/data/objects.csv'.format(os.path.dirname(os.path.abspath(__file__))))

company = company_reader.company(sys.argv[1])
if company is not None:
    print(json.dumps(company))
    sys.exit(0)
sys.exit(1)
