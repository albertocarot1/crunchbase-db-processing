import csv
import json

from typing import Any, Dict, Optional, TextIO, List, Set

from enum import Enum, auto
from time import time
class CategoryCodes(Enum):
    RED = auto()
    BLUE = auto()
    GREEN = auto()

# The challenge here is to build a 'pipeline' that will convert this dump into a stream of company data, combining data from various different files. The output should be a python script that outputs a json-line file, where each JSON line represents a company:
#
# {
#   "company": {},
#   "people" : [],
#   "funding_rounds": []
# }
# Only acceptable companies should be included, ensuring that they are:
#
# Companies (obviously)  row['entity_type'].strip() == 'Company' or id.startswith("c:"): found_companies += 1 if found_companies == max_companies: break
# The main company, not a child of another entity
# The script should take as input:
#
# the number of companies to output found_companies = 0  if found_companies == max_companies: break
# acceptable category_codes if obj['category_code'] in acc_category_codes
# minimum amount of total $ investment in a company (as the sum of the disclosed amounts in funding_rounds)

def cb_reader(open_file: TextIO):
    """ Get python reader suitable for crunchbase dump csv files. Note that empty cells are noted with a single 'N' """

    return csv.DictReader(open_file, skipinitialspace=True, delimiter=',', quotechar='"', escapechar="\\")


def convert_empty_fields(cb_dict: dict):
    """ Convert string N into None for cleaner data. """
    for key, val in cb_dict.items():
        if val.strip() == 'N':
            cb_dict[key] = None
    return cb_dict

class CrunchbaseReader:
    def __init__(self, project_root:str, objects_file_path: str,
                 funding_rounds_file_path: str,
                 ipos_file_path: str,
                 people_file_path:str,
                 relationships_file_path: str,
                 min_investments_usd: int,
                 num_companies_cap: int = -1,
                 acc_category_codes: CategoryCodes = None,
                 speed_up: bool = False):
        self.objects_file_path = project_root + objects_file_path
        self.funding_rounds_file_path = project_root + funding_rounds_file_path
        self.ipos_file_path = project_root + ipos_file_path
        self.people_file_path = project_root + people_file_path
        self.relationships_file_path = project_root + relationships_file_path
        self.num_companies_cap = num_companies_cap
        self.acc_category_codes = acc_category_codes
        self.min_investments_usd = min_investments_usd
        self.speed_up = speed_up

    def convert_companies(self, output: str):  # -> Optional[Dict[str, Any]]:
        """ Convert companies in the desired JSON output format, first to last in sequential order."""
        with open(self.objects_file_path, 'r') as open_file:
            reader = cb_reader(open_file)
            found_companies = 0
            start = time()
            for i, obj in enumerate(reader):
                if i%1000 == 0:
                    print(f"{i} object rows scanned so far")
                if (obj['entity_type'].strip() == 'Company' or obj['id'].startswith("c:")) and \
                        (self.acc_category_codes is None or obj['category_code'] in self.acc_category_codes) and \
                        obj.get('parent_id', 'N') == 'N':
                    rounds = self.get_investment_rounds(obj['id'])

                    if self.get_total_investment_usd(rounds) >= self.min_investments_usd:

                        entry = {
                          "company": convert_empty_fields(obj),
                          "people" : self.get_people(obj['id']),
                          "funding_rounds": rounds
                        }
                        with open(output, "a+") as open_json:
                            json.dump(entry,open_json)
                            open_json.write('\n')
                        found_companies += 1
                        # if found_companies % 1000 == 0:
                        print(f"{found_companies} companies found in {time() - start} seconds")
                if found_companies == self.num_companies_cap:
                    break

    def get_total_investment_usd(self, rounds: List[dict]):
        """ Return the total $ investment in a company as the sum of the disclosed amounts in funding_rounds."""
        total_amount = 0
        for round in rounds:
            if round['raised_amount_usd'] is not None:
                total_amount += int(round['raised_amount_usd'])
        return total_amount

    def get_investment_rounds(self, company_id: str):
        """ Return a list with all the investment rounds on a company """
        with open(self.funding_rounds_file_path, 'r') as open_file:
            reader = cb_reader(open_file)
            rounds = []
            for round in reader:
                if round['object_id'] == company_id:
                    rounds.append(convert_empty_fields(round))
        return rounds

    def get_people(self, company_id: str):
        # TODO see if people load can clog data (check size relationships, people)
        # First load relationships (starting from the company) and then integrate the data saved in memory with
        # data from people.csv. Check how the performances change by searching for people as soon as the
        # relationship with the company is found (save memory), or together after the whole relationships file has been
        # scanned (less rounds of file, data saved in memory).
        """ Return all the people belonging to a company, merging data from the same person across different files. """
        with open(self.relationships_file_path, 'r') as open_file:
            reader = cb_reader(open_file)
            people_roles = {}
            people_ids = set()
            for rel in reader:
                if rel["'relationship_object_id'"] == company_id:
                    del rel["'id'"]
                    del rel["'relationship_id'"]
                    if people_roles.get(rel["'person_object_id'"]) is not None:
                        people_roles[rel["'person_object_id'"]].append(convert_empty_fields(rel))
                    else:
                        people_roles[rel["'person_object_id'"]] = [convert_empty_fields(rel)]
                    people_ids.add(rel["'person_object_id'"])
        people_obj = self.get_people_from_object(people_ids)
        people_people = self.get_people_from_people(people_ids)
        people = []
        for p_id in people_ids:
            people.append({'roles': people_roles[p_id], **people_obj[p_id], **people_people[p_id]})
            # people[-1]['roles']: list = people_roles[p_id]
        # else:
        #     raise ValueError(f'some person not found in either object or people csv files for company id {company_id}')
        return people

    def get_people_from_object(self, people_ids: Set[str]):
        """ Get a person's data from the specified file, removing the id """
        with open(self.objects_file_path, 'r') as open_file:
            reader = cb_reader(open_file)
            people = {}
            for person in reader:
                if person['id'] in people_ids:
                    people[person['id']] = person
                    if len(people) == len(people_ids):
                        break
        return people

    def get_people_from_people(self, people_ids: Set[str]):
        """ Get a person's data from the specified file, removing the id """
        with open(self.people_file_path, 'r') as open_file:
            reader = cb_reader(open_file)
            people = {}
            for person in reader:
                if person['object_id'] in people_ids:
                    p_id = person['object_id']
                    del person['id']
                    del person['object_id']
                    people[p_id] = person
                    if len(people) == len(people_ids):
                        break
        return people

    def company(self, company_id: str) -> Optional[Dict[str, Any]]:
        """ Return a company given its id """
        with open(self.objects_file_path, 'r') as open_file:
            reader = cb_reader(open_file)
            for obj in reader:
                if obj['id'] == f'c:{company_id}':
                    return obj
        return None
