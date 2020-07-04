import csv
import json
import platform
import subprocess
from copy import deepcopy
from json import JSONDecodeError
from time import time
from typing import Any, Dict, Optional, TextIO, List, Set


def cb_reader(open_file: TextIO):
    """ Get python reader suitable for crunchbase dump csv files. Note that empty cells are noted with a single 'N' """

    return csv.DictReader(open_file, skipinitialspace=True, delimiter=',', quotechar='"', escapechar="\\")


def convert_empty_fields(cb_dict: dict) -> dict:
    """ In a non-nested dict, convert string N into None for cleaner data. """
    for key, val in cb_dict.items():
        if val.strip() == 'N':
            cb_dict[key] = None
    return cb_dict


def get_total_investment_usd(rounds: List[dict]) -> int:
    """ Return the total $ investment in a company as the sum of the disclosed amounts in funding_rounds."""
    total_amount = 0
    for round in rounds:
        if round['raised_amount_usd'] is not None:
            total_amount += int(round['raised_amount_usd'])
    return total_amount


def get_last_line(f: str, offset: int = 0) -> dict:
    """
    Get the last JSON line of a JSON-line file, and convert it in a dictionary.
    Raises an error if the system is Windows (not supported by the script)
    :param f: path to the json-line file
    :param offset: lines to skip from the bottom
    :return: Last JSON line of the file, converted to a dictionary
    """
    if platform.system() == "Windows":
        raise ValueError("Cannot continue from file when running this script on Windows.")
    with subprocess.Popen(['tail', '-n', str(1 + offset), f], stdout=subprocess.PIPE) as proc:
        lines = proc.stdout.readlines()
    try:
        last_line = json.loads(lines[0].decode())
    except JSONDecodeError:
        return get_last_line(f, offset + 1)
    return last_line


class CrunchbaseReader:
    """ Class to read the CrunchBase multi-csv database and extract companies from it. """

    def __init__(self, project_root: str, objects_file_path: str,
                 funding_rounds_file_path: str,
                 ipos_file_path: str,
                 people_file_path: str,
                 relationships_file_path: str,
                 min_investments_usd: int,
                 num_companies_cap: int = -1,
                 acc_category_codes: List[str] = None,
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

    def convert_companies(self, output: str, keep_going: bool = False):
        """
        Convert companies in the desired JSON output format, first to last in sequential order.
        :param output: JSON file where to write the companies
        :param keep_going: For a json file already filled with some companies, continue writing companies on that file starting from the latest found.
        """
        if keep_going:
            last = get_last_line(output)
            last_id = last['company']['id']
        else:
            with open(output, "w"):  # reset file
                pass
        skip_lines = keep_going
        print("Companies iteration started...")
        with open(self.objects_file_path, 'r') as open_file:
            reader = cb_reader(open_file)
            found_companies = 0
            start = time()
            for i, obj in enumerate(reader):
                if skip_lines:
                    if obj['id'] == last_id:
                        skip_lines = False
                    continue
                if (obj['entity_type'].strip() == 'Company' or obj['id'].startswith("c:")) and \
                        (not self.acc_category_codes or obj['category_code'] in self.acc_category_codes) and \
                        obj.get('parent_id', 'N') == 'N':

                    rounds = self.get_investment_rounds(obj['id'])

                    if get_total_investment_usd(rounds) >= self.min_investments_usd:
                        entry = {
                            "company": convert_empty_fields(obj),
                            "people": self.get_people(obj['id']),
                            "funding_rounds": rounds
                        }

                        with open(output, "a+") as open_json:
                            json.dump(entry, open_json)
                            open_json.write('\n')
                        found_companies += 1

                        print(f"{found_companies} companies found in {time() - start} seconds")
                if found_companies == self.num_companies_cap:
                    break
        print("Companies extraction completed.")

    def get_investment_rounds(self, company_id: str) -> List[dict]:
        """ Return a list with all the investment rounds on a company, given its id. """
        with open(self.funding_rounds_file_path, 'r') as open_file:
            reader = cb_reader(open_file)
            rounds = []
            for round in reader:
                if round['object_id'] == company_id:
                    rounds.append(convert_empty_fields(round))
        return rounds

    def get_people(self, company_id: str):
        """
            Given a company id, return all the people belonging to that company,
            merging data from the same person across different files.
        """
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
        return people

    def get_people_from_object(self, people_ids: Set[str]) -> Dict[str, dict]:
        """
            Get people data (given their ids) from the 'object' file.
            Ids not found return a dictionary with the person's fields empty.
        """
        with open(self.objects_file_path, 'r') as open_file:
            reader = cb_reader(open_file)
            people = {}
            empty_person = {key: None for key in reader.fieldnames}
            for person in reader:
                if person['id'] in people_ids:
                    people[person['id']] = convert_empty_fields(person)
                    if len(people) == len(people_ids):
                        break
            for p_id in people_ids:
                if people.get(p_id) is None:
                    person = deepcopy(empty_person)
                    person['id'] = p_id  # fill only the id field
                    people[p_id] = empty_person
        return people

    def get_people_from_people(self, people_ids: Set[str]) -> Dict[str, dict]:
        """
            Get people data (given their ids) from the 'people' file, removing the id.
            Ids not found retun a dictionary with the person's fields empty.
        """
        with open(self.people_file_path, 'r') as open_file:
            reader = cb_reader(open_file)
            people = {}
            empty_person = {key: None for key in reader.fieldnames}
            for person in reader:
                if person['object_id'] in people_ids:
                    p_id = person['object_id']
                    del person['id']
                    del person['object_id']
                    people[p_id] = convert_empty_fields(person)
                    if len(people) == len(people_ids):
                        break
            for p_id in people_ids:
                if people.get(p_id) is None:
                    people[p_id] = empty_person
        return people

    def company(self, company_id: str) -> Optional[Dict[str, Any]]:
        """ Return a company given its id """
        with open(self.objects_file_path, 'r') as open_file:
            reader = cb_reader(open_file)
            for obj in reader:
                if obj['id'] == f'c:{company_id}':
                    return obj
        return None
