import os
from unittest import TestCase
import json
from json import JSONDecodeError

from dump.CrunchbaseReader import CrunchbaseReader, get_total_investment_usd


class TestCrunchbaseReader(TestCase):

    def setUp(self) -> None:
        self.crunchbase_reader = CrunchbaseReader(f"{os.path.dirname(__file__)}/resources/", "objects.csv", "funding_rounds.csv", "ipos.csv", "people.csv", "relationships.csv", 100, num_companies_cap=1)

    def tearDown(self) -> None:
        if os.path.exists('companies_test.json'):
            os.remove('companies_test.json')
        self.crunchbase_reader.convert_companies('resources/companies_test.json')

    def test_convert_companies(self):
        self.crunchbase_reader.convert_companies('companies_test.json')
        self.assertEqual(os.path.exists('companies_test.json'), True)
        with open('companies_test.json') as open_json:
            first_row = json.loads(open_json.readline())
        self.assertEqual("c:1", first_row['company']['id'])

    def test_convert_companies_keep_going(self):
        with open('resources/companies_test.json') as open_json:
            first_row = json.loads(open_json.readline())
            with self.assertRaises(JSONDecodeError):
                second_row = json.loads(open_json.readline())
        self.crunchbase_reader.convert_companies('resources/companies_test.json',keep_going=True)
        with open('resources/companies_test.json') as open_json:
            first_row = json.loads(open_json.readline())
            second_row = json.loads(open_json.readline())
        self.assertEqual("c:5", second_row['company']['id'])
        self.crunchbase_reader.convert_companies('resources/companies_test.json')

    def test_get_investment_rounds(self):
        rounds = self.crunchbase_reader.get_investment_rounds('c:5')
        self.assertEqual("12700000", rounds[1]['raised_amount_usd'])

    def test_get_people_from_object(self):
        people = self.crunchbase_reader.get_people_from_object({"p:65698", "p:65699"})
        self.assertEqual("Person", people["p:65698"]["entity_type"])
        self.assertEqual("Person", people["p:65699"]["entity_type"])

    def test_get_people_from_object_missing(self):
        people = self.crunchbase_reader.get_people_from_object({"p:1"})
        self.assertEqual(None, people["p:1"]["entity_type"])

    def test_get_people_from_people(self):
        people = self.crunchbase_reader.get_people_from_people({"p:5", "p:6"})
        self.assertEqual('Ian', people["p:5"]["first_name"])

    def test_get_people(self):
        people = self.crunchbase_reader.get_people('c:1')
        self.assertEqual("Person", people[0]['entity_type'])

    def test_get_people_missing(self):
        people = self.crunchbase_reader.get_people('c:42')
        self.assertEqual([], people)

    def test_get_total_investment_usd(self):
        total_amount = get_total_investment_usd([{'raised_amount_usd': "1000000"}, {'raised_amount_usd': "1500000"}])
        self.assertEqual(2500000, total_amount)

    def test_get_company(self):
        company = self.crunchbase_reader.company('10')
        self.assertIsNotNone(company)
        self.assertEqual('c:10', company['id'])

    def test_do_not_get_company(self):
        self.assertIsNone(self.crunchbase_reader.company('213'))