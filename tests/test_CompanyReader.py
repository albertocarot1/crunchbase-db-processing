import os
from unittest import TestCase

from dump.CrunchbaseReader import CrunchbaseReader


class TestCompanyReader(TestCase):

    def setUp(self) -> None:
        self.__company_reader = CrunchbaseReader(f"{os.path.dirname(__file__)}/resources/", "objects.csv","funding_rounds.csv", "ipos.csv", "people.csv", "relationships.csv", 1000000000)

    def test_get_company(self):
        company = self.__company_reader.company('10')
        self.assertIsNotNone(company)
        self.assertEqual('c:10', company['id'])

    def test_do_not_get_company(self):
        self.assertIsNone(self.__company_reader.company('213'))

    def test_get_people(self):
        people = self.__company_reader.get_people('c:1')
        self.assertEqual("p:2", people[0]['object_id'])

    def test_get_investment_rounds(self):
        rounds = self.__company_reader.get_investment_rounds('c:5')
        self.assertEqual("12700000", rounds[1]['raised_amount_usd'])

    def test_get_total_investment_usd(self):
        total_amount = self.__company_reader.get_total_investment_usd('c:4')
        self.assertEqual(45000000, total_amount)