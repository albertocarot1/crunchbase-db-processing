import os
from unittest import TestCase

from dump.CompanyReader import CompanyReader


class TestCompanyReader(TestCase):

    def setUp(self) -> None:
        self.__company_reader = CompanyReader('{}/resources/objects.csv'.format(os.path.dirname(__file__)))

    def test_get_company(self):
        company = self.__company_reader.company('10')
        self.assertIsNotNone(company)
        self.assertEqual('c:10', company['id'])

    def test_do_not_get_company(self):
        self.assertIsNone(self.__company_reader.company('213'))
