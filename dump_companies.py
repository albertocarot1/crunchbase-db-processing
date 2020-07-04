import click

from dump.CrunchbaseReader import CrunchbaseReader

@click.command()
@click.option('--keep_going', default=False, show_default=True, is_flag=True, help='Flag. If added, continue on the same file from the latest company. Otherwise restart from the first company.')
@click.option('--out_file', type=str, default="companies.json", show_default=True, help='The JSON-line file where the companies will be written')
@click.option('--num_companies', type=int, help='The number of companies to output')
@click.option('-c','--category_codes', type=str, multiple=True, help='Acceptable category_codes. If empty, all category_codes are accepted.')
@click.option('--min_investments', type=int, required=True, help='Minimum amount of total USD investment in a company (as the sum of the disclosed amounts in funding_rounds)')
def extract_csv_to_json(keep_going, out_file, num_companies, category_codes, min_investments):
    """ Extract the csv into a json-line file, with described inputs as parameters. """
    company_reader = CrunchbaseReader(f"data/",
                                      "objects.csv",
                                      "funding_rounds.csv",
                                      "ipos.csv",
                                      "people.csv",
                                      "relationships.csv",
                                      min_investments_usd=min_investments,
                                      num_companies_cap=num_companies,
                                      acc_category_codes=category_codes)

    company_reader.convert_companies(out_file, keep_going=keep_going)


if __name__ == '__main__':
    extract_csv_to_json()