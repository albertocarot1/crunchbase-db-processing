import csv
from typing import Any, Dict, Optional


class CompanyReader:
    def __init__(self,
                 objects_file_path: str):
        self.__objects_file_path = objects_file_path

    def company(self, company_id: str) -> Optional[Dict[str, Any]]:
        with open(self.__objects_file_path, 'r') as objects_file:
            reader = csv.reader(objects_file, delimiter=',', quotechar='"')
            headers = next(reader, None)
            for obj in reader:
                if obj[0] == 'c:{}'.format(company_id):
                    return dict(zip(headers, obj))
        return None