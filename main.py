import csv
import json
import requests


class CSVParseToAPI:
    def __init__(self, path, farm_id, xoperation):
        self.path = path
        self.farm_id = farm_id
        self.xoperation = xoperation

    def read_and_sort_csv(self):
        rows = []
        farms_fields = []

        with open(self.path, 'r') as file:
            csvreader = csv.reader(file)
            _ = next(csvreader)

            for row in csvreader:
                stats = [[row[1]], [row[35]], [row[3]]]
                coordinates_json = json.loads(row[4])

                coordinates = []

                for dicts in coordinates_json:
                    coord_list = []
                    for k, v in dicts.items():
                        coord_list.append(v)

                    coordinates.append(coord_list)

                fake_list1 = [coordinates]
                fake_list2 = [fake_list1]
                stats.append(fake_list2)
                rows.append(stats)

        for i in rows:
            if int(i[0][0]) == self.farm_id:
                farms_fields.append(i)
                continue

        return farms_fields

    def post_or_update(self, list_of_fields):
        for i in list_of_fields:
            is_existing_field = requests.get(
                'https://fields-svc.cluster.us.dev.lite.granular.ag/v0/field-attribute/sync-keys',
                headers={
                    "x-operations": self.xoperation
                }

            )
            payload = {"end": "",
                       "farm_id": int(i[0][0]),
                       "field_id": i[1][0],
                       "name": i[2][0],
                       "revision_id": i[1][0],
                       "shape": {"type": "MultiPolygon",
                                 "coordinates": i[3]},
                       "source": "user_gen",
                       "start": "",
                       "zone_id": i[1][0]}

            json_response = is_existing_field.json()
            print(json_response)
            print('А ти не забув ввести Х-operation?')

            if json_response != {}:
                requests.put(
                    'https://fields-svc.cluster.us.dev.lite.granular.ag/v2/field/',
                    headers={
                        "x-operations": self.xoperation
                    },
                    data=json.dumps(payload),
                    params=i[1][0]
                )

            if json_response == {}:
                requests.post(
                    'https://fields-svc.cluster.us.dev.lite.granular.ag/v2/field',
                    headers={
                        "x-operations": self.xoperation
                    },
                    data=json.dumps(payload)
                )


test = CSVParseToAPI('Ups-sample.csv', 4472, 'uuid')
required_list = test.read_and_sort_csv()
test.post_or_update(required_list)
