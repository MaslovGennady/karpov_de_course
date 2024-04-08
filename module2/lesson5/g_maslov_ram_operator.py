import logging
from airflow.hooks.http_hook import HttpHook
from airflow.models.baseoperator import BaseOperator


class GMaslovRickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_char_page_count(self):
        """Returns count of page in API"""
        return self.run('api/location').json()['info']['pages']

    def get_char_page(self, page_num: str) -> list:
        """Returns count of page in API"""
        return self.run(f'api/location?page={page_num}').json()['results']


class GMaslovTop3LocationsByResidentsCount(BaseOperator):
    """
    Get info about top 3 locations by residents count
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        """
        getting info about all locations
        """

        hook = GMaslovRickMortyHook('dina_ram')
        all_locations_data = []
        for page in range(hook.get_char_page_count()):
            logging.info(f'reading api page {page + 1}')
            page_data = hook.get_char_page(str(page + 1))
            for location in page_data:
                all_locations_data.append(
                    [
                        location.get('id', ''),
                        location.get('name', ''),
                        location.get('type', ''),
                        location.get('dimension', ''),
                        len(location.get('residents', []))
                    ]
                )

        all_locations_data.sort(key=lambda a: a[4], reverse=True)
        all_locations_data = all_locations_data[0:3]
        all_locations_data = ','.join(map(str, [tuple(location) for location in all_locations_data]))
        logging.info(f'returning all_locations_data=:{str(all_locations_data)}')

        # return to xcom
        return all_locations_data
