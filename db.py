import os

from botocore.exceptions import ClientError
import collections
import dateutil
import logging
from decimal import Decimal

logger = logging.getLogger(__name__)

Observation = collections.namedtuple("Observation", ("observation_time", "temp", "pressure",
                                                     "rel_humidity", "wind_speed", "wind_dir", "dew_point"))


class TableWrapper:
    def __init__(self, table):
        self.table = table

    def add_observation(self, observation: Observation):
        try:
            data_to_send = self._convert_data(observation)
            logger.info("data to save %s", data_to_send)
            self.table.put_item(Item=data_to_send)
        except ClientError as err:
            logger.error(
                "Couldn't add observaton %s to table %s. Here's why: %s: %s",
                observation.observation_time, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def _convert_data(self, observation):

        converted_date = dateutil.parser.parse(observation.observation_time)
        converted_date = str(converted_date.timestamp())
        return {
            "observation_time": Decimal(converted_date),
            "temp": Decimal(observation.temp),
            "pressure": Decimal(observation.pressure),
            "rel_humidity": Decimal(observation.rel_humidity),
            "wind_speed": Decimal(observation.wind_speed),
            "wind_dir": Decimal(observation.wind_dir),
            "dew_point": Decimal(observation.dew_point)
        }
