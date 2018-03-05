from typing import Tuple, Dict, Any
from abc import abstractmethod
from enum import Enum

class Feature(Enum):
    ID = "id_number"
    PRICE = "price_nok"
    SQ_M = "square_meter"
    ZIP_CODE = "zip_code"


class RealEstateBase:

    @abstractmethod
    def get_price_ts(self):
        pass

    @abstractmethod
    def get_base_info(self):
        pass

    @abstractmethod
    def update_features(self):
        pass

class RealEstate(RealEstateBase):

    def __init__(self, features: Dict[str, Any]):
        self._base_info = features

    def get_price_ts(self):
        pass

    def get_base_info(self):
        return self._base_info

    def update_features(self, features: Dict[str, Any]):
        self._base_info.update(features)

    def spoonfeed_sql(self) -> Tuple:
        return (self.finn_id, self.address, self.sq_meters, self.price, self.price_pr_sqm)