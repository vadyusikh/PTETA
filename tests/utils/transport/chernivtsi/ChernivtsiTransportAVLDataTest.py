import json

import pytest
from transport.chernivtsi.ChernivtsiTransportAVLData import ChernivtsiTransportAVLData

import pytest


@pytest.fixture()
def response_row():
    json_str = """{"id": 335, "imei": "355227046451530", "name": "H17", 
    "stateCode": "used", "stateName": "used", 
    "lat": 48.32443166666667, "lng": 25.93422, 
    "speed": 0.0, "orientation": 96.56, 
    "gpstime": "2022-10-10 21:28:16", 
    "routeId": 7, "routeName": "6", 
    "routeColour": "sienna", 
    "inDepo": true, "busNumber": "1147", 
    "perevId": 1, "perevName": "Денисівка", 
    "remark": "1147 DNSNTNK", "online": true, "idBusTypes": 1, 
    "response_datetime": "2022-10-10 21:28:20"}"""
    return json.loads(json_str)


@pytest.fixture()
def avl_data_from_response_row() -> ChernivtsiTransportAVLData:
    return ChernivtsiTransportAVLData.from_response_row(response_row())


def test_minimum_functionality(response_row):
    obj = ChernivtsiTransportAVLData.from_response_row(response_row)


# def test_invariance():
#     pass
#
#
# def test_directional():
#     pass
#
#
# def test_overfit_batch(trainer_with_one_batch: Trainer):
#     train_result = trainer_with_one_batch.train()
#     metrics = train_result.metrics
#     assert metrics["train_loss"] < 0.01
#
#
# def test_train_to_completion(config_path: Path):
#     train(config_path=config_path)
#     result_path = Path("/tmp/results")
#     assert result_path.exists()
#     assert (result_path / "pytorch_model.bin").exists()
#     assert (result_path / "training_args.bin").exists()
#     assert (result_path / "all_results.json").exists()
#     assert (result_path / "README.md").exists()
#
