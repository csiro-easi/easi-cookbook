import logging
import pytest
import json
import os
from  tasks.gridded.tile_generator import TileGenerator



def test_tile_generator():
    logging.basicConfig(level=logging.DEBUG)

    # Input params for local test
    # TODO Should really have a range of fixtures to trial
    # TODO @pytest.parameterize is useful for testing different input/result combinations on a single test.

    input_params = [
        {
            "name": "product",
            "value": "landsat8_c2l2_sr",
        },
        {
            "name": "odc_query",
            "value": '{ "output_crs": "epsg:3085", "resolution": [30, 30], '
            '"group_by": "solar_day" }',
        },
        {
            "name": "roi",
            "value": '{"time_start": "2022-01-01", "time_end": "2022-02-28", '
            '"boundary": { "type": "Polygon", "crs": { "type": "name", "properties": '
            '{ "name": "EPSG:4326" } }, "coordinates": [ [ [ -90.27790030796821, '
            "34.99172934528377 ], [ -90.27790030796821,33.22184045132981 ], "
            "[ -88.08892787385837,33.22184045132981 ], [ -88.08892787385837, "
            "34.99172934528377 ], [ -90.27790030796821, 34.99172934528377 ] ] ] } }",
        },
        {
            "name": "size",
            "value": "61440",
        },
        {
            "name": "tiles_per_worker",
            "value": "2",
        },
        {
            "name": "aws_region",
            "value": "",
        },
    ]

    generator = TileGenerator(input_params)
    generator.generate_tiles()
    logging.info("Completed tile generation.")

    # check results
    assert os.path.exists(TileGenerator.FILEPATH_KEYS) is True
    assert os.path.exists(TileGenerator.FILEPATH_CELLS) is True

    with open(TileGenerator.FILEPATH_KEYS, "rb") as fh:
        keys = json.load(fh)
    assert isinstance(keys, list)
    assert len(keys) > 0
    # TODO should  test the content as well..

    # TODO Unpickle the product cells from file and check content
