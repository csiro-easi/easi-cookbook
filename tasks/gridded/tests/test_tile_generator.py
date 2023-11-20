import logging
import pytest
import json
import os
from  tasks.gridded.tile_generator import TileGenerator

# Input params for local test
# TODO Should really have a range of fixtures to trial
# TODO @pytest.parameterize is useful for testing different input/result combinations on a single test.

input_params = [
    {
        "name": "product",
        "value": "s2_l2a",
    },
    {
        "name": "odc_query",
        "value": '{ "output_crs": "EPSG:3577", "resolution": [-20, 20], '
        '"group_by": "solar_day" }',
    },
    {
        "name": "roi",
        "value": '{"time_start": "1901-01-01", "time_end": "2023-12-31", '
        '"boundary": { "type": "Polygon", "crs": { "type": "name", "properties": '
        '{ "name": "EPSG:4326" } }, "coordinates": [ [ [ 0.39683423961537, '
        "0.564988853107963 ], [ 0.39683423961537,-45.895066027070058 ], "
        "[ 179.7294526289578,-45.895066027070058 ], [ 179.7294526289578, "
        "0.564988853107963 ], [ 0.39683423961537, 0.564988853107963 ] ] ] } }",
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

def test_tile_generator():
    logging.basicConfig(level=logging.DEBUG)

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
