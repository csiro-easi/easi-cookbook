import boto3
import logging
import pytest
from  tasks.gridded.tile_processor import TileProcessor
from  tasks.gridded.tile_generator import TileGenerator

@pytest.fixture(autouse=True)
def tile_generator():
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
    return

def test_tile_processor():
    logging.basicConfig(level=logging.INFO)

    client = boto3.client('sts')
    userid = client.get_caller_identity()['UserId']

    # Input params for local test
    input_params = [
        {
            "name": "measurements",
            "value": '["red", "green", "blue", "pixel_qa"]',
        },
        {
            "name": "output",
            "value": f'{{"bucket": "eoda-dev-user-scratch", "prefix": "{userid}/geomedian"}}',
        },
        {
            "name": "key",
            "value": "[[41, 125], [41, 128]]",
        },
    ]
    logging.info("Start tile-process...")
    processor = TileProcessor(input_params)
    processor.process_tile()