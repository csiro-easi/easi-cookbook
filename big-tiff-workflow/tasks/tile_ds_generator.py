import datacube
from datacube.utils import geometry
from datacube.api import GridWorkflow
from datacube.model import GridSpec
import simplejson as json   # Needed to parse decimals in JSON w/o errors
import geojson
import shapely
import logging
import pickle
import sys
import boto3
import decimal

def tile_ds_generator(roi, aws_region, size=150000, tiles_per_cluster=1):
  logging.info("Start tile-ds-generator...")

  set_time = (roi["start_date"], roi["end_date"])
  logging.info(f"Time: {set_time}")
  # Set CRS
  # TODO Specify a suitable CRS
  set_crs = "epsg:3577"
  set_resolution = (30, 30)
  group_by = "solar_day"

  query_dict = {
      "time": set_time,
      "group_by": group_by,
  }


  bbox_exists = "bounding_box" in roi
  boundary_exists = "boundary" in roi
  aoi_exists = "aoi_id" in roi and "aoi_table" in roi and "aoi_table_pk" in roi
  # Make sure that at least one of a bounding_box, a boundary, or a area of interest ID exists in roi
  if not any([bbox_exists,boundary_exists,aoi_exists]):
    raise RuntimeError("No bounding_box, aoi_id or boundary polygon exists in roi")
  if sum([bbox_exists,boundary_exists,aoi_exists])>1:
    raise RuntimeError("roi can only have one of: boundary polygon, bounding_box or aoi_id.")
  if bbox_exists:
    bbox = roi['bounding_box']   # [west, south, east, north]
    study_area_lat = (bbox[1], bbox[3])
    study_area_lon = (bbox[0], bbox[2])

    query_dict["latitude"] = study_area_lat
    query_dict["longitude"] = study_area_lon

    logging.info("Study area bbox:")
    logging.info(f"Lat: {study_area_lat}, Lon: {study_area_lon}")
  if boundary_exists:
    try:
      boundary = geojson.loads(json.dumps(roi["boundary"]))
      assert(boundary.is_valid)
    except (TypeError, AttributeError) as e:
      sys.exit(f"{type(e).__name__}: Boundary polygon is not valid geojson")
    except AssertionError as e:
      sys.exit(f"{type(e).__name__}: Boundary polygon is geojson but validation failed. The error was: '{boundary.errors()}'")
    geopolygon = geometry.Geometry(shapely.geometry.shape(boundary), crs=geometry.CRS('EPSG:4327'))
    query_dict["geopolygon"] = geopolygon
  if aoi_exists:
    logging.info(f"ROI: '{roi}'")
    dynamodb = boto3.resource('dynamodb', region_name=aws_region)
    try:
      db_table = dynamodb.Table(roi["aoi_table"])
      table_pk = roi["aoi_table_pk"]
      def get_boundary(aoi_id):
        boundary = {}
        response = db_table.get_item(
          Key={
              table_pk: aoi_id
          }
        )
        if response.get('Item'):
          item = response['Item']
          aoi_json = json.dumps(item['geometry'])
          #logging.info(f"AOI JSON:\n{aoi_json}")
          try:
            boundary = geojson.loads(aoi_json)
            assert(boundary.is_valid)
          except (TypeError, AttributeError) as e:
            sys.exit(f"{type(e).__name__}: Boundary polygon is not valid geojson")
          except AssertionError as e:
            sys.exit(f"{type(e).__name__}: Boundary polygon is geojson but validation failed. The error was: '{boundary.errors()}'")
          logging.info(f"Successfully found AOI ID '{aoi_id}' in AOI database.")
        else:
          logging.info(f"Not able to find '{aoi_id}' in AOI database.")
        return boundary
      aoi_id = roi['aoi_id']
      if (not aoi_id or not isinstance(aoi_id, int)) and (not type(aoi_id).__name__ == 'list'):
        sys.exit(f"Exiting: Region of interest is empty, is not a valid integer or is not a valid list: '{aoi_id}'")
      # Handle aoi_id as list or as string
      if type(aoi_id).__name__ == 'list':
        boundaries = []
        for aoi in aoi_id:
          boundaries.append(geometry.asShape(get_boundary(aoi)))
        geom = cascaded_union(boundaries) # ? I think cascaded_union was replace by unary_union
        boundary = geometry.mapping(geom)
      else:
        boundary = get_boundary(aoi_id)
    except Exception as e:
      logging.info('Error retrieving data from AOI database')
      raise(e)
    bbox = shapely.geometry.shape(boundary).bounds
    geopolygon = geometry.Geometry(shapely.geometry.shape(boundary), crs=geometry.CRS('EPSG:4327'))
    query_dict["geopolygon"] = geopolygon


  t_size = (size, size)
  gs = GridSpec(
      crs=geometry.CRS(set_crs),
      tile_size=t_size,
      resolution=set_resolution,
  )
  dc = datacube.Datacube(app="Time Series")
  gw = GridWorkflow(dc.index, grid_spec=gs)

  # Set the data source
  products = ["landsat5_c2l2_sr", "landsat7_c2l2_sr", "landsat8_c2l2_sr"]
  # grab the cell list for all products and place on cluster
  logging.info("Generating cells from all products and placing on cluster...")
  product_cells = list()
  tile_keys = list()
  for product in products:
      cells = gw.list_cells(product=product, **query_dict)
      product_cells.append(cells)
      tile_keys += [
          *cells
      ]  # Python 3.5+ unpacking generalization syntax. unpacks the dictionary keys into a list. https://www.python.org/dev/peps/pep-0448/
  unique_keys = list(set(tile_keys))
  logging.info(f"Number of Unique Keys: {len(unique_keys)}")
  n = tiles_per_cluster # Number of Tiles to run on a single cluster
  keys = [unique_keys[i:i + n] for i in range(0, len(unique_keys), n)]
  logging.info(f"Number of Partitions '{len(keys)}'")
  with open('/tmp/keys.json','w') as outfile:
      json.dump(keys, outfile)
  # logging.info(f"Product Cells '{product_cells}'")
  with open('/tmp/product_cells.pickle','wb') as outfile:
      pickle.dump(product_cells, outfile)

