# TODO Dask Workers used for tile-process need to have the same python environment as the client
# ! Usually this is achieved by using the same Dask image on client (in this case the argo tile-process step)
# ! and the dask-workers.
# ! At the moment this argo step uses the default easi-dask image and contains the primary script for
# ! execution. It will be pickled for execution on the workers automatically.
# ! This means the argo template contains some quite detailed python code and is
# ! more difficult to maintain.

import functools
import logging
import os
import sys
from math import floor
import json
import pickle
from urllib.parse import urlparse

import boto3
from dask.distributed import get_client, secede, rejoin
from dask.distributed import Client
from dask_gateway import Gateway
from dask_gateway.auth import JupyterHubAuth
from datacube.utils.aws import configure_s3_access
from botocore.credentials import RefreshableCredentials

from botocore.exceptions import ClientError
from datacube.api import GridWorkflow
from datacube.model import GridSpec
from datacube.utils import geometry, masking
from datacube.utils.rio import configure_s3_access

import zarr

import numpy as np
import xarray as xr

# Define all Python Functions that will be used
def load_from_grid(key, product_cells, measurements):
    ds_for_key = []
    for pc in product_cells:
        cell = pc.get(key)
        # TODO is tile buffering needed for nbic?
        # buffer the tiles for use with function
        cell.geobox=cell.geobox.buffered(90,90)
        # All geoboxes for the tiles are the same shape
        # Use this for the chunk size in dask so each tile spatially is a single chunk
        # note that the geobox resolution is in y,x order
        # Note also the chunk size is the buffered tile size, not the original tile size
        # if this isn't accounted for the task graph blows out with the number of tiny slivers
        # that occur in the resulting tiny chunks around the edges
        chunk_dim = cell.geobox.shape
        chunks = {"x": chunk_dim[1], "y": chunk_dim[0]}
        ds_for_key.append(
            GridWorkflow.load(
                cell,
                measurements=measurements,
                dask_chunks=chunks,
                skip_broken_datasets=True,
            )
        )
    # TODO Tune the temporal chunking
    return xr.concat(ds_for_key, dim="time").sortby("time").chunk({"time": 5})

## set up mask for screening out Landsat data and normalize Landsat reflection values
def MaskNormalize(dataset, SR_bands):

    # Identify pixels that are either "valid", "water" or "snow"
    cloud_free_mask = masking.make_mask(
        dataset.pixel_qa, water="land_or_cloud", clear="clear", nodata=False
    )

    # Set all nodata pixels to `NaN`:
    # float32 has sufficient precision for original uint16 SR_bands and saves memory
    SR_masked = masking.mask_invalid_data(
        dataset[SR_bands].astype("float32", casting="same_kind")
    )  #  remove the invalid data on Surface reflectance bands prior to masking clouds
    dataset_masked = SR_masked.where(cloud_free_mask)

    # Normalise reflectance values
    # From: https://prd-wret.s3.us-west-2.amazonaws.com/assets/palladium/production/atoms/files/LSDS-1619_Landsat8-C2-L2-ScienceProductGuide-v2.pdf
    normalised_sr = dataset_masked * 0.0000275 + -0.2

    return normalised_sr

def get_NBR(nir, swir2):
    return (nir - swir2) / (nir + swir2)

def process_tiles(key, product_cells):
    # Set the measurements/bands to load
    # For this analysis, we'll load the red, green, blue and near-infrared bands
    SR_bands = ["nir", "swir2"]
    QA_bands = ["pixel_qa"]
    measurements = SR_bands + QA_bands

    # This will pad the tiles
    dataset = load_from_grid(key, product_cells,measurements)

    #### Normalize bands
    normalised_sr = MaskNormalize(dataset,SR_bands)

    ## Calculate an annual veg index
    NBR_index =  get_NBR(normalised_sr.nir, normalised_sr.swir2)

    #### Calculte an annual mean for each band
    yearly_mean_normalised_sr_nbr = NBR_index.resample(time="1Y").mean()

    # Drop nulls
    features = yearly_mean_normalised_sr_nbr.fillna(0)

    # rechunk ?
    features = features.chunk(chunks={'time': -1, 'y': 256, 'x': 256})

    # TODO rim off the buffer?
    result = result.isel(x=slice(3,-3),y=slice(3,-3)).chunk(chunks={"x": 1024, "y": 1024})

    return result

## Dask Cluster related functions
# TODO Dask cluster management can be done directly in the argo workflow by starting a scheduer and attaching worker pods
# Will obtain or create cluster_index cluster from the gateway - be sure to set the cluster_index of you are using more than one from the gateway_list_clusters cell above
def get_cluster_client(min_workers, max_workers, gateway: Gateway, cluster_name=None, new=True, options=None, adapt=False):
    if new:
        if cluster_name!=None:
            c = gateway.connect(cluster_name)
            c.shutdown()

        cluster_name = gateway.submit(cluster_options=options) # This is non-blocking, all clusters will be requested simultaneously

    cluster = gateway.connect(
        cluster_name
    )  # This will block until the cluster enters the running state
    if adapt:
        logging.info("Apapt")
        cluster.adapt(minimum=min_workers, maximum=max_workers)
    else:
        logging.info("scale")
        cluster.scale(min_workers)

    client = cluster.get_client(set_as_default=True)

    return client, cluster

def configure_client(client, run_prefix, filenames):
    configure_s3_access(aws_unsigned=False, requester_pays=True, client=client)

    def _worker_object_upload(dask_worker, bucket, prefix, filenames):
        # TODO There shoudl be another way to get this worker space via an API
        worker_path = [s for s in sys.path if "/dask-worker-space/" in s][
            0
        ]  # should only be one that matches

        s3 = boto3.client("s3")
        for filename in filenames:
            full_path = os.path.join(worker_path, filename)
            logging.info(f"download file: {bucket}, {prefix}/{filename}")
            r = s3.download_file(bucket, f'{prefix}/{filename}', full_path)
            logging.info(f"r")

    components = urlparse(run_prefix)
    bucket = components.netloc
    prefix = components.path[1:] # strip leading /

    logging.info(f"Register worker upload callback: {bucket}, {prefix}, {filenames}")

    client.register_worker_callbacks(
        setup=functools.partial(
            _worker_object_upload,
            bucket=bucket,
            prefix=f"{prefix}",
            filenames=filenames,
        )
    )

def compute_result(result,priority):
    client = get_client()

    futures = client.compute(result, priority=priority, retries=3)
    secede()
    r = client.gather(futures)
    rejoin()
    return r

def tile_process(address, api_token, dask_image, run_prefix, keys, product_cells, filenames, workflow_name, pod_name):
    # Start the Cluster - potentially takes a few minutes so begin early and get on with
    # other tasks locally before blocking and waiting for workers
    # TODO Could be replaced by direct argo creation of a cluster in the workflow (no dask-gateway)
    auth = JupyterHubAuth(api_token=api_token)
    gateway = Gateway(address=address, auth=auth)

    # Obtain AWS credentials from the serviceAccount assigned to this pod
    # Refresh if necessary and obtain the frozen version for use in the Dask Workers
    credentials = configure_s3_access(aws_unsigned=False,requester_pays=True)

    if credentials and isinstance(credentials, RefreshableCredentials):
        if credentials.refresh_needed():
            credentials = configure_s3_access(aws_unsigned=False,requester_pays=True)
    if isinstance(credentials, RefreshableCredentials):
        logging.info("%s seconds remaining", str(credentials._seconds_remaining()))

    creds = credentials.get_frozen_credentials()

    options = gateway.cluster_options()
    options.node_selection = "worker8x"
    options.worker_cores = 16
    options.worker_memory = 56
    options.image = dask_image
    options.aws_session_token = creds.token
    options.aws_access_key_id = creds.access_key
    options.aws_secret_access_key = creds.secret_key

    logging.info("Creating cluster...")
    # This is a rule of thumb
    min_workers = min((300//6),len(keys) * 10) # Avoid current AWS core limit = 300//parallelism
    max_workers = min((300//6),len(keys) * 50)
    adapt=True # No real need for an adaptive cluster with this workload
    if not adapt:
        min_workers = max_workers
    client, cluster = get_cluster_client(gateway=gateway, min_workers=min_workers, max_workers=max_workers, options=options, new=True, adapt=adapt )
    logging.info("Cluster created")

    configure_client(client, filenames=filenames, run_prefix=run_prefix)
    logging.info(client.dashboard_link)

    context_id = f'{workflow_name}/{pod_name}'

    storage_prefix = f'{run_prefix}/{context_id}'

    results = list()
    for key in keys:
        results.append(process_tiles(key, product_cells=product_cells, filenames=filenames))
    logging.info(results)

    # Wait for minimum workers
    # logging.info("Waiting for workers")
    # client.wait_for_workers(n_workers=min_workers)

    logging.info("Submitting to cluster")
    # priority = list(reversed(range(len(results))))

    # futures = client.map(compute_result, results, priority)

    # logging.info("Waiting for results...")
    # results = client.gather(futures)
    rslts = list()
    for rslt in results:
        a_rslt = rslt.compute(retries=3)
        rslts.append(a_rslt)

    results = rslts

    logging.info(results)

    # Shutdown the cluster
    client.close()
    cluster.shutdown()
    logging.info("Cluster shutdown")

    ### Save the results for this workflow
    # TODO Minor improvement would be to make this run in parallel as the futures complete
    for i,r in enumerate(results):
        out_uri = f'{storage_prefix}/output_{i}'
        logging.info(out_uri)
        store = zarr.storage.FSStore(
                    out_uri, normalize_keys=False
                )
        r.to_zarr(store=store)

    logging.info(f"Results stored for: {storage_prefix}")
    with open('/tmp/dataset_uri.txt','w') as outfile:
        outfile.write(storage_prefix)

#------------------------------------------ Main Script ------------------------------------------#
logging.basicConfig(level=logging.INFO)
logging.info("Start tile-process...")

# Resolve the Area-of-Interest ID from the passed 'roi' JSON, exit if not passed or not valid
roi_json='{{inputs.parameters.roi}}'
roi = json.loads(roi_json)

if not "aoi_id" in roi:
    sys.exit(f"Exiting: No region of interest was passed.")

aoi_id = roi['aoi_id']
if not aoi_id or not isinstance(aoi_id, int):
  sys.exit(f"Exiting: Region of interest is blank or not a valid integer")

# Resolve the filename names to run for the given Area-of-Interest
filenames = list()
filenames_json = '{{inputs.parameters.filenames}}'
filenames = json.loads(filenames_json)
for filename in filenames['filename-names']:
  filenames.append(filename + filenames['filename-aoi-prefix'] + str(aoi_id) + filenames['filename-suffix'])
logging.info(f"filenames to run: {filenames}")

with open('/root/.jhub-api-token','r') as f:
    api_token = f.read()

address = '{{inputs.parameters.dask_gateway_address}}'
dask_image = '{{workflow.parameters.dask_image}}'

run_prefix = '{{inputs.parameters.run_prefix}}'

key_json ='{{inputs.parameters.key}}'
key_array = json.loads(key_json)

with open('/tmp/product_cells.pickle','rb') as f:
    product_cells = pickle.load(f)

keys = [tuple(k) for k in key_array]
logging.info(f"Keys: {[keys]}")

workflow_name = '{{workflow.name}}'
pod_name = '{{pod.name}}'
logging.info(f'context_id: {workflow_name}/{pod_name}')

tile_process(address, api_token, dask_image, run_prefix, keys, product_cells, filenames, workflow_name, pod_name)

logging.info("Completed tile processing.")