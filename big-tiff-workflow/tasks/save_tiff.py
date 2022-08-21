import logging
import xarray as xr
import s3fs
import datacube.utils.cog

def save_tiff(output_prefix, dataset_uris, workflow_name, model_suffix):
    s3 = s3fs.S3FileSystem()

    tiles = list()
    for uri in dataset_uris:
        logging.info(f"uri:{uri}")
        outputs = s3.glob(f"{uri}/output_*")
        logging.info(f"outputs:{outputs}")
        for output in outputs:
            logging.info(f"output:{output}")
            tile = xr.open_dataset('s3://'+output, engine="zarr")
            tiles.append(tile)

    actual_result = xr.combine_by_coords(tiles)
    logging.info(f"Actual_result:{actual_result}")

    for var_name, array in actual_result.data_vars.items():
        out_file_name = var_name.replace(model_suffix, '')
        result_file = datacube.utils.cog.write_cog(geo_im=array, fname=f'/tmp/{var_name}.tiff', overwrite=True)
        s3.put_file(result_file,f"{output_prefix}/{workflow_name}/{out_file_name}.tif")
    # clean up result zarrs
    for uri in dataset_uris:
        logging.info(f"uri:{uri}")
        s3.rm(uri,recursive=True)