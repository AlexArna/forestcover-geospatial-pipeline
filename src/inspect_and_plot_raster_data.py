import boto3
import pandas as pd
import rasterio
from rasterio.windows import Window
import requests
import numpy as np
import matplotlib.pyplot as plt

# S3 setup
bucket = 'gfw-tropical-tree-cover-bucket'
key = 'Tropical_Tree_Cover_sample.csv'

s3 = boto3.client('s3')
# Download the csv file from S3 directly to memory (as bytes)
obj = s3.get_object(Bucket=bucket, Key=key)
# Read CSV directly from S3
df = pd.read_csv(obj['Body'])

# Inspect CSV
print("First few rows of CSV:")
print(df.head())
print("First few download URLs:")
print(df['half_hectare_download'].head())

# Get the raster URL from the first row of half_hectare_download column
url = df['half_hectare_download'][0]
# https://data-api.globalforestwatch.org/dataset/wri_tropical_tree_cover/v2020/download/geotiff?grid=10/40000&tile_id=00N_010E&pixel_meaning=percent&x-api-key=2d60cd88-8348-4c0f-a6d5-bd9adb585a8c

# Inspect raster metadata/profile
with rasterio.open(url) as src:
    print("Raster profile:")
    print(src.profile)  

# Example raster profile:
'''
{'driver': 'GTiff', 'dtype': 'uint16', 'nodata': 255.0, 'width': 40000, 
'height': 40000, 'count': 1, 'crs': CRS.from_wkt('GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AXIS["Latitude",NORTH],AXIS["Longitude",EAST],AUTHORITY["EPSG","4326"]]'), 
'transform': Affine(0.00025, 0.0, 10.0, 0.0, -0.00025, 0.0), 'blockxsize': 400, 
'blockysize': 400, 'tiled': True, 'compress': 'deflate', 'interleave': 'band'}
'''

# Note: Reading the full raster with shape (40000, 40000) caused memory error:
# numpy._core._exceptions._ArrayMemoryError: Unable to allocate 2.98 GiB for an array 
# with shape (1, 40000, 40000) and data type uint16

# Read a manageable window from raster (5000x5000 window from the upper-left corner).
# rasterio.Env() creates a controlled environment for rasterio/GDAL operations
with rasterio.Env():
    with rasterio.open(url) as dataset:
        print("Raster opened!")
        # Get nodata value
        nodata_value = dataset.nodata
        print("Missing value code:",  nodata_value) # Expected 255.0

        # Define a window: top-left corner, 5000x5000 pixels
        window = Window(col_off=0, row_off=0, width=5000, height=5000)
        print("About to read raster window...")
        try:
            array = dataset.read(1, window=window)
            print("Array shape:", array.shape)
            print("Array dtype:", array.dtype)
        except Exception as e:
            print("Error during read:", e)
            array = None
        if array is not None:
            # Mask nodata: replace nodata values with np.nan
            array_masked = np.where(array == nodata_value, np.nan, array)
            print("Min value:", np.nanmin(array_masked)) # Should be 0
            print("Max value:", np.nanmax(array_masked)) # Should be 100
            print("Average value:", round(np.nanmean(array_masked), 2)) # Should be 89.34
            print("Number of bands/layers:", dataset.count) # Should be 1

            # Plot and save result
            plt.imshow(array_masked, cmap='Greens')
            plt.colorbar(label='Tree Cover (%)')
            # plt.show() is omitted as it is not suitable for EC2 or other headless servers
            # Save the plot to file
            plt.savefig("treecover_window_1.png", dpi=200)
            plt.close()
            print("Plotting complete!")
            
            # Upload plot to S3
            s3_plot_key = 'treecover_window_1.png'
            s3.upload_file("treecover_window_1.png", bucket, s3_plot_key)
            print(f"Plot uploaded to s3://{bucket}/{s3_plot_key}")
        else:
            print("No array data to process.")