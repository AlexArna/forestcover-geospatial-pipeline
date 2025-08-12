"""
Script to tile a raster dataset into windows, compute tree cover statistics for each window,
and write the results to a CSV file.
This module:
- Downloads a CSV file from S3 containing raster URLs.
- Loads the raster URL.
- Splits the raster into a grid of windows.
- Reads each window, masks nodata pixels, calculates min/max/mean tree cover,
  and writes results to a CSV.
"""

import boto3
import pandas as pd
import rasterio
from rasterio.windows import Window
import numpy as np
import math
# S3 setup
bucket = 'gfw-tropical-tree-cover-bucket'
key = 'Tropical_Tree_Cover_sample.csv'

s3 = boto3.client('s3')
# Download the csv file from S3 directly to memory (as bytes)
obj = s3.get_object(Bucket=bucket, Key=key)
# Read CSV directly from S3
df = pd.read_csv(obj['Body'])
# Get the raster URL from the first row of half_hectare_download column
url = df['half_hectare_download'][0]

# Window definition example (not used in script, for reference)
# top-left corner, 5000x5000 pixels
# window = Window(col_off=0, row_off=0, width=5000, height=5000)

def create_windows(dataset, nb_of_rows, nb_of_cols):
    """
    Splits a raster dataset into a grid of windows.

    Args:
        dataset (rasterio.DatasetReader): The opened raster dataset.
        nb_of_rows (int): Number of rows in the grid.
        nb_of_cols (int): Number of columns in the grid.

    Returns:
        List[Window]: List of rasterio Window objects, each representing a tile.
    """
    windows_list = []
    # Compute tile size (may be slightly larger than needed to cover raster edge pixels)
    tile_height=math.ceil(dataset.height/nb_of_rows)
    tile_width=math.ceil(dataset.width/nb_of_cols)
    for i in range(nb_of_rows):
        for j in range(nb_of_cols):
            # Compute offsets for current window
            row_off_val = i*tile_height
            col_off_val = j*tile_width
            # Adjust height for last row (ensure coverage to bottom edge)
            if i == nb_of_rows-1:
                height_val=dataset.height-row_off_val
            else:
                height_val=tile_height
            # Adjust width for last column (ensure coverage to right edge)
            if j == nb_of_cols-1:
                width_val=dataset.width-col_off_val
            else:
                width_val=tile_width
            # Create window and add to list
            window = Window(col_off=col_off_val, row_off=row_off_val, \
                    width=width_val, height=height_val)
            windows_list.append(window)
    return windows_list

# Open the raster file, process windows, and write statistics to CSV
with rasterio.Env():
    with rasterio.open(url) as dataset:
        # Create a grid of windows (8x8 tiles)
        windows = create_windows(dataset, 8, 8)
        # Nodata pixel value for masking (255)
        nodata_value = dataset.nodata
        # Optional: print raster shape info
        # print("Shape: ", dataset.count, dataset.height, dataset.width) 1 40000 40000
        id = 1
        columns = "array,minimum_cover,maximum_cover,average_cover"
        # Open output CSV file for writing statistics
        with open("treecover_stats.csv", 'w') as file:
            file.write(columns + '\n')
            for wind in windows:
                # Optional: log window for debugging
                print(wind)
                try:
                    # Read window as numpy array (band 1)
                    array = dataset.read(1, window=wind)
                except Exception as e:
                    print(f"Error reading array of id {id}:", e)
                    continue
                # Mask nodata pixels with np.nan
                array_masked = np.where(array == nodata_value, np.nan, array)
                array_id = "array_" + str(id)
                # Check to avoid errors when the array has only NaN
                if np.all(np.isnan(array_masked)):
                    min_cover = max_cover = avg_cover = None
                else:
                    min_cover = np.nanmin(array_masked)
                    max_cover = np.nanmax(array_masked)
                    avg_cover = round(np.nanmean(array_masked), 2)
                # Write statistics for this window to CSV
                file.write((array_id + "," + str(min_cover) + "," + str(max_cover) + "," + str(avg_cover) + '\n'))
                id += 1
