import firebase_admin
from firebase_admin import credentials, db, storage, firestore
import folium
import numpy as np
import pandas as pd
from scipy.interpolate import griddata
from shapely.geometry import Polygon, Point
from scipy.spatial import ConvexHull
import geopandas as gpd
from concurrent.futures import ThreadPoolExecutor
from firebase_init import get_firestore_client
import os
import shutil
from dbConnection import get_db_connection
from psycopg2 import sql
# Function to fetch all timestamps for a given device
def fetch_timestamps(device_id):
    path = f'/realtimedevices/{device_id}'
    ref = db.reference(path)
    timestamps = ref.get()
    if timestamps:
        return list(timestamps.keys())
    return []

def create_devices_table():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Create devices table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS devices (
                device_id TEXT PRIMARY KEY
            )
        """)

        # Create dates table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dates (
                date_id SERIAL PRIMARY KEY,
                device_id TEXT REFERENCES devices(device_id) ON DELETE CASCADE,
                date TEXT NOT NULL,
                UNIQUE (device_id, date)
            )
        """)

        # Create attribute_data table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS attribute_data (
                attribute_id SERIAL PRIMARY KEY,
                date_id INT REFERENCES dates(date_id) ON DELETE CASCADE,
                attribute_name TEXT NOT NULL,
                csv_path TEXT NOT NULL,
                UNIQUE (date_id, attribute_name)
            )
        """)

        conn.commit()
        print("Tables 'devices', 'dates', and 'attribute_data' created or already exist.")
    except Exception as e:
        print(f"Error creating tables: {e}")
    finally:
        if cursor:
            cursor.close()

# Function to insert or update device data
def insert_or_update_device_data(device_id, date, attribute, csv_path):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Insert or update device
        cursor.execute("""
            INSERT INTO devices (device_id)
            VALUES (%s)
            ON CONFLICT (device_id) DO NOTHING
        """, (device_id,))

        # Insert or update date
        cursor.execute("""
            INSERT INTO dates (device_id, date)
            VALUES (%s, %s)
            ON CONFLICT (device_id, date) DO UPDATE
            SET date = EXCLUDED.date
            RETURNING date_id
        """, (device_id, date))

        date_id = cursor.fetchone()[0]

        # Insert or update attribute data
        cursor.execute("""
            INSERT INTO attribute_data (date_id, attribute_name, csv_path)
            VALUES (%s, %s, %s)
            ON CONFLICT (date_id, attribute_name) DO UPDATE
            SET csv_path = EXCLUDED.csv_path
        """, (date_id, attribute.lower(), csv_path))

        conn.commit()
        print(f"Data inserted/updated for device {device_id}, date {date}, attribute {attribute}.")
    except Exception as e:
        print(f"Error inserting/updating data: {e}")
    finally:
        if cursor:
            cursor.close()





# Function to fetch data from Firebase for a specific device and timestamp
def fetch_data(device_id, timestamp):
    path = f'/realtimedevices/{device_id}/{timestamp}'
    ref = db.reference(path)
    data = ref.get()
    if data:
        if isinstance(data, dict):
            data = [data]
        return pd.DataFrame(data)
    return pd.DataFrame()

# Function to determine the color for phosphorus levels
def get_phosphorus_color(phosphorus):
    if 0 <= phosphorus <= 10.99:
        return "lightyellow"
    elif 11 <= phosphorus <= 20.99:
        return "lightblue"
    elif 21 <= phosphorus <= 40:
        return "blue"
    elif phosphorus > 40:
        return "darkblue"
    return "gray"

def get_nitrogen_color(nitrogen):
    if 0 <= nitrogen <= 10.99:
        return "lightyellow"
    elif 11 <= nitrogen <= 20.99:
        return "lightgreen"
    elif 21 <= nitrogen <= 40:
        return "green"
    elif nitrogen > 40:
        return "darkgreen"
    return "gray"  # Default for out-of-range values

def get_Conductivity_color(Conductivity):
    if 0 <= Conductivity <= 200:
        return "beige"
    elif 200.1 <= Conductivity <= 404.0:
        return "purple"
    elif 405 <= Conductivity <= 800:
        return "orange"
    elif 801 <= Conductivity <= 1600:
        return "darkorange"
    elif Conductivity > 1600:
        return "red"
    return "gray"  # Default for out-of-range values

def get_ph_color(ph):
    if 0 <= ph <= 200:
        return "beige"
    elif 200.1 <= ph <= 404.0:
        return "purple"
    elif 405 <= ph <= 800:
        return "orange"
    elif 801 <= ph <= 1600:
        return "darkorange"
    elif ph > 1600:
        return "red"
    return "gray"  # Default for out-of-range values

def get_moisture_color(moisture):
    if 0 < moisture < 15:
        return "lightcyan"
    elif 15 < moisture <= 30.99:
        return "cyan"
    elif 31 <= moisture <= 60.99:
        return "lightblue"
    elif 61 <= moisture <= 80.99:
        return "blue"
    elif 81 <= moisture <= 100:
        return "darkblue"
    return "gray"  # Default for out-of-range values

def get_potassium_color(k):
    if 0 <= k <= 52.9999999999999:
        return "white"  # Deficient
    elif 53 <= k <= 85:
        return "peachpuff"  # Low
    elif 86 <= k <= 120:
        return "orange"  # Optimum
    elif 121 <= k <= 155:
        return "red"  # High
    elif k > 155:
        return "darkred"  # Excessive
    return "gray"  # Default for out-of-range values

# Function to calculate distance between two coordinates (in meters)
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000  # Radius of Earth in meters
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    d_phi = phi2 - phi1
    d_lambda = np.radians(lon2 - lon1)
    a = np.sin(d_phi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(d_lambda / 2) ** 2
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

def process_attribute(data, device_id, date, attribute, color_function, distance_threshold, output_folder="heatmaps"):
    try:
        # Ensure columns are numeric
        data['latitude'] = pd.to_numeric(data.get('latitude', pd.Series()), errors='coerce')
        data['longitude'] = pd.to_numeric(data.get('longitude', pd.Series()), errors='coerce')
        data[attribute] = pd.to_numeric(data.get(attribute, pd.Series()), errors='coerce')

        # Drop rows with missing or invalid data for the attribute
        data = data.dropna(subset=['latitude', 'longitude', attribute])

        # Calculate mean latitude and longitude
        mean_lat, mean_lon = data[['latitude', 'longitude']].mean()
        data['Distance'] = data.apply(
            lambda row: haversine(row['latitude'], row['longitude'], mean_lat, mean_lon), axis=1
        )

        # Filter rows based on the distance threshold
        filtered_data = data[data['Distance'] <= distance_threshold]

        # Prepare data for heatmap generation
        valid_data = filtered_data[(filtered_data['latitude'] != 0) & (filtered_data['longitude'] != 0)]
        points = valid_data[['latitude', 'longitude']].values
        values = valid_data[attribute].values

        # Check if there are enough valid points
        if len(points) < 5:
            print(f"Skipping heatmap for {attribute}: Not enough valid points.")
            return None  # Return None to indicate insufficient data

        # Generate a grid for interpolation
        grid_x, grid_y = np.mgrid[
            valid_data['latitude'].min():valid_data['latitude'].max():100j,
            valid_data['longitude'].min():valid_data['longitude'].max():100j,
        ]
        grid_z = griddata(points, values, (grid_x, grid_y), method='nearest')

        # Check if the points are not all the same
        lat_diff = np.ptp(points[:, 0])  # Range in latitude
        lon_diff = np.ptp(points[:, 1])  # Range in longitude

        if lat_diff < 1e-5 and lon_diff < 1e-5:
            print(f"Skipping heatmap generation for device {device_id} on {date}: Points are too similar.")
            return None  # Return None to indicate insufficient data

        # Convex hull for boundary
        hull = ConvexHull(points)
        polygon = Polygon([points[vertex] for vertex in hull.vertices])

        # Create grid for interpolation
        grid_lat = np.linspace(points[:, 0].min(), points[:, 0].max(), 100)
        grid_lon = np.linspace(points[:, 1].min(), points[:, 1].max(), 100)
        grid_lon, grid_lat = np.meshgrid(grid_lon, grid_lat)
        grid_points = np.vstack([grid_lat.ravel(), grid_lon.ravel()]).T

        # Filter grid points within convex hull
        grid_within = np.array([point for point in grid_points if polygon.contains(Point(point))])
        if grid_within.size == 0:
            print(f"No grid points within the convex hull for device {device_id} on {date}.")
            return None  # Return None to indicate insufficient data

        grid_lat_within, grid_lon_within = grid_within[:, 0], grid_within[:, 1]
        grid = griddata(points, values, (grid_lat_within, grid_lon_within), method='nearest')

        # Prepare data to save in CSV
        csv_data = []
        for lat, lon, value in zip(grid_lat_within, grid_lon_within, grid):
            if not np.isnan(value):
                color = color_function(value)
                csv_data.append({
                    "latitude": lat,
                    "longitude": lon,
                    attribute: value,
                    "color": color
                })

        # Save the CSV data
        csv_file = f"{output_folder}/{device_id}/{date}/{attribute}.csv"
        pd.DataFrame(csv_data).to_csv(csv_file, index=False)
        print(f"CSV data saved to {csv_file}")

        # Insert or update device data in the database
        insert_or_update_device_data(device_id, date, attribute, csv_file)  # Pass the date argument
        return csv_file  # Return the path to the saved CSV file

    except Exception as e:
        print(f"Error processing attribute {attribute}: {e}")
        return None  # Return None to indicate failure

def create_heatmap(data, device_id, date, attributes, color_functions, distance_threshold=1000, output_folder="heatmaps"):
    # Check if there is enough data to proceed
    valid_data = data.dropna(subset=['latitude', 'longitude'])
    if len(valid_data) < 4:
        print(f"Insufficient data for device {device_id} on {date}. Skipping folder creation.")
        return "insufficient_data"  # Return a status indicating insufficient data

    # Create the output folder if it doesn't exist
    folder_path = f"{output_folder}/{device_id}/{date}"
    os.makedirs(folder_path, exist_ok=True)

    # Process each attribute
    csv_files = []
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                process_attribute, data.copy(), device_id, date, attr, color_functions[attr], distance_threshold, output_folder
            )
            for attr in attributes if attr in color_functions
        ]
        for future in futures:
            result = future.result()
            if result and result != "insufficient_data":  # Only append if the result is not "insufficient_data"
                csv_files.append(result)

    # If no CSV files were generated, delete the folder
    if not csv_files:
        print(f"No CSV files generated for device {device_id} on {date}. Deleting folder.")
        shutil.rmtree(folder_path)
        return "insufficient_data"  # Return a status indicating insufficient data

    return "success"  # Return a status indicating success

def process_device_data_f2f(devices, attributes, color_functions, output_folder="heatmaps", specific_device_id=None, specific_date=None):
    """
    Process device data and generate heatmaps for specific devices and dates.

    Args:
        devices (list): List of device IDs to process.
        attributes (list): List of attributes to process.
        color_functions (dict): Dictionary mapping attributes to their respective color functions.
        output_folder (str): Folder to save the generated heatmaps.
        specific_device_id (str): Specific device ID to process (optional).
        specific_date (str): Specific date to process (optional).

    Returns:
        str: A message indicating the result of the operation.
    """
    create_devices_table()

    # If a specific device ID is provided, process only that device
    if specific_device_id:
        devices = [specific_device_id]

    for device in devices:
        timestamps = fetch_timestamps(device)
        if not timestamps:
            return f"No timestamps found for device {device}."

        # Group timestamps by date
        date_groups = {}
        for timestamp in timestamps:
            date = "-".join(timestamp.split("-")[:3])  # Assuming 'YYYY-MM-DD HH-MM-SS' format
            date_groups.setdefault(date, []).append(timestamp)

        # If a specific date is provided, process only that date
        if specific_date:
            if specific_date in date_groups:
                date_groups = {specific_date: date_groups[specific_date]}
            else:
                return f"No data found for device {device} on date {specific_date}."

        # Create a heatmap for each date
        for date, ts_list in date_groups.items():
            all_data = pd.concat([fetch_data(device, ts) for ts in ts_list], ignore_index=True)
            print(f"Data for device {device} on {date}:\n", all_data)
            if not all_data.empty:
                result = create_heatmap(all_data, device, date, attributes, color_functions, distance_threshold=500, output_folder=output_folder)
                print(result)
                if result == "success":
                    return f"Heatmap generated successfully for device {device} on date {date}."
                else:
                    return f"No valid data points available for device {device} on date {date}."
            else:
                return f"No valid data points available for device {device} on date {date}."