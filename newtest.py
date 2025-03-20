
import numpy as np
import pandas as pd
from scipy.interpolate import griddata
from shapely.geometry import Polygon, Point
from scipy.spatial import ConvexHull
from concurrent.futures import ThreadPoolExecutor
import os
import re
from dbConnection import get_db_connection
from psycopg2 import sql
# Function to sanitize filenames
def sanitize_filename(name):
    """Remove invalid characters from a filename."""
    return re.sub(r'[<>:"/\\|?* -]',"", name)
def sanitize_filedate(name):
    """Remove invalid characters from a filename."""
    return re.sub(r'[<>:"/\\|?* -]',"-", name)

# Color functions
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
    return "gray"

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
    return "gray"

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
    return "gray"

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
    return "gray"

def get_potassium_color(k):
    if 0 <= k <= 52.9999999999999:
        return "white"
    elif 53 <= k <= 85:
        return "peachpuff"
    elif 86 <= k <= 120:
        return "orange"
    elif 121 <= k <= 155:
        return "red"
    elif k > 155:
        return "darkred"
    return "gray"

# Color functions dictionary
color_functions = {
    'phosphor': get_phosphorus_color,
    'conductivity': get_Conductivity_color,
    'nitrogen': get_nitrogen_color,
    'moisture': get_moisture_color,
    'pH': get_ph_color,
    'potassium': get_potassium_color
}




# Function to create the devices table
def create_devices_table():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS devices (
                device_id TEXT PRIMARY KEY,
                phosphor_path TEXT,
                conductivity_path TEXT,
                nitrogen_path TEXT,
                moisture_path TEXT,
                ph_path TEXT,
                potassium_path TEXT
            )
        """)
        conn.commit()
        print("Table 'devices' created or already exists.")
    except Exception as e:
        print(f"Error creating table 'devices': {e}")
    finally:
        if cursor:
            cursor.close()

# Function to insert or update device data
def insert_or_update_device_data(device_id, attribute, csv_path):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = sql.SQL("""
        INSERT INTO devices (device_id, {}) 
        VALUES (%s, %s)
        ON CONFLICT (device_id) 
        DO UPDATE SET {} = %s
    """).format(
        sql.Identifier(f"{attribute}_path"),
        sql.Identifier(f"{attribute}_path")
    )
    cursor.execute(query, (device_id, csv_path, csv_path))
    conn.commit()
    cursor.close()
    conn.close()
# Function to calculate distance between two coordinates (in meters)
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000  # Radius of Earth in meters
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    d_phi = phi2 - phi1
    d_lambda = np.radians(lon2 - lon1)
    a = np.sin(d_phi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(d_lambda / 2) ** 2
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

# Function to save data to a CSV file
def save_to_csv(data, file_name="heatmap_data.csv"):
    """
    Save heatmap data to a CSV file.

    Args:
        data (list): List of dictionaries containing heatmap data.
        file_name (str): Name of the CSV file to save.
    """
    if not data:
        print("No data to save.")
        return

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(data)

    # Save the DataFrame to a CSV file
    df.to_csv(file_name, index=False)
    print(f"Heatmap data saved to {file_name}")

# Function to process attribute and save data to CSV
def process_attribute(data, device_id, date, attribute, color_function, distance_threshold, output_folder="heatmaps"):
    try:
        # Sanitize device_id and date
        device_id = sanitize_filename(str(device_id))
        date = sanitize_filedate(str(date))

        # Ensure the output folder exists
        os.makedirs(f"{output_folder}/{device_id}/{date}", exist_ok=True)

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

        if len(points) < 4:
            print(f"Skipping heatmap for {attribute}: Not enough valid points.")
            return

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
            return

        # Create grid for interpolation
        grid_lat = np.linspace(points[:, 0].min(), points[:, 0].max(), 100)
        grid_lon = np.linspace(points[:, 1].min(), points[:, 1].max(), 100)
        grid_lon, grid_lat = np.meshgrid(grid_lon, grid_lat)
        grid_points = np.vstack([grid_lat.ravel(), grid_lon.ravel()]).T

        # Filter grid points within convex hull
        hull = ConvexHull(points)
        polygon = Polygon(points[hull.vertices])
        grid_within = np.array([point for point in grid_points if polygon.contains(Point(point))])
        if grid_within.size == 0:
            print(f"No grid points within the convex hull for device {device_id} on {date}.")
            return

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
                    attribute: f"{attribute}:{value}",
                    "color": color
                })

        # Save the CSV data
        csv_file = f"{output_folder}/{device_id}/{date}/{attribute}.csv"
        save_to_csv(csv_data, csv_file)
        print(f"CSV data saved to {csv_file}")

        # Insert or update the CSV file path in the database
        insert_or_update_device_data(device_id, attribute, csv_file)

    except Exception as e:
        print(f"Error processing attribute {attribute}: {e}")
        return None

# Function to create heatmaps for all attributes
def create_heatmap(data, device_id, date, attributes, color_functions, distance_threshold=1000, output_folder="heatmaps"):
    heatmap_data_list = []
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                process_attribute, data.copy(), str(device_id), date, attr, color_functions[attr], distance_threshold, output_folder
            )
            for attr in attributes if attr in color_functions
        ]
        for future in futures:
            result = future.result()
            if result:
                heatmap_data_list.append(result)

    # Save all heatmap data to a CSV file
    save_to_csv(heatmap_data_list, file_name=f"{output_folder}/{device_id}/{date}/heatmap_data.csv")

def fetch_timestamps(device, data):
    """Extract timestamps from data for a specific device."""
    # Assuming `data` has a `device_id` and `timestamp` column.
    device_data = data[data['device_id'] == device]
    print("device_id", data["device_id"])
    return device_data['timestamp'].tolist()
    
# Main function to process device data
def process_device_data(data, attributes, color_functions, output_folder="heatmaps"):
    devices = data['device_id'].unique()
    print(f"Unique device IDs: {devices} (type: {type(devices)})")
    create_devices_table()
    for device in devices:
        # Fetch timestamps for the device
        timestamps = fetch_timestamps(device, data)
        
        if not timestamps:
            print(f"No timestamps found for device {device}.")
            continue

        # Group timestamps by date
        date_groups = {}
        for timestamp in timestamps:
            date = "-".join(timestamp.split(" ")[0].split("-")[:3])  # Extract the 'YYYY-MM-DD' date part
            date_groups.setdefault(date, []).append(timestamp)

        # Create a heatmap for each date
        for date, ts_list in date_groups.items():
            all_data = pd.concat([data[data['timestamp'] == ts] for ts in ts_list], ignore_index=True)
            print(f"Data for device {device} on {date}:\n", all_data)
            
            if not all_data.empty:
                create_heatmap(all_data, device, date, attributes, color_functions, output_folder=output_folder)
            else:
                print(f"No data found for device {device} on {date}.")

# Example usage
csv_file = "sensor_data1.csv"
data = pd.read_csv(csv_file)

attributes = ['phosphor', 'conductivity', 'nitrogen', 'moisture', 'ph', 'potassium']
color_functions = {
    'phosphor': get_phosphorus_color,
    'conductivity': get_Conductivity_color,
    'nitrogen': get_nitrogen_color,
    'moisture': get_moisture_color,
    'ph': get_ph_color,
    'potassium': get_potassium_color
}

