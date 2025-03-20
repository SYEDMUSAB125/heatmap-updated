from flask import Flask, request, jsonify
import pandas as pd
from newtest import process_device_data, get_phosphorus_color, get_Conductivity_color, get_nitrogen_color, get_moisture_color, get_ph_color, get_potassium_color
from new_f2f import process_device_data_f2f
from dbConnection import get_db_connection
from flask_cors import CORS
import os

# Initialize Flask app
app = Flask(__name__)
CORS(app)



BASE_FOLDER = "heatmaps"
@app.route('/devices', methods=['GET'])
def get_all_devices():
    try:
        # List all device folders in the heatmaps directory
        device_folders = [f for f in os.listdir(BASE_FOLDER) if os.path.isdir(os.path.join(BASE_FOLDER, f))]
        
        # Return the list of device IDs
        return jsonify({"devices": device_folders})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint to fetch all dates for a specific device ID
@app.route('/devices/<device_id>/dates', methods=['GET'])
def get_dates_for_device(device_id):
    try:
        # Construct the path to the device folder
        device_path = os.path.join(BASE_FOLDER, device_id)
        
        # Check if the device folder exists
        if not os.path.exists(device_path):
            return jsonify({"error": f"Device ID '{device_id}' not found."}), 404
        
        # List all date folders for the device
        date_folders = [f for f in os.listdir(device_path) if os.path.isdir(os.path.join(device_path, f))]
        
        # Return the list of dates
        return jsonify({"dates": date_folders})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
@app.route('/get_heatmap_data', methods=['POST'])
def get_heatmap_data():

    device_id = request.json.get('device_id')
    date = request.json.get('date')
    attribute = request.json.get('attribute')

    print(device_id, date, attribute)
    if not device_id or not date or not attribute:
        return jsonify({"error": "Missing device_id, date, or attribute parameter"}), 400

    # Construct the file path
    file_path = os.path.join(BASE_FOLDER, device_id, date, f"{attribute}.csv")
    print(file_path)
    # Check if the file exists
    if not os.path.exists(file_path):
        return jsonify({"error": "File not found"}), 404

    # Read the CSV file
    try:
        df = pd.read_csv(file_path)

        # Prepare the response data
        coordinates = df[['latitude', 'longitude']].values.tolist()
        data = [
            {
                "coordinates": [row["latitude"], row["longitude"]],
                "value": row[attribute],
                "color": row["color"]
            }
            for _, row in df.iterrows()
        ]

        response = {
            "all_coordinates": coordinates,
            "data": data
        }

        return jsonify(response), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500







def fetch_data_from_csv(csv_file):
    try:
        data = pd.read_csv(csv_file)
        return data
    except Exception as e:
        return str(e)

# Function to process the device data and create heatmap
def process_and_create_heatmap(csv_file_new):
    # Fetch data from the CSV file
    data = fetch_data_from_csv(csv_file_new)

    if isinstance(data, str):  # If an error message was returned from fetch_data_from_csv
        return {"error": data}

    # Define attributes and color functions
    attributes = ['potassium', 'conductivity', 'nitrogen', 'moisture', 'ph', 'phosphor']
    color_functions = {
        'phosphor': get_phosphorus_color,
        'conductivity': get_Conductivity_color,
        'nitrogen': get_nitrogen_color,
        'moisture': get_moisture_color,
        'ph': get_ph_color,
        'potassium': get_potassium_color
    }

    # Process the device data and create heatmaps
    result = process_device_data(data, attributes, color_functions)

    return {"message": result}

@app.route('/process_csv', methods=['POST'])
def process_csv():
    # Get the file from the request
    csv_file = None
    for file_key in request.files:
        csv_file = request.files[file_key]
        break  # Only pick the first file uploaded

    # Check if a file is uploaded
    if not csv_file:
        return jsonify({"error": "CSV file is required."}), 400

    # Check if the file has a valid name
    if csv_file.filename == '':
        return jsonify({"error": "No file selected."}), 400

    # Get the custom file name from the form (optional)
    custom_filename = request.form.get('custom_filename', csv_file.filename)

    # Save the file locally with the custom filename
    csv_file_path = f"{custom_filename}"
    csv_file.save(csv_file_path)

    # Process the file and save data to the database
    try:
        result = process_and_create_heatmap(csv_file_path)
        if "error" in result:
            return jsonify(result), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"message":"heatmap created succefully through csv"}), 200

@app.route('/process_data', methods=['POST'])
def process_data():
    try:
        # Extract device ID and date from the request payload
        payload = request.json
        device_id = payload.get('device_id')
        date = payload.get('date')  # Extract the date from the payload
        devices = [device_id]  # Wrap the device ID in a list for compatibility

        # Validate the payload
        if not device_id:
            return jsonify({'error': 'Device ID is required'}), 400
        if not date:
            return jsonify({'error': 'Date is required'}), 400

        # Define attributes and color functions
        attributes = ['phosphor', 'conductivity', 'nitrogen', 'moisture', 'pH', 'potassium']
        color_functions = {
            'phosphor': get_phosphorus_color,
            'conductivity': get_Conductivity_color,
            'nitrogen': get_nitrogen_color,
            'moisture': get_moisture_color,
            'pH': get_ph_color,
            'potassium': get_potassium_color
        }

        # Call the processing function with the specific device ID and date
        result_message = process_device_data_f2f(
            devices=devices,
            attributes=attributes,
            color_functions=color_functions,
            specific_device_id=device_id,  # Pass the specific device ID
            specific_date=date  # Pass the specific date
        )
     
        # Check the result message and send the appropriate response
        if "successfully" in result_message.lower():
            return jsonify({'message': result_message}), 200
        else:
            return jsonify({'message': result_message}), 400

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    


@app.route("/get_device_id", methods=['GET'])
def get_device_id():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users")
        devices = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify({"devices": devices}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)