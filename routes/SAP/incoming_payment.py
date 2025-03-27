import requests  # type: ignore
from dotenv import load_dotenv # type: ignore
import os
from datetime import datetime, timedelta  # type: ignore
from flask import Blueprint, request, jsonify
import json
from setting.db_connections import ms_query_db, cursor_ms

sap_bp = Blueprint("sap", __name__)

def make_api_request(url, http_method):
    payload = {}

    load_dotenv()
    headers = {
        "Authorization": f"Basic {os.getenv('SAP_API_TOKEN')}",  # Base64 encoded 'username:password'
    }

    try:
        # Make the API request
        response = requests.request(
            http_method, url, headers=headers, data=payload
        )
        return response.json()
    except Exception as e:
        print(f"Error while making API request: {e}")
        return None  


def utc_to_ist(utc_datetime, return_format="date"):

    try:
        # Convert input string to datetime object (ignoring milliseconds & timezone info)
        utc_time = datetime.strptime(utc_datetime[:19], "%Y-%m-%dT%H:%M:%S")

        # Add IST offset (5 hours 30 minutes)
        ist_offset = timedelta(hours=5, minutes=30)
        ist_time = utc_time + ist_offset

        # Return format based on the parameter
        if return_format == "datetime":
            return ist_time.strftime("%Y-%m-%d %H:%M:%S")  # Full date & time
        else:
            return ist_time.strftime("%Y-%m-%d")  # Only date

    except Exception as e:
        return None


def insert_incoming_sql(jsondata):
    try:
        # Ensure json_data is always initialized
        json_data = jsondata
        if isinstance(jsondata, str):
            json_data = json.loads(jsondata)
        conn, cursor = cursor_ms()
        cursor.execute("TRUNCATE TABLE Incoming_Payments_T_Tbl")
        insert_count = 0
        for record in json_data:
            record["DocDate"] = utc_to_ist(record["DocDate"], "date")
            record["InvDate"] = utc_to_ist(record["InvDate"], "date")
            record["LastCreateUpdateTime"] = utc_to_ist(record["LastCreateUpdateTime"], "datetime")
            record["createdat"] = datetime.now()
            columns = ", ".join(record.keys())
            placeholders = ", ".join(["?" for _ in record.values()])
            values = tuple(record.values())
            sql_query = f"INSERT INTO Incoming_Payments_T_Tbl ({columns}) VALUES ({placeholders})"

            cursor.execute(sql_query, values)
            insert_count += 1

        conn.commit()
        cursor.close()
        conn.close()

        return "Data inserted successfully"

    except Exception as e:
        return f"Error inserting data: {str(e)}"


@sap_bp.route("/incoming", methods=["GET"])
def incoming_payment():
    try:
        start_date = request.args.get("startDate")
        end_date = request.args.get("endDate")

        if not start_date or not end_date:
            return jsonify(
                {"message": "Missing required parameters: startDate or endDate"}
            ), 400

        try:
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
            end_date = end_date.replace(hour=23, minute=59, second=0)
        except ValueError:
            return jsonify({"message": "Invalid date format. Expected YYYY-MM-DD"}), 400

        sap_url = "/ebiz/v2/incoming-payments"
        url = f"{os.getenv('SAP_API_URL')}{sap_url}"
        method = "GET"
        Fdate = start_date.strftime("%Y-%m-%d%%20%H%d%%3A%M")
        Tdate = end_date.strftime("%Y-%m-%d%%20%H%d%%3A%M")

        url = f"{url}?FDate={Fdate}&TDate={Tdate}"
        data = make_api_request(url, method)

        if data:    
            insert_status = insert_incoming_sql(data)
            return jsonify({"message": insert_status}), 200

        
    except Exception as e:
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500

