import requests  # type: ignore
from dotenv import load_dotenv # type: ignore
import os
from datetime import datetime, timedelta  # type: ignore
from flask import Blueprint, request, jsonify
import json
from setting.db_connections import ms_query_db



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
    
def insert_incoming_sql(jsondata):
    if isinstance(jsondata, str):
        json_data = json.loads(jsondata)

    for record in json_data:
        columns = ", ".join(record.keys())
        placeholders = ", ".join(["?" for _ in record.values()])
        values = tuple(record.values())
        sql_query = f"INSERT INTO Incoming_Payments_T_Tbl ({columns}) VALUES ({placeholders})"

        ms_query_db(sql_query, values, commit=True)


    

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
            # Convert string to datetime
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            # print(start_date)
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
            # print(start_date)
        except ValueError:
            return jsonify({"message": "Invalid date format. Expected YYYY-MM-DD"}), 400

        sap_url = "/ebiz/v2/incoming-payments"
        url = f"{os.getenv('SAP_API_URL')}{sap_url}"
        method = "GET"
        Fdate = start_date.strftime("%Y-%m-%d%%20%H%d%%3A%M")
        Tdate = end_date.strftime("%Y-%m-%d%%20%H%d%%3A%M")

        url = f"{url}?FDate={Fdate}&TDate={Tdate}"
        data = make_api_request(url, method)

        return jsonify(data)
    except Exception as e:
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500

