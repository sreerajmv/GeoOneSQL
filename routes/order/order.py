from setting.db_connections import ms_query_db
from flask import Blueprint, request, jsonify
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv  # type: ignore
import os
import requests # type: ignore

order_bp = Blueprint("order", __name__)




def make_api_request(url, http_method, querystring):
    payload = {}

    load_dotenv()
    headers = {
        "Authorization": f"Basic {os.getenv('SAP_API_TOKEN')}",  # Base64 encoded 'username:password'
    }

    try:
        # Make the API request
        response = requests.request(
            http_method, url, headers=headers, data=payload, params=querystring
        )
        return response.json()
    except Exception as e:
        print(f"Error while making API request: {e}")
        return None


@order_bp.route("/order", methods=["GET"])
def get_order():
    try:
        # Fetch and validate query parameters
        order_id = request.args.get("order_id")
        user_id = request.args.get("user_id")
        if not (order_id and order_id.isdigit() and user_id and user_id.isdigit()):
            return jsonify({"message": "Missing or invalid query parameters."}), 400
        # SQL query to fetch order details
        query = """
            SELECT  
                A.SlNo,
                A.OrderType,
                A.LocationID,
                A.CustCode,
                A.NetAmount,
                A.MakingTime,
                A.MakerId,
                C.Location AS LocationName,
                B.CardName AS CustomerName,
                (
                    SELECT 
                        D.ProductCode,
                        D.ProdName,
                        D.Qty,
                        D.DiscountPerc,
                        D.LineTotal
                    FROM 
                        TBL_SalesOrderProductDetails AS D
                    WHERE 
                        D.SOID = A.SlNo
                    FOR JSON PATH
                ) AS Products
            FROM 
                TBL_SalesOrderDetails AS A
            LEFT JOIN CustomerMaster_M_Tbl AS B ON A.CustCode = B.CardCode
            LEFT JOIN LocationMaster_M_Tbl AS C ON A.LocationID = C.LocationId
            LEFT JOIN TBL_Users AS D ON D.UserID = A.MakerId
            WHERE 
                A.Status = 'N' AND A.SlNo = ? AND D.Usr_Name = ?
            FOR JSON PATH, INCLUDE_NULL_VALUES;
        """
        params = (order_id, user_id)
        # Execute the query and fetch the result
        orderdetails = ms_query_db(query, params)
        # Process the result
        if orderdetails:
            json_key = "JSON_F52E2B61-18A1-11d1-B105-00805F49916B"
            raw_data = json.loads(orderdetails[0].get(json_key, "[]"))
            formatted_data = (
                raw_data[0] if isinstance(raw_data, list) and raw_data else {}
            )
        else:
            formatted_data = {}
        # check formatted_data is empty or not
        if not formatted_data:
            return jsonify({"message": "No data found."}), 404
        return jsonify(formatted_data), 200
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"message": "Internal server error."}), 500

@order_bp.route("/orderApprove", methods=["POST"])
def approve_order():
    try:
        soid = request.args.get("soid")
        user_id = request.args.get("user_id")
        if not (soid and soid.isdigit() and user_id and user_id.isdigit()):
            return jsonify({"message": "Missing or invalid query parameters."}), 400
        params = (soid, user_id)
        query = "uSP_ApproveSalesOrderDetails_ManualPost @SOID=?,@userId=?"
        result = ms_query_db(query, args=params, commit=False, fetch_one=True)
        if result:
            return jsonify(result), 200
        else:
            return jsonify({"message": "No data found."}), 404
    except Exception as e:
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500
    

@order_bp.route("/openOrder/<customer_code>", methods=["GET"])
def open_order(customer_code):
    try:
        query = "Proc_Customer_OrderDetails @CardCode=?"      
        params = (customer_code,)
        open_order = ms_query_db(query, params, fetch_one=True)
        url = f"{os.getenv('SAP_API_URL')}/api/master/customer-balance/{customer_code}"
        response = make_api_request(url, "GET", None)
        if not response or not isinstance(response, list) or len(response) == 0:
            return jsonify(
                {"message": "Failed to fetch or unexpected customer balance response"}
            ), 500
        balance_info = response[0]
        response = {
            "CardName": open_order["CardName"],
            "Territory": open_order["Territory"],
            "TerritoryName": open_order["TerritoryName"],
            "CDOverdueAmount": float(open_order["CDOverdueAmount"]),
            "LastOpenBillDate": open_order["LastOpenBillDate"],
            "OverdueAmount": float(open_order["OverdueAmount"]),
            "OverdueBillCount": int(open_order["OverdueBillCount"]),
            "ApprovedAmount": float(open_order["ApprovedAmount"]),
            "DraftAmount": float(open_order["DraftAmount"]),
            "current_balance": float(balance_info.get("AccountBalance")),
            "credit_limit": float(balance_info.get("CreditBalance")),
        }
        return jsonify(response), 200
    except Exception as e:  
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500
    

@order_bp.route("/approve_discount_request", methods=["POST"])
def approve_order_discount_request():
    try:
        data = request.get_json()
        cardcode = data.get("CardCode")
        validity = int(data.get("validity"))  # Ensure validity is an integer
        group_code = data.get("Group_code")
        rate = float(data.get("Rate"))  # Ensure rate is a float
        createdBy = 123

        # Calculate Fromdate and Todate
        Fromdate = datetime.now()
        Todate = Fromdate + timedelta(days=validity-1)

        # Convert to string format
        Fromdate_str = Fromdate.strftime("%Y-%m-%d")
        Todate_str = Todate.strftime("%Y-%m-%d")
        query = """
        EXEC uSP_CreateDiscountSettings 
        @CardCode = ?, 
        @GroupCode = ?, 
        @FromDate = ?, 
        @Todate = ?, 
        @Rate = ?, 
        @CreatedBy = ?
        """
        params = (cardcode, group_code, Fromdate_str, Todate_str, rate, createdBy)

        # Run query with appropriate fetch or commit
        ms_query_db(query, args=params, commit=True)  # Removed fetch_one=True

        # If the stored procedure doesn't return anything, return a success message
        return jsonify({"message": "Discount request approved successfully"}), 200

    except Exception as e:
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500
