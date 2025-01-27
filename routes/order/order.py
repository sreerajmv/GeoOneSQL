from setting.db_connections import ms_query_db
from flask import Blueprint, request, jsonify

import json
# from datetime import datetime

order_bp = Blueprint("order", __name__)

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
        # Define SQL query with parameterized values (if applicable)
        query = "uSP_ApproveSalesOrderDetails_ManualPost @SOID=?,@userId=?"
        # Execute the query
        result = ms_query_db(query, args=params, commit=False, fetch_one=True)

        print(f"Query Result: {result}")

        # Check and return appropriate response
        if result:

            return jsonify(result), 200
        else:
            return jsonify({"message": "No data found."}), 404


    except Exception as e:
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500