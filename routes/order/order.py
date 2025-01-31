from setting.db_connections import ms_query_db
from flask import Blueprint, request, jsonify
import json
from datetime import datetime, timedelta

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
    

@order_bp.route("/openOrder/<customer_code>", methods=["GET"])
def open_order(customer_code):
    try:
        query = """
                    SELECT
                    ISNULL(SUM(CAST (DocTotal AS NUMERIC(18,2))),0) AS Amount 
                    FROM SAP_SalesOrder_M_Tbl S
                    INNER JOIN TBL_SalesOrderStatus A on S.SalesOrderStatus=A.Name
                    WHERE A.SlNo not in (2,1003,1004,1005,1006,1009)  AND CustomerCode=?
                """
        query2 = """
                    SELECT 
                    SUM(NetAmount) AS Amount 

                    FROM TBL_SalesOrderDetails 
                    WHERE Status='N' AND CustCode=?
                """
        query3 = "select CardName, Territory from CustomerMaster_M_Tbl where CardCode = ?"
        
        params = (customer_code,)
        open_order = ms_query_db(query, params, fetch_one=True)
        draft_order = ms_query_db(query2, params, fetch_one=True)
        customer = ms_query_db(query3, params, fetch_one=True)

        response = {
            "open_order": float(open_order["Amount"])
            if open_order and open_order["Amount"] is not None
            else None,
            "draft_order": float(draft_order["Amount"])
            if draft_order and draft_order["Amount"] is not None
            else None,
            "customer": customer["CardName"],
            "territory": customer["Territory"],
        }
        return jsonify(response), 200

    except Exception as e:  
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500
    
@order_bp.route("/approve_discount_request", methods=["POST"])
def approve_order_discount_request():
    try:
        data = request.get_json()
        cardcode = data.get("cardcode")
        validity = int(data.get("validity"))  # Ensure validity is an integer
        group_code = data.get("group_code")
        rate = float(data.get("Rate"))  # Ensure rate is a float
        createdBy = 123

        # Calculate Fromdate and Todate
        Fromdate = datetime.now()
        Todate = Fromdate + timedelta(days=validity)

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
