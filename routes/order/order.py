from setting.db_connections import ms_query_db, cursor_ms
from flask import Blueprint, request, jsonify
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv  # type: ignore
import os
import requests # type: ignore
# import pyodbc

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
        cardcode = request.args.get("cardcode")
        # if not (order_id and order_id.isdigit() and user_id and user_id.isdigit()):
        #     return jsonify({"message": "Missing or invalid query parameters."}), 400
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
            WHERE 
                A.Status IN ('N' ,'P') AND A.SlNo = ? AND B.CardCode = ?
            FOR JSON PATH, INCLUDE_NULL_VALUES;
        """
        params = (order_id, cardcode)
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
        conn, cursor = cursor_ms()
        # Execute the stored procedure
        query = "EXEC uSP_ApproveSalesOrderDetails_ManualPost @SOID=?, @userId=?"
        cursor.execute(query, params)
        result = cursor.fetchall()

        # Default values
        status = 0
        message = "Unknown error"


        if result:
            first_tuple = result[0]
            status = first_tuple[0]
            message = first_tuple[1]


        if status == 1: 
            conn.commit()
            return jsonify({"status": status, "message": message}), 200
        else:
            # If SP failed, Rollback (just to be safe) and return the error
            conn.rollback() 
            # print(f"SP Failed with: {message}") # This will help you see the error in logs
            return jsonify({"status": status, "message": message}),  500



    except Exception as e:
        # Rollback in case of error
        if "conn" in locals():
            conn.rollback()
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500



@order_bp.route("/outstanding/<customer_code>", methods=["GET"])
def outstanding(customer_code):
    try:
        query = """
                    SELECT
                            A.InvoiceDate,
                            A.InvoiceNum AS InvoiceNo,
                            CASE WHEN DATEDIFF(DAY,CONVERT(datetime,A.InvoiceDate,103),GETDATE()) >= 45 THEN 'Yes'
                                ELSE NULL END AS 'OverDue',
                            CONVERT(decimal, A.PendingAmount) AS Pending,
                    DATEDIFF(DAY,CONVERT(datetime,A.InvoiceDate,103),GETDATE()) as [Days],
                            A.PaymentTermsValue AS [PTerms],
                            A.BillAmount,
                            A.PaidSum ,
                            ISNULL(f.Location,'Nil') AS Location
                        FROM 
                            OutstandingMaster_M_Tbl AS A
                        INNER JOIN CustomerMaster_M_Tbl AS B ON B.CardCode = A.CardCode
                        LEFT JOIN Bde_Territory_M_Tbl AS C ON C.TerritoryID = B.Territory
                        LEFT JOIN Area_M_Tbl AS D ON D.AreaID = C.AreaID
                        LEFT JOIN dbo.Region_M_Tbl AS REG ON C.RegionID=REG.RegionID
                        LEFT JOIN 
                            (
                                SELECT DISTINCT DocEntry, LocationID
                                FROM SAP_AR_Invoice_Line_Details_M_Tbl
                            ) AS e ON A.DocEntry = e.DocEntry 
                        LEFT JOIN LocationMaster_M_Tbl as f on f.LocationId = e.LocationID
                        WHERE A.DocStatus = 'OPEN'  AND A.CardCode = ?
                        order by DATEDIFF(DAY,CONVERT(datetime,A.InvoiceDate,103),GETDATE())
                """
        params = (customer_code,)
        outstanding_details = ms_query_db(query, params, fetch_one=False)

        outstanding_details = ms_query_db(query, params, fetch_one=False)
        
        # Check if ms_query_db returned None (error case)
        if outstanding_details is None:
            return jsonify({"message": "Database query failed"}), 500
            
        # Return empty array if no results
        return jsonify(outstanding_details or []), 200

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


    
@order_bp.route("/approved_open_orders/<int:user_id>", methods=["GET"])
def get_approved_orders(user_id):
    try:
        territory_id = request.args.get("territory_id")
        created_date = request.args.get("created_date")

        query = """

                    SELECT
                        S.SalesOrderNo AS [Order_No],
                        S.NOrderType AS OrderType,
                        S.CreatedDateTime AS MakingTime,
                        S.CustomerCode,
                        E.CardName,
                        U.Name,
                        C.Description AS [itemName],
                        F.Location,
                        CAST(
                            ISNULL(
                                CASE 
                                    WHEN D.UomCode = 'SQM' THEN 
                                        TRY_CAST(SL.Quantity * TRY_CAST(C.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2)) / 1000
                                    WHEN D.UomCode = 'KGS' THEN 
                                        TRY_CAST(SL.Quantity AS NUMERIC(10,2)) / 1000
                                    WHEN D.UomCode = 'MTR' THEN 
                                        TRY_CAST((SL.Quantity * TRY_CAST(C.Width AS NUMERIC(10,2))) * TRY_CAST(C.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2)) / 1000
                                    WHEN D.UomCode = 'NOS' THEN 
                                        TRY_CAST(SL.Quantity * TRY_CAST(C.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2)) / 1000
                                    ELSE 0 
                                END,
                            0
                            ) AS NUMERIC(10,2)
                        ) AS [Tonnage],
                        S.SalesOrderStatus AS [Status]
                    FROM SAP_SalesOrderLine_M_Tbl SL
                    INNER JOIN SAP_SalesOrder_M_Tbl S ON S.DocEntry = SL.DocEntry
                    INNER JOIN ItemMaster_M_Tbl C ON C.ItemCode = SL.ItemCode  AND C.MainGroup IN ('Geoclad Cladding Sheet', 'Georoof Roofing Sheet')
                    INNER JOIN Uom_Master_M_Tbl D ON D.UomId = C.UOM
                    INNER JOIN CustomerMaster_M_Tbl E ON E.CardCode = S.CustomerCode
                    INNER JOIN LocationMaster_M_Tbl F ON F.Code = SL.LocCode
                    INNER JOIN TBL_SalesOrderStatus A ON S.SalesOrderStatus = A.Name  AND A.SlNo NOT IN (2, 1004, 1005, 1006, 1009)
                    INNER JOIN TBL_SalesOrderDetails SD ON S.EbizOrderId = SD.SlNo
                    INNER JOIN TBL_Users U ON U.UserID = SD.MakerID
                    INNER JOIN SalesEmployeeMaster_M_Tbl SE ON SD.SalesPerson=SE.SalesEmployeeCode
                    INNER JOIN Employee_Master_M_Tbl EM ON EM.SapEmployeeId=SE.EmployeeId
                        
        
            """
        conditions = []
        params = []
    
        if user_id:
            conditions.append("EM.EmployeeId = ?")
            params.append(user_id)

        if territory_id:
            conditions.append("E.Territory = ?")
            params.append(territory_id)

        if created_date:
            conditions.append("convert(date, S.DocDate, 103)= ?")
            params.append(created_date)
    
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
    
   
        params = tuple(params)
        approved_orders = ms_query_db(query, params, fetch_one=False)
        return jsonify(approved_orders), 200
    except Exception as e:
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500    


@order_bp.route("/approved_open_orders_summary/<int:user_id>", methods=["GET"])
def approved_open_orders_summary(user_id):
    try:
        territory_id = request.args.get("territory_id")
        created_date = request.args.get("created_date")
        query = """

                    SELECT 
                        SUM(
                            CAST(
                                ISNULL(
                                    CASE 
                                        WHEN D.UomCode = 'SQM' THEN 
                                            TRY_CAST(SL.Quantity * TRY_CAST(C.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2)) / 1000
                                        WHEN D.UomCode = 'KGS' THEN 
                                            TRY_CAST(SL.Quantity AS NUMERIC(10,2)) / 1000
                                        WHEN D.UomCode = 'MTR' THEN 
                                            TRY_CAST((SL.Quantity * TRY_CAST(C.Width AS NUMERIC(10,2))) * TRY_CAST(C.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2)) / 1000
                                        WHEN D.UomCode = 'NOS' THEN 
                                            TRY_CAST(SL.Quantity * TRY_CAST(C.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2)) / 1000
                                        ELSE 0 
                                    END,
                                0
                                ) AS NUMERIC(10,2)
                            )
                        ) AS TotalTonnage

                    -- same FROM and JOINs --

                    FROM SAP_SalesOrderLine_M_Tbl SL
                    INNER JOIN SAP_SalesOrder_M_Tbl S ON S.DocEntry = SL.DocEntry
                    INNER JOIN ItemMaster_M_Tbl C ON C.ItemCode = SL.ItemCode  AND C.MainGroup IN ('Geoclad Cladding Sheet', 'Georoof Roofing Sheet')
                    INNER JOIN Uom_Master_M_Tbl D ON D.UomId = C.UOM
                    INNER JOIN CustomerMaster_M_Tbl E ON E.CardCode = S.CustomerCode
                    INNER JOIN LocationMaster_M_Tbl F ON F.Code = SL.LocCode
                    INNER JOIN TBL_SalesOrderStatus A ON S.SalesOrderStatus = A.Name  AND A.SlNo NOT IN (2, 1004, 1005, 1006, 1009)
                    INNER JOIN TBL_SalesOrderDetails SD ON S.EbizOrderId = SD.SlNo
                    INNER JOIN TBL_Users U ON U.UserID = SD.MakerID
                    INNER JOIN SalesEmployeeMaster_M_Tbl SE ON SD.SalesPerson=SE.SalesEmployeeCode
                    INNER JOIN Employee_Master_M_Tbl EM ON EM.SapEmployeeId=SE.EmployeeId

        """
        conditions = []
        params = []

        if user_id:
            conditions.append("EM.EmployeeId = ?")
            params.append(user_id)

        if territory_id:
            conditions.append("E.Territory = ?")
            params.append(territory_id)

        if created_date:
            conditions.append("convert(date, S.DocDate, 103)= ?")
            params.append(created_date)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        params = tuple(params)
        approved_orders = ms_query_db(query, params, fetch_one=True)

        if approved_orders is None or "TotalTonnage" not in approved_orders:
            return jsonify({"TotalTonnage": 0}), 200


        return jsonify(approved_orders), 200
    except Exception as e:
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500


@order_bp.route("/draft_orders/<int:user_id>", methods=["GET"])
def draft_orders(user_id):
    try:
        status = request.args.get("status")
        created_date = request.args.get("created_date")
        territory_id = request.args.get("territory_id")
        approved_date = request.args.get("approved_date")

        # Fixed Aliases:
        # Ord = TBL_SalesOrderDetails
        # Prod = TBL_SalesOrderProductDetails
        # Itm = ItemMaster_M_Tbl
        # Uom = Uom_Master_M_Tbl
        # Cust = CustomerMaster_M_Tbl
        # Emp = Employee_Master_M_Tbl

        query = """
            SELECT 
                Ord.SlNo [Order_No],
                CASE 
                    WHEN Ord.OrderType='WO' THEN 'WorkOrder'
                    WHEN Ord.OrderType='SO' THEN 'SalesOrder' 
                    ELSE '' 
                END AS OrderType,
                Ord.MakingTime,
                Ord.CustCode AS CustomerCode,
                Cust.CardName,
                U.Name,
                Itm.Description [itemName],
                Loc.Location,
                CAST(ISNULL(
                    CASE 
                        WHEN Uom.UomCode ='SQM' THEN TRY_CAST(Prod.Qty * TRY_CAST(Itm.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2))/1000
                        WHEN Uom.UomCode ='KGS' THEN TRY_CAST(Prod.Qty AS NUMERIC(10,2))/1000
                        WHEN Uom.UomCode ='MTR' THEN TRY_CAST((Prod.Qty * TRY_CAST(Itm.Width AS NUMERIC(10,2))) * TRY_CAST(Itm.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2))/1000
                        WHEN Uom.UomCode ='NOS' THEN TRY_CAST(Prod.Qty * TRY_CAST(Itm.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2))/1000
                        ELSE 0 
                    END, 0) AS NUMERIC(10,2)) AS [Tonnage],
                CASE 
                    WHEN Ord.Status = 'C' THEN 'Cancelled'
                    WHEN Ord.Status = 'Y' THEN 'Posted to SAP'
                    WHEN Ord.Status = 'E' THEN 'Expired'
                    WHEN Ord.Status = 'N' THEN 'Draft'
                    WHEN Ord.Status = 'R' THEN 'Dealer Confirmed'
                    WHEN Ord.Status = 'P' THEN 'Dealer Draft'
                END AS [Status]
            FROM 
                TBL_SalesOrderProductDetails Prod
                INNER JOIN TBL_SalesOrderDetails Ord ON Prod.SOID = Ord.SlNo
                INNER JOIN ItemMaster_M_Tbl Itm ON Itm.ItemCode = Prod.ProductCode
                INNER JOIN Uom_Master_M_Tbl Uom ON Uom.UomId = Itm.UOM
                INNER JOIN CustomerMaster_M_Tbl Cust ON Cust.CardCode = Ord.CustCode
                INNER JOIN LocationMaster_M_Tbl Loc ON Loc.Code = Ord.LocationID
                INNER JOIN TBL_Users U ON U.UserID = Prod.MakerID
                INNER JOIN SalesEmployeeMaster_M_Tbl SE ON Ord.SalesPerson = SE.SalesEmployeeCode
                INNER JOIN Employee_Master_M_Tbl Emp ON Emp.SapEmployeeId = SE.EmployeeId
        """

        conditions = []
        params = []

        conditions.append("Ord.Status != 'C'")

        # 1. Base User Filter
        if user_id:
            conditions.append("Emp.EmployeeId = ?")
            params.append(user_id)

            # Hardcoded Item Group Filter
            conditions.append(
                "Itm.MainGroup IN ('Geoclad Cladding Sheet', 'Georoof Roofing Sheet')"
            )

        # 2. Status Logic (Fixed the Logic Conflict)
        if status:
            if status == "open":
                conditions.append("Ord.Status IN (?, ?, ?)")
                params.extend(["N", "R", "P"])
            elif status == "cancelled":
                conditions.append("Ord.Status = ?")
                params.append("C")
            else:
                conditions.append("Ord.Status = ?")
                params.append(status)
        else:
            # DEFAULT BEHAVIOR: If no status is requested, hide Cancelled orders
            # This replicates your original intent without breaking the "cancelled" filter
            conditions.append("Ord.Status != 'C'")

        # 3. Date Filters
        if created_date:
            # Ensure the frontend sends DD/MM/YYYY or change format here
            conditions.append("CONVERT(date, Ord.MakingTime, 103) = ?")
            params.append(created_date)

        if territory_id:
            conditions.append("Cust.Territory = ?")
            params.append(territory_id)

        if approved_date:
            conditions.append("CONVERT(date, Ord.ApproveCancelOn, 103) = ?")
            params.append(approved_date)

        # Build Final Query
        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        # Execute
        draft_orders = ms_query_db(query, params, fetch_one=False)
        return jsonify(draft_orders), 200

    except Exception as e:
        # Good practice: Print the error to console logs so you can debug it
        print(f"Error in draft_orders: {e}")
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500





@order_bp.route("/draft_orders_summary/<int:user_id>", methods=["GET"])
def draft_orders_summary(user_id):
    try:
        # sales_person = request.args.get("sales_person")
        status = request.args.get("status")
        created_date = request.args.get("created_date")
        territory_id = request.args.get("territory_id")
        approved_date = request.args.get("approved_date")

        # Removed unused JOINs (TBL_Users, LocationMaster) for better performance
        query = """
            SELECT 
                SUM(CAST(ISNULL(CASE 
                    WHEN D.UomCode = 'SQM' THEN TRY_CAST(A.Qty*TRY_CAST(C.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2))/1000
                    WHEN D.UomCode = 'KGS' THEN TRY_CAST(A.Qty AS NUMERIC(10,2))/1000
                    WHEN D.UomCode = 'MTR' THEN TRY_CAST((A.Qty*TRY_CAST(C.Width AS NUMERIC(10,2))) * TRY_CAST(C.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2))/1000
                    WHEN D.UomCode = 'NOS' THEN TRY_CAST(A.Qty*TRY_CAST(C.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2))/1000
                    ELSE 0 
                END, 0) AS NUMERIC(10,2))) AS total_tonnage
            FROM 
                TBL_SalesOrderProductDetails A
                INNER JOIN TBL_SalesOrderDetails B ON A.SOID = B.SlNo
                INNER JOIN ItemMaster_M_Tbl C ON C.ItemCode = A.ProductCode
                INNER JOIN Uom_Master_M_Tbl D ON D.UomId = C.UOM
                INNER JOIN CustomerMaster_M_Tbl E ON E.CardCode = B.CustCode
                INNER JOIN SalesEmployeeMaster_M_Tbl SE ON B.SalesPerson=SE.SalesEmployeeCode
                INNER JOIN Employee_Master_M_Tbl EM ON EM.SapEmployeeId=SE.EmployeeId
        """
        
        conditions = []
        params = []

        # REMOVED: conditions.append("Ord.Status != 'C'") 
        # Reason 1: 'Ord' alias does not exist (it is 'B').
        # Reason 2: It conflicts with the status check below.


        conditions.append("B.Status != 'C' ")
        if user_id:
            conditions.append(
                "EM.EmployeeId = ? AND C.MainGroup IN ('Geoclad Cladding Sheet', 'Georoof Roofing Sheet') "
            )
            params.append(user_id)

        # Logic Fix for Status
        if status:
            if status == "open":
                conditions.append("B.Status IN (?, ?, ?)")
                params.extend(["N", "R", "P"])
            elif status == "cancelled":
                conditions.append("B.Status = ?")
                params.append("C")
            else:
                conditions.append("B.Status = ?")
                params.append(status)
        else:
            # Default behavior: If no status specified, hide cancelled
            conditions.append("B.Status != ?")
            params.append("C")

        # Date Filters
        if created_date:
            conditions.append("CONVERT(date, B.MakingTime, 103) = ?")
            params.append(created_date)

        if territory_id:
            conditions.append("E.Territory = ?")
            params.append(territory_id)

        if approved_date:
            conditions.append("CONVERT(date, B.ApproveCancelOn, 103) = ?")
            params.append(approved_date)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        summary_data = ms_query_db(query, params, fetch_one=True)

        if summary_data is None or summary_data.get('total_tonnage') is None:
            return jsonify({"total_tonnage": 0}), 200

        return jsonify(summary_data), 200

    except Exception as e:
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500






# @order_bp.route("/draft_orders/<int:user_id>", methods=["GET"])
# def draft_orders(user_id):
#     try:
#         # sales_person = request.args.get("sales_person")
#         status = request.args.get("status")
#         created_date = request.args.get("created_date")
#         territory_id = request.args.get("territory_id")
#         approved_date = request.args.get("approved_date")
#         query = """
#                     SELECT 
#                         B.SlNo [Order_No],
#                         CASE WHEN B.OrderType='WO' THEN 'WorkOrder'
#                         WHEN B.OrderType='SO' THEN 'SalesOrder' 
#                         ELSE '' END AS OrderType,
#                         B.MakingTime,
#                         B.CustCode AS CustomerCode,
#                         E.CardName,
#                         U.Name,
#                         C.Description [itemName],
#                         F.Location,
#                         CAST(ISNULL(CASE WHEN D.UomCode ='SQM' THEN TRY_CAST(A.Qty*TRY_CAST(C.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2))/1000
#                             WHEN D.UomCode ='KGS' THEN TRY_CAST(A.Qty AS NUMERIC(10,2))/1000
#                             WHEN D.UomCode ='MTR' THEN TRY_CAST((A.Qty*TRY_CAST(C.Width AS NUMERIC(10,2))) * TRY_CAST(C.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2))/1000
#                             WHEN D.UomCode ='NOS' THEN TRY_CAST(A.Qty*TRY_CAST(C.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2))/1000
#                             ELSE 0 END ,0) AS NUMERIC(10,2))  AS [Tonnage],
#                         CASE WHEN B.Status = 'C' THEN 'Cancelled'
#                         WHEN B.Status = 'Y' THEN 'Posted to SAP'
#                         WHEN B.Status = 'E'  THEN 'Expired'
#                         WHEN B.Status = 'N' THEN 'Draft'
#                         WHEN B.Status = 'R' THEN 'Dealer Confirmed'
#                         WHEN B.Status = 'P' then 'Dealer Draft'
#                         END AS [Status]
#                     FROM 
#                         TBL_SalesOrderProductDetails A
#                         INNER JOIN TBL_SalesOrderDetails B ON A.SOID = B.SlNo
#                         INNER JOIN ItemMaster_M_Tbl C ON C.ItemCode = A.ProductCode
#                         INNER JOIN Uom_Master_M_Tbl D ON D.UomId = C.UOM
#                         INNER JOIN CustomerMaster_M_Tbl E ON E.CardCode = B.CustCode
#                         INNER JOIN LocationMaster_M_Tbl F ON F.Code = B.LocationID
#                         INNER JOIN TBL_Users U ON U.UserID=A.MakerID
#                         INNER JOIN SalesEmployeeMaster_M_Tbl SE ON B.SalesPerson=SE.SalesEmployeeCode
#                         INNER JOIN Employee_Master_M_Tbl EM ON EM.SapEmployeeId=SE.EmployeeId
#                 """
#         conditions = []
#         params = []

#         if user_id:
#             conditions.append(
#                 "EM.EmployeeId = ? AND C.MainGroup IN ('Geoclad Cladding Sheet', 'Georoof Roofing Sheet') AND B.Status != 'C'"
#             )
#             params.append(user_id)

#         if status:
#             if status == "open":
#                 conditions.append("B.Status IN (?, ?, ?)")
#                 params.extend(["N", "R", "P"])
#             elif status == "cancelled":
#                 conditions.append("B.Status = ?")
#                 params.append("C")
           
#             else:
#                 conditions.append("B.Status = ?")
#                 params.append(status)

#         if created_date:
#             conditions.append("CONVERT(date, B.MakingTime,103) = ?")
#             params.append(created_date)

#         if territory_id:
#             conditions.append("E.Territory = ?")
#             params.append(territory_id)

#         if approved_date:
#             conditions.append("CONVERT(date, B.ApproveCancelOn,103) = ?")
#             params.append(approved_date)

#         if conditions:
#             query += " WHERE " + " AND ".join(conditions)

#         draft_orders = ms_query_db(query, params, fetch_one=False)
#         return jsonify(draft_orders), 200
#     except Exception as e:
#         return jsonify({"message": f"Internal server error: {str(e)}"}), 500


# @order_bp.route("/draft_orders_summary/<int:user_id>", methods=["GET"])
# def draft_orders_summary(user_id):
#     try:
#         # sales_person = request.args.get("sales_person")
#         status = request.args.get("status")
#         created_date = request.args.get("created_date")
#         territory_id = request.args.get("territory_id")
#         approved_date = request.args.get("approved_date")
#         query = """
#                    SELECT 
#                         SUM(CAST(ISNULL(CASE 
#                             WHEN D.UomCode = 'SQM' THEN TRY_CAST(A.Qty*TRY_CAST(C.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2))/1000
#                             WHEN D.UomCode = 'KGS' THEN TRY_CAST(A.Qty AS NUMERIC(10,2))/1000
#                             WHEN D.UomCode = 'MTR' THEN TRY_CAST((A.Qty*TRY_CAST(C.Width AS NUMERIC(10,2))) * TRY_CAST(C.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2))/1000
#                             WHEN D.UomCode = 'NOS' THEN TRY_CAST(A.Qty*TRY_CAST(C.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2))/1000
#                             ELSE 0 
#                         END, 0) AS NUMERIC(10,2))) AS total_tonnage
    
#                     FROM 
#                         TBL_SalesOrderProductDetails A
#                         INNER JOIN TBL_SalesOrderDetails B ON A.SOID = B.SlNo
#                         INNER JOIN ItemMaster_M_Tbl C ON C.ItemCode = A.ProductCode
#                         INNER JOIN Uom_Master_M_Tbl D ON D.UomId = C.UOM
#                         INNER JOIN CustomerMaster_M_Tbl E ON E.CardCode = B.CustCode
#                         INNER JOIN LocationMaster_M_Tbl F ON F.Code = B.LocationID
#                         INNER JOIN TBL_Users U ON U.UserID=A.MakerID
#                         INNER JOIN SalesEmployeeMaster_M_Tbl SE ON B.SalesPerson=SE.SalesEmployeeCode
#                         INNER JOIN Employee_Master_M_Tbl EM ON EM.SapEmployeeId=SE.EmployeeId
#                 """
#         conditions = []
#         params = []

#         conditions.append("Ord.Status != 'C'")

#         if user_id:
#             conditions.append(
#                 "EM.EmployeeId = ? AND C.MainGroup IN ('Geoclad Cladding Sheet', 'Georoof Roofing Sheet') "
#             )
#             params.append(user_id)

#         if status:
#             if status == "open":
#                 conditions.append("B.Status IN (?, ?, ?)")
#                 params.extend(["N", "R", "P"])
#             elif status == "cancelled":
#                 conditions.append("B.Status = ?")
#                 params.append("C")
           
#             else:
#                 conditions.append("B.Status = ?")
#                 params.append(status)

#         if created_date:
#             conditions.append("CONVERT(date, B.MakingTime,103) = ?")
#             params.append(created_date)

#         if territory_id:
#             conditions.append("E.Territory = ?")
#             params.append(territory_id)

#         if approved_date:
#             conditions.append("CONVERT(date, B.ApproveCancelOn,103) = ?")
#             params.append(approved_date)

#         if conditions:
#             query += " WHERE " + " AND ".join(conditions)

#         draft_orders = ms_query_db(query, params, fetch_one=True)

#         if draft_orders is None or draft_orders.get('total_tonnage') is None:
#             return jsonify({"total_tonnage": 0}), 200


#         return jsonify(draft_orders), 200
#     except Exception as e:
#         return jsonify({"message": f"Internal server error: {str(e)}"}), 500
    


@order_bp.route("/orders_to_invoice", methods=["GET"])
def orders_to_invoice():
    try:
        cardcode = request.args.get("cardcode")
        orderType = request.args.get("orderType")
        fromdate = request.args.get("fromdate")
        todate = request.args.get("todate")

        query = """
            SELECT 
                S.SalesOrderNo,
                CONVERT(VARCHAR(10), SD.MakingTime, 105) AS DraftDate,
                S.DocDate AS OrderDate,
                B.CustomerId,
                B.CustomerName,
                A.ItemName,
                S.orderType,
                SUM(CASE 
                    WHEN I.UOM IN (1,4)
                        THEN CAST((CAST(A.Quantity AS numeric(10,3)) * CAST(I.altuntcom1 AS numeric(10,3)))/1000 AS numeric(10,2))
                    WHEN I.UOM IN (3)
                        THEN CAST((CAST(A.Quantity AS numeric(10,3)))/1000 AS numeric(10,2))  
                    ELSE '0' END) AS [Planned_Qty],
                B.InvoiceNo,
                B.InvoiceDate,
                L.Location,
                DATEDIFF(DAY, CONVERT(DATE, S.DocDate,105), CONVERT(DATE, B.InvoiceDate, 105)) AS OrdertoInvoiceDays
            FROM 
                SAP_AR_Invoice_Line_Details_M_Tbl A
                INNER JOIN SAP_AR_Invoice_Details_M_Tbl B ON A.DocEntry = B.DocEntry AND A.InvoiceNo = B.InvoiceNo
                INNER JOIN SAP_SalesOrder_M_Tbl S ON S.DocEntry=A.SalesOrderDocEntryNo
                INNER JOIN TBL_SalesOrderDetails SD ON S.EbizOrderId=SD.SlNo
                INNER JOIN ItemMaster_M_Tbl I ON I.ItemCode=A.ItemCode
                INNER JOIN LocationMaster_M_Tbl L ON A.LocationID=L.LocationId
        """

        conditions = []
        params = []

        # ✅ Always add this condition
        conditions.append("B.InvoiceStatus NOT IN ('Cancelled')")

        if fromdate and todate:
            conditions.append("CONVERT(date,B.InvoiceDate,103) BETWEEN ? AND ?")
            params.append(fromdate)
            params.append(todate)

        if orderType:
            conditions.append("S.NOrderType = ?")
            params.append(orderType)

        if cardcode:
            conditions.append("B.CustomerId = ?")
            params.append(cardcode)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        # ✅ Add GROUP BY for all non-aggregated columns including orderType
        query += """
            GROUP BY 
                S.SalesOrderNo,
                CONVERT(VARCHAR(10), SD.MakingTime, 105),
                S.DocDate,
                B.CustomerId,
                B.CustomerName,
                S.orderType,  -- ✅ Now included in GROUP BY
                S.NOrderType,
                B.InvoiceNo,
                B.InvoiceDate,
                L.Location,
                A.ItemName
        """
        # print(query)
        orders = ms_query_db(query, params, fetch_one=False)

        return jsonify(orders), 200
    except Exception as e:
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500
    

@order_bp.route("/partial_invoice", methods=["GET"])
def partial_invoice():
    try:
        cardcode = request.args.get("cardcode")
        fromdate = request.args.get("fromdate")
        todate = request.args.get("todate")
        associated = request.args.get("associated")

        # Base query
        query = """
            SELECT 
                SOH.SalesOrderNo,
                FORMAT(CONVERT(DATE, SOH.CreatedDateTime, 103),'dd-MM-yyyy')  AS OrderDate,
                SOH.CustomerCode,
                SOH.CustomerName,
                LOC.Location,
                ITM.Description AS ITEM,

                SUM(
                    CASE 
                        WHEN ITM.UOM IN (1, 4) THEN 
                            CAST((CAST(SOL.Quantity AS numeric(10,3)) * CAST(ITM.altuntcom1 AS numeric(10,3))) / 1000 AS numeric(10,2))
                        WHEN ITM.UOM = 3 THEN 
                            CAST(CAST(SOL.Quantity AS numeric(10,3)) / 1000 AS numeric(10,2))
                        ELSE 0
                    END
                ) AS OrderQuantity,

                SUM(
                    CASE 
                        WHEN ITM.UOM IN (1, 4) THEN 
                            CAST((CAST(INL.Quantity AS numeric(10,3)) * CAST(ITM.altuntcom1 AS numeric(10,3))) / 1000 AS numeric(10,2))
                        WHEN ITM.UOM = 3 THEN 
                            CAST(CAST(INL.Quantity AS numeric(10,3)) / 1000 AS numeric(10,2))
                        ELSE 0
                    END
                ) AS InvoicedQuantity

            FROM SAP_SalesOrder_M_Tbl SOH
            INNER JOIN SAP_SalesOrderLine_M_Tbl SOL 
                ON SOH.DocEntry = SOL.DocEntry
            INNER JOIN SAP_AR_Invoice_Line_Details_M_Tbl INL 
                ON INL.SalesOrderDocEntryNo = SOL.DocEntry 
                AND SOL.ItemCode = INL.ItemCode 
                AND SOL.Quantity <> INL.Quantity 
            INNER JOIN ItemMaster_M_Tbl ITM 
                ON SOL.ItemCode = ITM.ItemCode
            INNER JOIN LocationMaster_M_Tbl LOC
                ON SOL.LocCode = LOC.LocationId
        """

        conditions = []
        params = []

        # Mandatory
        conditions.append("ITM.UOM NOT IN (3)")

        # Date filter
        if fromdate and todate:
            conditions.append("CONVERT(date, SOH.CreatedDateTime, 103) BETWEEN ? AND ?")
            params.extend([fromdate, todate])

        # Associated group filter
        if associated == "Georoof":
            conditions.append("""
                ITM.MainGroup IN (
                    'Georoof Roofing Sheet', 'Geoclad Cladding Sheet',
                    'Posco Cladding Sheet', 'Posco Roofing Sheet'
                )
            """)

        elif associated == "AMNS":
            conditions.append("""
                ITM.MainGroup IN (
                    'AMNS Cladding Sheet', 'AMNS Roofing Sheet'
                )
            """)

        elif associated == "both":
            conditions.append("""
                ITM.MainGroup IN (
                    'Georoof Roofing Sheet', 'Geoclad Cladding Sheet',
                    'Posco Cladding Sheet', 'Posco Roofing Sheet',
                    'AMNS Cladding Sheet', 'AMNS Roofing Sheet'
                )
            """)

        # Customer filter
        if cardcode:
            conditions.append("SOH.CustomerCode = ?")
            params.append(cardcode)

        # Apply WHERE clause
        if conditions:
            query += " WHERE " + " AND ".join([c.strip() for c in conditions])

        # GROUP BY section (fixed indentation + spacing)
        query += """
            GROUP BY 
                SOH.SalesOrderNo,
                SOH.CustomerCode,
                SOH.CustomerName,
                ITM.Description,
                FORMAT(CONVERT(DATE, SOH.CreatedDateTime, 103),'dd-MM-yyyy'),
                LOC.Location
        """

        # Execute query
        orders = ms_query_db(query, params, fetch_one=False)
        return jsonify(orders), 200

    except Exception as e:
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500
