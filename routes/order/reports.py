from setting.db_connections import ms_query_db
from flask import Blueprint, request, jsonify
  # type: ignore
# import pyodbc

ms_reports_bp = Blueprint("ms_reports", __name__)



@ms_reports_bp.route("/approved_open_orders", methods=["GET"])
def get_approved_orders():
    try:
        territory_id = request.args.get("territory_id")
        created_date = request.args.get("created_date")
        associated = request.args.get("associated")

        query = """
    				SELECT 
					S.SalesOrderNo AS [Order_No],	
					S.NOrderType AS OrderType,
					S.CreatedDateTime AS MakingTime,
					S.CustomerCode,
					E.CardName,
					C.Description AS [itemName],
					U.Name,
					SE.U_Associated,
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
					INNER JOIN CustomerMaster_M_Tbl E ON E.CardCode = S.CustomerCode
					INNER JOIN ItemMaster_M_Tbl C ON C.ItemCode = SL.ItemCode  AND C.MainGroup IN ('Geoclad Cladding Sheet', 'Georoof Roofing Sheet', 'AMNS Cladding Sheet', 'AMNS Roofing Sheet')
					INNER JOIN TBL_SalesOrderDetails SD ON S.EbizOrderId = SD.SlNo
					INNER JOIN TBL_Users U ON U.UserID = SD.MakerID
					INNER JOIN LocationMaster_M_Tbl F ON F.Code = SL.LocCode
					INNER JOIN Uom_Master_M_Tbl D ON D.UomId = C.UOM
					INNER JOIN SalesEmployeeMaster_M_Tbl SE ON SD.SalesPerson=SE.SalesEmployeeCode
					INNER JOIN TBL_SalesOrderStatus A ON S.SalesOrderStatus = A.Name  AND A.SlNo NOT IN (2, 1004, 1005, 1006, 1009)
            """
        conditions = []
        params = []



        if territory_id:
            conditions.append("E.Territory = ?")
            params.append(territory_id)

        if created_date:
            conditions.append("convert(date, S.DocDate, 103)= ?")
            params.append(created_date)

        if associated in ("AMNS", "Georoof"):
            conditions.append("SE.U_Associated = ?")
            params.append(associated)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        params = tuple(params)
        approved_orders = ms_query_db(query, params, fetch_one=False)
        return jsonify(approved_orders), 200
    except Exception as e:
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500

@ms_reports_bp.route("/approved_open_orders_summary", methods=["GET"])
def approved_open_orders_summary():
    try:
        territory_id = request.args.get("territory_id")
        created_date = request.args.get("created_date")
        associated = request.args.get("associated")
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
                        INNER JOIN CustomerMaster_M_Tbl E ON E.CardCode = S.CustomerCode
                        INNER JOIN ItemMaster_M_Tbl C ON C.ItemCode = SL.ItemCode  AND C.MainGroup IN ('Geoclad Cladding Sheet', 'Georoof Roofing Sheet', 'AMNS Cladding Sheet', 'AMNS Roofing Sheet')
                        INNER JOIN TBL_SalesOrderDetails SD ON S.EbizOrderId = SD.SlNo
                        INNER JOIN TBL_Users U ON U.UserID = SD.MakerID
                        INNER JOIN LocationMaster_M_Tbl F ON F.Code = SL.LocCode
                        INNER JOIN Uom_Master_M_Tbl D ON D.UomId = C.UOM
                        INNER JOIN SalesEmployeeMaster_M_Tbl SE ON SD.SalesPerson=SE.SalesEmployeeCode
                        INNER JOIN TBL_SalesOrderStatus A ON S.SalesOrderStatus = A.Name  AND A.SlNo NOT IN (2, 1004, 1005, 1006, 1009)

        """
        conditions = []
        params = []

        if territory_id:
            conditions.append("E.Territory = ?")
            params.append(territory_id)

        if created_date:
            conditions.append("convert(date, S.DocDate, 103)= ?")
            params.append(created_date)

        if associated in ("AMNS", "Georoof"):
            conditions.append("SE.U_Associated = ?")
            params.append(associated)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        params = tuple(params)
        approved_orders = ms_query_db(query, params, fetch_one=True)

        if approved_orders is None or "TotalTonnage" not in approved_orders:
            return jsonify({"TotalTonnage": 0}), 200

        return jsonify(approved_orders), 200
    except Exception as e:
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500

@ms_reports_bp.route("/draft_orders", methods=["GET"])
def draft_orders():
    try:
        # sales_person = request.args.get("sales_person")
        status = request.args.get("status")
        created_date = request.args.get("created_date")
        territory_id = request.args.get("territory_id")
        approved_date = request.args.get("approved_date")
        associated = request.args.get("associated")
        query = """
                    SELECT 
                        B.SlNo [Order_No],
                        CASE WHEN B.OrderType='WO' THEN 'WorkOrder'
                        WHEN B.OrderType='SO' THEN 'SalesOrder' 
                        ELSE '' END AS OrderType,
                        B.MakingTime,
                        B.CustCode AS CustomerCode,
                        E.CardName,
                        U.Name,
                        C.Description [itemName],
                        F.Location,
                        CAST(ISNULL(CASE WHEN D.UomCode ='SQM' THEN TRY_CAST(A.Qty*TRY_CAST(C.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2))/1000
                            WHEN D.UomCode ='KGS' THEN TRY_CAST(A.Qty AS NUMERIC(10,2))/1000
                            WHEN D.UomCode ='MTR' THEN TRY_CAST((A.Qty*TRY_CAST(C.Width AS NUMERIC(10,2))) * TRY_CAST(C.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2))/1000
                            WHEN D.UomCode ='NOS' THEN TRY_CAST(A.Qty*TRY_CAST(C.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2))/1000
                            ELSE 0 END ,0) AS NUMERIC(10,2))  AS [Tonnage],
                        CASE WHEN B.Status = 'C' THEN 'Cancelled'
                        WHEN B.Status = 'Y' THEN 'Posted to SAP'
                        WHEN B.Status = 'E'  THEN 'Expired'
                        WHEN B.Status = 'N' THEN 'Draft'
                        WHEN B.Status = 'R' THEN 'Dealer Confirmed'
                        WHEN B.Status = 'P' then 'Dealer Draft'
                        END AS [Status]
                    FROM 
                        TBL_SalesOrderProductDetails A
                        INNER JOIN TBL_SalesOrderDetails B ON A.SOID = B.SlNo
                        INNER JOIN ItemMaster_M_Tbl C ON C.ItemCode = A.ProductCode  AND C.MainGroup IN ('Geoclad Cladding Sheet', 'Georoof Roofing Sheet', 'AMNS Cladding Sheet', 'AMNS Roofing Sheet')
                        INNER JOIN Uom_Master_M_Tbl D ON D.UomId = C.UOM
                        INNER JOIN CustomerMaster_M_Tbl E ON E.CardCode = B.CustCode
                        INNER JOIN LocationMaster_M_Tbl F ON F.Code = B.LocationID
                        INNER JOIN TBL_Users U ON U.UserID=A.MakerID
                        INNER JOIN SalesEmployeeMaster_M_Tbl SE ON B.SalesPerson=SE.SalesEmployeeCode
                        INNER JOIN Employee_Master_M_Tbl EM ON EM.SapEmployeeId=SE.EmployeeId
                """
        conditions = []
        params = []


        if associated in ("AMNS", "Georoof"):
            conditions.append("SE.U_Associated = ?")
            params.append(associated)




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

        if created_date:
            conditions.append("CONVERT(date, B.MakingTime,103) = ?")
            params.append(created_date)

        if territory_id:
            conditions.append("E.Territory = ?")
            params.append(territory_id)

        if approved_date:
            conditions.append("CONVERT(date, B.ApproveCancelOn,103) = ?")
            params.append(approved_date)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        draft_orders = ms_query_db(query, params, fetch_one=False)
        return jsonify(draft_orders), 200
    except Exception as e:
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500



@ms_reports_bp.route("/draft_orders_summary", methods=["GET"])
def draft_orders_summary():
    try:
        # sales_person = request.args.get("sales_person")
        status = request.args.get("status")
        created_date = request.args.get("created_date")
        territory_id = request.args.get("territory_id")
        approved_date = request.args.get("approved_date")
        associated = request.args.get("associated")
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
                        INNER JOIN ItemMaster_M_Tbl C ON C.ItemCode = A.ProductCode  AND C.MainGroup IN ('Geoclad Cladding Sheet', 'Georoof Roofing Sheet', 'AMNS Cladding Sheet', 'AMNS Roofing Sheet')
                        INNER JOIN Uom_Master_M_Tbl D ON D.UomId = C.UOM
                        INNER JOIN CustomerMaster_M_Tbl E ON E.CardCode = B.CustCode
                        INNER JOIN LocationMaster_M_Tbl F ON F.Code = B.LocationID
                        INNER JOIN TBL_Users U ON U.UserID=A.MakerID
                        INNER JOIN SalesEmployeeMaster_M_Tbl SE ON B.SalesPerson=SE.SalesEmployeeCode
                        INNER JOIN Employee_Master_M_Tbl EM ON EM.SapEmployeeId=SE.EmployeeId
                """
        conditions = []
        params = []

        # if user_id:
        #     conditions.append(
        #         "EM.EmployeeId = ? AND C.MainGroup IN ('Geoclad Cladding Sheet', 'Georoof Roofing Sheet')"
        #     )
        #     params.append(user_id)

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

        if created_date:
            conditions.append("CONVERT(date, B.MakingTime,103) = ?")
            params.append(created_date)

        if territory_id:
            conditions.append("E.Territory = ?")
            params.append(territory_id)

        if approved_date:
            conditions.append("CONVERT(date, B.ApproveCancelOn,103) = ?")
            params.append(approved_date)

        if associated in ("AMNS", "Georoof"):
            conditions.append("SE.U_Associated = ?")
            params.append(associated)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        draft_orders = ms_query_db(query, params, fetch_one=True)

        if draft_orders is None or draft_orders.get("total_tonnage") is None:
            return jsonify({"total_tonnage": 0}), 200

        return jsonify(draft_orders), 200
    except Exception as e:
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500
