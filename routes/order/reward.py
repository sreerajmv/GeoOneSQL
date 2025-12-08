from setting.db_connections import ms_query_db
from flask import Blueprint, request, jsonify


reward_bp = Blueprint("reward", __name__)





@reward_bp.route("/fab_data", methods=["GET"])
def fab_data():
    try:
        territory = request.args.get("territory")
        cardcode = request.args.get("cardcode")
        if not territory:
            return jsonify({"error": "Territory ID is required"}), 400
        query = "[dbo].[Sch_Proc_FabData] @TerritoryID = ?"
        params = (territory,)
        if cardcode:
            query += ", @CardCode = ?"
            params += (cardcode,)
        fab_data = ms_query_db(query, params)
        if fab_data is None:
            return jsonify({"error": "Database query returned no results"}), 404
        return jsonify(fab_data), 200
    except Exception as e:
        return jsonify({"error": f"Internal server error {str(e)}"}), 500
    

@reward_bp.route("/party_order_expiry", methods=["GET"])
def party_order_expiry():
    try:
        regionId = request.args.get("regionid")
        fromDate = request.args.get("fromDate")
        toDate = request.args.get("toDate")

        # Validate required parameters
        if not fromDate or not toDate:
            return jsonify({"error": "Both fromDate and toDate are required."}), 400

        query = """
            SELECT 
                C.CardCode,
                C.CardName,
                InventoryNo AS OrderNo,
                BT.Descript AS Territory,
                FORMAT(CONVERT(DATE, InventoryDate, 103),'dd-MM-yyyy')  AS OrderDate,
                I.Description AS Item,
                Quantity
            FROM DealerPointStock_T_Tbl D
                INNER JOIN CustomerMaster_M_Tbl C ON D.CardCode=C.CardCode
                INNER JOIN Bde_Territory_M_Tbl BT ON BT.TerritoryID=C.Territory
                INNER JOIN Region_M_Tbl R ON BT.RegionID=R.RegionID
                INNER JOIN ItemMaster_M_Tbl I ON D.Itemcode=I.ItemCode
        """

        conditions = []
        params = []

        # FIX 1: Removed double "AND AND" and cleaned up string
        # Note: Ideally, prefix these columns with their table alias (e.g., D.Type) to avoid ambiguity
        conditions.append(
            "Type='EXP' AND OrderPriority='P' AND AllocationStatus='E' AND MainGroup IN ('Georoof Roofing Sheet','Geoclad Cladding Sheet')"
        )

        if regionId:
            conditions.append("R.RegionID = ?")
            params.append(regionId)

        if fromDate and toDate:
            # Note: Ensure your DB expects dates in dd/mm/yyyy format due to style 103
            conditions.append("CONVERT(DATE, D.CreatedOn, 103) BETWEEN ? AND ?")
            # FIX 2: append() takes only one argument. Use extend() for multiple items.
            params.extend([fromDate, toDate])

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        # Execute Query
        party_order_expiry_data = ms_query_db(query, params)

        if party_order_expiry_data is None:
            # Optional: Return empty list [] instead of 404 if just no records found,
            # but 404 is fine if that's your API standard.
            return jsonify({"error": "No records found"}), 404

        return jsonify(party_order_expiry_data), 200

    except Exception as e:
        # It is helpful to log 'e' here for debugging
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500
    

@reward_bp.route("/stock_order_expiry", methods=["GET"])
def get_stock_order_expiry():
    try:
        fromDate = request.args.get("fromDate")
        toDate = request.args.get("toDate")
        regionId = request.args.get("regionid")
        # Validate required parameters
        if not fromDate or not toDate:
            return jsonify({"error": "Both fromDate and toDate are required."}), 400

        query = """
                    SELECT 
                        C.CardCode,
                        C.CardName,
                        I.Description AS Item,
                        BT.Descript AS Territory,
                        SUM(CAST(Quantity  AS NUMERIC(18,2))) AS Quantity
                    FROM DealerPointStock_T_Tbl D
                        INNER JOIN CustomerMaster_M_Tbl C ON D.CardCode=C.CardCode
                        INNER JOIN Bde_Territory_M_Tbl BT ON BT.TerritoryID=C.Territory
                        INNER JOIN Region_M_Tbl R ON BT.RegionID=R.RegionID
                        INNER JOIN ItemMaster_M_Tbl I ON D.Itemcode=I.ItemCode
                """

        conditions = []
        params = []

        # FIX 1: Removed double "AND AND" and cleaned up string
        # Note: Ideally, prefix these columns with their table alias (e.g., D.Type) to avoid ambiguity
        conditions.append(
            "Type='EXP' AND OrderPriority='S'  AND MainGroup IN ('Georoof Roofing Sheet','Geoclad Cladding Sheet')"
        )

        if regionId:
            conditions.append("R.RegionID = ?")
            params.append(regionId)

        if fromDate and toDate:
            # Note: Ensure your DB expects dates in dd/mm/yyyy format due to style 103
            conditions.append("CONVERT(DATE, D.CreatedOn, 103) BETWEEN ? AND ?")
            # FIX 2: append() takes only one argument. Use extend() for multiple items.
            params.extend([fromDate, toDate])

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        # GROUP BY section (fixed indentation + spacing)
        query += """
                    GROUP BY 
                        C.CardCode,
                        C.CardName,
                        I.Description,
                        BT.Descript
                 """

        # Execute Query
        stock_order_expiry_data = ms_query_db(query, params)

        if stock_order_expiry_data is None:
            # Optional: Return empty list [] instead of 404 if just no records found,
            # but 404 is fine if that's your API standard.
            return jsonify({"error": "No records found"}), 404

        return jsonify(stock_order_expiry_data), 200

    except Exception as e:
        # It is helpful to log 'e' here for debugging
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


