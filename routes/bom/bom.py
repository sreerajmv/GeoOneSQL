from setting.db_connections import ms_query_db
from flask import Blueprint, request, jsonify
import json
# from datetime import datetime

bom_bp = Blueprint("bom", __name__)


@bom_bp.route("/itemgroup", methods=["GET"])
def get_itemgroup():
    try:
        # Get the query parameter 'group'
        group = request.args.get("group", type=str)
        color = request.args.get("color", type=str)

        if "group" in request.args and group is None or group == "":
            return jsonify({"message": "Group cannot be None"}), 400

        if "color" in request.args and color is None or color == "":
            return jsonify({"message": "Color cannot be None"}), 400

        # Initialize query and parameters
        base_query = """
            SELECT DISTINCT(Group3)
            FROM ItemMaster_M_Tbl
        """
        conditions = []
        params = []

        # Add conditions based on 'group' parameter
        if group == "fg":
            conditions.append("SubGroup IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            params.extend(
                [
                    "Posco PPGL Roofing Sheet",
                    "Georoof Metalux Roofing Sheet",
                    "AMNS PPGI Roofing Sheet",
                    "Georoof PPGI Roofing Sheet",
                    "Georoof PPGLX Roofing Sheet",
                    "Geoclad PPGL Cladding Sheet",
                    "Georoof Geolux Roofing Sheet",
                    "AMNS PPGI Cladding Sheet",
                    "Posco PPGL Cladding Sheet",
                    "PPGI Roofing Sheet",
                ]
            )
        elif group == "rm":
            conditions.append("SubGroup IN (?, ?, ?, ?)")
            params.extend(
                ["Georoof Baby Coil", "Georoof Coil", "AMNS Coil", "AMNS Baby Coil"]
            )

        # Add conditions based on 'color' parameter
        if color:
            conditions.append("Color = ?")
            params.append(color)

        # Append conditions if any
        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)

        # Query the database
        itemgroup = ms_query_db(base_query, tuple(params))

        return jsonify(itemgroup), 200
    except Exception as e:
        return jsonify({"message": str(e)}), 400


@bom_bp.route("/colour", methods=["GET"])
def get_colour():
    try:
        # Get the query parameter 'group'
        group = request.args.get("group", type=str)

        if "group" in request.args and group is None or group == "":
            return jsonify({"message": "Group cannot be None"}), 400

        # Initialize query and parameters
        base_query = """
                        SELECT 
                        DISTINCT(Color) 
                        FROM ItemMaster_M_Tbl
                    """
        conditions = []
        params = []

        # Add conditions based on 'group' parameter
        if group:
            conditions.append("Group3 = ?")
            params.append(group)

        # Append conditions if any
        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)

        # Query the database
        itemgroup = ms_query_db(base_query, tuple(params))

        return jsonify(itemgroup), 200
    except Exception as e:
        return jsonify({"message": str(e)}), 400


@bom_bp.route("/bomitem", methods=["GET"])
def get_rm_item():
    try:
        # Extract query parameters
        group = request.args.get("group", type=str)
        calculation = request.args.get("calculation", type=str)
        color = request.args.get("color", type=str)
        thickness = request.args.get("thickness", type=str)
        without_bom = request.args.get(
            "without_bom", type=lambda v: v.lower() in ("true", "1", "yes")
        )

        # Validate parameters
        if "group" in request.args and group is None or group == "":
            return jsonify({"message": "Group cannot be None"}), 400
        if "calculation" in request.args and calculation is None or calculation == "":
            return jsonify({"message": "Calculation cannot be None"}), 400
        if "color" in request.args and color is None or color == "":
            return jsonify({"message": "Color cannot be None"}), 400
        if "thickness" in request.args and thickness is None or thickness == "":
            return jsonify({"message": "Thickness cannot be None"}), 400
        if "without_bom" in request.args and without_bom is None or without_bom == "":
            return jsonify({"message": "Without BOM cannot be None"}), 400

        # Build the query and parameters
        query = "SELECT ItemCode, Description, Color, Thickness, calculation FROM ItemMaster_M_Tbl"
        conditions = []
        params = []

        if group:
            conditions.append("Group3 = ?")
            params.append(group)
        if calculation:
            conditions.append("Calculation = ?")
            params.append(calculation)
        if color:
            conditions.append("Color = ?")
            params.append(color)
        if thickness:
            conditions.append("Thickness = ?")
            params.append(thickness)

        if without_bom is not None:
            if without_bom:  # If true
                # print("without_bom is true")
                conditions.append(
                    "ItemCode NOT IN (SELECT FG_CODE FROM WorkOrderRM_ConversionValue_M_Tbl)"
                )
            else:  # If false
                # print("without_bom is false")
                conditions.append(
                    "ItemCode IN (SELECT FG_CODE FROM WorkOrderRM_ConversionValue_M_Tbl)"
                )

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        # Execute the query
        result = ms_query_db(query, tuple(params), fetch_one=False)

        # Handle result
        if not result:
            return jsonify({"message": "Item not found"}), 404

        # Return the result
        return jsonify(result), 200

    except Exception as e:
        # Handle unexpected errors
        return jsonify({"message": str(e)}), 500


@bom_bp.route("/bom", methods=["POST"])
def create_bom():
    try:
        data = request.get_json()
        fg_code = data.get("fg_code")
        fg_item = data.get("fg_item")
        FG_Produce_from_How_Many_Raw_Material = data.get(
            "FG_Produce_from_How_Many_Raw_Material"
        )
        rm_code = data.get("rm_code")
        rm_item = data.get("rm_item")
        rm_conversion_value = data.get("rm_conversion_value")

        # Validate all fields
        if not all(
            [
                fg_code,
                fg_item,
                FG_Produce_from_How_Many_Raw_Material,
                rm_code,
                rm_item,
                rm_conversion_value,
            ]
        ):
            return jsonify({"message": "All fields are required"}), 400

        # SQL Query
        query = """
            INSERT INTO dbo.WorkOrderRM_ConversionValue_M_Tbl 
            (FG_CODE, FG_ITEM_NAME, FG_Produce_from_How_Many_Raw_Material, SAP_RM_Code, RM_Item_Name, RM_Conversion_Value)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        params = (
            fg_code,
            fg_item,
            FG_Produce_from_How_Many_Raw_Material,
            rm_code,
            rm_item,
            rm_conversion_value,
        )

        # Execute query
        ms_query_db(query, args=params, commit=True)

        # Success response
        return jsonify({"message": "BOM created successfully"}), 201

    except Exception as e:
        # Error response
        return jsonify({"message": str(e)}), 400


@bom_bp.route("/bom", methods=["GET"])
@bom_bp.route("/bom/<string:bom_id>", methods=["GET"])
def get_bom(bom_id=None):
    try:
        query = """
                    SELECT 
                        A.ID,
                        A.FG_CODE,
                        B.Description,
                        A.FG_Produce_from_How_Many_Raw_Material,
                        A.SAP_RM_Code,
                        B.Group3 AS FG_Item_Group,
                        C.group3 AS RM_Item_Group,
                        C.Description AS [RM_Item_Name],
                        A.RM_Conversion_Value,
                        B.Thickness,
                        B.Color,
                        B.Tolerance
                    FROM
                    WorkOrderRM_ConversionValue_M_Tbl AS A
                    LEFT JOIN ItemMaster_M_Tbl AS B ON A.FG_CODE = B.ItemCode
                    LEFT JOIN ItemMaster_M_Tbl AS C ON A.SAP_RM_Code = C.ItemCode
                """
        params = ()
        if bom_id:
            query += " WHERE A.FG_CODE = ?;"
            params = (bom_id,)
        # execute query
        result = ms_query_db(query, params, fetch_one=bom_id is not None)
        if not result:
            return jsonify({"message": "BOM not found"}), 404
        return jsonify(result), 200

    except Exception as e:
        return jsonify({"message": str(e)}), 400


@bom_bp.route("/bom/<string:fg_code>", methods=["PUT"])
def update_bom(fg_code):
    try:
        # Get data from the request body
        print("Hello")
        data = request.get_json()
        if not data:
            return jsonify({"message": "Invalid request, JSON data is required"}), 400

        # Extract required fields from the JSON data
        FG_Produce_from_How_Many_Raw_Material = data.get(
            "FG_Produce_from_How_Many_Raw_Material"
        )
        rm_code = data.get("rm_code")
        rm_item = data.get("rm_item")
        rm_conversion_value = data.get("rm_conversion_value")

        # Check if all required fields are provided
        if FG_Produce_from_How_Many_Raw_Material is None:
            return jsonify(
                {"message": "FG Produce from How Many Raw Material is required"}
            ), 400
        if not rm_code:
            return jsonify({"message": "RM Code is required"}), 400
        if not rm_item:
            return jsonify({"message": "RM Item is required"}), 400
        if rm_conversion_value is None:
            return jsonify({"message": "RM Conversion Value is required"}), 400

        # Prepare the SQL query and parameters
        query = """
            UPDATE dbo.WorkOrderRM_ConversionValue_M_Tbl
            SET 
                FG_Produce_from_How_Many_Raw_Material = ?,
                SAP_RM_Code = ?,
                RM_Item_Name = ?,
                RM_Conversion_Value = ?
            WHERE FG_CODE = ?;
        """
        params = (
            FG_Produce_from_How_Many_Raw_Material,
            rm_code,
            rm_item,
            rm_conversion_value,
            fg_code,
        )

        # Execute the query
        ms_query_db(query, params, commit=True)

        # Success response
        return jsonify({"message": "BOM updated successfully"}), 200

    except Exception as e:
        # Log the exception (optional) and return an error response
        print(f"Error occurred: {e}")
        return jsonify({"message": f"An error occurred: {str(e)}"}), 500
    

@bom_bp.route("/order", methods=["GET"])
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
