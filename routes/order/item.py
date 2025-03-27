from setting.db_connections import ms_query_db
from flask import Blueprint, jsonify, request

item_bp = Blueprint("item", __name__)



@item_bp.route("/itemgroup", methods=["GET"])
def get_itemgroup():
    try:
        # Initialize query and parameters
        base_query = "select  ItmsGrpCod, ItmsGrpNam from ItemGroupMaster_M_Tbl"

        # Query the database
        itemgroup = ms_query_db(base_query)

        if itemgroup:
            return jsonify(itemgroup), 200
    except Exception as e:
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500

@item_bp.route("/stock_summary", methods=["GET"])
def get_stock_summary():
    try:
        # Get all possible parameters (all optional)
        params = {
            "@location": request.args.get("location"),
            "@Group": request.args.get("group"),
            "@Category": request.args.get("category"),
            "@Color": request.args.get("color"),
            "@Thickness": request.args.get("thickness"),
        }

        # Filter out None values
        sp_params = {k: v for k, v in params.items() if v is not None}

        # Prepare the parameter placeholders and values
        # For pyodbc, we need to format as "@param=?" and provide values in order
        param_placeholders = []
        param_values = []
        for param_name, param_value in sp_params.items():
            param_placeholders.append(f"{param_name}=?")
            param_values.append(param_value)

        # Build the EXEC statement
        if param_placeholders:
            query = f"EXEC Stock_Summary {','.join(param_placeholders)}"
        else:
            query = "EXEC Stock_Summary"

        # Call the stored procedure
        result = ms_query_db(query, args=tuple(param_values), fetch_one=False)

        if not result:
            return jsonify({"message": "No data found for the given criteria"}), 404

        return jsonify(result), 200

    except Exception as e:
        return jsonify(
            {"message": "Failed to fetch stock summary", "error": str(e)}
        ), 500