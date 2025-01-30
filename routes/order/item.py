from setting.db_connections import ms_query_db
from flask import Blueprint, jsonify

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

