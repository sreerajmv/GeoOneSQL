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
    
