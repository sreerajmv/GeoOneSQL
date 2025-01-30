from setting.db_connections import ms_query_db
from flask import Blueprint, request, jsonify

customer_bp = Blueprint("customer", __name__)


def fetch_employee_territory(employee_code):
    """Helper function to get territory IDs from the database"""
    try:
        query = """
                    SELECT 
                        A.TeritoryID
                    FROM TBL_UserTeritory AS A
                    LEFT JOIN TBL_Users AS B ON A.UserID = B.UserID
                    WHERE 
                        RefCode = ?
                """
        params = (str(employee_code),)
        territory_id = ms_query_db(query, params, fetch_one=False)
        return [row["TeritoryID"] for row in territory_id]  # Return a list of IDs

    except Exception as e:
        raise Exception(f"Database error: {str(e)}")


@customer_bp.route("/employee_territory/<employee_code>", methods=["GET"])
def get_employee_territory(employee_code):
    """API to fetch employee territory"""
    try:
        territory_ids = fetch_employee_territory(employee_code)
        return jsonify(territory_ids), 200

    except Exception as e:
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500


@customer_bp.route("/customer", methods=["GET"])
def get_customer():
    """API to fetch customer details based on employee territory"""
    try:
        employee_code = request.args.get("employee_code")

        # Validate employee_code
        if not employee_code:
            return jsonify({"message": "Employee Code cannot be None"}), 400

        # Fetch territories using the helper function (not the API)
        territories = fetch_employee_territory(employee_code)

        if not territories:
            return jsonify({"message": "No territories found for this employee"}), 404

        # Generate placeholders dynamically for SQL query
        placeholders = ", ".join("?" * len(territories))
        query = f"SELECT CardCode, CardName, Territory FROM CustomerMaster_M_Tbl WHERE Territory IN ({placeholders})"

        params = tuple(territories)  # Convert list to tuple for SQL execution

        customer = ms_query_db(query, params, fetch_one=False)
        return jsonify(customer), 200

    except Exception as e:
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500
