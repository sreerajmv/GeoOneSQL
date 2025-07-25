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
    

@customer_bp.route("/cse_territory/<employee_code>", methods=["GET"])   
def fetch_cse_territory(employee_code):
    """Helper function to get territory IDs and names from the database for a given employee"""
    try:
        query = """
                SELECT 
                    A.TerritoryID,
                    A.Descript AS Territory
                FROM 
                    Bde_Territory_M_Tbl A
                    LEFT JOIN Zone_M_Tbl B ON A.ZoneID = B.ZoneID
                WHERE 
                    B.EmpId = ?
                """
        params = (employee_code,)
        territories = ms_query_db(query, params, fetch_one=False)

        # Return list of dictionaries with both ID and Territory name
        return [
            {"TerritoryID": row["TerritoryID"], "Territory": row["Territory"]}
            for row in territories
        ]

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
        query = f"SELECT CardCode, CardName, Territory FROM CustomerMaster_M_Tbl where CardType = 'c' AND Territory IN ({placeholders})"

        params = tuple(territories)  # Convert list to tuple for SQL execution

        customer = ms_query_db(query, params, fetch_one=False)
        return jsonify(customer), 200

    except Exception as e:
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500
    
@customer_bp.route("/by_territory", methods=["GET"])
def get_customer_by_territory():
    """API to fetch customer details based on employee territory"""
    try:
        territory_code = request.args.get("territory")

        # Validate employee_code
        if not territory_code:
            return jsonify({"message": "Territory Code cannot be None"}), 400

        query = "SELECT CardCode, CardName, Territory FROM CustomerMaster_M_Tbl where CardType = 'c' AND Territory = ?"

        params = (territory_code,)  # Convert list to tuple for SQL execution

        customer = ms_query_db(query, params, fetch_one=False)
        return jsonify(customer), 200

    except Exception as e:
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500

    
    
@customer_bp.route("/territory", methods=["GET"])
def get_territory():
    """API to fetch customer details based on employee territory"""
    try:
        query = "select TerritoryID, Descript, ZoneID, AreaID, RegionID, EmpId, OrderBy, Mail from Bde_Territory_M_Tbl"
        territory = ms_query_db(query, fetch_one=False)
        return jsonify(territory), 200

    except Exception as e:
        return jsonify({"message": f"Internal server error: {str(e)}"}), 500
