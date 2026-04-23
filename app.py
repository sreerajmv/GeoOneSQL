from flask import (
    Flask,
    jsonify,
    request,
    render_template,
    session,
    redirect,
    url_for,
    abort,
)
from routes import register_blueprints
from flask_cors import CORS  # type: ignore
from setting.db_connections import ms_query_db
# import os

app = Flask(__name__)

app.secret_key = "super_secret_development_key"

VALID_USERNAME = "admin"
VALID_PASSWORD = "secure123"

# Define the list of allowed IP addresses here.
# 127.0.0.1 allows you to test it locally.
ALLOWED_IPS = ["127.0.0.1", "10.10.0.204", "10.10.0.218", "10.10.0.231", "10.10.0.123"]

CORS(app)
register_blueprints(app)


@app.route("/")
def index():
    return jsonify({"message": "Hello, World!"})


# --- 1. LOGIN ROUTE ---
@app.route("/login", methods=["GET", "POST"])
def login():
    message = None

    if session.get("logged_in"):
        return redirect(url_for("update_order_discount"))

    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")

        if username == VALID_USERNAME and password == VALID_PASSWORD:
            session["logged_in"] = True
            return redirect(url_for("update_order_discount"))
        else:
            message = "Invalid username or password."

    return render_template("login.html", message=message)


# --- 2. LOGOUT ROUTE ---
@app.route("/logout")
def logout():
    session.pop("logged_in", None)
    return redirect(url_for("login"))


# --- 3. SECURE DISCOUNT ROUTE ---
@app.route("/test", methods=["GET", "POST"])
def update_order_discount():
    # ---------------------------------------------------------
    # 1. IP WHITELIST CHECK
    # ---------------------------------------------------------
    # Get the IP address of the user making the request
    client_ip = request.remote_addr

    # If the IP is not in our list, instantly reject the request
    if client_ip not in ALLOWED_IPS:
        # abort(403) throws a standard "Forbidden" HTTP error page
        abort(
            403,
            description="Access Denied: Your IP address is not authorized to view this page.",
        )

    # ---------------------------------------------------------
    # 2. SESSION AUTHENTICATION CHECK
    # ---------------------------------------------------------
    if not session.get("logged_in"):
        return redirect(url_for("login"))

    message = None
    message_category = None

    if request.method == "POST":
        try:
            new_discount_amount = request.form.get("NewDiscountAmount")
            target_soid = request.form.get("TargetSOID")

            if not new_discount_amount or not target_soid:
                message = "Missing required fields."
                message_category = "danger"
            else:
                new_discount_amount = float(new_discount_amount)
                target_soid = int(target_soid)

                query = """
                BEGIN TRAN;
                DECLARE @NewDiscountAmount DECIMAL(18, 2) = ?; 
                DECLARE @TargetSOID INT = ?;
                DECLARE @GSTMultiplier DECIMAL(18, 4) = 1.18;

                UPDATE TBL_SalesOrderProductDetails
                SET 
                    DiscountAmount = @NewDiscountAmount,
                    DiscountPerc = CAST((@NewDiscountAmount / NULLIF(TRY_CAST(REPLACE(Rate, ',', '') AS DECIMAL(18, 4)), 0)) * 100 AS DECIMAL(18, 2))
                WHERE SOID = @TargetSOID;

                UPDATE Header
                SET 
                    Header.NetTaxableAmount = Calc.NewNetTaxableAmount,
                    Header.NetAmount = ROUND(Calc.NewNetTaxableAmount * @GSTMultiplier, 0),
                    Header.RoundOff = ROUND(Calc.NewNetTaxableAmount * @GSTMultiplier, 0) - (Calc.NewNetTaxableAmount * @GSTMultiplier)
                FROM TBL_SalesOrderDetails Header
                INNER JOIN (
                    SELECT SOID,
                        SUM(ISNULL(TRY_CAST(REPLACE(LineTotal, ',', '') AS DECIMAL(18, 4)), 0)) - 
                        SUM((ISNULL(TRY_CAST(REPLACE(Qty, ',', '') AS DECIMAL(18, 4)), 0) * @NewDiscountAmount) / @GSTMultiplier) AS NewNetTaxableAmount
                    FROM TBL_SalesOrderProductDetails
                    WHERE SOID = @TargetSOID
                    GROUP BY SOID
                ) Calc ON Header.SlNo = Calc.SOID;

                COMMIT TRAN;
                """

                params = (new_discount_amount, target_soid)
                ms_query_db(query, args=params, commit=True)

                message = f"Order discount for SOID {target_soid} updated successfully!"
                message_category = "success"

        except ValueError:
            message = "Invalid input format."
            message_category = "danger"
        except Exception as e:
            message = f"Internal server error: {str(e)}"
            message_category = "danger"

    return render_template(
        "update_discount.html", message=message, message_category=message_category
    )


if __name__ == "__main__":
    app.run(debug=True, port=5001)
