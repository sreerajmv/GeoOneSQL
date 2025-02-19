from flask import Flask
from routes.bom.bom import bom_bp
from routes.order.order import order_bp
from routes.order.customer import customer_bp
from routes.order.item import item_bp
from routes.SAP.incoming_payment import sap_bp


def register_blueprints(app: Flask):
    app.register_blueprint(bom_bp, url_prefix="/bom")
    app.register_blueprint(order_bp, url_prefix="/order")
    app.register_blueprint(customer_bp, url_prefix="/customer")
    app.register_blueprint(item_bp, url_prefix="/item")
    app.register_blueprint(sap_bp, url_prefix="/sap")
