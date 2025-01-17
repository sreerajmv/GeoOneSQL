from flask import Flask
from routes.bom.bom import bom_bp
from routes.order.order import order_bp

def register_blueprints(app: Flask):
    app.register_blueprint(bom_bp, url_prefix="/bom")
    app.register_blueprint(order_bp, url_prefix="/order")