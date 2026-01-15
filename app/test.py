from .main import connection
from .database import get_product_cost


cursor = connection.cursor()

SKU = 'BL.9BWWA.587'

print( SKU + " cost: " + str(get_product_cost(cursor=cursor, seller_sku=SKU)))