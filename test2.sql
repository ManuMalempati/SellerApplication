SELECT FNSKU FROM spapi_app_user.ProductMappingTest
GROUP BY FNSKU
HAVING COUNT(*) > 1