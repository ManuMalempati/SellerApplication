SELECT
    SKU,
    ASIN,
    FNSKU,
    SSKU,
    [FBA-Stock],
    [Sellable-Qty],
    [Unsellable-Qty],
    Title,
    [Sale-Price],
    Charges,
    [Est-VAT],
    [Est-Net],
    COG,
    Profit,
    Brand,
    Category,
    TotalOrderItems_L30,
    OrderedProductSales_L30,
    UnitsRefunded_L30,
    BuyBoxPercentage_L30,
    fba_updated_at
FROM spapi_app_user.FBAProductSummary
ORDER BY SKU;
