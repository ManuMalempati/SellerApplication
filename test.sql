SELECT *
FROM (
    SELECT *,
           COUNT(*) OVER (PARTITION BY PartNumber) AS Cnt
    FROM InventoryReport
) t
WHERE Cnt > 1;
