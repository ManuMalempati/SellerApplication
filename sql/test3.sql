SELECT *
FROM InventoryReportCopy
WHERE PartNumber NOT IN (
    SELECT PartNumber
    FROM InventoryReport
    WHERE PartNumber IS NOT NULL
);
