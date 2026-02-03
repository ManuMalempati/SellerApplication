UPDATE OrderItems
SET 
    OrderDate = DATEADD(hour, 4, OrderDate),
    LastUpdateDate = DATEADD(hour, 4, LastUpdateDate);
