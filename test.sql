-- Add FBA-related columns to ProductMapping table
ALTER TABLE ProductMapping ADD COLUMN fnsku VARCHAR(50);
ALTER TABLE ProductMapping ADD COLUMN fba_stock INT DEFAULT 0;
ALTER TABLE ProductMapping ADD COLUMN sellable_qty INT DEFAULT 0;
ALTER TABLE ProductMapping ADD COLUMN unsellable_qty INT DEFAULT 0;
ALTER TABLE ProductMapping ADD COLUMN condition_type VARCHAR(50);
ALTER TABLE ProductMapping ADD COLUMN warehouse_condition VARCHAR(50);
ALTER TABLE ProductMapping ADD COLUMN title VARCHAR(500);
ALTER TABLE ProductMapping ADD COLUMN cog DECIMAL(10,2);
ALTER TABLE ProductMapping ADD COLUMN brand VARCHAR(100);
ALTER TABLE ProductMapping ADD COLUMN category VARCHAR(100);
ALTER TABLE ProductMapping ADD COLUMN total_order_items_l30 INT;
ALTER TABLE ProductMapping ADD COLUMN ordered_product_sales_l30 DECIMAL(10,2);
ALTER TABLE ProductMapping ADD COLUMN units_refunded_l30 INT;
ALTER TABLE ProductMapping ADD COLUMN buybox_percentage_l30 DECIMAL(5,2);
ALTER TABLE ProductMapping ADD COLUMN sale_price DECIMAL(10,2);
ALTER TABLE ProductMapping ADD COLUMN est_fee DECIMAL(10,2);
ALTER TABLE ProductMapping ADD COLUMN est_fba_fee DECIMAL(10,2);
ALTER TABLE ProductMapping ADD COLUMN est_vat DECIMAL(10,2);
ALTER TABLE ProductMapping ADD COLUMN est_net DECIMAL(10,2);
ALTER TABLE ProductMapping ADD COLUMN fba_updated_at TIMESTAMP;

-- Create index on fnsku for faster lookups
CREATE INDEX idx_productmapping_fnsku ON ProductMapping(fnsku);