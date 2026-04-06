-- ============================================================================
-- POS Denormalization and Materialized Views 
--   1. Create a view (v_ProductBuyers)
--   2. Create a simulated materialized view table (mv_ProductBuyers)
--   3. Create triggers that keep the materialized view synchronized

-- ============================================================================
-- Standard View: v_ProductBuyers
-- Includes all products, even unsold ones
-- Customers formatted as: ID First Last
-- Distinct customers only
-- Customer list sorted by customer ID
-- Final result sorted by productID
-- ============================================================================
USE POS;
CREATE VIEW v_ProductBuyers AS
SELECT
    p.id AS productID,
    p.name AS productName,
    IFNULL(
        GROUP_CONCAT(
            DISTINCT CONCAT(c.id, ' ', c.firstName, ' ', c.lastName)
            ORDER BY c.id ASC
            SEPARATOR ', '
        ),
        ''
    ) AS customers
FROM Product p
LEFT JOIN Orderline ol ON p.id = ol.product_id
LEFT JOIN `Order` o ON ol.order_id = o.id
LEFT JOIN Customer c ON o.customer_id = c.id
GROUP BY p.id, p.name
ORDER BY p.id;

-- ============================================================================
-- Materialized View simulation
-- ============================================================================
CREATE TABLE mv_ProductBuyers AS
SELECT *
FROM v_ProductBuyers;

CREATE INDEX idx_mv_productbuyers_productID
  ON mv_ProductBuyers (productID);

-- ============================================================================
-- Trigger: refresh one affected mv_ProductBuyers row after INSERT on Orderline
-- ============================================================================
DELIMITER //

CREATE TRIGGER trg_orderline_ai_refresh_mv
AFTER INSERT ON Orderline
FOR EACH ROW
BEGIN
  UPDATE mv_ProductBuyers
  SET customers = (
    SELECT IFNULL(
      GROUP_CONCAT(
        DISTINCT CONCAT(c.id, ' ', c.firstName, ' ', c.lastName)
        ORDER BY c.id
        SEPARATOR ', '
      ),
      ''
    )
    FROM Product p
    LEFT JOIN Orderline ol ON p.id = ol.product_id
    LEFT JOIN `Order` o ON ol.order_id = o.id
    LEFT JOIN Customer c ON o.customer_id = c.id
    WHERE p.id = NEW.product_id
    GROUP BY p.id
  )
  WHERE productID = NEW.product_id;
END//

-- ============================================================================
-- Trigger: refresh one affected mv_ProductBuyers row after DELETE on Orderline
-- ============================================================================
CREATE TRIGGER trg_orderline_ad_refresh_mv
AFTER DELETE ON Orderline
FOR EACH ROW
BEGIN
  UPDATE mv_ProductBuyers
  SET customers = (
    SELECT IFNULL(
      GROUP_CONCAT(
        DISTINCT CONCAT(c.id, ' ', c.firstName, ' ', c.lastName)
        ORDER BY c.id
        SEPARATOR ', '
      ),
      ''
    )
    FROM Product p
    LEFT JOIN Orderline ol ON p.id = ol.product_id
    LEFT JOIN `Order` o ON ol.order_id = o.id
    LEFT JOIN Customer c ON o.customer_id = c.id
    WHERE p.id = OLD.product_id
    GROUP BY p.id
  )
  WHERE productID = OLD.product_id;
END//

-- ============================================================================
-- Trigger: log price changes only when currentPrice actually changes
-- ============================================================================
CREATE TRIGGER trg_product_au_pricehistory
AFTER UPDATE ON Product
FOR EACH ROW
BEGIN
  IF OLD.currentPrice <> NEW.currentPrice THEN
    INSERT INTO PriceHistory (oldPrice, newPrice, product_id)
    VALUES (OLD.currentPrice, NEW.currentPrice, NEW.id);
  END IF;
END//
DELIMITER ;
