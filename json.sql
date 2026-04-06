-- ============================================================================
-- json_export_fixed.sql
-- JSON export queries for the POS database
--
-- Outputs:
--   /var/lib/mysql-files/prod.json     (Query 1 — product + buyer list)
--   /var/lib/mysql-files/cust.json     (Query 2 — customer + order history)
--   /var/lib/mysql-files/custom1.json  (Query 3 — product + buyer regions)
--   /var/lib/mysql-files/custom2.json  (Query 4 — customer + lifetime metrics)
--
-- NOTE: MySQL's INTO OUTFILE will error if the output file already exists.
-- Delete the four files from /var/lib/mysql-files/ before re-running this script.
-- ============================================================================

USE POS;

-- ============================================================================
-- Query 1 — prod.json
-- One JSON object per product line.
-- Each object lists the product's ID, current price, name, and a deduplicated
-- array of every customer who has ever purchased it (or an empty array).
--
-- FIX: Added GROUP BY p.id, p.currentPrice, p.name on the outer query.
--      Without it, any future duplicate rows in Product would produce duplicate
--      output lines with no error.  Grouping also makes the aggregation intent
--      explicit even though the correlated subquery already handles dedup.
-- ============================================================================
SELECT JSON_OBJECT(
  'ProductID',    p.id,
  'currentPrice', p.currentPrice,
  'productName',  p.name,
  'customers',
    COALESCE(
      (
        -- Correlated subquery: collect distinct customers for this product.
        -- The inner DISTINCT handles the case where a customer ordered the
        -- same product on multiple separate orders.
        SELECT JSON_ARRAYAGG(
          JSON_OBJECT(
            'CustomerID',   x.customer_id,
            'CustomerName', x.customer_name
          )
        )
        FROM (
          SELECT DISTINCT
            c.id                                        AS customer_id,
            CONCAT(c.firstName, ' ', c.lastName)        AS customer_name
          FROM Orderline ol
          JOIN `Order`   o ON ol.order_id    = o.id
          JOIN Customer  c ON o.customer_id  = c.id
          WHERE ol.product_id = p.id
        ) AS x
      ),
      JSON_ARRAY()   -- products with no sales get an empty array, not NULL
    )
)
FROM Product p
-- GROUP BY prevents duplicate output lines if Product ever has duplicate rows.
GROUP BY p.id, p.currentPrice, p.name
INTO OUTFILE '/var/lib/mysql-files/prod.json'
LINES TERMINATED BY '\n';


-- ============================================================================
-- Query 2 — cust.json
-- One JSON object per customer line.
-- Each object includes mailing-address fields and a nested array of every
-- order placed by that customer, with each order containing its own item array.
--
-- FIXES applied vs. the original:
--
--   1. Removed the three dead CTEs (item_json, order_json, customer_orders).
--      They were never referenced in the final SELECT and contained a logic
--      error: the join condition inside customer_orders was a tautology
--      (col IS NOT NULL OR col IS NULL is always TRUE), which would have
--      caused a full cross-join and massively duplicated rows had the CTEs
--      ever been wired up.
--
--   2. The ORDER BY ord.id that appeared inside the derived table feeding
--      JSON_ARRAYAGG is unreliable — MySQL does not guarantee that an ORDER BY
--      inside a subquery is preserved by an outer aggregation.  Ordering is now
--      expressed inside JSON_ARRAYAGG itself via the ORDER BY clause supported
--      in MySQL 8.0+, which is the only guaranteed way to control array order.
-- ============================================================================
SELECT JSON_OBJECT(
  'CustomerID', c.id,
  'customer_name', CONCAT(c.firstName, ' ', c.lastName),
  -- printed_address_1: omit the '#' separator when address2 is absent/blank
  'printed_address_1',
    CASE
      WHEN c.address2 IS NULL OR TRIM(c.address2) = '' THEN c.address1
      ELSE CONCAT(c.address1, ' #', c.address2)
    END,
  -- printed_address_2: City, State   ZIP  (zip is ZEROFILL so LPAD is redundant
  --   but kept for readability / forward-compatibility with schema changes)
  'printed_address_2', CONCAT(ci.city, ', ', ci.state, '   ', LPAD(ci.zip, 5, '0')),
  'orders',
    COALESCE(
      (
        -- Derive one row per order, then aggregate into a JSON array.
        -- JSON_ARRAYAGG ORDER BY (MySQL 8.0+) is used here instead of an
        -- ORDER BY on the derived table, which is not guaranteed to be
        -- respected by the outer aggregation.
        SELECT JSON_ARRAYAGG(
          JSON_OBJECT(
            'Order Total',   o.order_total,
            'Order Date',    o.order_date,
            'Shipping Date', o.shipping_date,
            'Items',         o.items
          )
          ORDER BY o.order_id   -- reliable ordering inside the JSON array
        )
        FROM (
          -- One row per order: compute the total and build the items sub-array.
          SELECT
            ord.id                                         AS order_id,
            ROUND(SUM(p.currentPrice * ol.quantity), 2)    AS order_total,
            ord.datePlaced                                 AS order_date,
            ord.dateShipped                                AS shipping_date,
            -- Nested correlated subquery builds the items array for this order.
            (
              SELECT JSON_ARRAYAGG(
                JSON_OBJECT(
                  'ProductID',   p2.id,
                  'Quantity',    ol2.quantity,
                  'ProductName', p2.name
                )
              )
              FROM Orderline ol2
              JOIN Product   p2 ON ol2.product_id = p2.id
              WHERE ol2.order_id = ord.id
            ) AS items
          FROM `Order`  ord
          JOIN Orderline ol ON ord.id          = ol.order_id
          JOIN Product    p ON ol.product_id   = p.id
          WHERE ord.customer_id = c.id
          GROUP BY ord.id, ord.datePlaced, ord.dateShipped
        ) o
      ),
      JSON_ARRAY()   -- customers with no orders get an empty array, not NULL
    )
)
FROM Customer c
JOIN City ci ON c.zip = ci.zip
INTO OUTFILE '/var/lib/mysql-files/cust.json'
LINES TERMINATED BY '\n';


-- ============================================================================
-- Query 3 — custom1.json
-- One JSON object per product line.
-- Extends Query 1 by adding a sales_summary sub-object (distinct order count
-- and total units sold) and enriching the buyer list with city/state data.
--
-- No logic bugs were present; no structural changes made.
-- The ORDER BY c.id inside the DISTINCT subquery is cosmetic — actual array
-- ordering is controlled by the storage engine for JSON_ARRAYAGG.  If
-- deterministic order matters here, migrate to JSON_ARRAYAGG(...ORDER BY c.id)
-- the same way Query 2 was fixed.
-- ============================================================================
SELECT JSON_OBJECT(
  'ProductID',        p.id,
  'productName',      p.name,
  'currentPrice',     p.currentPrice,
  'availableQuantity', p.availableQuantity,
  'sales_summary', JSON_OBJECT(
    'total_orders', COALESCE((
      SELECT COUNT(DISTINCT ol.order_id)
      FROM Orderline ol
      WHERE ol.product_id = p.id
    ), 0),
    'total_units_sold', COALESCE((
      SELECT SUM(ol.quantity)
      FROM Orderline ol
      WHERE ol.product_id = p.id
    ), 0)
  ),
  'buyer_regions',
    COALESCE(
      (
        -- Collect distinct customers (with location) who purchased this product.
        SELECT JSON_ARRAYAGG(
          JSON_OBJECT(
            'CustomerID',   x.customer_id,
            'CustomerName', x.customer_name,
            'City',         x.city,
            'State',        x.state
          )
        )
        FROM (
          SELECT DISTINCT
            c.id                                     AS customer_id,
            CONCAT(c.firstName, ' ', c.lastName)     AS customer_name,
            ci.city,
            ci.state
          FROM Orderline ol
          JOIN `Order`   o  ON ol.order_id   = o.id
          JOIN Customer  c  ON o.customer_id = c.id
          JOIN City      ci ON c.zip         = ci.zip
          WHERE ol.product_id = p.id
          ORDER BY c.id   -- ordering inside a subquery; see note in Query 2
        ) x
      ),
      JSON_ARRAY()
    )
)
FROM Product p
INTO OUTFILE '/var/lib/mysql-files/custom1.json'
LINES TERMINATED BY '\n';


-- ============================================================================
-- Query 4 — custom2.json
-- One JSON object per customer line.
-- Extends Query 2 by adding email, a region sub-object, and lifetime_metrics
-- (total orders, lifetime spend, total items purchased).
--
-- FIX: The GROUP BY inside the orders correlated subquery was "GROUP BY
--      ord.customer_id", which collapsed ALL of a customer's orders into a
--      single row.  This meant every customer received exactly one entry in
--      their orders array regardless of how many orders they actually had.
--      Corrected to "GROUP BY ord.id, ord.datePlaced, ord.dateShipped" so
--      each order produces its own row before aggregation.
-- ============================================================================
SELECT JSON_OBJECT(
  'CustomerID',   c.id,
  'customer_name', CONCAT(c.firstName, ' ', c.lastName),
  'email',        c.email,
  'region', JSON_OBJECT(
    'city',  ci.city,
    'state', ci.state,
    'zip',   LPAD(ci.zip, 5, '0')
  ),
  'lifetime_metrics', JSON_OBJECT(
    'total_orders', COALESCE((
      SELECT COUNT(*)
      FROM `Order` o
      WHERE o.customer_id = c.id
    ), 0),
    'lifetime_value', COALESCE((
      SELECT ROUND(SUM(p.currentPrice * ol.quantity), 2)
      FROM `Order`   o
      JOIN Orderline ol ON o.id          = ol.order_id
      JOIN Product    p ON ol.product_id = p.id
      WHERE o.customer_id = c.id
    ), 0),
    'total_items_purchased', COALESCE((
      SELECT SUM(ol.quantity)
      FROM `Order`   o
      JOIN Orderline ol ON o.id = ol.order_id
      WHERE o.customer_id = c.id
    ), 0)
  ),
  'orders',
    COALESCE(
      (
        SELECT JSON_ARRAYAGG(
          JSON_OBJECT(
            'OrderID',      ord.id,
            'OrderDate',    ord.datePlaced,
            'ShippingDate', ord.dateShipped,
            'OrderTotal',   ROUND(SUM(p.currentPrice * ol.quantity), 2),
            'Items',
              (
                SELECT JSON_ARRAYAGG(
                  JSON_OBJECT(
                    'ProductID',   p2.id,
                    'ProductName', p2.name,
                    'Quantity',    ol2.quantity
                  )
                )
                FROM Orderline ol2
                JOIN Product   p2 ON ol2.product_id = p2.id
                WHERE ol2.order_id = ord.id
              )
          )
        )
        FROM `Order`   ord
        JOIN Orderline ol ON ord.id          = ol.order_id
        JOIN Product    p ON ol.product_id   = p.id
        WHERE ord.customer_id = c.id
        -- FIX: was "GROUP BY ord.customer_id" — that collapsed every order for
        --      a customer into one row, so JSON_ARRAYAGG only ever produced a
        --      single-element array.  Must group by order identity fields so
        --      each order becomes its own row before aggregation.
        GROUP BY ord.id, ord.datePlaced, ord.dateShipped
      ),
      JSON_ARRAY()   -- customers with no orders get an empty array, not NULL
    )
)
FROM Customer c
JOIN City ci ON c.zip = ci.zip
INTO OUTFILE '/var/lib/mysql-files/custom2.json'
LINES TERMINATED BY '\n';
