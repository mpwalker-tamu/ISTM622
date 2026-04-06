-- ============================================================================
-- json_export_mariadb.sql
-- JSON export queries for the POS database — MariaDB compatible
--
-- Outputs:
--   /var/lib/mysql-files/prod.json     (Query 1 — product + buyer list)
--   /var/lib/mysql-files/cust.json     (Query 2 — customer + order history)
--   /var/lib/mysql-files/custom1.json  (Query 3 — regional delivery manifest)
--   /var/lib/mysql-files/custom2.json  (Query 4 — product sales & inventory report)
--
-- MariaDB compatibility note:
--   MariaDB does not resolve outer-query table references inside correlated
--   subqueries as permissively as MySQL 8.  To avoid "Unknown column" errors,
--   ALL correlated subqueries have been replaced with pre-aggregated derived
--   tables that are LEFT JOINed back to the main query.  The engine sees a
--   single flat join rather than nested scope lookups.
--
-- NOTE: INTO OUTFILE will error if the target file already exists.
--   Delete all four files from /var/lib/mysql-files/ before re-running.
-- ============================================================================

USE POS;

-- ============================================================================
-- Query 1 — prod.json
-- Business case: Product catalog view — which customers have bought each item.
-- One JSON object per product line.
-- Fields: ProductID, currentPrice, productName,
--         customers[ { CustomerID, CustomerName } ]
--
-- Strategy: pre-aggregate every product→customer relationship into one derived
-- table (buyer_agg), then LEFT JOIN it to Product so unsold products receive
-- an empty array via COALESCE.
-- ============================================================================
SELECT JSON_OBJECT(
  'ProductID',    p.id,
  'currentPrice', p.currentPrice,
  'productName',  p.name,
  'customers',    COALESCE(buyer_agg.customer_list, JSON_ARRAY())
)
FROM Product p
LEFT JOIN (
  -- Step 1: get one distinct (product, customer) pair per row.
  -- DISTINCT handles customers who ordered the same product multiple times.
  -- Step 2: group by product and collapse into a JSON array.
  SELECT
    x.product_id,
    JSON_ARRAYAGG(
      JSON_OBJECT(
        'CustomerID',   x.customer_id,
        'CustomerName', x.customer_name
      )
      ORDER BY x.customer_id   -- consistent array order
    ) AS customer_list
  FROM (
    SELECT DISTINCT
      ol.product_id,
      c.id                                 AS customer_id,
      CONCAT(c.firstName, ' ', c.lastName) AS customer_name
    FROM Orderline ol
    JOIN `Order`  o ON ol.order_id   = o.id
    JOIN Customer c ON o.customer_id = c.id
  ) AS x
  GROUP BY x.product_id
) AS buyer_agg ON buyer_agg.product_id = p.id
ORDER BY p.id
INTO OUTFILE '/var/lib/mysql-files/prod.json'
LINES TERMINATED BY '\n';


-- ============================================================================
-- Query 2 — cust.json
-- Business case: Customer order history with full mailing address formatting.
-- One JSON object per customer line.
-- Fields: CustomerID, customer_name, printed_address_1, printed_address_2,
--         orders[ { Order Total, Order Date, Shipping Date,
--                   Items[ { ProductID, Quantity, ProductName } ] } ]
--
-- Address formatting rules per spec:
--   printed_address_1 — address2 is appended as "# <address2>" only when
--                       address2 is non-NULL and non-blank.
--   printed_address_2 — "City, State   Zip" (comma+space, three spaces before zip)
--
-- Two levels of nesting require two pre-aggregated derived tables:
--   item_agg  — collapses orderline rows into a JSON items array per order
--   order_agg — collapses orders (with their item arrays) into a JSON orders
--               array per customer
-- Both are LEFT JOINed so customers with no orders still appear.
-- ============================================================================
SELECT JSON_OBJECT(
  'CustomerID',      c.id,
  'customer_name',   CONCAT(c.firstName, ' ', c.lastName),
  -- printed_address_1: omit the '#' separator when address2 is absent or blank
  'printed_address_1',
    CASE
      WHEN c.address2 IS NULL OR TRIM(c.address2) = '' THEN c.address1
      ELSE CONCAT(c.address1, ' #', c.address2)
    END,
  -- printed_address_2: "City, State   Zip"
  -- Zip is DECIMAL ZEROFILL so LPAD is technically redundant, but kept for
  -- readability and resilience against future schema changes.
  'printed_address_2', CONCAT(ci.city, ', ', ci.state, '   ', LPAD(ci.zip, 5, '0')),
  'orders', COALESCE(order_agg.order_list, JSON_ARRAY())
)
FROM Customer c
JOIN City ci ON c.zip = ci.zip
LEFT JOIN (
  -- order_agg: one row per customer containing their full orders JSON array.
  SELECT
    ord_totals.customer_id,
    JSON_ARRAYAGG(
      JSON_OBJECT(
        'Order Total',   ord_totals.order_total,
        'Order Date',    ord_totals.order_date,
        'Shipping Date', ord_totals.shipping_date,
        'Items',         COALESCE(item_agg.item_list, JSON_ARRAY())
      )
      ORDER BY ord_totals.order_id   -- consistent order of orders in array
    ) AS order_list
  FROM (
    -- ord_totals: one row per order with its total and date fields.
    SELECT
      ord.id                                        AS order_id,
      ord.customer_id,
      ROUND(SUM(p.currentPrice * ol.quantity), 2)   AS order_total,
      ord.datePlaced                                AS order_date,
      ord.dateShipped                               AS shipping_date
    FROM `Order`   ord
    JOIN Orderline ol ON ord.id          = ol.order_id
    JOIN Product    p ON ol.product_id   = p.id
    GROUP BY ord.id, ord.customer_id, ord.datePlaced, ord.dateShipped
  ) AS ord_totals
  LEFT JOIN (
    -- item_agg: one row per order containing its items JSON array.
    -- Pre-aggregated here so ord_totals can LEFT JOIN without a correlated ref.
    SELECT
      ol.order_id,
      JSON_ARRAYAGG(
        JSON_OBJECT(
          'ProductID',   p.id,
          'Quantity',    ol.quantity,
          'ProductName', p.name
        )
      ) AS item_list
    FROM Orderline ol
    JOIN Product   p ON ol.product_id = p.id
    GROUP BY ol.order_id
  ) AS item_agg ON item_agg.order_id = ord_totals.order_id
  GROUP BY ord_totals.customer_id
) AS order_agg ON order_agg.customer_id = c.id
ORDER BY c.id
INTO OUTFILE '/var/lib/mysql-files/cust.json'
LINES TERMINATED BY '\n';


-- ============================================================================
-- Query 3 — custom1.json
-- Business case: Regional Delivery Manifest
--
-- A furniture manufacturer ships large items by region.  The warehouse and
-- logistics team needs a manifest grouped by state so they can plan truck
-- routes and confirm delivery addresses before dispatch.
--
-- Structure (one object per state):
--   state
--   total_shipments      — count of distinct orders shipping to this state
--   orders[              — every order shipping to this state
--     {
--       OrderID
--       OrderDate
--       ShippingDate     (NULL = not yet shipped / still on the floor)
--       ShipTo {         — full delivery address for the driver
--         CustomerName
--         address_line_1
--         address_line_2   ("City, State   Zip")
--       }
--       furniture_items[ — what is actually on the truck for this order
--         { ProductID, ProductName, Quantity }
--       ]
--     }
--   ]
--
-- Strategy:
--   item_agg  — items array per order (no outer reference needed)
--   order_agg — order objects grouped by state, joined with item_agg
-- ============================================================================
SELECT JSON_OBJECT(
  'state',           ship_info.state,
  'total_shipments', COUNT(DISTINCT ship_info.order_id),
  'orders',          COALESCE(order_agg.order_list, JSON_ARRAY())
)
FROM (
  -- ship_info: one row per order with state and formatted address fields.
  -- Derived here so order_agg can join on state without a correlated reference.
  SELECT
    o.id                                          AS order_id,
    o.datePlaced                                  AS order_date,
    o.dateShipped                                 AS shipping_date,
    ci.state,
    CONCAT(c.firstName, ' ', c.lastName)          AS customer_name,
    CASE
      WHEN c.address2 IS NULL OR TRIM(c.address2) = '' THEN c.address1
      ELSE CONCAT(c.address1, ' #', c.address2)
    END                                           AS address_line_1,
    CONCAT(ci.city, ', ', ci.state, '   ', LPAD(ci.zip, 5, '0'))
                                                  AS address_line_2
  FROM `Order`   o
  JOIN Customer  c  ON o.customer_id = c.id
  JOIN City      ci ON c.zip         = ci.zip
) AS ship_info
LEFT JOIN (
  -- order_agg: one row per state containing all order objects for that state.
  SELECT
    si.state,
    JSON_ARRAYAGG(
      JSON_OBJECT(
        'OrderID',      si.order_id,
        'OrderDate',    si.order_date,
        'ShippingDate', si.shipping_date,
        'ShipTo', JSON_OBJECT(
          'CustomerName',   si.customer_name,
          'address_line_1', si.address_line_1,
          'address_line_2', si.address_line_2
        ),
        'furniture_items', COALESCE(item_agg.item_list, JSON_ARRAY())
      )
      ORDER BY si.order_id
    ) AS order_list
  FROM (
    -- Repeat the same ship_info derivation so order_agg has its own scope.
    -- MariaDB does not allow a derived table in the outer FROM to be referenced
    -- inside a LEFT JOIN subquery, so we define it again here.
    SELECT
      o.id                                          AS order_id,
      o.datePlaced                                  AS order_date,
      o.dateShipped                                 AS shipping_date,
      ci.state,
      CONCAT(c.firstName, ' ', c.lastName)          AS customer_name,
      CASE
        WHEN c.address2 IS NULL OR TRIM(c.address2) = '' THEN c.address1
        ELSE CONCAT(c.address1, ' #', c.address2)
      END                                           AS address_line_1,
      CONCAT(ci.city, ', ', ci.state, '   ', LPAD(ci.zip, 5, '0'))
                                                    AS address_line_2
    FROM `Order`   o
    JOIN Customer  c  ON o.customer_id = c.id
    JOIN City      ci ON c.zip         = ci.zip
  ) AS si
  LEFT JOIN (
    -- item_agg: furniture items array per order.
    SELECT
      ol.order_id,
      JSON_ARRAYAGG(
        JSON_OBJECT(
          'ProductID',   p.id,
          'ProductName', p.name,
          'Quantity',    ol.quantity
        )
      ) AS item_list
    FROM Orderline ol
    JOIN Product   p ON ol.product_id = p.id
    GROUP BY ol.order_id
  ) AS item_agg ON item_agg.order_id = si.order_id
  GROUP BY si.state
) AS order_agg ON order_agg.state = ship_info.state
GROUP BY ship_info.state, order_agg.order_list
ORDER BY ship_info.state
INTO OUTFILE '/var/lib/mysql-files/custom1.json'
LINES TERMINATED BY '\n';


-- ============================================================================
-- Query 4 — custom2.json
-- Business case: Product Sales & Inventory Report
--
-- A furniture manufacturer's product manager needs to know, for each item in
-- the catalog, how it is selling and who is buying it — so they can make
-- restocking and pricing decisions.
--
-- Structure (one object per product):
--   ProductID
--   productName
--   currentPrice
--   availableQuantity    — current stock on hand
--   revenue_summary {
--     total_orders       — how many distinct orders included this product
--     total_units_sold   — sum of all quantities sold across all orders
--     total_revenue      — total revenue generated (price × qty, all orders)
--   }
--   order_history[       — every order that included this product
--     {
--       OrderID
--       OrderDate
--       ShippingDate
--       QuantityOrdered  — units of THIS product in this specific order
--       CustomerID
--       CustomerName
--     }
--   ]
--
-- Strategy:
--   rev      — revenue summary figures per product in a single scan
--   hist_agg — order history array per product
-- ============================================================================
SELECT JSON_OBJECT(
  'ProductID',         p.id,
  'productName',       p.name,
  'currentPrice',      p.currentPrice,
  'availableQuantity', p.availableQuantity,
  'revenue_summary', JSON_OBJECT(
    'total_orders',     COALESCE(rev.total_orders,     0),
    'total_units_sold', COALESCE(rev.total_units_sold, 0),
    'total_revenue',    COALESCE(rev.total_revenue,    0)
  ),
  'order_history', COALESCE(hist_agg.order_list, JSON_ARRAY())
)
FROM Product p
LEFT JOIN (
  -- rev: summary figures for each product in a single scan.
  SELECT
    ol.product_id,
    COUNT(DISTINCT ol.order_id)                 AS total_orders,
    SUM(ol.quantity)                            AS total_units_sold,
    ROUND(SUM(p.currentPrice * ol.quantity), 2) AS total_revenue
  FROM Orderline ol
  JOIN Product p ON ol.product_id = p.id
  GROUP BY ol.product_id
) AS rev ON rev.product_id = p.id
LEFT JOIN (
  -- hist_agg: one row per product containing the full order history array.
  -- Each entry records who ordered this product, when, and how many units.
  SELECT
    x.product_id,
    JSON_ARRAYAGG(
      JSON_OBJECT(
        'OrderID',         x.order_id,
        'OrderDate',       x.order_date,
        'ShippingDate',    x.shipping_date,
        'QuantityOrdered', x.quantity,
        'CustomerID',      x.customer_id,
        'CustomerName',    x.customer_name
      )
      ORDER BY x.order_id
    ) AS order_list
  FROM (
    -- One row per (product, order) pair with customer and date context.
    SELECT
      ol.product_id,
      o.id                                         AS order_id,
      o.datePlaced                                 AS order_date,
      o.dateShipped                                AS shipping_date,
      ol.quantity,
      c.id                                         AS customer_id,
      CONCAT(c.firstName, ' ', c.lastName)         AS customer_name
    FROM Orderline ol
    JOIN `Order`  o ON ol.order_id   = o.id
    JOIN Customer c ON o.customer_id = c.id
  ) AS x
  GROUP BY x.product_id
) AS hist_agg ON hist_agg.product_id = p.id
ORDER BY p.id
INTO OUTFILE '/var/lib/mysql-files/custom2.json'
LINES TERMINATED BY '\n';
