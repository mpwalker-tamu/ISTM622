USE POS;

SELECT JSON_OBJECT(
  'ProductID', p.id,
  'currentPrice', p.currentPrice,
  'productName', p.name,
  'customers',
    COALESCE(
      (
        SELECT JSON_ARRAYAGG(
          JSON_OBJECT(
            'CustomerID', x.customer_id,
            'CustomerName', x.customer_name
          )
        )
        FROM (
          SELECT DISTINCT
            c.id AS customer_id,
            CONCAT(c.firstName, ' ', c.lastName) AS customer_name
          FROM Orderline ol
          JOIN `Order` o ON ol.order_id = o.id
          JOIN Customer c ON o.customer_id = c.id
          WHERE ol.product_id = p.id
        ) AS x
      ),
      JSON_ARRAY()
    )
)
FROM Product p
INTO OUTFILE '/var/lib/mysql-files/prod.json'
LINES TERMINATED BY '\n';

WITH item_json AS (
  SELECT
    o.id AS order_id,
    JSON_ARRAYAGG(
      JSON_OBJECT(
        'ProductID', p.id,
        'Quantity', ol.quantity,
        'ProductName', p.name
      )
    ) AS items
  FROM `Order` o
  JOIN Orderline ol ON o.id = ol.order_id
  JOIN Product p ON ol.product_id = p.id
  GROUP BY o.id
),
order_json AS (
  SELECT
    o.customer_id,
    JSON_ARRAYAGG(
      JSON_OBJECT(
        'Order Total', ROUND(SUM(p.currentPrice * ol.quantity), 2),
        'Order Date', o.datePlaced,
        'Shipping Date', o.dateShipped,
        'Items', ij.items
      )
    ) AS orders
  FROM `Order` o
  JOIN Orderline ol ON o.id = ol.order_id
  JOIN Product p ON ol.product_id = p.id
  JOIN item_json ij ON o.id = ij.order_id
  GROUP BY o.customer_id, o.id, o.datePlaced, o.dateShipped, ij.items
),
customer_orders AS (
  SELECT
    customer_id,
    JSON_ARRAYAGG(order_obj) AS orders
  FROM (
    SELECT
      oj.customer_id,
      JSON_OBJECT(
        'Order Total', JSON_VALUE(j.doc, '$."Order Total"'),
        'Order Date', JSON_VALUE(j.doc, '$."Order Date"'),
        'Shipping Date', JSON_VALUE(j.doc, '$."Shipping Date"'),
        'Items', JSON_EXTRACT(j.doc, '$.Items')
      ) AS order_obj
    FROM (
      SELECT
        o.customer_id,
        JSON_OBJECT(
          'Order Total', ROUND(SUM(p.currentPrice * ol.quantity), 2),
          'Order Date', o.datePlaced,
          'Shipping Date', o.dateShipped,
          'Items', ij.items
        ) AS doc
      FROM `Order` o
      JOIN Orderline ol ON o.id = ol.order_id
      JOIN Product p ON ol.product_id = p.id
      JOIN item_json ij ON o.id = ij.order_id
      GROUP BY o.customer_id, o.id, o.datePlaced, o.dateShipped, ij.items
    ) j
    JOIN (
      SELECT DISTINCT id, customer_id
      FROM `Order`
    ) oj
      ON JSON_VALUE(j.doc, '$."Order Date"') IS NOT NULL OR JSON_VALUE(j.doc, '$."Order Date"') IS NULL
  ) x
  GROUP BY customer_id
)
SELECT JSON_OBJECT(
  'CustomerID', c.id,
  'customer_name', CONCAT(c.firstName, ' ', c.lastName),
  'printed_address_1',
    CASE
      WHEN c.address2 IS NULL OR TRIM(c.address2) = '' THEN c.address1
      ELSE CONCAT(c.address1, ' #', c.address2)
    END,
  'printed_address_2', CONCAT(ci.city, ', ', ci.state, '   ', LPAD(ci.zip, 5, '0')),
  'orders',
    COALESCE(
      (
        SELECT JSON_ARRAYAGG(
          JSON_OBJECT(
            'Order Total', o.order_total,
            'Order Date', o.order_date,
            'Shipping Date', o.shipping_date,
            'Items', o.items
          )
        )
        FROM (
          SELECT
            ord.id AS order_id,
            ROUND(SUM(p.currentPrice * ol.quantity), 2) AS order_total,
            ord.datePlaced AS order_date,
            ord.dateShipped AS shipping_date,
            (
              SELECT JSON_ARRAYAGG(
                JSON_OBJECT(
                  'ProductID', p2.id,
                  'Quantity', ol2.quantity,
                  'ProductName', p2.name
                )
              )
              FROM Orderline ol2
              JOIN Product p2 ON ol2.product_id = p2.id
              WHERE ol2.order_id = ord.id
            ) AS items
          FROM `Order` ord
          JOIN Orderline ol ON ord.id = ol.order_id
          JOIN Product p ON ol.product_id = p.id
          WHERE ord.customer_id = c.id
          GROUP BY ord.id, ord.datePlaced, ord.dateShipped
          ORDER BY ord.id
        ) o
      ),
      JSON_ARRAY()
    )
)
FROM Customer c
JOIN City ci ON c.zip = ci.zip
INTO OUTFILE '/var/lib/mysql-files/cust.json'
LINES TERMINATED BY '\n';

SELECT JSON_OBJECT(
  'ProductID', p.id,
  'productName', p.name,
  'currentPrice', p.currentPrice,
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
        SELECT JSON_ARRAYAGG(
          JSON_OBJECT(
            'CustomerID', x.customer_id,
            'CustomerName', x.customer_name,
            'City', x.city,
            'State', x.state
          )
        )
        FROM (
          SELECT DISTINCT
            c.id AS customer_id,
            CONCAT(c.firstName, ' ', c.lastName) AS customer_name,
            ci.city,
            ci.state
          FROM Orderline ol
          JOIN `Order` o ON ol.order_id = o.id
          JOIN Customer c ON o.customer_id = c.id
          JOIN City ci ON c.zip = ci.zip
          WHERE ol.product_id = p.id
          ORDER BY c.id
        ) x
      ),
      JSON_ARRAY()
    )
)
FROM Product p
INTO OUTFILE '/var/lib/mysql-files/custom1.json'
LINES TERMINATED BY '\n';

SELECT JSON_OBJECT(
  'CustomerID', c.id,
  'customer_name', CONCAT(c.firstName, ' ', c.lastName),
  'email', c.email,
  'region', JSON_OBJECT(
    'city', ci.city,
    'state', ci.state,
    'zip', LPAD(ci.zip, 5, '0')
  ),
  'lifetime_metrics', JSON_OBJECT(
    'total_orders', COALESCE((
      SELECT COUNT(*)
      FROM `Order` o
      WHERE o.customer_id = c.id
    ), 0),
    'lifetime_value', COALESCE((
      SELECT ROUND(SUM(p.currentPrice * ol.quantity), 2)
      FROM `Order` o
      JOIN Orderline ol ON o.id = ol.order_id
      JOIN Product p ON ol.product_id = p.id
      WHERE o.customer_id = c.id
    ), 0),
    'total_items_purchased', COALESCE((
      SELECT SUM(ol.quantity)
      FROM `Order` o
      JOIN Orderline ol ON o.id = ol.order_id
      WHERE o.customer_id = c.id
    ), 0)
  ),
  'orders',
    COALESCE(
      (
        SELECT JSON_ARRAYAGG(
          JSON_OBJECT(
            'OrderID', ord.id,
            'OrderDate', ord.datePlaced,
            'ShippingDate', ord.dateShipped,
            'OrderTotal', ROUND(SUM(p.currentPrice * ol.quantity), 2),
            'Items',
              (
                SELECT JSON_ARRAYAGG(
                  JSON_OBJECT(
                    'ProductID', p2.id,
                    'ProductName', p2.name,
                    'Quantity', ol2.quantity
                  )
                )
                FROM Orderline ol2
                JOIN Product p2 ON ol2.product_id = p2.id
                WHERE ol2.order_id = ord.id
              )
          )
        )
        FROM `Order` ord
        JOIN Orderline ol ON ord.id = ol.order_id
        JOIN Product p ON ol.product_id = p.id
        WHERE ord.customer_id = c.id
        GROUP BY ord.customer_id
      ),
      JSON_ARRAY()
    )
)
FROM Customer c
JOIN City ci ON c.zip = ci.zip
INTO OUTFILE '/var/lib/mysql-files/custom2.json'
LINES TERMINATED BY '\n';
