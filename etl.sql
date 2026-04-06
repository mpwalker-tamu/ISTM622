DROP DATABASE IF EXISTS POS;
CREATE DATABASE POS;
USE POS;

CREATE TABLE City (
  zip DECIMAL(5,0) ZEROFILL NOT NULL,
  city VARCHAR(32) NOT NULL,
  state VARCHAR(4) NOT NULL,
  PRIMARY KEY (zip)
) ENGINE=InnoDB;

CREATE TABLE Customer (
  id SERIAL,
  firstName VARCHAR(32) NOT NULL,
  lastName VARCHAR(30) NOT NULL,
  email VARCHAR(128) NOT NULL,
  address1 VARCHAR(100) NOT NULL,
  address2 VARCHAR(50) NULL,
  phone VARCHAR(32) NULL,
  birthdate DATE NULL,
  zip DECIMAL(5,0) ZEROFILL NOT NULL,
  PRIMARY KEY (id),
  CONSTRAINT fk_customer_city_zip
    FOREIGN KEY (zip) REFERENCES City(zip)
    ON UPDATE CASCADE
    ON DELETE RESTRICT
) ENGINE=InnoDB;

CREATE TABLE Product (
  id SERIAL,
  name VARCHAR(128) NOT NULL,
  currentPrice DECIMAL(6,2) NOT NULL,
  availableQuantity INT NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB;

CREATE TABLE `Order` (
  id SERIAL,
  datePlaced DATE NOT NULL,
  dateShipped DATE NULL,
  customer_id BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (id),
  INDEX idx_order_customer (customer_id),
  CONSTRAINT fk_order_customer
    FOREIGN KEY (customer_id) REFERENCES Customer(id)
    ON UPDATE CASCADE
    ON DELETE RESTRICT
) ENGINE=InnoDB;

CREATE TABLE Orderline (
  order_id BIGINT UNSIGNED NOT NULL,
  product_id BIGINT UNSIGNED NOT NULL,
  quantity INT NOT NULL,
  PRIMARY KEY (order_id, product_id),
  CONSTRAINT fk_orderline_order
    FOREIGN KEY (order_id) REFERENCES `Order`(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  CONSTRAINT fk_orderline_product
    FOREIGN KEY (product_id) REFERENCES Product(id)
    ON UPDATE CASCADE
    ON DELETE RESTRICT
) ENGINE=InnoDB;

CREATE TABLE PriceHistory (
  id SERIAL,
  oldPrice DECIMAL(6,2) NOT NULL,
  newPrice DECIMAL(6,2) NOT NULL,
  ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  product_id BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (id),
  INDEX idx_pricehistory_product (product_id),
  CONSTRAINT fk_pricehistory_product
    FOREIGN KEY (product_id) REFERENCES Product(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE stg_customers (
  ID VARCHAR(32),
  FN VARCHAR(64),
  LN VARCHAR(64),
  CT VARCHAR(64),
  ST VARCHAR(16),
  ZP VARCHAR(16),
  S1 VARCHAR(255),
  S2 VARCHAR(255),
  EM VARCHAR(255),
  BD VARCHAR(64)
) ENGINE=InnoDB;

CREATE TABLE stg_orders (
  OID VARCHAR(32),
  CID VARCHAR(32),
  Ordered VARCHAR(64),
  Shipped VARCHAR(64)
) ENGINE=InnoDB;

CREATE TABLE stg_orderlines (
  OID VARCHAR(32),
  PID VARCHAR(32)
) ENGINE=InnoDB;

CREATE TABLE stg_products (
  ID VARCHAR(32),
  Name VARCHAR(255),
  Price VARCHAR(64),
  QOH VARCHAR(32)
) ENGINE=InnoDB;

SET SESSION sql_mode = REPLACE(@@sql_mode, 'STRICT_TRANS_TABLES', '');
SET SESSION sql_mode = REPLACE(@@sql_mode, 'NO_ZERO_DATE', '');
SET SESSION sql_mode = REPLACE(@@sql_mode, 'NO_ZERO_IN_DATE', '');

LOAD DATA LOCAL INFILE '/home/mwalker/customers.csv'
INTO TABLE stg_customers
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(ID,FN,LN,CT,ST,ZP,S1,S2,EM,BD);

LOAD DATA LOCAL INFILE '/home/mwalker/orders.csv'
INTO TABLE stg_orders
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(OID,CID,Ordered,Shipped);

LOAD DATA LOCAL INFILE '/home/mwalker/orderlines.csv'
INTO TABLE stg_orderlines
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(OID,PID);

LOAD DATA LOCAL INFILE '/home/mwalker/products.csv'
INTO TABLE stg_products
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(ID,Name,Price,@qoh_raw)
SET QOH = @qoh_raw;

INSERT INTO City (zip, city, state)
SELECT DISTINCT
  CAST(NULLIF(TRIM(ZP), '') AS DECIMAL(5,0)) AS zip,
  TRIM(CT) AS city,
  TRIM(ST) AS state
FROM stg_customers
WHERE NULLIF(TRIM(ZP), '') IS NOT NULL;

INSERT INTO Customer (id, firstName, lastName, email, address1, address2, phone, birthdate, zip)
SELECT
  CAST(TRIM(ID) AS UNSIGNED) AS id,
  TRIM(FN) AS firstName,
  TRIM(LN) AS lastName,
  TRIM(EM) AS email,
  TRIM(S1) AS address1,
  NULLIF(TRIM(S2), '') AS address2,
  NULL AS phone,
  CASE
    WHEN NULLIF(TRIM(BD), '') IS NULL THEN NULL
    WHEN TRIM(BD) IN ('0000-00-00','0000-00-00 00:00:00') THEN NULL
    ELSE COALESCE(
      STR_TO_DATE(TRIM(BD), '%c/%e/%Y'),
      STR_TO_DATE(TRIM(BD), '%m/%d/%Y'),
      STR_TO_DATE(TRIM(BD), '%Y-%m-%d')
    )
  END AS birthdate,
  CAST(NULLIF(TRIM(ZP), '') AS DECIMAL(5,0)) AS zip
FROM stg_customers;

INSERT INTO Product (id, name, currentPrice, availableQuantity)
SELECT
  CAST(TRIM(ID) AS UNSIGNED) AS id,
  TRIM(Name) AS name,
  CAST(REPLACE(REPLACE(TRIM(Price), '$', ''), ',', '') AS DECIMAL(6,2)) AS currentPrice,
  CAST(NULLIF(TRIM(QOH), '') AS SIGNED) AS availableQuantity
FROM stg_products;

INSERT INTO `Order` (id, datePlaced, dateShipped, customer_id)
SELECT
  CAST(TRIM(OID) AS UNSIGNED) AS id,
  DATE(
    CASE
      WHEN NULLIF(TRIM(Ordered), '') IS NULL THEN NULL
      WHEN TRIM(Ordered) IN ('0000-00-00','0000-00-00 00:00:00') THEN NULL
      ELSE COALESCE(
        STR_TO_DATE(TRIM(Ordered), '%Y-%m-%d %H:%i:%s'),
        STR_TO_DATE(TRIM(Ordered), '%Y-%m-%d'),
        STR_TO_DATE(TRIM(Ordered), '%m/%d/%Y %H:%i:%s'),
        STR_TO_DATE(TRIM(Ordered), '%m/%d/%Y')
      )
    END
  ) AS datePlaced,
  DATE(
    CASE
      WHEN NULLIF(TRIM(Shipped), '') IS NULL THEN NULL
      WHEN TRIM(Shipped) = 'Cancelled' THEN NULL
      WHEN TRIM(Shipped) IN ('0000-00-00','0000-00-00 00:00:00') THEN NULL
      ELSE COALESCE(
        STR_TO_DATE(TRIM(Shipped), '%Y-%m-%d %H:%i:%s'),
        STR_TO_DATE(TRIM(Shipped), '%Y-%m-%d'),
        STR_TO_DATE(TRIM(Shipped), '%m/%d/%Y %H:%i:%s'),
        STR_TO_DATE(TRIM(Shipped), '%m/%d/%Y')
      )
    END
  ) AS dateShipped,
  CAST(TRIM(CID) AS UNSIGNED) AS customer_id
FROM stg_orders;

INSERT INTO Orderline (order_id, product_id, quantity)
SELECT
  CAST(TRIM(OID) AS UNSIGNED) AS order_id,
  CAST(TRIM(PID) AS UNSIGNED) AS product_id,
  COUNT(*) AS quantity
FROM stg_orderlines
GROUP BY
  CAST(TRIM(OID) AS UNSIGNED),
  CAST(TRIM(PID) AS UNSIGNED);

DROP TABLE IF EXISTS stg_customers;
DROP TABLE IF EXISTS stg_orders;
DROP TABLE IF EXISTS stg_orderlines;
DROP TABLE IF EXISTS stg_products;
