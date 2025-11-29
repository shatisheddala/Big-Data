-- INIT database
CREATE TABLE Product (
  ProductID INT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
  Name VARCHAR2(100),
  Description VARCHAR2(255)
);

INSERT INTO Product(Name, Description) VALUES ('Entity Framework Extensions', 'Use <a href="https://entityframework-extensions.net/" target="_blank">Entity Framework Extensions</a> to extend your DbContext with high-performance bulk operations.');
INSERT INTO Product(Name, Description) VALUES ('Dapper Plus', 'Use <a href="https://dapper-plus.net/" target="_blank">Dapper Plus</a> to extend your IDbConnection with high-performance bulk operations.');
INSERT INTO Product(Name, Description) VALUES ('C# Eval Expression', 'Use <a href="https://eval-expression.net/" target="_blank">C# Eval Expression</a> to compile and execute C# code at runtime.');

-- QUERY database
SELECT * FROM Product;
SELECT * FROM Product WHERE ProductID = 1;

-- 1. Create master table
CREATE TABLE customers_master (
    customer_id    NUMBER PRIMARY KEY,
    customer_name  VARCHAR2(50),
    phone          VARCHAR2(20)
);
-- Create updates (snapshot) table
CREATE TABLE customers_updates (
    customer_id    NUMBER PRIMARY KEY,
    customer_name  VARCHAR2(50),
    phone          VARCHAR2(20)
);

-- 2.Insert initial data into customers_master
INSERT INTO customers_master (customer_id, customer_name, phone) VALUES (101, 'Alice',   '111-111');
INSERT INTO customers_master (customer_id, customer_name, phone) VALUES (102, 'Bob',     '222-222');
INSERT INTO customers_master (customer_id, customer_name, phone) VALUES (103, 'Charlie', '333-333');
INSERT INTO customers_master (customer_id, customer_name, phone) VALUES (104, 'David',   '444-444');

--3. Insert new snapshot data into customers_updates
INSERT INTO customers_updates (customer_id, customer_name, phone)
VALUES (101, 'Alice M', '111-999');    -- should UPDATE

INSERT INTO customers_updates (customer_id, customer_name, phone)
VALUES (103, 'Charlie', '333-333');    -- no change

INSERT INTO customers_updates (customer_id, customer_name, phone)
VALUES (105, 'Eva', '555-555');        -- should INSERT

-- 4.MERGE: handle UPDATE + INSERT

MERGE INTO customers_master m
USING customers_updates u
   ON (m.customer_id = u.customer_id)
WHEN MATCHED THEN
    UPDATE SET
        m.customer_name = u.customer_name,
        m.phone         = u.phone
WHEN NOT MATCHED THEN
    INSERT (customer_id, customer_name, phone)
    VALUES (u.customer_id, u.customer_name, u.phone);

--5 DELETE: remove rows that disappeared from source

DELETE FROM customers_master m
WHERE NOT EXISTS (
    SELECT 1
    FROM customers_updates u
    WHERE u.customer_id = m.customer_id
);

-- result --

SELECT * FROM customers_master ORDER BY customer_id;


