-- create a commerce schema
CREATE SCHEMA payment;

-- Use commerce schema
SET
    search_path TO payment;
-- Customers Table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,  -- Unique ID for each customer
    x_location FLOAT NOT NULL,       -- X coordinate of customer location
    y_location FLOAT NOT NULL        -- Y coordinate of customer location
);

-- Terminals Table
CREATE TABLE terminals (
    terminal_id SERIAL PRIMARY KEY,  -- Unique ID for each terminal
    x_location FLOAT NOT NULL,       -- X coordinate of terminal location
    y_location FLOAT NOT NULL        -- Y coordinate of terminal location
);


-- Transactions Table (Linking Customers & Terminals)
CREATE TABLE transactions (
    tx_id SERIAL PRIMARY KEY,        -- Unique ID for each transaction
    tx_datetime TIMESTAMP NOT NULL,  -- Timestamp of the transaction
    customer_id INT NOT NULL,        -- References customers table
    terminal_id INT NOT NULL,        -- References terminals table
    tx_amount DECIMAL(10,2) NOT NULL,-- Transaction amount

    -- Foreign Key Constraints
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE,
    FOREIGN KEY (terminal_id) REFERENCES terminals(terminal_id) ON DELETE CASCADE
);