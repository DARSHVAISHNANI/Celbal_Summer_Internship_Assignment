-- Create the two databases
CREATE DATABASE source_db;
CREATE DATABASE local_db;

-- Switch to the source database to set it up
USE source_db;

-- Create the 'customers' table with a PRIMARY KEY
CREATE TABLE customers (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Insert some initial data to test with
INSERT INTO customers (id, name, email) VALUES (1, 'Saanvi Sharma', 'saanvi@example.com');
INSERT INTO customers (id, name, email) VALUES (2, 'Aarav Patel', 'aarav@example.com');

-- Tell MySQL which database to use
USE source_db;

-- Add a brand new customer
INSERT INTO customers (id, name, email) VALUES (3, 'Vivaan Singh', 'vivaan@example.com');

-- Update the name of an existing customer
UPDATE customers SET name = 'Aarav P. Patel' WHERE id = 2;