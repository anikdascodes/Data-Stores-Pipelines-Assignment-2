-- Food Delivery Orders Table Creation
-- Assignment 2 - Data Stores & Pipelines
-- Student: Anik Das (2025EM1100026)

CREATE TABLE IF NOT EXISTS orders_2025EM1100026 (
    order_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    restaurant_name VARCHAR(100) NOT NULL,
    item VARCHAR(200) NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    order_status VARCHAR(20) NOT NULL CHECK (order_status IN ('PLACED', 'PREPARING', 'DELIVERED', 'CANCELLED')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on created_at for efficient polling
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders_2025EM1100026(created_at);

-- Insert 10 initial sample records as required
INSERT INTO orders_2025EM1100026 (customer_name, restaurant_name, item, amount, order_status) VALUES
('Anik Das', 'Burger Junction', 'Veg Burger', 220.00, 'PLACED'),
('John Doe', 'Pizza Palace', 'Margherita Pizza', 450.00, 'PLACED'),
('Jane Smith', 'KFC Express', 'Zinger Burger', 280.00, 'PREPARING'),
('Mike Johnson', 'Dominos Pizza', 'Pepperoni Pizza', 520.00, 'DELIVERED'),
('Sarah Wilson', 'McDonald''s', 'Big Mac Meal', 320.00, 'PLACED'),
('David Brown', 'Subway', 'Chicken Teriyaki Sub', 380.00, 'PREPARING'),
('Emma Davis', 'Starbucks', 'Caffe Latte', 250.00, 'DELIVERED'),
('Chris Miller', 'Taco Bell', 'Crunchwrap Supreme', 290.00, 'PLACED'),
('Lisa Anderson', 'Chipotle', 'Burrito Bowl', 410.00, 'CANCELLED'),
('Tom White', 'Panda Express', 'Orange Chicken', 360.00, 'PLACED');

-- Verify data insertion
SELECT COUNT(*) as total_orders FROM orders_2025EM1100026;
SELECT * FROM orders_2025EM1100026 ORDER BY created_at DESC LIMIT 5;