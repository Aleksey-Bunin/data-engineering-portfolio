-- Customer Analytics Schema

-- Customers master table
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(200),
    email VARCHAR(200),
    phone VARCHAR(50),
    contract_start_date DATE,
    contract_end_date DATE,
    plan_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Billing history table
CREATE TABLE IF NOT EXISTS billing_history (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) REFERENCES customers(customer_id),
    billing_date DATE,
    amount_charged DECIMAL(10,2),
    amount_paid DECIMAL(10,2),
    payment_status VARCHAR(20),
    data_usage_mb DECIMAL(10,2),
    call_minutes INTEGER,
    sms_count INTEGER,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Support tickets table
CREATE TABLE IF NOT EXISTS support_tickets (
    ticket_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) REFERENCES customers(customer_id),
    ticket_date DATE,
    issue_type VARCHAR(100),
    priority VARCHAR(20),
    status VARCHAR(20),
    resolution_time_hours INTEGER,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Churn scores table
CREATE TABLE IF NOT EXISTS churn_scores (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) REFERENCES customers(customer_id),
    score_date DATE,
    churn_probability DECIMAL(5,2),
    risk_category VARCHAR(20),
    late_payment_score DECIMAL(5,2),
    support_ticket_score DECIMAL(5,2),
    usage_decline_score DECIMAL(5,2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Market intelligence table
CREATE TABLE IF NOT EXISTS market_intelligence (
    id SERIAL PRIMARY KEY,
    report_date DATE,
    competitor_name VARCHAR(100),
    plan_type VARCHAR(50),
    price DECIMAL(10,2),
    data_allowance_gb INTEGER,
    our_price DECIMAL(10,2),
    price_difference DECIMAL(10,2),
    competitive_position VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Network events (real-time) table
CREATE TABLE IF NOT EXISTS network_events (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50),
    event_time TIMESTAMP,
    event_type VARCHAR(50),
    data_usage_mb DECIMAL(10,2),
    call_duration_seconds INTEGER,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- SMS delivery table
CREATE TABLE IF NOT EXISTS sms_delivery (
    id SERIAL PRIMARY KEY,
    sms_id VARCHAR(50),
    customer_id VARCHAR(50),
    sent_time TIMESTAMP,
    delivery_status VARCHAR(20),
    failure_reason VARCHAR(200),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX idx_billing_customer ON billing_history(customer_id);
CREATE INDEX idx_tickets_customer ON support_tickets(customer_id);
CREATE INDEX idx_churn_customer ON churn_scores(customer_id);
CREATE INDEX idx_network_customer ON network_events(customer_id);
CREATE INDEX idx_sms_customer ON sms_delivery(customer_id);
