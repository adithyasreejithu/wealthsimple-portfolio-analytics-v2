SELECT 
    t.ticker_id AS t_ticker_id,
    t.date AS t_execDate,
    t.remap as t_transaction,
    e.ticker_id AS e_ticker_id,
    e.date AS e_date,
    e.transaction as e_transaction
FROM transactions t
FULL OUTER JOIN Email_Transactions e
    on t.ticker_id = e.ticker_id
    AND t.date = e.date
    AND t_transaction = e.new_trans;

CREATE TABLE IF NOT EXISTS transactions_map(
    org_value       VARCHAR(100),
    new_value       VARCHAR(100)
);

INSERT INTO transactions_map VALUES
    ('DIV', 'dividend'),
    ('BUY', 'buy'),
    ('Dividend Reinvestment Buy', 'buy'),
    ('Dividend', 'dividend'),
    ('Fractional Buy','buy');

ALTER TABLE transactions ADD COLUMN remap VARCHAR;

UPDATE transactions t
SET remap = m.new_value
FROM transactions_map m                                    
WHERE LOWER(TRIM(t.transaction)) = LOWER(TRIM(m.org_value));  

ALTER TABLE Email_Transactions ADD COLUMN new_trans VARCHAR;

UPDATE Email_Transactions et 
SET new_trans = m.new_value
FROM transactions_map m 
WHERE et.transaction = m.org_value;



SELECT 
COALESCE(t.ticker_id, e.ticker_id) AS ticker_id,
COALESCE(t.execDate, e.date) AS 'date',
t.transaction
FROM transactions t 
FULL OUTER JOIN Email_Transactions e
    ON t.ticker_id = e.ticker_id
    AND t.execDate = e.date; 


-- Need to do data validation checks for where columns dont have data IE the fucking purchases. 