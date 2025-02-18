/*
Script that gets the total sum for each category from a categorical column.
It simulates a situation where there are boolean columns and it is needed to get the totals
*/

DECLARE payments STRING;
DECLARE payments_str STRING;
DECLARE payments_arr ARRAY<STRING>;
DECLARE sum_expression STRING;

-- Get the unique payment types formatted for PIVOT
SET payments = (
    SELECT CONCAT('("', STRING_AGG(DISTINCT payment_type, '", "'), '")')
    FROM ecommerce.ecomm_payments
);

-- Store the unique payment types as an array
SET payments_arr = (
    SELECT ARRAY_AGG(DISTINCT payment_type)
    FROM ecommerce.ecomm_payments
);

-- Generate the SUM expressions dynamically
SET sum_expression = (
    SELECT STRING_AGG(FORMAT("SUM(%s) AS %s", field, field), ', ')
    FROM UNNEST(payments_arr) AS field
);

SET payments_str = (
    SELECT STRING_AGG(DISTINCT payment_type),
    FROM ecommerce.ecomm_payments
    );

-- Execute the PIVOT query and reshape output using UNPIVOT
EXECUTE IMMEDIATE FORMAT("""
    WITH bool_table AS (
        SELECT *
        FROM ecommerce.ecomm_payments
        PIVOT (
            COUNT(payment_type) FOR payment_type IN %s
        )
    ),
    summed AS (
        SELECT %s FROM bool_table
    )
    SELECT category, sum_value
    FROM summed
    UNPIVOT (sum_value FOR category IN (%s));
""", payments, sum_expression, payments_str);

