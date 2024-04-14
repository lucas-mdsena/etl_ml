WITH tb_join AS (
    SELECT
        DISTINCT -- Obtém clientes e vendedores distintos.
        t1.order_id as idPedido
        ,t1.customer_id as idCliente
        ,t2.seller_id as idVendedor
        ,t3.customer_state as descUF

    FROM tb_orders AS t1

    LEFT JOIN tb_order_items AS t2
    ON t1.order_id = t2.order_id

    LEFT JOIN tb_customers as t3
    ON t1.customer_id = t3.customer_id

    WHERE order_purchase_timestamp < '2018-01-01'
    AND order_purchase_timestamp >= DATE('2018-01-01', '-6 month')
)

SELECT
    *
FROM tb