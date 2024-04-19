WITH tb_pedido AS (
    SELECT
        DISTINCT
        t1.order_id AS idPedido
        ,t2.seller_id AS idVendedor
    FROM tb_orders AS t1

    LEFT JOIN tb_order_items AS t2
    ON t1.order_id = t2.order_id

    WHERE order_purchase_timestamp < '2018-01-01'
    AND order_purchase_timestamp >= DATE('2018-01-01', '-6 month')
    AND t2.seller_id IS NOT NULL
)
,


tb_join AS (
    SELECT
        t1.*
        ,t2.review_score AS vlNota

    FROM tb_pedido AS t1

    LEFT JOIN tb_order_reviews AS t2
    ON t1.idPedido = t2.order_id
),

tb_summary AS (
    SELECT
        idVendedor
        ,avg(vlNota) AS avgNota
        -- calcular mediana
        ,max(vlNota) AS maxNota
        ,min(vlNota) AS minNota
        ,count(vlNota) / count(idPedido) AS pctAvaliacao
    FROM tb_join

    GROUP BY 1
)

SELECT 
    '2018-01-01' AS dtReference
    ,*
FROM tb_summary
