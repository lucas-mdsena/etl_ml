-- Obtém o portfólio de produtos do vendedor
WITH tb_join AS (
    SELECT DISTINCT
        t2.seller_id as idVendedor
        ,t3.*

    FROM tb_orders AS t1

    LEFT JOIN tb_order_items AS t2
    ON t1.order_id = t2.order_id

    LEFT JOIN tb_products as t3
    ON t2.product_id = t3.product_id

    WHERE t1.order_purchase_timestamp < '2018-01-01'
    AND t1.order_purchase_timestamp >= DATE('2018-01-01', '-6 month')
    AND t2.seller_id is NOT NULL
)


tb_summary AS (
    SELECT 
        idVendedor
        ,avg(COALESCE(product_photos_qty,0)) AS avgQtdeFotos
        ,avg(product_length_cm * product_height_cm * product_width_cm) avgVolumeProduto
        ,max(product_length_cm * product_height_cm * product_width_cm) maxVolumeProduto
        ,min(product_length_cm * product_height_cm * product_width_cm) minVolumeProduto
        -- Calcular mediana do volume

    FROM tb_join

    GROUP BY idVendedor
)