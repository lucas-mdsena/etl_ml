-- Esta query retorna as top 15 categorias com mais vendas
SELECT 
    product_category_name as descPedido

FROM tb_order_items AS t2

LEFT JOIN tb_products as t3
ON t2.product_id = t3.product_id

WHERE t2.seller_id is NOT NULL

GROUP BY 1
ORDER BY count(DISTINCT order_id) DESC
LIMIT 15