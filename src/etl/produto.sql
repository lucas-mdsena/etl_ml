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
),


-- Obtém as estatística de produtos por vendedor
tb_summary AS (
    SELECT 
        idVendedor
        ,avg(COALESCE(product_photos_qty,0)) AS avgQtdeFotos
        ,avg(product_length_cm * product_height_cm * product_width_cm) avgVolumeProduto
        ,max(product_length_cm * product_height_cm * product_width_cm) maxVolumeProduto
        ,min(product_length_cm * product_height_cm * product_width_cm) minVolumeProduto
        -- Calcular mediana do volume
        ,count(DISTINCT CASE WHEN product_category_name = 'cama_mesa_banho' THEN product_id END) * 100.0/100.0 / count(DISTINCT product_id) AS pctCategoriacama_mesa_banho
        ,count(DISTINCT CASE WHEN product_category_name = 'beleza_saude' THEN product_id END) * 100.0/100.0 / count(DISTINCT product_id) AS pctCategoriabeleza_saude
        ,count(DISTINCT CASE WHEN product_category_name = 'esporte_lazer' THEN product_id END) * 100.0/100.0 / count(DISTINCT product_id) AS pctCategoriaesporte_lazer
        ,count(DISTINCT CASE WHEN product_category_name = 'informatica_acessorios' THEN product_id END) * 100.0/100.0 / count(DISTINCT product_id) AS pctCategoriainformatica_acessorios
        ,count(DISTINCT CASE WHEN product_category_name = 'moveis_decoracao' THEN product_id END) * 100.0/100.0 / count(DISTINCT product_id) AS pctCategoriamoveis_decoracao
        ,count(DISTINCT CASE WHEN product_category_name = 'utilidades_domesticas' THEN product_id END) * 100.0/100.0 / count(DISTINCT product_id) AS pctCategoriautilidades_domesticas
        ,count(DISTINCT CASE WHEN product_category_name = 'relogios_presentes' THEN product_id END) * 100.0/100.0 / count(DISTINCT product_id) AS pctCategoriarelogios_presentes
        ,count(DISTINCT CASE WHEN product_category_name = 'telefonia' THEN product_id END) * 100.0/100.0 / count(DISTINCT product_id) AS pctCategoriatelefonia
        ,count(DISTINCT CASE WHEN product_category_name = 'automotivo' THEN product_id END) * 100.0/100.0 / count(DISTINCT product_id) AS pctCategoriaautomotivo
        ,count(DISTINCT CASE WHEN product_category_name = 'brinquedos' THEN product_id END) * 100.0/100.0 / count(DISTINCT product_id) AS pctCategoriabrinquedos
        ,count(DISTINCT CASE WHEN product_category_name = 'cool_stuff' THEN product_id END) * 100.0/100.0 / count(DISTINCT product_id) AS pctCategoriacool_stuff
        ,count(DISTINCT CASE WHEN product_category_name = 'ferramentas_jardim' THEN product_id END) * 100.0/100.0 / count(DISTINCT product_id) AS pctCategoriaferramentas_jardim
        ,count(DISTINCT CASE WHEN product_category_name = 'perfumaria' THEN product_id END) * 100.0/100.0 / count(DISTINCT product_id) AS pctCategoriaperfumaria
        ,count(DISTINCT CASE WHEN product_category_name = 'bebes' THEN product_id END) * 100.0/100.0 / count(DISTINCT product_id) AS pctCategoriabebes
        ,count(DISTINCT CASE WHEN product_category_name = 'eletronicos' THEN product_id END) * 100.0/100.0 / count(DISTINCT product_id) AS pctCategoriaeletronicos

    FROM tb_join

    GROUP BY idVendedor
)


SELECT 
    '2018-01-01' as dtReference
    ,* 
FROM tb_summary



