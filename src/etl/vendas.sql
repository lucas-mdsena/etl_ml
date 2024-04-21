
--  intervalo médio entre vendas

WITH tb_pedido_item AS (
    SELECT 
        t2.*
        ,t1.order_purchase_timestamp AS dtPedido
    FROM tb_orders AS t1

    LEFT JOIN tb_order_items AS t2
    ON t1.order_id = t2.order_id

    WHERE t1.order_purchase_timestamp < '2018-01-01'
    AND t1.order_purchase_timestamp >= DATE('2018-01-01', '-6 month')
    AND t2.seller_id IS NOT NULL
),

tb_summary AS (
    SELECT 
        seller_id AS idVendedor
        ,count(DISTINCT order_id) AS qtdPedidos
        ,count(DISTINCT DATE(dtPedido)) AS qtdDias
        ,count(product_id) AS qtdItens
        ,CAST(round(min(julianday('2018-01-01') - julianday(dtPedido))) AS INT) AS qtdRecencia -- Retorna a quanto tempo ocorreu a última venda de cada vendedor
        ,sum(price) / count(DISTINCT order_id) AS avgTicket -- Usa-se distinct no order_id, pois a query anterior está em nível de produto                                                                                                                                                                                                     der_id) AS avgTicket -- Feito distinct no order_id, pois a query anterior está em nível de produto
        ,avg(price) AS avgValorProduto
        ,max(Price) AS maxValorProduto
        ,min(Price) AS minValorProduto
        ,count(product_id) / count(DISTINCT order_id) as avgProdutoPedido -- Usa-se distinct no order_id, pois a query anterior está em nível de produto  

    FROM tb_pedido_item
    GROUP BY 1
),

tb_pedido_summary AS (
    SELECT 
        seller_id AS idVendedor
        ,order_id AS idPedido
        ,sum(price) AS vlPreco
    FROM tb_pedido_item

    GROUP BY seller_id, order_id
),

tb_min_max AS (
    SELECT 
        idVendedor
        ,min(vlPreco) AS minVlPedido
        ,max(vlPreco) AS maxVlPedido
    FROM tb_pedido_summary

    GROUP BY idVendedor
),

tb_life AS (
    SELECT 
        t2.seller_id AS idVendedor
        ,sum(t2.price) AS LTV
        ,CAST(round(max(julianday('2018-01-01') - julianday(t1.order_purchase_timestamp))) AS INT) AS qtdeDiasBase -- tempo desde que a pessoa fez a sua primeira venda
    FROM tb_orders AS t1

    LEFT JOIN tb_order_items AS t2
    ON t1.order_id = t2.order_id

    WHERE t1.order_purchase_timestamp < '2018-01-01'
    AND t2.seller_id IS NOT NULL

    GROUP BY t2.seller_id
),

tb_dtpedido AS (
    SELECT
        DISTINCT seller_id as idVendedor
        ,DATE(dtPedido) as dtPedido
    FROM tb_pedido_item

    ORDER BY 1, 2
),

tb_lag AS (
    SELECT 
        *
        ,LAG(dtPedido) OVER (PARTITION BY idVendedor ORDER BY dtPedido) AS lag1 -- cria um coluna que seleciona o valor superior ao da coluna dtPedido, mas particionando por vendedor
                                                                                -- sempre na primeira ocorrência de um vendedor, não haverá valor superior para ele (NULL)
    FROM tb_dtpedido
),

tb_intervalo AS (
    SELECT 
        idVendedor
        ,avg(julianday(dtPedido) - julianday(lag1)) AS avgIntervaloVendas -- os NULL são referentes a vendedores que possuem apenas uma venda
    FROM tb_lag

    GROUP BY idVendedor
)

SELECT 
    '2018-01-01' AS dtReference
    ,t1.*
    ,t2.minVlPedido
    ,t2.maxVlPedido
    ,t3.LTV
    ,t3.qtdeDiasBase
    ,t4.avgIntervaloVendas
FROM tb_summary AS t1

LEFT JOIN tb_min_max AS t2
ON t1.idVendedor = t2.idVendedor

LEFT JOIN tb_life AS t3
ON t1.idVendedor = t3.idVendedor

LEFT JOIN tb_intervalo AS t4
ON t1.idVendedor = t4.idVendedor
