WITH tb_pedido AS(
    SELECT
        t1.order_id AS idPedido 
        ,t2.seller_id AS idVendedor
        ,t1.order_status AS descSituacao
        ,t1.order_purchase_timestamp AS dtPedido
        ,t1.order_approved_at AS dtAprovado
        ,t1.order_delivered_customer_date AS dtEntregue	
        ,t1.order_estimated_delivery_date AS dtEstimativaEntrega
        ,sum(freight_value) AS totalFrete -- soma, pois um vendedor pode ter mais de um pedido por item, e soma-se o valor total do frete por pedido

    FROM tb_orders AS t1

    LEFT JOIN tb_order_items AS t2
    ON t1.order_id = t2.order_id

    WHERE t1.order_purchase_timestamp < '{date}'
    AND t1.order_purchase_timestamp >= DATE('{date}', '-6 month')
    AND t2.seller_id IS NOT NULL

    GROUP BY
        t1.order_id
        ,t2.seller_id
        ,t1.order_status
        ,t1.order_purchase_timestamp
        ,t1.order_approved_at
        ,t1.order_delivered_customer_date
        ,t1.order_estimated_delivery_date
)

SELECT 
    '{date}' AS dtReference
    ,DATE('now') as dtIngestion
    ,idVendedor
    ,count(DISTINCT CASE WHEN descSituacao = 'canceled' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoCancelado
    ,count(DISTINCT CASE WHEN DATE(COALESCE(dtEntregue,'{date}')) > DATE(dtEstimativaEntrega) THEN idPedido END) * 100.0/100.0 / count(DISTINCT CASE WHEN descSituacao = 'delivered' THEN idPedido END) AS pctPedidoAtraso
    ,avg(totalFrete) AS avgFrete
    ,max(totalFrete) AS maxFrete
    ,min(totalFrete) AS minFrete
    -- calcular a mediana do frete

    ,avg(julianday(COALESCE(dtEntregue,'{date}')) - julianday(dtAprovado)) as qtdDiasAprovadoEntrega
    ,avg(julianday(COALESCE(dtEntregue,'{date}')) - julianday(dtPedido)) as qtdDiasPedidoEntrega
    --,avg(datediff(dtEntregue, dtAprovado)) as qtdDiasAprovadoEntrega (o teo calculou assim os resultados dele ficaram um pouco menores)
    --,avg(datediff(dtEntregue, dtPedido)) as qtdDiasPedidoEntrega (o teo calculou assim os resultados dele ficaram um pouco menores)
    ,avg(julianday(dtEstimativaEntrega) - julianday(COALESCE(dtEntregue,'{date}'))) as qtdDiasEntregaPromessa
FROM tb_pedido
GROUP BY idVendedor