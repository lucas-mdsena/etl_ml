WITH tb_join AS (
    SELECT DISTINCT 
        t1.order_id as idPedido
        ,t1.customer_id idCliente
        ,t2.seller_id as idVendedor
        ,t3.customer_state as descUF

    FROM tb_orders AS t1

    LEFT JOIN tb_order_items AS t2
    ON t1.order_id = t2.order_id

    LEFT JOIN tb_customers as t3
    ON t1.customer_id = t3.customer_id

    WHERE t1.order_purchase_timestamp < '2018-01-01'
    AND t1.order_purchase_timestamp >= DATE('2018-01-01', '-6 month')
    AND t2.seller_id is NOT NULL
),

-- Obtém o percentual de pedidos em cada estado para cada vendedor
tb_group AS(
    SELECT
        idVendedor
        ,count(DISTINCT descUF) as qtdeUFsPedidos
        ,count(DISTINCT CASE WHEN descUF = 'AC' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoAC
        ,count(DISTINCT CASE WHEN descUF = 'AL' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoAL
        ,count(DISTINCT CASE WHEN descUF = 'AM' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoAM
        ,count(DISTINCT CASE WHEN descUF = 'AP' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoAP
        ,count(DISTINCT CASE WHEN descUF = 'BA' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoBA
        ,count(DISTINCT CASE WHEN descUF = 'CE' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoCE
        ,count(DISTINCT CASE WHEN descUF = 'DF' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoDF
        ,count(DISTINCT CASE WHEN descUF = 'ES' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoES
        ,count(DISTINCT CASE WHEN descUF = 'GO' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoGO
        ,count(DISTINCT CASE WHEN descUF = 'MA' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoMA
        ,count(DISTINCT CASE WHEN descUF = 'MG' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoMG
        ,count(DISTINCT CASE WHEN descUF = 'MS' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoMS
        ,count(DISTINCT CASE WHEN descUF = 'MT' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoMT
        ,count(DISTINCT CASE WHEN descUF = 'PA' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoPA
        ,count(DISTINCT CASE WHEN descUF = 'PB' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoPB
        ,count(DISTINCT CASE WHEN descUF = 'PE' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoPE
        ,count(DISTINCT CASE WHEN descUF = 'PI' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoPI
        ,count(DISTINCT CASE WHEN descUF = 'PR' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoPR
        ,count(DISTINCT CASE WHEN descUF = 'RJ' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoRJ
        ,count(DISTINCT CASE WHEN descUF = 'RN' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoRN
        ,count(DISTINCT CASE WHEN descUF = 'RO' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoRO
        ,count(DISTINCT CASE WHEN descUF = 'RR' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoRR
        ,count(DISTINCT CASE WHEN descUF = 'RS' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoRS
        ,count(DISTINCT CASE WHEN descUF = 'SC' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoSC
        ,count(DISTINCT CASE WHEN descUF = 'SE' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoSE
        ,count(DISTINCT CASE WHEN descUF = 'SP' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoSP
        ,count(DISTINCT CASE WHEN descUF = 'TO' THEN idPedido END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctPedidoTO
    FROM tb_join
    GROUP BY idVendedor
)

-- Query final com dados de em quantos estados um vendedor fez vendas e quantas vendas para cada estado
SELECT
    '2018-01-01' AS dtReference
    ,*
FROM tb_group
