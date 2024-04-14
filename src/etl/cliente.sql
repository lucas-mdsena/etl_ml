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
)


SELECT
    idVendedor
    ,count(CASE WHEN descUF = 'AC' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteAC
    ,count(CASE WHEN descUF = 'AL' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteAL
    ,count(CASE WHEN descUF = 'AM' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteAM
    ,count(CASE WHEN descUF = 'AP' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteAP
    ,count(CASE WHEN descUF = 'BA' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteBA
    ,count(CASE WHEN descUF = 'CE' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteCE
    ,count(CASE WHEN descUF = 'DF' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteDF
    ,count(CASE WHEN descUF = 'ES' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteES
    ,count(CASE WHEN descUF = 'GO' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteGO
    ,count(CASE WHEN descUF = 'MA' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteMA
    ,count(CASE WHEN descUF = 'MG' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteMG
    ,count(CASE WHEN descUF = 'MS' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteMS
    ,count(CASE WHEN descUF = 'MT' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteMT
    ,count(CASE WHEN descUF = 'PA' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClientePA
    ,count(CASE WHEN descUF = 'PB' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClientePB
    ,count(CASE WHEN descUF = 'PE' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClientePE
    ,count(CASE WHEN descUF = 'PI' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClientePI
    ,count(CASE WHEN descUF = 'PR' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClientePR
    ,count(CASE WHEN descUF = 'RJ' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteRJ
    ,count(CASE WHEN descUF = 'RN' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteRN
    ,count(CASE WHEN descUF = 'RO' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteRO
    ,count(CASE WHEN descUF = 'RR' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteRR
    ,count(CASE WHEN descUF = 'RS' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteRS
    ,count(CASE WHEN descUF = 'SC' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteSC
    ,count(CASE WHEN descUF = 'SE' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteSE
    ,count(CASE WHEN descUF = 'SP' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteSP
    ,count(CASE WHEN descUF = 'TO' THEN idPedido  END) * 100.0/100.0 / count(DISTINCT idPedido) AS pctClienteTO
FROM tb_join
GROUP BY idVendedor
HAVING idVendedor LIKE '0ea22c1%'


