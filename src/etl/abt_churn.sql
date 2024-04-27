
-- Será feito agora a construção da variável resposta (alvo).
-- Desta vez, o filtro de datas será a partir do dia 2018-01-01. A construção das features foi filtrado o contrário, até 2017-12-31, PORTANTO NÃO HÁ DATA LEAKAGE.
-- Construímos as features preditoras com dados entre 2017-07-01 a 2017-12-31, e vamos construir a variável alvo a partir de 2018-01-01 + 45 dias para frente.
-- Conforme o brainstorming, vamos considerar que houve churn quando o seller ficou 45 dias sem vender.


-- Esta query retorna quais vendedores tiveram aos menos uma venda nos 45 dias seguidos após a data de referência 2018-01-01 e em qual dia vendeu.
DROP TABLE IF EXISTS abt_olist_churn;
CREATE TABLE abt_olist_churn AS
WITH tb_activate AS (
    SELECT
        t2.seller_id AS idVendedor
        ,min(DATE(t1.order_purchase_timestamp)) AS dtAtivacao

    FROM tb_orders AS t1

    LEFT JOIN tb_order_items AS t2
    ON t1.order_id = t2.order_id

    WHERE t1.order_purchase_timestamp >= '2018-01-01'
    AND t1.order_purchase_timestamp <= DATE('2018-01-01', '45 day')
    AND t2.seller_id IS NOT NULL

    GROUP BY 1
)

SELECT
    t1.*
    ,t2.*
    ,t3.*
    ,t4.*
    ,t5.*
    ,t6.*
    ,t7.dtAtivacao
    ,CASE WHEN t7.idVendedor IS NULL THEN 1 ELSE 0 END as flChurn

FROM fs_vendedor_vendas AS t1

LEFT JOIN fs_vendedor_avaliacao AS t2
ON t1.idVendedor =  t2.idVendedor
AND t1.dtReference = t2.dtReference

LEFT JOIN fs_vendedor_cliente AS t3
ON t1.idVendedor =  t3.idVendedor
AND t1.dtReference = t3.dtReference

LEFT JOIN fs_vendedor_entrega AS t4
ON t1.idVendedor =  t4.idVendedor
AND t1.dtReference = t4.dtReference

LEFT JOIN fs_vendedor_pagamento AS t5
ON t1.idVendedor =  t5.idVendedor
AND t1.dtReference = t5.dtReference

LEFT JOIN fs_vendedor_produto AS t6
ON t1.idVendedor =  t6.idVendedor
AND t1.dtReference = t6.dtReference

LEFT JOIN tb_activate AS t7
ON t1.idVendedor =  t7.idVendedor
AND (julianday(t7.dtAtivacao) - julianday(t1.dtReference)) + t1.qtdRecencia <=45

WHERE t1.qtdRecencia <=45
