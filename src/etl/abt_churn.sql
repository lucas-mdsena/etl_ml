
-- Será feito agora a construção da variável resposta (alvo).
-- Desta vez, o filtro de datas será a partir do dia 2018-01-01. A construção das features foi filtrado o contrário, até 2017-12-31, PORTANTO NÃO HÁ DATA LEAKAGE.
-- Construímos as features preditoras com dados entre 2017-07-01 a 2017-12-31, e vamos construir a variável alvo a partir de 2018-01-01 + 45 dias para frente.
-- Conforme o brainstorming, vamos considerar que houve churn quando o seller ficou 45 dias sem vender.


-- Esta query retorna quais vendedores tiveram vendas nos 45 dias seguidos após a data de referência 2018-01-01 e em qual dia vendeu.
SELECT
    DISTINCT t2.seller_id AS idVendedor
    ,DATE(t1.order_purchase_timestamp) AS dtAtivacao
FROM tb_orders AS t1

LEFT JOIN tb_order_items AS t2
ON t1.order_id = t2.order_id

WHERE t1.order_purchase_timestamp >= '2018-01-01'
AND t1.order_purchase_timestamp <= DATE('2018-01-01', '45 day')
AND t2.seller_id IS NOT NULL

ORDER BY 1, 2
 