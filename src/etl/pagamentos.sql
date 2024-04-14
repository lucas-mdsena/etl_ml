-- Definido que o período de análise será entre 01/07/2017 a 01/01/2018.

-- Filtra os pedido que ocorreram entre o intervalo determinado e obtém o id do vendedor para cada.
-- O DISTINCT elimina as linhas repetidas devido ao join, pois um pedido por ter mais de 1 vendedor.
-- Esta é a nossa fatia de pedidos que servirá como referência da análise.
WITH tb_pedidos AS (
    SELECT
        DISTINCT
        t1.order_id
        ,t2.seller_id
    FROM tb_orders AS t1

    LEFt JOIN tb_order_items AS t2
    ON t1.order_id = t2.order_id

    WHERE t1.order_purchase_timestamp < '2018-01-01'
    AND t1.order_purchase_timestamp >= DATE('2018-01-01', '-6 month')
    AND t2.seller_id IS NOT NULL
),

-- A partir da query acima, obtém os pagamentos para cada pedido.
tb_join AS (
    SELECT
        t1.seller_id 
        ,t2.*
    FROM tb_pedidos AS t1

    LEFT JOIN tb_order_payments AS t2
    ON t1.order_id = t2.order_id
),

-- Calcula a quantidade que cada vendedor tem de cada tipo de pagamento (se um vendedor tem 3 formas de pgto, gera 3 linhas para esse vendedor).
tb_group as (
    SELECT 
        seller_id as idVendedor
        ,payment_type as descTipoPagamento
        ,count(DISTINCT order_id) as qtdePedidoMeioPagamento
        ,sum(payment_value) as vlPedidoMeioPagamento
    FROM tb_join
    GROUP BY idVendedor, descTipoPagamento
    ORDER BY idVendedor, descTipoPagamento
)

-- Transforma cada tipo de pagamento em uma coluna e calcula a quantidade de vendas por vendedor (cada vendedor sendo uma linha única)
SELECT 
    idVendedor

    ,sum(case when descTipoPagamento = 'credit_card' then qtdePedidoMeioPagamento else 0 end) as qtde_credit_card_pedido
    ,sum(case when descTipoPagamento = 'boleto' then qtdePedidoMeioPagamento else 0 end) as qtde_boleto_pedido
    ,sum(case when descTipoPagamento = 'debit_card' then qtdePedidoMeioPagamento else 0 end) as qtde_debit_card_pedido
    ,sum(case when descTipoPagamento = 'voucher' then qtdePedidoMeioPagamento else 0 end) as qtde_voucher_pedido

    ,sum(case when descTipoPagamento = 'credit_card' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtde_credit_card_pedido
    ,sum(case when descTipoPagamento = 'boleto' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtde_boleto_pedido
    ,sum(case when descTipoPagamento = 'debit_card' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtde_debit_card_pedido
    ,sum(case when descTipoPagamento = 'voucher' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtde_voucher_pedido

    ,sum(case when descTipoPagamento = 'credit_card' then vlPedidoMeioPagamento else 0 end) as valor_credit_card_pedido
    ,sum(case when descTipoPagamento = 'boleto' then vlPedidoMeioPagamento else 0 end) as valor_boleto_pedido
    ,sum(case when descTipoPagamento = 'debit_card' then vlPedidoMeioPagamento else 0 end) as valor_debit_card_pedido
    ,sum(case when descTipoPagamento = 'voucher' then vlPedidoMeioPagamento else 0 end) as valor_voucher_pedido

    ,sum(case when descTipoPagamento = 'credit_card' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_credit_card_pedido
    ,sum(case when descTipoPagamento = 'boleto' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_boleto_pedido
    ,sum(case when descTipoPagamento = 'debit_card' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_debit_card_pedido
    ,sum(case when descTipoPagamento = 'voucher' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_voucher_pedido

FROM tb_group
GROUP BY 1
ORDER BY 1


