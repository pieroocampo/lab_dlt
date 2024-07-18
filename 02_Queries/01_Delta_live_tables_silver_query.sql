SELECT
	`timestamp` as `Hora de la Transacción`,
	rcct.card_network as `Marca de la tarjeta`,
	1.0*rcct.bill_value as `Valor de la cuenta`,
	rcct.merchant_type as `Categoría del establecimiento`,
	rcct.merchant_name as `Nombre del establecimiento`,
	rcct.installments as `Cuotas`,
	rcct.transaction_type as `Tipo de transacción`
FROM
	workspace.workshop.merchant_credit_card_transactions_<SU NOMBRE> rcct
