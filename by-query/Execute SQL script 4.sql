UPDATE stg.sales
set 
	quantity = COALESCE(quantity, 0),
	discount = COALESCE(quantity, 0)