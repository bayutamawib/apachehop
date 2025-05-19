insert into dm.dim_sales (productid, productname, categoryid, class, price)
select 
	ProductID,
	ProductName,
	CategoryID,
	Class,
	Price
from stg.products