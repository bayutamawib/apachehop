with sq as(
select
	s.productid,
	sum(s.quantity) as quantity
from
	stg.sales s
group by s.productid
order by s.productid
)
update dm.dim_sales ds
set quantity = sq.quantity
from sq
where ds.productid = sq.productid;