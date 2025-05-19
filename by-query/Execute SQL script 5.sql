with ts1 as (
select
	s.productid,
	s.quantity,
	((100-s.discount)/100) as discmultiplier
from stg.sales s
group by s.productid, s.quantity, s.discount
order by productid
),
ts2 as (
select
	ts1.productid,
	p.price,
	ts1.quantity,
	ts1.discmultiplier
from ts1
left join stg.products p
on ts1.productid = p.productid
), 
ts3 as (
select
	ts2.productid,
	ts2.price,
	ts2.quantity,
	ts2.discmultiplier,
	(ts2.price * ts2.quantity * ts2.discmultiplier) as totalsales
from ts2
order by productid 
), 
ts4 as (
select
	ts3.productid,
	sum(ts3.totalsales) as totalsales
from ts3
group by productid
)
update dm.dim_sales ds
set totalsales = ts4.totalsales
from ts4
where ds.productid = ts4.productid;