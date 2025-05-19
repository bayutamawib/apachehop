with ad as(
select
	s.productid,
	avg(s.discount) as avgdiscount
from
	stg.sales s
group by s.productid
order by s.productid
)
update dm.dim_sales ds
set avgdiscount = ad.avgdiscount
from ad
where ds.productid = ad.productid;