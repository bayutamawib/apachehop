update dm.dim_sales ds
set avgprice = ds.totalsales / NULLIF(ds.quantity, 0);
