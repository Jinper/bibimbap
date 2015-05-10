

create view v1 as
(
select cust,month,avg(quant) as avg_x
from sales
where year = 1997
group by cust,month
);

create view v2 as
(
select v1.cust,v1.month,avg(quant) as avg_y
from sales,v1
where year =1997 and v1.cust=sales.cust and v1.month>sales.month
group by v1.cust,v1.month
);

create view v3 as
(
select v1.cust,v1.month,avg(quant) as avg_z
from sales,v1
where year =1997 and v1.cust=sales.cust and v1.month<sales.month
group by v1.cust,v1.month
);

select v1.cust,v1.month,avg_y,avg_x,avg_z
from v1,v2,v3
where v1.cust=v2.cust and v2.cust=v3.cust and v1.month=v2.month and v2.month=v3.month;

