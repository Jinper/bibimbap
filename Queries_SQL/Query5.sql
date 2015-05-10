create view v1 as
(
select cust,prod,avg(quant) as avg_x
from sales
group by cust,prod
);

create view v2 as
(
select v1.cust,v1.prod,avg(quant) as avg_y
from sales as s, v1
where v1.cust<>s.cust and v1.prod=s.prod
group by v1.cust,v1.prod
);

select v1.cust,v1.prod,avg_x,avg_y
from v1,v2
where v1.cust=v2.cust and v2.prod=v1.prod;