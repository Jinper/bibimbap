
create view v1 as
(
select cust,avg(quant) as avg_x
from sales
where year=1997 and state = 'NY'
group by cust
);

create view v2 as
(
select cust,avg(quant) as avg_y
from sales
where year=1997 and state='NJ'
group by cust
);

create view v3 as
(
select cust,avg(quant) as avg_z
from sales
where year=1997 and state='CT'
group by cust
);

select v1.cust,avg_x,avg_y,avg_z
from v1,v2,v3
where v1.cust=v2.cust and v2.cust=v3.cust and avg_x>avg_y and avg_x>avg_z;