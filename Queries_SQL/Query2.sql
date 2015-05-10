

create view v1 as
(
select prod,month,sum(quant) as sum_x
from sales
where year = 1997
group by prod, month
);

create view v2 as 
(
select prod,sum(quant) as sum_y
from sales
where year = 1997
group by prod
);

select v1.prod,v1.month,cast(sum_x as float)/sum_y
from v1,v2
where v1.prod=v2.prod;

