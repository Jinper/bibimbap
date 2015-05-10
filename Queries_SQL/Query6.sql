
create view v1 as
(
select prod,quant
from sales
group by prod,quant
);

create view v2 as
(
select v1.prod,v1.quant,count(v1.quant) as count_x
from sales, v1
where sales.prod=v1.prod
group by v1.prod,v1.quant
);

create view v3 as
(
select v1.prod,v1.quant,count(v1.quant) as count_y
from sales, v1
where sales.prod=v1.prod and sales.quant<v1.quant
group by v1.prod,v1.quant
);

select v2.prod,v2.quant
from v2,v3
where v2.quant=v3.quant and v2.prod=v3.prod and count_y=count_x/2;