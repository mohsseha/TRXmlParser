select
x.year, c.id_3char, sh.subject, x.x
from year_country_subject_x as x
join subject_hash as sh
join countries as c
on x.subject_hash=sh.hash and x.country=lower(c.comtrade_name);