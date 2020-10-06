insert into
bi_analytic.pageviews_postcode_now_count
(date ,
 hour ,
postcode ,
pageviews_count)
 select
puh.pageview_datetime :: date ,
hour (puh.pageview_datetime) ,
puh.postcode,
count(puh.*) -- assume pageview is count for any page from any user
from  bi_analytic.Pageviews_user_history as puh
where puh.pageview_datetime :: date >= %(date_cutoff)s
and   hour(puh.pageview_datetime) >= %(hour_cutoff)s
group by 1 , 2, 3
