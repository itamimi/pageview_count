insert into bi_analytic.pageviews_postcode_now_count
(date ,
 hour ,
postcode ,
pageviews_count)
select
puh.pageview_datetime :: date ,
hour (puh.pageview_datetime) ,
ue.postcode,  -- get postcode from User table for up to date user address
count(puh.*) -- assume pageview is count for any page from any user
from  bi_analytic.Pageviews_user_history as puh
join  bi_db.users_extract as ue
on puh.user_id = ue.id
where puh.pageview_datetime :: date >= %(date_cutoff)s
and   hour(puh.pageview_datetime) >= %(hour_cutoff)s
group by 1 , 2, 3
