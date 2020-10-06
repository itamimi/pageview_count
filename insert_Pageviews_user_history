insert into  bi_analytic.Pageviews_user_history
(
pageview_datetime  ,
user_id ,
url  ,
postcode
)
select
pe.pageview_datetime ,
pe.user_id,
pe.url,
ue.postcode
from bi_db.pageviews_extract  as pe
join bi_db.users_extract as ue
on pe.user_id = ue.id
