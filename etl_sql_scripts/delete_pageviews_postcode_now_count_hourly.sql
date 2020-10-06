delete from bi_analytic.pageviews_postcode_now_count
where date >= %(date_cutoff)s and hour >= %(hour_cutoff)s
