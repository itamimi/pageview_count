delete from bi_analytic.pageviews_postcode_history_count 
where date >= %(date_cutoff)s and hour >= %(hour_cutoff)s
