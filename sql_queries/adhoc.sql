select *
from dm_drought_weather_analytics
order by map_date desc
limit 100;


select max(weekly_avg_pressure), county, map_date
from dm_drought_weather_analytics
-- order by map_date desc
group by county, map_date
order by max(weekly_avg_pressure) desc 
limit 100;