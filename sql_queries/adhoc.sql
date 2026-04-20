--4/9
select max(date)
from ca_historic_weather

;
select * from ca_counties;

SELECT date, COUNT(*) 
FROM ca_drought_rain.ca_historic_weather 
WHERE date >= DATE '2026-04-14'
GROUP BY date
ORDER BY date;

delete FROM     ca_historic_weather where 1=1;
--4/14
select max(map_date)
from ca_county_drought; ca_county_drought.county;
select * from dm_drought_weather_analytics