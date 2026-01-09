SELECT
    d.county,
    d.map_date,
    d.dsci,
    sum(w.precipitation) as weekly_percipitation,
    avg(w.temperature_average) as weekly_avg_temperature,
    avg(w.wind_speed) as weekly_avg_winspeed,
    sum(w.snow) as weekly_snow,
    sum(w.daily_sun_minutes) as weekly_sun_minutes

FROM {{ source('drought_weather_data', 'ca_county_drought') }} d
JOIN {{ source('drought_weather_data', 'ca_historic_weather') }} w
    ON d.county = w.county_name
    AND w.map_date = d.map_date

GROUP BY d.county, d.map_date, d.dsci
ORDER BY d.map_date