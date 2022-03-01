select T1.ticker, T1.hour, T2.ts as datetime, T1.highest_hourly_stock_price
from (
        select name as ticker,
			hour(date_add('hour', -4, cast(ts as timestamp))) as hour,
			max(high) as highest_hourly_stock_price
		from "sta9760f2021stream11"
		group BY 1, 2
		order BY 1, 2) T1,
	"sta9760f2021stream11" T2
where T1.ticker = T2.name
	and T1.hour = hour(date_add('hour', -4, cast(T2.ts as timestamp)))
	and T1.highest_hourly_stock_price = T2.high
order by 1, 3;
