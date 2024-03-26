select symbol, max(update_timestamp) as max_update
from daily_ohlc
where symbol in {symbols}
group by symbol