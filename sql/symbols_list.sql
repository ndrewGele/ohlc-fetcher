with curr as (
  select
    fetcher_name,
    max(update_timestamp) as update_timestamp
  from symbols
  group by fetcher_name
)
select distinct symbol
from symbols
  inner join curr
  using ( fetcher_name, update_timestamp )
