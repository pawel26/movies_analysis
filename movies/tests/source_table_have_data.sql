select
    count(*) as row_count
from {{ ref('raw_source') }}
having count(*) > 0