select
    geometry,
    safe_cast(neighbourhood_group as string) as neighbourhood_group,
    safe_cast(neighbourhood as string) as neighbourhood
from {{ source('dataset', 'raw_neighbourhoods') }}
