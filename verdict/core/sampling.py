"""
THE METADATA TO KEEP

1. For each column, we need
    - max(count(*)) group by key_col
    - max(sum(key_col)) group by key_col


HOW TO GENERATE SAMPLES

1. We generate a sample using a SQL query. The SQL expression for sampling is expressed as follows:

CREATE TABLE {sample_table_name}
WITH (
    partitioned_by = ARRAY['verdictcol'],
    format='PARQUET'
)
AS
SELECT *
FROM (
    SELECT *, partitioning_expr(verdict_rand_key) verdictcol
    FROM (
        SELECT *, rand_expr(key_col) as verdict_rand_key
        FROM {source};
    ) t1
) t2
WHERE verdictcol > 0;

{sample_table_name} := catalog.schema.table

{source} := a possibly pre-joined table name


2. The samllest sample will be used for caching. The cached samples are used at query time
to estimate selectivity and also to give immediate answers if the required sample sizes are small.

The sample sizes are determined as follows (in ratio):
    sample1  sample2  sample3  sample4  sample4  sample5
     16        8        4        2        1        1

That is, the sampling ratio for the block with 'verdictcol' = i is set as 1 / 2^min(K-1, i)

where K is the total number of blocks. K is determined by finding the smallest integer K such that

    N * ( TOTAL_SAMPLE_RATIO / 2^(K-1) ) <= int(CACHE_ROW_COUNT_LIMIT)

where N = ORIGINAL_ROW_COUNT. This means 

    K = 1 + ceil( log2(max(1.0, N * TOTAL_SAMPLE_RATIO / CACHE_ROW_COUNT_LIMIT)) )
"""

import textwrap
from math import ceil, floor, log2
from ..common.logging import log


# This column is used to control the size of samples
PARTITIONING_COLUMN_NAME = 'verdictcol'

RANDOMIZED_KEY_ALIAS = 'verdict_rand_key'

# The total sample size
TOTAL_SAMPLE_RATIO = 1.00



class UniformRandom(object):

    def __init__(self):
        pass

    def gen_sampling_ratios(self, source_row_count, cache_row_count):
        """
        @param cache_row_count  The smallest sample size. This number is determined by testing data 
                                loading time from redis. This cache is preloaded on server startup
               Note: this serves as the upper limit, not the actual size.
        """
        # determine the number of samples
        K = 1 + ceil(log2(max(1.0, source_row_count * TOTAL_SAMPLE_RATIO / cache_row_count)))

        ratios = self.gen_sampling_ratios_from_parts_count(K)
        return ratios
        
    def gen_sampling_ratios_from_parts_count(self, parts_count):
        # determine the sampling ratios
        # sampling ratios are respect to the original table size
        K = parts_count
        ratios = {}
        for i in range(K):
            ratios[i+1] = (TOTAL_SAMPLE_RATIO / 2**min(i+1, K-1))
        assert sum([r[1] for r in ratios.items()]) == TOTAL_SAMPLE_RATIO
        return ratios

