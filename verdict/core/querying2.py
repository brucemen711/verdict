"""This module contains the query processing logic.
"""

import concurrent.futures
import copy
import json
import math
import pandas as pd
import pdb
import pprint
import queue
import threading
from typing import Callable, List
from ..interface import *
from ..common.logging import *
from ..common.tools import *



class AggMerger(object):
    """Given the answers computed on summaries, computes statistically unbiased results.

    This class has two main methods.

    1. ``merge``: Merges a new answer computed on a summary.
    2. ``unbiased_result``: Obtains an unbiased result after proper scaling.

    When instantiated, the following fields are initialized:

    1. ``self._merged_result``:         used to store the merged result (before limit)
    2. ``self._merged_sampling_ratio``: the effective sampling ratio of the merged table
    3. ``self._merge_func``:            the functions used to merge two aggregate pd.Series
    4. ``self._limit``:                 the DerivedTable instance for limit() operation
    5. ``self._orderby``:               the DerivedTable instance for orderby() operation

    :param final_table: The DerivedTable instance used for computing the result.
    :param orderby:     A list of (alias_name, sort_order) in str. Both groups and agg work for 
                        the names. sort_order must either be 'asc' or 'desc'.
    :param limit:       The number of final rows to output.
    """

    def __init__(self, final_table, orderby=None, limit=None):
        if limit is not None:
            assert_type(limit, int)
        if orderby is not None:
            assert_type(orderby, (list, tuple))
            for alias_sort in orderby:
                assert_type(alias_sort, (list, tuple))
                assert_equal(len(alias_sort), 2)
                alias, sort = alias_sort
                assert_type(alias, BaseAttr)
                assert sort == 'asc' or sort == 'desc'

        self._merged_result = None
        self._merged_sampling_ratio = 0.0
        self._orderby = orderby
        self._limit = limit
        self._includes_agg = False

        # assert final_table.is_agg()
        first_agg_table = final_table
        assert_type(first_agg_table, DerivedTable)
        while not first_agg_table.is_agg():
            if isinstance(first_agg_table, BaseTable):
                log('No agg() found.', 'WARN')
                first_agg_table = None
                break
            first_agg_table = first_agg_table._source

        if first_agg_table is not None:
            self._includes_agg = True
            agg_types = []
            for agg_func in [a[0] for a in first_agg_table.relop_args()]:
                assert isinstance(agg_func, AggFunc)
                agg_types.append(agg_func.op())
            self._agg_types = agg_types

        def merge_sum(df1_ratio, df2_ratio):
            # maintain partial sum (i.e., the sum of only the sample)
            df1 = df1_ratio[0]
            df2 = df2_ratio[0]
            assert_type(df1, pd.core.series.Series)
            assert_type(df2, pd.core.series.Series)
            return df1.fillna(0.0) + df2.fillna(0.0)

        def merge_count(df1_ratio, df2_ratio):
            # maintain partial count (i.e., the count of only the sample)
            df1 = df1_ratio[0]
            df2 = df2_ratio[0]
            assert_type(df1, pd.core.series.Series)
            assert_type(df2, pd.core.series.Series)
            return df1.fillna(0) + df2.fillna(0)

        def merge_avg(df1_ratio, df2_ratio):
            # maintain unbiased avg (of the sample)
            df1, prev_ratio = df1_ratio
            df2, sampling_ratio = df2_ratio
            assert_type(df1, pd.core.series.Series)
            assert_type(df2, pd.core.series.Series)
            return ((df1.fillna(0.0) * prev_ratio + df2.fillna(0.0) * sampling_ratio) 
                        / (prev_ratio + sampling_ratio))

        self._merge_func = { 'sum': merge_sum, 'count': merge_count, 'avg': merge_avg }

        # # extract limit() information
        # first_limit = final_table
        # while not first_limit.is_limit():
        #     if isinstance(first_limit, BaseTable):
        #         first_limit = None
        #     first_limit = first_limit._source
        # self._limit = None if first_limit is None else copy.deepcopy(first_limit)

        # # extract orderby() information
        # first_orderby = final_table
        # while not first_orderby.is_orderby():
        #     if isinstance(first_orderby, BaseTable):
        #         first_orderby = None
        #     first_orderby = first_orderby._source
        # self._orderby = None if first_order is None else copy.deepcopy(first_orderby)

    def clear(self):
        self._merged_result = None
        self._merged_sampling_ratio = 0.0

    def merge(self, result, sampling_ratio):
        """Merge a new result into an existing merged result. The merged result is expected to be a
        more accurate result.

        :param result:  The pandas.DataFrame instance that contains the computed aggregation
        :param sampling_ratio:  The sampling ratio of the primary table used for the result

        :return:  A statistically unbiased answer.
        """
        log(f"A raw answer from the engine:\n {result}", 'debug')
        log(f"with the following sampling ratio: {sampling_ratio*100}%", 'debug')
        assert isinstance(result, pd.core.frame.DataFrame)
        # print(result)

        if self._merged_result is None:
            self._merged_result = result
            self._merged_sampling_ratio = sampling_ratio

        elif self._includes_agg is False:
            # We vertically append (i.e., union operation in relational algebra)
            self._merged_result = pd.concat([self._merged_result, result], axis=0)
            self._merged_sampling_ratio += sampling_ratio

        else:
            # If the operations include agg(), we can properly merge aggregate values using groupby
            # list as join keys.

            prev_ratio = self._merged_sampling_ratio
            columns_count = len(result.columns)
            groups_count = columns_count - len(self._agg_types)
            group_cols = result.columns[0:groups_count].to_list()
            agg_cols = result.columns[groups_count:columns_count].to_list()

            # print(self._merged_result.columns)
            # print(result.columns)
            assert_array_equal(self._merged_result.columns, result.columns)

            if groups_count == 0:
                # when concatenated, the column names don't change; thus, there will be two columns
                # with exactly the same name; calling joined['name'] returns a pd.DataFrame.

                # joined = pd.concat([self._merged_result, result], axis=1)
                merged = self._merged_result
                assert_type(merged, pd.core.frame.DataFrame)
                for i, agg_col in enumerate(agg_cols):
                    left = merged[agg_col]
                    right = result[agg_col]
                    assert_type(left, pd.core.series.Series)
                    assert_type(right, pd.core.series.Series)
                    merged[agg_col] = self._merge_func[self._agg_types[i]](
                                            (left, prev_ratio), 
                                            (right, sampling_ratio))

                # # we append new columns by naming them '_merged_' + aggcol
                # for i, agg_col in enumerate(agg_cols):
                #     two_cols = joined[agg_col]
                #     assert_equal(len(two_cols.columns), 2)
                #     joined['_merged_' + agg_col] = \
                #         self._merge_func[self._agg_types[i]](
                #             two_cols.iloc[:,[0]], two_cols.iloc[:,[1]])

                # # now, we only select those merged columns
                # merged = joined[['_merged_' + a for a in agg_cols]]
                # merged.columns = agg_cols

            else:
                # when joined, the agg column names will look like total_price_x, total_price_y
                # print(self._merged_result)
                # print(result)
                # print(group_cols.to_list())
                joined = pd.merge(self._merged_result, result, how='outer', on=group_cols)

                # merge agg columns (e.g., total_price_x, total_price_y) into one (total_price)
                for i, agg_col in enumerate(agg_cols):
                    to_merge_cols = []
                    # find matching columns
                    for jc in joined.columns:
                        if jc.startswith(agg_col):
                            assert jc != agg_col
                            to_merge_cols.append(jc)
                    assert_equal(len(to_merge_cols), 2)

                    joined[agg_col] = self._merge_func[self._agg_types[i]](
                                            (joined[to_merge_cols[0]], prev_ratio), 
                                            (joined[to_merge_cols[1]], sampling_ratio))
                merged = joined[group_cols + agg_cols]

            self._merged_result = merged
            self._merged_sampling_ratio += sampling_ratio

        return self.unbiased_result()

    def unbiased_result(self):
        """Obtains a statistically unbiased answer from a series of answers that have been merged.

        :return:  A statistically unbiased answer.
        """
        if self._merged_result is None:
            return None

        # When there's no aggregation, we don't need to scale anything
        if self._includes_agg is False:
            result = self._merged_result

            if self._orderby is not None:
                by = [a[0].name() for a in self._orderby]
                ascending = [True if a[1] == 'asc' else False for a in self._orderby]
                result.sort_values(by=by, ascending=ascending, inplace=True)

            if self._limit is None:
                return result
            else:
                return result.head(self._limit)

        # When agg() is included, we need to properly scale the results
        ratio = self._merged_sampling_ratio
        result = self._merged_result

        agg2scale = { 'sum': 1.0/ratio, 'avg': 1.0, 'count': 1.0/ratio }
        scale_factors = [agg2scale[t] for t in self._agg_types]

        groups_count = len(result.columns) - len(scale_factors)
        assert groups_count >= 0
        if groups_count == 0:
            scaled_result = result * scale_factors
        else:   # groupby().agg() query
            groups_df = result[result.columns[0:groups_count]]
            agg_df = result[result.columns[groups_count:len(result.columns)]]
            scaled_result = pd.concat([groups_df, agg_df * scale_factors], axis=1)

        # we round for count()
        for i, agg_type in enumerate(self._agg_types):
            if agg_type == 'count':
                col_to_round = result.columns[groups_count+i]
                scaled_result[col_to_round] = scaled_result[col_to_round].round().astype(int)

        # Finally, apply ordering and limit
        if self._orderby is not None:
            by = [a[0].name() for a in self._orderby]
            for attr in by:
                if attr not in scaled_result.columns:
                    raise ValueError(f"The ordering column '{attr}' is not found in the result "
                                      "set's column names: " + str(list(scaled_result.columns)))
            ascending = [True if a[1] == 'asc' else False for a in self._orderby]
            log(f'The result is ordered by: {by}, {ascending}', 'debug')
            scaled_result.sort_values(by=by, ascending=ascending, inplace=True)
            scaled_result = scaled_result.reset_index()[scaled_result.columns]

        if self._limit is None:
            return scaled_result
        else:
            return scaled_result.head(self._limit)


class Querying(object):
    """The instance must be created for every query.
    """

    # TODO: make this adjustable at query time
    # Currently this value (10000) can ensure 1% relative error for count queries
    # More details on derivation
    # 1. count
    #    According to Hoeffding's inequality, we need at least log(2/a) r^2 / (2*t^2) samples
    #    to acquire (1-a) confidence interval of size t, where r is max - min. For example, 
    #    suppose g is the selectivity for the smallest group. To have 1% relative error (or 
    #    equivalently, t=0.01*g), and a=0.05; we need at least 18,445 / g^2 samples.
    r = 1.0
    COUNT_SAMPLE_SIZE_COEFF = math.log(2/0.05) * r**2 / 2.0
    # COUNT_MIN_SAMPLE_SIZE = 18445

    # 2. avg
    #    We apply again Hoeffding's inequality. Suppose r (max-min) is smaller than 2*mu.
    #    Then, we need 73778 samples for each group.
    AVG_MIN_SAMPLE_SIZE = 73778

    def __init__(self, engine, cache_engine):
        self._engine = engine
        self._cache_engine = cache_engine

    def json(self, query_request, rel_err_bound):
        """Computes an accuracy-guaranteed answer using automatically chosen sample sizes.

        :param query_request:  A query object. The query must in the following form:

            .. code-block:: 

                {
                    "type": "single_agg",
                    "source": relops,
                    "agg": {
                        "alias": agg_func, ...
                    },
                    "groupby": [alias, ...],
                    "orderby": ["alias asc", ...],
                    "limit": int,
                    "options": {
                        "bypass_cache": bool
                    },
                    "sample_info": {
                        base_name: [
                            {
                                "sample_id": str,
                                "key_col": str,
                                "part_col": str,
                                "row_count": int,
                                "partitions": [
                                    ...
                                ]
                            }
                        ]
                    }
                } 

        :param rel_err_bound:  The required relative error
        """
        assert_type(query_request, dict)
        assert 'single_agg' == query_request['type']
        source_rel = query_request['source']
        base2sample_info = query_request['sample_info']
        assert_type(source_rel, Table)
        options = {
            "bypass_cache": False
        }
        if 'options' in query_request:
            options.update(query_request['options'])
        orderby = None if 'orderby' not in query_request else query_request['orderby']
        limit = None if 'limit' not in query_request else query_request['limit']
        assert_type(limit, (type(None), int))


        # Find the sample candidates to replace base tables
        # we need to pass this
        # base2sample_id = {
        #     "base_table_name": {
        #         "use_sample": True,
        #         "samples": {
        #             "key_col": sample_id, ...
        #         }
        #     },
        #     ...
        # }
        base2sample_id = {}
        for base_name, sample_infos in base2sample_info.items():
            base2sample_id[base_name] = {}
            base2sample_id[base_name]['samples'] = {}
            for sample_info in sample_infos:
                key_col = sample_info['key_col']
                sample_id = sample_info['sample_id']
                base2sample_id[base_name]['samples'][key_col] = sample_id
        base2chosen = determine_sample_ids(find_replacables(source_rel, base2sample_id))
        log(f'The base tables in the query will be replaced as follows: {base2chosen}', 'debug')

        # Run it using the cache engine to check the selectivity
        query_to_run = source_rel
        if "groupby" in query_request:
            query_to_run = query_to_run.groupby(query_request['groupby'])
        agg_args = query_request['agg']
        query_to_run = query_to_run.agg(agg_args)
        # base2chosen is in the following structure:
        # {
        #   base_table: { "key_col": str, 
        #                 "sample_id": str 
        #   },
        #   ...
        # }
        
        # this method sets some metadata to "self" object
        # thus, the instance of this class must be created for every query by the front-end
        cache_result, cache_ratio = self.run_on_cache(query_to_run, base2chosen)
        agg_merger = AggMerger(query_to_run, orderby, limit)
        unbiased_result = agg_merger.merge(cache_result, cache_ratio)
        log("The result using cache:", 'debug')
        log(unbiased_result, 'debug')

        # Check if we can simply use the cache
        required_ratio = self.estimate_required_sampling_ratio(query_to_run)
        log(f"We need at least {required_ratio*100}% of data for accurate answers.", 'debug')

        # is_accurate = self.is_accurate(query_to_run)
        if options['bypass_cache']:
            log(f'Bypassing the cache result according to the option.')

        if cache_ratio >= required_ratio:
            log(f"The cache is large enough ({cache_ratio*100}%); we return its answer.", "debug")
            return unbiased_result

        # Otherwise, we compose a query with the required sampling ratio
        # required_ratio = self.estimate_required_sampling_ratio(cache_ratio)

        # we need to pass the following structure for base2chosen
        # { base_name: { "sample_id": str,
        #                "key_col": str,
        #                "part_col": str,
        #                "partitions": [ ... ] },
        #   ...
        # }
        for base_name, col_and_sample in base2chosen.items():
            infos = base2sample_info[base_name]
            for info in infos:
                if info['key_col'] == col_and_sample['key_col']:
                    col_and_sample['partitions'] = info['partitions']
                    col_and_sample['part_col'] = info['part_col']
                    continue
            assert 'partitions' in col_and_sample
        sample_result, actual_ratio = self.run_on_samples(query_to_run, required_ratio, base2chosen)

        # Scale and return
        agg_merger = AggMerger(query_to_run, orderby, limit)
        unbiased_result = agg_merger.merge(sample_result, actual_ratio)
        log("The result using samples:", 'debug')
        log(unbiased_result, 'debug')
        return unbiased_result

    def run_on_cache(self, query, base2chosen):
        """Process the query using an in-memory engine.

        For every base table (e.g., named "base.table"), we replace it with "base.table.key_col",
        and process it using an in-memory engine.

        :param query:  A query object

        :param base2chosen:  A mapping from base_table to 

            .. code-block:: 

                { 
                    "key_col": str, 
                    "sample_id": str,
                    "sampling_ratio": float
                }
        """
        query = copy.deepcopy(query)
        old2new_name = {}
        for base, col_and_sample in base2chosen.items():
            sample_id = col_and_sample['sample_id']
            old2new_name[base] = sample_id
        new_query = replace_table_name(query, old2new_name)
        query_to_db = to_verdict_query(new_query)
        result, meta = self._cache_engine.execute(query_to_db)
        ratio = meta['ratio']
        assert_type(ratio, float)
        self._cache_ratio = ratio
        self._min_group_size_on_cache = meta['min_group_size']
        self._max_group_size_on_cache = meta['max_group_size']
        self._total_cache_size = meta['total_cache_size']
        log(f'The smallest group size is {self._min_group_size_on_cache}.', 'debug')
        log(f'The biggest group size is {self._max_group_size_on_cache}.', 'debug')
        log(f'The size of cache is {self._total_cache_size}.', 'debug')
        return result, ratio


    def estimate_required_sampling_ratio(self, query, rel_err_bound=0.01):
        """Estimates the minimum sample size that is large enough to satisfy the specified error.

        :param required_rel_acc:  A relative error requirement
        """
        count_coeff = Querying.COUNT_SAMPLE_SIZE_COEFF
        avg_min_sample_size   = Querying.AVG_MIN_SAMPLE_SIZE

        cache_ratio = self._cache_ratio
        min_cache_group_size = self._min_group_size_on_cache
        max_cache_group_size = self._max_group_size_on_cache
        total_cache_size = self._total_cache_size       # in terms of # of unique keys

        # Get the aggregation types
        first_agg_table = query
        while not first_agg_table.is_agg():
            if isinstance(first_agg_table, BaseTable):
                raise ValueError('No agg() found.')
            first_agg_table = first_agg_table._source
        agg_funcs = [a[0] for a in first_agg_table.relop_args()]
        agg_name_set = set([aggfunc.op() for aggfunc in agg_funcs])

        if cache_ratio >= 1.0:
            return True

        required_ratios = []
        for agg_name in agg_name_set:
            if agg_name == 'count':
                # Only count or sum
                # this ensures less than rel_err_bound for each group
                est_selectivity = min_cache_group_size / float(total_cache_size)
                # required_size = count_coeff / rel_err_bound**2 / est_selectivity**2
                required_size = (1.0/est_selectivity - 1) / rel_err_bound**2    # CLT
                required_ratios.append(cache_ratio * required_size / total_cache_size)

            else:
                # this ensures every avg is accurate
                required_ratios.append(cache_ratio * avg_min_sample_size / min_cache_group_size)

        return min(max(required_ratios), 1.0)
        # required_sampling_ratio = cache_ratio * MIN_SAMPLE_SIZE / float(min_cache_group_size)
        # required_sampling_ratio = min(1.0, required_sampling_ratio)
        # log(f'The cache was {cache_ratio*100:.2f}% of data.')
        # log(f'We need {required_sampling_ratio*100:.2f}% of data for accurate answers.', 'debug')
        # return required_sampling_ratio

    def run_on_samples(self, query, required_ratio, base2sample):
        """
        :param query:  A query object
        :param base2sample:  This must in the following structure

            .. code-block:: 

                {
                    base_name: {
                        "sample_id": str,
                        "key_col": str,
                        "partitions": [
                            ...
                        ]
                    }
                }
        """
        query = copy.deepcopy(query)
        for base_name, info in base2sample.items():
            assert 'sample_id' in info
            assert 'key_col' in info
            assert 'partitions' in info
        query_with_samples, actual_ratio = set_required_samples(query, required_ratio, base2sample)
        query_to_db = to_verdict_query(query_with_samples)
        sample_result = self._engine.execute(query_to_db)
        return sample_result, actual_ratio

    def run_on_sample_part(self, query, primary_base, part_value, base2sample):
        """
        :param query:        A query object
        :param primary_base: The table associated with the part_value
        :param part_value:   The column value that indicates the parts we need to process

        :param base2sample:  The information about the sample tables to use. This must in the 
                             following structure:

            .. code-block:: 

                {
                    base_name: {
                        "sample_id": str,
                        "key_col": str,
                        "partitions": [
                            ...
                        ]
                    }
                }
        """
        assert_type(query, Table)
        query = copy.deepcopy(query)
        for base_name, info in base2sample.items():
            assert 'sample_id' in info
            assert 'key_col' in info
            assert 'partitions' in info
        query_with_samples, actual_ratio = \
            set_required_parts(query, primary_base, part_value, base2sample)
        query_to_db = to_verdict_query(query_with_samples)
        sample_result = self._engine.execute(query_to_db)
        return sample_result, actual_ratio


    def json_stream(self, query_request):
        """Computes aggregate answers in a streaming fashion.

        :param query:  A query object. The query must in the following form:

            .. code-block:: 

                {
                    "type": "stream_agg",
                    "source": relops,
                    "agg": {
                        "alias": agg_func, ...
                    },
                    "groupby": [alias, ...],
                    "orderby": ["alias asc", ...],
                    "limit": int,
                    "options": {
                        "bypass_cache": bool
                    },
                    "sample_info": {
                        {
                            base_name: [
                                {
                                    "sample_id": str,
                                    "key_col": str,
                                    "part_col": str,
                                    "row_count": int,
                                    "partitions": [
                                        ...
                                    ]
                                }
                            ]
                        }
                    }
                }   
        """
        assert_type(query_request, dict)
        assert 'stream_agg' in query_request['type']
        source_rel = query_request['source']
        base2sample_info = query_request['sample_info']
        assert_type(source_rel, Table)
        options = {
            "bypass_cache": False
        }
        if 'options' in query_request:
            options.update(query_request['options'])
        orderby = None if 'orderby' not in query_request else query_request['orderby']
        limit = None if 'limit' not in query_request else query_request['limit']


        # Find the sample candidates to replace base tables
        # we need to pass this
        # base2sample_id = {
        #     "base_table_name": {
        #         "use_sample": True,
        #         "samples": {
        #             "key_col": sample_id, ...
        #         }
        #     },
        #     ...
        # }
        base2sample_id = {}
        for base_name, sample_infos in base2sample_info.items():
            base2sample_id[base_name] = {}
            base2sample_id[base_name]['samples'] = {}
            for sample_info in sample_infos:
                key_col = sample_info['key_col']
                sample_id = sample_info['sample_id']
                base2sample_id[base_name]['samples'][key_col] = sample_id
        # We get this:
        # { base_name: { "key_col": str,
        #                "sample_id": str},
        #   ...                             
        # }
        base2chosen = determine_sample_ids(find_replacables(source_rel, base2sample_id))
        if bool(base2chosen):
            log(f'The base tables in the query will be replaced as follows: {base2chosen}', 'debug')
        else:
            raise ValueError(f"No replaceable samples are found.")

        # We base2chosen as follows:
        # {
        #     base_name: {
        #         "key_col": str,
        #         "sample_id": str,
        #         "part_col": str,
        #         "partitions": [
        #             ...
        #         ]
        #     }
        # }
        for base_name, col_and_sample in base2chosen.items():
            infos = base2sample_info[base_name]
            for info in infos:
                if info['key_col'] == col_and_sample['key_col']:
                    col_and_sample['partitions'] = info['partitions']
                    col_and_sample['part_col'] = info['part_col']
                    continue
            assert 'partitions' in col_and_sample

        # Compose a query to run
        query_to_run = source_rel
        if "groupby" in query_request:
            query_to_run = query_to_run.groupby(query_request['groupby'])
        agg_args = query_request['agg']
        query_to_run = query_to_run.agg(agg_args)

        # 1. We process (joins of) samples based on the part_col_values of the table which has
        #    the greatest number of parts. For example, if lineitem and orders are joined, this 
        #    stream processing will be based on the part_col_values of the lineitem table. This rule
        #    is due to our design choice that only the smallest part is cached.

        # sanity check
        primary_base = None
        largest_part_value = -1
        for base, chosen in base2chosen.items():
            verify_sample_partitions(chosen['partitions'])
            this_largest = max([p['col_value'] for p in chosen['partitions']])
            if this_largest > largest_part_value:
                primary_base = base
                largest_part_value = this_largest
        col_values_and_ratios = base2chosen[primary_base]['partitions']
        cache_col_value = largest_part_value

        # 3. Perform all operations concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            query_futures = []
            col_values_and_ratios = sorted(col_values_and_ratios, 
                                       key=lambda c: c['col_value'], reverse=True)
            agg_merger = AggMerger(query_to_run, orderby, limit)

            for part in col_values_and_ratios:
                col_value = part['col_value']
                ratio = part['sampling_ratio']
                if col_value == cache_col_value:
                    query_futures.append(
                        executor.submit(self.run_on_cache, query_to_run, base2chosen))
                else:
                    query_futures.append(
                        executor.submit(self.run_on_sample_part, query_to_run, primary_base, 
                                        col_value, base2chosen))

            for f in concurrent.futures.as_completed(query_futures):
                result, sampling_ratio = f.result()
                unbiased_result = agg_merger.merge(result, sampling_ratio)
                yield unbiased_result



def verify_sample_partitions(partitions):
    """Ensures that the sample partitions are in the expected structure.

    1. The partition sizes must be decreasing as the partition number increases.

    :param partitions:  A list of ``{ 'col_value': int, 'sampling_ratio': float }``
    """
    sorted_partitions = sorted(partitions, key=lambda p: p['col_value'])
    largest_ratio = sorted_partitions[0]['sampling_ratio']
    smallest_ratio = sorted_partitions[-1]['sampling_ratio']
    assert smallest_ratio <= largest_ratio


def set_required_parts(source_rel, primary_base, part_value, base2sample):
    """The effective ratio for the chosen combination is based on the table with the most parts.

    :param part_value:  The value that indicates what parts of each sample must be processed.
                        Specifically, the parts whose associated values are equal to the value
                        must be processed. If there is no such part, then the greatest value that is
                        smaller than the part_value must be processed. In this way, we ensure that
                        for each part_value, all matching tuples are processed.

    :param base2sample: The mapping from base table name to the sample table

        .. code-block:: 
        
            { base_name: { "sample_id": str,
                           "key_col": str,
                           "part_col": str,
                           "row_count": int,
                           "partitions": [
                               { "col_value": int, "sampling_ratio": float}, ...
                           ] } ... }

    :return:  The following structure:

        .. code-block:: 

            { base_name: { 'sample_name': sample_id,
                           'part_col_values': [ ... ],
                           'ratio': float },
              ...}
    """
    assert_type(source_rel, Table)
    assert_type(primary_base, str)
    assert_type(part_value, int)
    assert part_value >= 1

    # assert that the sampling ratios are in the expected order
    for base_name, info in base2sample.items():
        verify_sample_partitions(info['partitions'])

    # We set the parts (for samples) so that they include the specified part (by the part_value).
    effective_ratio = None
    base2sample_part = {}
    for base_name, info in base2sample.items():
        base2sample_part[base_name] = {
            'sample_name': info['sample_id']
        }
        base2sample_part[base_name]['part_col_values'] = []
        parts = sorted(info['partitions'], key=lambda a: a['col_value'], reverse=True)
        for p in parts:
            this_value = p['col_value']
            ratio = p['sampling_ratio']
            if part_value >= this_value:
                # this value will be the largest value that is still not larger than part_value
                base2sample_part[base_name]['part_col_values'].append(this_value)
                base2sample_part[base_name]['ratio'] = ratio
                if base_name == primary_base:
                    effective_ratio = ratio
                break
            else:
                continue
        assert len(base2sample_part[base_name]['part_col_values']) > 0

    sample_rel = replace_base_with_sample(source_rel, base2sample_part)
    return sample_rel, effective_ratio


def set_required_samples(source_rel, required_ratio, base2sample):
    """Replaces the base tables with the minimum required samples.

    :param base2sample:  The mapping from base table name to the sample table

        .. code-block:: 

            { base_name: { "sample_id": str,
                           "key_col": str,
                           "part_col": str,
                           "row_count": int,
                           "partitions": [
                               { "col_value": int, "sampling_ratio": float}, ...
                           ] } ... }

    :return:    The following structure:

        .. code-block:: 

            { base_name: { 'sample_name': sample_id,
                           'part_col_values': [ ... ],
                           'ratio': float  },
              ...}
    """
    assert_type(source_rel, Table)
    assert_type(required_ratio, float)
    assert required_ratio <= 1.0

    # assert that the sampling ratios are in the expected order
    for base_name, info in base2sample.items():
        verify_sample_partitions(info['partitions'])

    # We set the parts (for samples) so that their combined ratios are just above what's required.
    base2min_sample = {}
    for base_name, info in base2sample.items():
        base2min_sample[base_name] = {
            'sample_name': info['sample_id']
        }
        base2min_sample[base_name]['part_col_values'] = []
        parts = sorted(info['partitions'], key=lambda a: a['col_value'], reverse=True)
        cumul_ratio = 0.0
        for p in parts:
            col_value, ratio = p['col_value'], p['sampling_ratio']
            base2min_sample[base_name]['part_col_values'].append(col_value)
            cumul_ratio += ratio
            if cumul_ratio >= required_ratio:
                base2min_sample[base_name]['ratio'] = cumul_ratio
                break

    effective_ratio = min([a['ratio'] for a in base2min_sample.values()])
    sample_rel = replace_base_with_sample(source_rel, base2min_sample)
    return sample_rel, effective_ratio


def replace_table_name(rel_obj, old2new_name):
    """Replaces an old name with a new name

    :param old2new_name:    A dictionary of ``{ old_name: new_name }``.
                            Both key and value are str type.
    """
    def replace(o):
        return replace_table_name(o, old2new_name)

    if isinstance(rel_obj, str):
        return rel_obj

    elif isinstance(rel_obj, List):
        return [replace(a) for a in rel_obj]

    elif isinstance(rel_obj, Attr):
        return rel_obj

    elif isinstance(rel_obj, tuple):
        return tuple([replace(a) for a in rel_obj])

    elif isinstance(rel_obj, BaseTable):
        if rel_obj.name() in old2new_name:
            return SampleTable(old2new_name[rel_obj.name()])
        else:
            return rel_obj

    elif isinstance(rel_obj, DerivedTable):
        rel_obj.set_source(replace(rel_obj.source()))
        rel_obj.set_relop_args(replace(rel_obj.relop_args()))
        return rel_obj
    
    else:
        raise ValueError(type(rel_obj))


def replace_base_with_sample(final_table, base2sample_info):
    """Replaces the occurrence of BaseTables in 'table' with a corresponding sample table.

    :param final_table: All eligible base tables in this final_table object will be replaced by
                        sample tables.
    :param base2sample_info:  Has the replacement information:

        .. code-block:: 

            {
                base_table_name: {
                    "sample_name": sample_table_name,
                    "part_col": key_col_name
                    "part_col_values": [value1, value2, ... ]
                }
            }

        The size of the sample table (when filtered by those values) must be right above the
        required sampling ratios (but not too larger). Here, "part_col" is the partitioning column
        name.
    """
    base2sample_info = copy.deepcopy(base2sample_info)
    # for base_name, sample_info in base2sample_info.items():
    #     sample_name = sample_info['sample_name']
    #     part_col_values = sample_info['part_col_values']
    #     if len(part_col_values) == 0:
    #         sample_info['sample_table'] = None
    #     else:
    #         sample_info['sample_table'] = SampleTable(sample_name, part_col_values)
    return replace_base_with_sample_inner(final_table, base2sample_info)


def replace_base_with_sample_inner(element, base2sample_info):
    """
    :param element:     This can either be Table or Attr. If BaseAttr, we need to replace its
                        internal BaseTable
    :param base2sample_info:    A map from a base table's full name to its sample information.
                                See above for more details.
    """
    def replace(o):
        if o is None:
            return o
        return replace_base_with_sample_inner(o, base2sample_info)

    if isinstance(element, BaseAttr):
        # return BaseAttr(replace(element.table()), element.name())
        # attr_table = element.table()
        # if isinstance(attr_table, BaseTable):
        #     sample_info = base2sample_info[attr_table.full_name()]
        #     if sample_info['sample_table'] is not None:
        #         # return BaseAttr(sample_info['sample_table'], element.name())
        #         return BaseAttr(element.name())
        return element

    elif isinstance(element, AttrOp):
        element.set_args([replace(a) for a in element.args()])
        return element

    elif isinstance(element, Constant):
        return element

    elif isinstance(element, str) and (element in DerivedTable.join_types):
        return element

    elif isinstance(element, dict):
        # a dictionary is used as an argument of project() and agg()
        return dict([(k, replace(v)) for k, v in element.items()])

    elif isinstance(element, (list, tuple)):
        # a list or tuple is used as an argument of orderby()
        return element

    elif isinstance(element, AggFunc):
        element.set_arg(replace(element.arg()))
        return element

    elif isinstance(element, BaseTable):
        # when there is no source
        sample_info = base2sample_info[element.name()]
        sample_name = sample_info['sample_name']
        part_col_values = sample_info['part_col_values']
        # sample_name = sample_info['sample_name']
        # part_col = sample_info['part_col']
        # part_col_values = sample_info['part_col_values']
        # sample_table = sample_info['sample_table']
        if len(part_col_values) == 0:
            return element
        else:
            sample_table = SampleTable(sample_name, part_col_values)
            return sample_table

    elif isinstance(element, DerivedTable):
        old_source = element.source()
        new_source = replace(old_source)
        # if (element.is_join() and isinstance(old_source, BaseTable) and 
        #     isinstance(new_source, DerivedTable)):
            # the base table has been replaced with sample table with a predicate
        element.set_source(new_source)
        element.set_relop_args([replace(a) for a in element.relop_args()])
        return element      

    else:
        raise ValueError(element)


def determine_sample_ids(candidates):
    """
    :param candidates:  This has the mapping from a projected column name to feasible samples

        .. code-block:: 

            {
                projected_col_name: {
                    base_name: {
                        "key_col": str,
                        "sample_id": str
                    }
                },
            }

    :return:  A mapping from base_table to ``{ "key_col": str, "sample_id": str }``
    """
    if len(candidates.keys()) == 0:
        return {}

    if "_rowid" in candidates:
        # this is preferred (i.e., when there are no joins between fact tables)
        return candidates["_rowid"]

    # Otherwise, use any
    return candidates[list(candidates.keys())[0]]


def find_replacables(rel_obj, sample_info):
    """Finds the base tables that can be replaced with sample tables. We also identify the key_col 
    for those sample tables.

    :param rel_obj:     This must be the object before applying the final aggregation.

    :param sample_info: The information about available sample tables. It has the following
                        structure:

        .. code-block:: 

            {
                "base_table_name": {
                    "use_sample": True,
                    "samples": {
                        "key_col": sample_id, 
                        ...
                    }
                },
                ...
            }

    :return:  The replacement information that will be propagated recursively.

       .. code-block:: 

            {
                projected_col_name: {
                    base_name: {
                        "key_col": str,
                        "sample_id": str
                    }
                },
            }

        This structure represents the base tables that can be replaced by corresponding sample
        tables when those projected_col_name is considered as the key_col.
    """
    log(f"Finding base->summary for {rel_obj}", "debug")
    log(f"The current summary info: {sample_info}", "debug")

    if isinstance(rel_obj, BaseTable):
        table_name = rel_obj.name()
        candidates = {}
        if table_name not in sample_info:
            return candidates
            # raise ValueError(f"Sample information not found for {table_name}.")
        base_info = sample_info[table_name]
        if 'use_sample' not in base_info or base_info['use_sample'] is True:
            for key_col, sample_id in base_info['samples'].items():
                candidates[key_col] = {
                    table_name: {
                        "key_col": key_col,
                        "sample_id": sample_id
                    }
                }
        log(f"Found base->summary: {candidates}", "debug")
        return candidates

    if isinstance(rel_obj, DerivedTable):
        source_candidates = find_replacables(rel_obj.source(), sample_info)

        if rel_obj.is_project():
            new_candidates = {}
            def extract_base_attr(attr):
                if isinstance(attr, BaseAttr):
                    return [attr]
                elif isinstance(attr, AttrOp):
                    extracted = []
                    for a in attr.args():
                        extracted.extend(extract_base_attr(a))
                    return extracted
                else:
                    return []
            args = rel_obj.relop_args()
            if "_rowid" in source_candidates:
                # phony column
                # args.append((BaseAttr("_rowid"), "_rowid"))
                new_candidates["_rowid"] = source_candidates["_rowid"]
            for a in args:
                assert_type(a, (List, tuple))
                base_attrs = extract_base_attr(a[0])
                alias = a[1]
                for base_attr in base_attrs:
                    base_attr_name = base_attr.name()
                    if base_attr_name in source_candidates:
                        new_candidates[alias] = source_candidates[base_attr_name]
            new_candidates.update(source_candidates)
            log(f"Found base->summary: {new_candidates}", "debug")
            return new_candidates

        elif rel_obj.is_agg():
            if rel_obj.source().is_groupby():
                # If groupby() proceeds, we can simply respect the candidates computed from the 
                # sources
                log(f"Found base->summary: {source_candidates}", "debug")
                return source_candidates
            else:
                # If there is no groupby(), there is no key_col for this source
                log(f"Found base->summary: None", "debug")
                return {}

        elif rel_obj.is_select():
            log(f"Found base->summary: {source_candidates}", "debug")
            return source_candidates

        elif rel_obj.is_join():
            new_candidates = {}
            right_join_table = rel_obj.right_join_table()
            right_table_candidates = find_replacables(right_join_table, sample_info)

            # we select only the matching key_col
            left_join_col = rel_obj.left_join_col()
            right_join_col = rel_obj.right_join_col()

            # the join column must exist in the projected list
            left_name = left_join_col.name()
            right_name = right_join_col.name()
            if left_name in source_candidates and right_name in right_table_candidates:
                combined = source_candidates[left_name]
                combined.update(right_table_candidates[right_name])
                new_candidates[left_name] = combined
                new_candidates[right_name] = combined
            log(f"Found base->summary: {new_candidates}", "debug")
            return new_candidates

        elif rel_obj.is_groupby():
            new_candidates = {}
            groups = rel_obj.relop_args()
            for g in groups:
                assert_type(g, BaseAttr)
                group_name = g.name()
                if group_name in source_candidates:
                    new_candidates[group_name] = source_candidates[group_name]
            log(f"Found base->summary: {new_candidates}", "debug")
            return new_candidates

        elif rel_obj.is_orderby():
            log(f"Found base->summary: {source_candidates}", "debug")
            return source_candidates

        elif rel_obj.is_limit():
            log(f"Found base->summary: None", "debug")
            return {}

        else:
            raise ValueError(rel_obj)
