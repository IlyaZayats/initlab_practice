import json
import time
import bisect
from .utils import logger
import numpy as np


ts = int(time.time())
logger = logger.get_logger('autotune', 'log/tune_database_{}.log'.format(ts))
INTERNAL_METRICS_LEN = 51

KNOBS = [
         # 'sync_binlog',
         # 'innodb_flush_log_at_trx_commit',
         # 'innodb_max_dirty_pages_pct',
         # 'innodb_io_capacity_max',
         # 'innodb_io_capacity',
         # 'innodb_max_dirty_pages_pct_lwm',
         # 'innodb_thread_concurrency',
         # 'innodb_lock_wait_timeout',
         # 'innodb_lru_scan_depth',
         #
         #'table_open_cache',
         #'innodb_buffer_pool_size',
         #'innodb_buffer_pool_instances',
         #'innodb_purge_threads',
         #'innodb_read_io_threads',
         #'innodb_write_io_threads',
         #'innodb_read_ahead_threshold',
         #'innodb_sync_array_size',
         #'innodb_sync_spin_loops',
         #'innodb_thread_concurrency',
         #'metadata_locks_hash_instances',
         #'innodb_adaptive_hash_index',
         #'tmp_table_size',
         #'innodb_random_read_ahead',
         #'table_open_cache_instances',
         #'thread_cache_size',
         #'innodb_io_capacity',
         #'innodb_lru_scan_depth',
         #'innodb_spin_wait_delay',
         #'innodb_adaptive_hash_index_parts',
         #'innodb_page_cleaners',
         #'innodb_flush_neighbors'
         #
         #'innodb_max_dirty_pages_pct',
         #'innodb_io_capacity_max',
         #'innodb_io_capacity',
         #'innodb_max_dirty_pages_pct_lwm',
         #'innodb_thread_concurrency',
         #'innodb_lock_wait_timeout',
         #'innodb_lru_scan_depth',
         #'innodb_buffer_pool_size',
         #'innodb_purge_threads',
         #'innodb_read_io_threads',
         #'innodb_write_io_threads',
         #'innodb_spin_wait_delay'
         #
         'innodb_max_dirty_pages_pct',
         #'innodb_io_capacity_max',
         'innodb_io_capacity',
         'innodb_max_dirty_pages_pct_lwm',
         'innodb_thread_concurrency',
         'innodb_lock_wait_timeout',
         'innodb_lru_scan_depth',
         #'innodb_buffer_pool_size',
         'innodb_buffer_pool_instances',
         'innodb_purge_threads',
         'innodb_read_io_threads',
         'innodb_write_io_threads',
         'innodb_spin_wait_delay',
         'table_open_cache',
         'binlog_cache_size',
         #'max_binlog_cache_size',
         'innodb_adaptive_max_sleep_delay',
         'innodb_change_buffer_max_size',
         'innodb_flush_log_at_timeout',
         'innodb_flushing_avg_loops',
         'innodb_max_purge_lag',
         'innodb_read_ahead_threshold',
         'innodb_sync_array_size',
         'innodb_sync_spin_loops',
         'metadata_locks_hash_instances',
         #'innodb_adaptive_hash_index',
         'tmp_table_size',
         #'innodb_random_read_ahead',
         'table_open_cache_instances',
         'thread_cache_size',
         'innodb_adaptive_hash_index_parts',
         'innodb_page_cleaners',
         'innodb_flush_neighbors',
]
KNOB_DETAILS = None
EXTENDED_KNOBS = None
num_knobs = len(KNOBS)


# Deprecated function
def init_knobs(num_total_knobs):
    global memory_size
    global disk_size
    global KNOB_DETAILS
    global EXTENDED_KNOBS

    # TODO: Test the request

    memory_size = 32 * 1024 * 1024 * 1024
    disk_size = 300 * 1024 * 1024 * 1024

    KNOB_DETAILS = {
        # 'sync_binlog': ['integer', [1, 1000, 1]],
        # 'innodb_flush_log_at_trx_commit': ['integer', [0, 2, 1]],
        # 'innodb_max_dirty_pages_pct': ['integer', [0, 99, 75]],
        # 'innodb_io_capacity_max': ['integer', [2000, 100000, 100000]],
        # 'innodb_io_capacity': ['integer', [100, 2000000, 20000]],
        # 'innodb_max_dirty_pages_pct_lwm': ['integer', [0, 99, 10]],
        # 'innodb_thread_concurrency': ['integer', [0, 10000, 0]],
        # 'innodb_lock_wait_timeout': ['integer', [1, 1073741824, 50]],
        # 'innodb_lru_scan_depth': ['integer', [100, 10240, 1024]],
        #
        #'table_open_cache': ['integer', [1, 10240, 512]],
        #'innodb_buffer_pool_size': ['integer', [1024 * 1024 * 1024, memory_size, 24 * 1024* 1024 * 1024]],
        #'innodb_buffer_pool_instances': ['integer', [1, 64, 8]],
        #'innodb_purge_threads': ['integer', [1, 32, 1]],
        #'innodb_read_io_threads': ['integer', [1, 64, 1]],
        #'innodb_write_io_threads': ['integer', [1, 64, 1]],
        #'innodb_read_ahead_threshold': ['integer', [0, 64, 56]],
        #'innodb_sync_array_size': ['integer', [1, 1024, 1]],
        #'innodb_sync_spin_loops': ['integer', [0, 100, 30]],
        #'innodb_thread_concurrency': ['integer', [0, 100, 16]],
        #'metadata_locks_hash_instances': ['integer', [1, 1024, 8]],
        #'innodb_adaptive_hash_index': ['boolean', ['ON', 'OFF']],
        #'tmp_table_size': ['integer', [1024, 1073741824, 1073741824]],
        #'innodb_random_read_ahead': ['boolean', ['ON', 'OFF']],
        #'table_open_cache_instances': ['integer', [1, 64, 16]],
        #'thread_cache_size': ['integer', [0, 1000, 512]],
        #'innodb_io_capacity': ['integer', [100, 2000000, 20000]],
        #'innodb_lru_scan_depth': ['integer', [100, 10240, 1024]],
        #'innodb_spin_wait_delay': ['integer', [0, 60, 6]],
        #'innodb_adaptive_hash_index_parts': ['integer', [1, 512, 8]],
        #'innodb_page_cleaners': ['integer', [1, 64, 4]],
        #'innodb_flush_neighbors': ['enum', [0, 2, 1]],
        #
        #'innodb_max_dirty_pages_pct': ['integer', [0, 99, 75]],
        #'innodb_io_capacity_max': ['integer', [2000, 100000, 100000]],
        #'innodb_io_capacity': ['integer', [100, 20000, 2000]],
        #'innodb_max_dirty_pages_pct_lwm': ['integer', [0, 99, 10]],
        #'innodb_thread_concurrency': ['integer', [0, 10000, 32]],
        #'innodb_lock_wait_timeout': ['integer', [1, 1073741824, 50]],
        #'innodb_lru_scan_depth': ['integer', [100, 10240, 1024]],
        #'innodb_buffer_pool_size': ['integer', [1024 * 1024 * 1024, memory_size, 8 * 1024* 1024 * 1024]],
        #'innodb_purge_threads': ['integer', [1, 32, 1]],
        #'innodb_read_io_threads': ['integer', [1, 64, 1]],
        #'innodb_write_io_threads': ['integer', [1, 64, 1]],
        #'innodb_spin_wait_delay': ['integer', [0, 60, 6]],
        #
        'innodb_max_dirty_pages_pct': ['integer', [0, 99, 75]],
        'innodb_io_capacity': ['integer', [100, 20000, 2000]],
        'innodb_max_dirty_pages_pct_lwm': ['integer', [0, 99, 10]],
        'innodb_thread_concurrency': ['integer', [0, 10000, 32]],
        'innodb_lock_wait_timeout': ['integer', [1, 1073741824, 50]],
        'innodb_lru_scan_depth': ['integer', [100, 10240, 1024]],
        #'innodb_buffer_pool_size': ['integer', [1024 * 1024 * 1024, memory_size, 8 * 1024* 1024 * 1024]],
        'innodb_buffer_pool_instances': ['integer', [1, 16, 8]],
        'innodb_purge_threads': ['integer', [1, 32, 1]],
        'innodb_read_io_threads': ['integer', [1, 64, 1]],
        'innodb_write_io_threads': ['integer', [1, 64, 1]],
        'innodb_spin_wait_delay': ['integer', [0, 60, 6]],
        'table_open_cache': ['integer', [1, 10240, 512]],
        'binlog_cache_size': ['integer', [4096, 4294967295, 32768]],
        'innodb_adaptive_max_sleep_delay': ['integer', [0, 10000000, 150000]],
        'innodb_change_buffer_max_size': ['integer', [0, 50, 25]],
        'innodb_flush_log_at_timeout': ['integer', [1, 2700, 1]],
        'innodb_flushing_avg_loops': ['integer', [1, 1000, 30]],
        'innodb_max_purge_lag': ['integer', [0, 4294967295, 0]],
        'innodb_read_ahead_threshold': ['integer', [0, 64, 56]],
        'innodb_sync_array_size': ['integer', [1, 1024, 1]],
        'innodb_sync_spin_loops': ['integer', [0, 100, 30]],
        'metadata_locks_hash_instances': ['integer', [1, 1024, 8]],
        #'innodb_adaptive_hash_index': ['boolean', ['ON', 'OFF']],
        'tmp_table_size': ['integer', [1024, 1073741824, 1073741824]],
        #'innodb_random_read_ahead': ['boolean', ['ON', 'OFF']],
        'table_open_cache_instances': ['integer', [1, 64, 16]],
        'thread_cache_size': ['integer', [0, 1000, 512]],
        'innodb_adaptive_hash_index_parts': ['integer', [1, 512, 8]],
        'innodb_page_cleaners': ['integer', [1, 64, 4]],
        'innodb_flush_neighbors': ['enum', [0, 2, 1]],
    }
    # TODO: ADD Knobs HERE! Format is the same as the KNOB_DETAILS
    UNKNOWN = 0
    EXTENDED_KNOBS = {
        ##'thread_stack' : ['integer', [131072, memory_size, 524288]],
        #'back_log' : ['integer', [1, 65535, 900]],
    }
    # ADD Other Knobs, NOT Random Selected
    i = 0
    EXTENDED_KNOBS = dict(sorted(EXTENDED_KNOBS.items(), key=lambda d: d[0]))
    for k, v in EXTENDED_KNOBS.items():
        if i < num_total_knobs - num_knobs:
            KNOB_DETAILS[k] = v
            KNOBS.append(k)
            i += 1
        else:
            break


def gen_continuous(action):
    knobs = {}
    for idx in range(len(KNOBS)):
        name = KNOBS[idx]
        value = KNOB_DETAILS[name]
        knob_type = value['type']
        if knob_type == 'integer':
            min_val, max_val = value['min'], value['max']
            delta = int((max_val - min_val) * action[idx])
            eval_value = min_val + delta
            eval_value = max(eval_value, min_val)
            if value.get('stride'):
                all_vals = np.arange(min_val, max_val, value['stride'])
                indx = bisect.bisect_left(all_vals, eval_value)
                if indx == len(all_vals): indx -= 1
                eval_value = all_vals[indx]
            knobs[name] = eval_value
        if knob_type == 'float':
            min_val, max_val = value['min'], value['max']
            delta = (max_val - min_val) * action[idx]
            eval_value = min_val + delta
            eval_value = max(eval_value, min_val)
            all_vals = np.arange(min_val, max_val, value['stride'])
            indx = bisect.bisect_left(all_vals, eval_value)
            if indx == len(all_vals): indx -= 1
            eval_value = all_vals[indx]
            knobs[name] = eval_value
        elif knob_type == 'enum':
            enum_size = len(value['enum_values'])
            enum_index = int(enum_size * action[idx])
            enum_index = min(enum_size - 1, enum_index)
            eval_value = value['enum_values'][enum_index]
            knobs[name] = eval_value
        elif knob_type == 'combination':
            enum_size = len(value['combination_values'])
            enum_index = int(enum_size * action[idx])
            enum_index = min(enum_size - 1, enum_index)
            eval_value = value['combination_values'][enum_index]
            knobs_names = name.strip().split('|')
            knobs_value = eval_value.strip().split('|')
            for k, knob_name_tmp in enumerate(knobs_names):
                knobs[knob_name_tmp] = knobs_value[k]
    return knobs


def save_knobs(knobs, external_metrics):
    knob_json = json.dumps(knobs)
    result_str = '{},{},{},'.format(external_metrics[0], external_metrics[1], external_metrics[2])
    result_str += knob_json


def initialize_knobs(knobs_config, num, keys=[]):
    global KNOBS
    global KNOB_DETAILS
    if num == -1:
        f = open(knobs_config)
        KNOB_DETAILS = json.load(f)
        KNOBS = list(KNOB_DETAILS.keys())
        f.close()
    else:
        f = open(knobs_config)
        knob_tmp = json.load(f)
        i = 0
        KNOB_DETAILS = {}
        while i < num:
            key = list(knob_tmp.keys())[i]
            if (len(keys)>0 and key in keys) or (len(keys) == 0):
                KNOB_DETAILS[key] = knob_tmp[key]
            i = i + 1
        KNOBS = list(KNOB_DETAILS.keys())
        f.close()
        if len(KNOB_DETAILS.keys()) < num:
            for k in keys:
                if k not in KNOB_DETAILS.keys():
                    KNOB_DETAILS[k] = knob_tmp[key]
    return KNOB_DETAILS


def get_default_knobs():
    default_knobs = {}
    for name, value in KNOB_DETAILS.items():
        if not value['type'] == "combination":
            default_knobs[name] = value['default']
        else:
            knobL = name.strip().split('|')
            valueL = value['default'].strip().split('|')
            for i in range(0, len(knobL)):
                default_knobs[knobL[i]] = int(valueL[i])
    return default_knobs
