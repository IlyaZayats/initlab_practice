import tensorflow as tf
from tensorflow import keras
import numpy as np
import math
from environment import MySQLEnv


knobs_default = {
    'innodb_adaptive_flushing_lwm': 10,  # 0-10-70
    #'innodb_adaptive_hash_index': True,  # OFF-ON-ON
    'innodb_adaptive_max_sleep_delay': 150000,  # 0-150000-1000000
    'innodb_buffer_pool_instances': 8,  # 1-8-64
    'innodb_buffer_pool_size': 134217728,  # 5242880-134217728-2**64-1
    #'innodb_change_buffering': 'all',  # (none-inserts-deletes-changes-purges-all)-all
    'innodb_io_capacity': 200,  # 100-200-2**64-1
    'innodb_log_file_size': 50331648,  # 4194304-50331648-(512GB/innodb_log_files_in_group)
    'innodb_max_dirty_pages_pct': 75,  # 0-75-99.99
    'innodb_max_dirty_pages_pct_lwm': 0,  # 0-0-99.99
    'innodb_sync_array_size': 1,  # 1-1-1024
    'innodb_thread_concurrency': 0,  # 0-0-1000
    'max_heap_table_size': 16777216,  # 16384-16777216-18446744073709550592
    'thread_cache_size': 0,  # 0-(-1)-16384
    'tmp_table_size': 16777216,  # 1024-16777216-18446744073709551615
    'binlog_cache_size': 32768,  # 4096-32768-18446744073709547520
    'binlog_max_flush_queue_time': 0,  # 0-0-100000
    'binlog_stmt_cache_size': 32768,  # 4096-32768-18446744073709547520
    'eq_range_index_dive_limit': 200,  # 0-200-4294967295
    'host_cache_size': 0,  # 0-(-1)-65536
    #'innodb_adaptive_flushing': True,  # OFF-ON-ON
    'innodb_autoextend_increment': 64,  # 1-64-1000
    #'innodb_buffer_pool_dump_now': False,  # OFF-OFF-ON
    #'innodb_buffer_pool_load_at_startup': True,  # OFF-ON-ON
    #'innodb_buffer_pool_load_now': False,  # OFF-OFF-ON
    'innodb_change_buffer_max_size': 25,  # 0-25-50
    'innodb_commit_concurrency': 0,  # 0-0-1000
    'innodb_compression_failure_threshold_pct': 5,  # 0-5-100
    'innodb_compression_level': 6,  # 0-6-9
    'innodb_compression_pad_pct_max': 50,  # 0-50-75
    'innodb_concurrency_tickets': 5000,  # 1-5000-4294967295
    'innodb_flush_log_at_timeout': 1,  # 1-1-2700
    'innodb_flush_neighbors': 1,  # (0-1-2)-1
    'innodb_flushing_avg_loops': 30,  # 1-30-1000
    'innodb_ft_cache_size': 8000000,  # 1600000-8000000-80000000
    'innodb_ft_result_cache_limit': 2000000000,  # 1000000-2000000000-2**32-1
    'innodb_ft_sort_pll_degree': 2,  # 1-2-32
    'innodb_io_capacity_max': 100,  # 100-?-2**32-1
    'innodb_lock_wait_timeout': 50,  # 1-50-1073741824
    'innodb_log_buffer_size': 16777216,  # 1048576-16777216-4294967295
    'innodb_lru_scan_depth': 1024,  # 100-1024-2**64-1
    'innodb_max_purge_lag': 0,  # 0-0-4294967295
    'innodb_max_purge_lag_delay': 0,  # 0-0-10000000
    'innodb_old_blocks_pct': 37,  # 5-37-95
    'innodb_old_blocks_time': 1000,  # 0-1000-2**32-1
    'innodb_online_alter_log_max_size': 134217728,  # 65536-134217728-2**64-1
    #'innodb_page_size': 16384,  # (4096-8192-16384-32768-65536)-16384
    'innodb_purge_batch_size': 300,  # 1-300-5000
    'innodb_purge_threads': 41,  # 1-4-32
    #'innodb_random_read_ahead': False,  # OFF-OFF-ON
    'innodb_read_ahead_threshold': 56,  # 0-56-64
    'innodb_read_io_threads': 4,  # 1-4-64
    'innodb_replication_delay': 0,  # 0-0-4294967295
    'innodb_rollback_segments': 128,  # 1-128-128
    'innodb_sort_buffer_size': 1048576,  # 65536-1048576-67108864
    'innodb_spin_wait_delay': 6,  # 0-6-2**64-1
    'innodb_sync_spin_loops': 30,  # 0-30-4294967295
    'innodb_thread_sleep_delay': 10000,  # 0-10000-1000000
    #'innodb_use_native_aio': True,  # OFF-ON-ON
    'innodb_write_io_threads': 4,  # 1-4-64
    'join_buffer_size': 262144,  # 128-262144-4294967168
    'lock_wait_timeout': 31536000,  # 1-31536000-31536000
    'max_binlog_cache_size': 18446744073709547520,  # 4096-18446744073709547520-18446744073709547520
    'max_binlog_size': 1073741824,  # 4096-1073741824-1073741824
    'max_binlog_stmt_cache_size': 18446744073709547520,  # 4096-18446744073709547520-18446744073709547520
    'max_delayed_threads': 20,  # 0-20-16384
    'max_insert_delayed_threads': 20,  # 0-20-16384
    'max_join_size': 18446744073709551615,  # 1-18446744073709551615-18446744073709551615
    'max_length_for_sort_data': 1024,  # 4-1024-8388608
    'max_seeks_for_key': 4294967295,  # 1-4294967295-4294967295
    'max_sort_length': 1024,  # 4-1024-8388608
    'max_sp_recursion_depth': 0,  # 0-0-255
    'max_write_lock_count': 4294967295,  # 1-4294967295-4294967295
    'metadata_locks_cache_size': 1024,  # 1-1024-1048576
    'optimizer_prune_level': 1,  # 0-1-1
    'optimizer_search_depth': 62,  # 0-62-62
    'preload_buffer_size': 32768,  # 1024-32768-1073741824
    'query_alloc_block_size': 8192,  # 1024-8192-4294966272
    'query_prealloc_size': 8192,  # 8192-8192-18446744073709550592
    'range_alloc_block_size': 4096,  # 4096-4096-18446744073709550592
    'read_buffer_size': 131072,  # 8192-131072-2147479552
    'read_rnd_buffer_size': 262144,  # 1-262144-2147483647
    'slave_checkpoint_group': 512,  # 32-512-524280
    'slave_checkpoint_period': 300,  # 1-300-4294967295
    'slave_parallel_workers': 4,  # 0-4-1024
    'slave_pending_jobs_size_max': 1024*128,  # 1024-128M-16EiB
    'sort_buffer_size': 262144,  # 32768-262144-4294967295
    'stored_program_cache': 256,  # 16-256-524288
    'table_definition_cache': 400,  # 400-(-1)-524288
    'table_open_cache': 2000,  # 1-2000-524288
    'table_open_cache_instances': 16,  # 1-16-64
    'thread_stack': 262144,  # 131072-262144-18446744073709550592
    'transaction_alloc_block_size': 8192,  # 1024-8192-131072
    'transaction_prealloc_size': 8192,  # 1024-8192-131072
    #'innodb_dedicated_server': False,  # OFF-OFF-ON
    'innodb_doublewrite_batch_size': 0,  # 0-0-256
    'innodb_doublewrite_files': 8,  # 2-8-256
    'innodb_doublewrite_pages': 4,  # 4-4-512
    'innodb_log_files_in_group': 2,  # 2-2-100
    'innodb_log_spin_cpu_abs_lwm': 80,  # 0-80-4294967295
    'innodb_log_spin_cpu_pct_hwm': 50,  # 0-50-100
    'innodb_log_wait_for_flush_spin_hwm': 400,  # 0-400-2**64-1
    'max_relay_log_size': 0,  # 0-0-1073741824
    'relay_log_space_limit': 0,  # 0-0-18446744073709551615
    'rpl_read_size': 8192,  # 8192-8192-4294959104
    'stored_program_definition_cache': 256,  # 256-256-524288
    'tablespace_definition_cache': 256,  # 256-256-524288
    'temptable_max_ram':  1073741824# 2097152-1073741824-2^64-1
}

knobs_max = {
    'innodb_adaptive_flushing_lwm': 70,  # 0-10-70
    #'innodb_adaptive_hash_index': True,  # OFF-ON-ON
    'innodb_adaptive_max_sleep_delay': 1000000,  # 0-150000-1000000
    'innodb_buffer_pool_instances': 64,  # 1-8-64
    'innodb_buffer_pool_size': (2**64)-1,  # 5242880-134217728-2**64-1
    #'innodb_change_buffering': 'all',  # (none-inserts-deletes-changes-purges-all)-all
    'innodb_io_capacity': (2**64)-1,  # 100-200-2**64-1
    'innodb_log_file_size': 512*1024*1024,  # 4194304-50331648-(512GB/innodb_log_files_in_group)
    'innodb_max_dirty_pages_pct': 99.99,  # 0-75-99.99
    'innodb_max_dirty_pages_pct_lwm': 99.99,  # 0-0-99.99
    'innodb_sync_array_size': 1024,  # 1-1-1024
    'innodb_thread_concurrency': 1000,  # 0-0-1000
    'max_heap_table_size': 18446744073709550592,  # 16384-16777216-18446744073709550592
    'thread_cache_size': 16384,  # 0-(-1)-16384
    'tmp_table_size': 18446744073709551615,  # 1024-16777216-18446744073709551615
    'binlog_cache_size': 18446744073709547520,  # 4096-32768-18446744073709547520
    'binlog_max_flush_queue_time': 100000,  # 0-0-100000
    'binlog_stmt_cache_size': 18446744073709547520,  # 4096-32768-18446744073709547520
    'eq_range_index_dive_limit': 4294967295,  # 0-200-4294967295
    'host_cache_size': 65536,  # 0-(-1)-65536
    #'innodb_adaptive_flushing': True,  # OFF-ON-ON
    'innodb_autoextend_increment': 1000,  # 1-64-1000
    #'innodb_buffer_pool_dump_now': True,  # OFF-OFF-ON
    #'innodb_buffer_pool_load_at_startup': True,  # OFF-ON-ON
    #'innodb_buffer_pool_load_now': True,  # OFF-OFF-ON
    'innodb_change_buffer_max_size': 50,  # 0-25-50
    'innodb_commit_concurrency': 1000,  # 0-0-1000
    'innodb_compression_failure_threshold_pct': 100,  # 0-5-100
    'innodb_compression_level': 9,  # 0-6-9
    'innodb_compression_pad_pct_max': 75,  # 0-50-75
    'innodb_concurrency_tickets': 4294967295,  # 1-5000-4294967295
    'innodb_flush_log_at_timeout': 2700,  # 1-1-2700
    'innodb_flush_neighbors': 2,  # (0-1-2)-1
    'innodb_flushing_avg_loops': 1000,  # 1-30-1000
    'innodb_ft_cache_size': 80000000,  # 1600000-8000000-80000000
    'innodb_ft_result_cache_limit': (2**32)-1,  # 1000000-2000000000-2**32-1
    'innodb_ft_sort_pll_degree': 32,  # 1-2-32
    'innodb_io_capacity_max': (2**32)-1,  # 100-?-2**32-1
    'innodb_lock_wait_timeout': 1073741824,  # 1-50-1073741824
    'innodb_log_buffer_size': 4294967295,  # 1048576-16777216-4294967295
    'innodb_lru_scan_depth': (2**64)-1,  # 100-1024-2**64-1
    'innodb_max_purge_lag': 4294967295,  # 0-0-4294967295
    'innodb_max_purge_lag_delay': 10000000,  # 0-0-10000000
    'innodb_old_blocks_pct': 95,  # 5-37-95
    'innodb_old_blocks_time': (2**32)-1,  # 0-1000-2**32-1
    'innodb_online_alter_log_max_size': (2**64)-1,  # 65536-134217728-2**64-1
    #'innodb_page_size': 65536,  # (4096-8192-16384-32768-65536)-16384
    'innodb_purge_batch_size': 5000,  # 1-300-5000
    'innodb_purge_threads': 32,  # 1-4-32
    #'innodb_random_read_ahead': True,  # OFF-OFF-ON
    'innodb_read_ahead_threshold': 64,  # 0-56-64
    'innodb_read_io_threads': 64,  # 1-4-64
    'innodb_replication_delay': 4294967295,  # 0-0-4294967295
    'innodb_rollback_segments': 128,  # 1-128-128
    'innodb_sort_buffer_size': 67108864,  # 65536-1048576-67108864
    'innodb_spin_wait_delay': (2**64)-1,  # 0-6-2**64-1
    'innodb_sync_spin_loops': 4294967295,  # 0-30-4294967295
    'innodb_thread_sleep_delay': 1000000,  # 0-10000-1000000
    #'innodb_use_native_aio': True,  # OFF-ON-ON
    'innodb_write_io_threads': 64,  # 1-4-64
    'join_buffer_size': 4294967168,  # 128-262144-4294967168
    'lock_wait_timeout': 31536000,  # 1-31536000-31536000
    'max_binlog_cache_size': 18446744073709547520,  # 4096-18446744073709547520-18446744073709547520
    'max_binlog_size': 1073741824,  # 4096-1073741824-1073741824
    'max_binlog_stmt_cache_size': 18446744073709547520,  # 4096-18446744073709547520-18446744073709547520
    'max_delayed_threads': 16384,  # 0-20-16384
    'max_insert_delayed_threads': 16384,  # 0-20-16384
    'max_join_size': 18446744073709551615,  # 1-18446744073709551615-18446744073709551615
    'max_length_for_sort_data': 8388608,  # 4-1024-8388608
    'max_seeks_for_key': 4294967295,  # 1-4294967295-4294967295
    'max_sort_length': 8388608,  # 4-1024-8388608
    'max_sp_recursion_depth': 255,  # 0-0-255
    'max_write_lock_count': 4294967295,  # 1-4294967295-4294967295
    'metadata_locks_cache_size': 1048576,  # 1-1024-1048576
    'optimizer_prune_level': 1,  # 0-1-1
    'optimizer_search_depth': 62,  # 0-62-62
    'preload_buffer_size': 1024,  # 1024-32768-1073741824
    'query_alloc_block_size': 1073741824,  # 1024-8192-4294966272
    'query_prealloc_size': 18446744073709550592,  # 8192-8192-18446744073709550592
    'range_alloc_block_size': 18446744073709550592,  # 4096-4096-18446744073709550592
    'read_buffer_size': 2147479552,  # 8192-131072-2147479552
    'read_rnd_buffer_size': 2147483647,  # 1-262144-2147483647
    'slave_checkpoint_group': 524280,  # 32-512-524280
    'slave_checkpoint_period': 4294967295,  # 1-300-4294967295
    'slave_parallel_workers': 1024,  # 0-4-1024
    'slave_pending_jobs_size_max': 1024**6,  # 1024-128M-16EiB
    'sort_buffer_size': 4294967295,  # 32768-262144-4294967295
    'stored_program_cache': 524288,  # 16-256-524288
    'table_definition_cache': 524288,  # 400-(-1)-524288
    'table_open_cache': 524288,  # 1-2000-524288
    'table_open_cache_instances': 64,  # 1-16-64
    'thread_stack': 18446744073709550592,  # 131072-262144-18446744073709550592
    'transaction_alloc_block_size': 131072,  # 1024-8192-131072
    'transaction_prealloc_size': 131072,  # 1024-8192-131072
    #'innodb_dedicated_server': True,  # OFF-OFF-ON
    'innodb_doublewrite_batch_size': 256,  # 0-0-256
    'innodb_doublewrite_files': 256,  # 2-8-256
    'innodb_doublewrite_pages': 512,  # 4-4-512
    'innodb_log_files_in_group': 100,  # 2-2-100
    'innodb_log_spin_cpu_abs_lwm': 4294967295,  # 0-80-4294967295
    'innodb_log_spin_cpu_pct_hwm': 100,  # 0-50-100
    'innodb_log_wait_for_flush_spin_hwm': (2**64)-1,  # 0-400-2**64-1
    'max_relay_log_size': 1073741824,  # 0-0-1073741824
    'relay_log_space_limit': 18446744073709551615,  # 0-0-18446744073709551615
    'rpl_read_size': 4294959104,  # 8192-8192-4294959104
    'stored_program_definition_cache': 524288,  # 256-256-524288
    'tablespace_definition_cache': 524288,  # 256-256-524288
    'temptable_max_ram':  (2**64)-1# 2097152-1073741824-2^64-1
}

knobs_min = {
    'innodb_adaptive_flushing_lwm': 0,  # 0-10-70
    #'innodb_adaptive_hash_index': True,  # OFF-ON-ON
    'innodb_adaptive_max_sleep_delay': 0,  # 0-150000-1000000
    'innodb_buffer_pool_instances': 1,  # 1-8-64
    'innodb_buffer_pool_size': 5242880,  # 5242880-134217728-2**64-1
    #'innodb_change_buffering': 'all',  # (none-inserts-deletes-changes-purges-all)-all
    'innodb_io_capacity': 100,  # 100-200-2**64-1
    'innodb_log_file_size': 4194304,  # 4194304-50331648-(512GB/innodb_log_files_in_group)
    'innodb_max_dirty_pages_pct': 0,  # 0-75-99.99
    'innodb_max_dirty_pages_pct_lwm': 0,  # 0-0-99.99
    'innodb_sync_array_size': 1,  # 1-1-1024
    'innodb_thread_concurrency': 0,  # 0-0-1000
    'max_heap_table_size': 16384,  # 16384-16777216-18446744073709550592
    'thread_cache_size': 0,  # 0-(-1)-16384
    'tmp_table_size': 1024,  # 1024-16777216-18446744073709551615
    'binlog_cache_size': 4096,  # 4096-32768-18446744073709547520
    'binlog_max_flush_queue_time': 0,  # 0-0-100000
    'binlog_stmt_cache_size': 4096,  # 4096-32768-18446744073709547520
    'eq_range_index_dive_limit': 0,  # 0-200-4294967295
    'host_cache_size': 0,  # 0-(-1)-65536
    #'innodb_adaptive_flushing': False,  # OFF-ON-ON
    'innodb_autoextend_increment': 1,  # 1-64-1000
    #'innodb_buffer_pool_dump_now': False,  # OFF-OFF-ON
    #'innodb_buffer_pool_load_at_startup': False,  # OFF-ON-ON
    #'innodb_buffer_pool_load_now': False,  # OFF-OFF-ON
    'innodb_change_buffer_max_size': 0,  # 0-25-50
    'innodb_commit_concurrency': 0,  # 0-0-1000
    'innodb_compression_failure_threshold_pct': 0,  # 0-5-100
    'innodb_compression_level': 0,  # 0-6-9
    'innodb_compression_pad_pct_max': 0,  # 0-50-75
    'innodb_concurrency_tickets': 1,  # 1-5000-4294967295
    'innodb_flush_log_at_timeout': 1,  # 1-1-2700
    'innodb_flush_neighbors': 0,  # (0-1-2)-1
    'innodb_flushing_avg_loops': 1,  # 1-30-1000
    'innodb_ft_cache_size': 1600000,  # 1600000-8000000-80000000
    'innodb_ft_result_cache_limit': 1000000,  # 1000000-2000000000-2**32-1
    'innodb_ft_sort_pll_degree': 1,  # 1-2-32
    'innodb_io_capacity_max': 100,  # 100-?-2**32-1
    'innodb_lock_wait_timeout': 1,  # 1-50-1073741824
    'innodb_log_buffer_size': 1048576,  # 1048576-16777216-4294967295
    'innodb_lru_scan_depth': 100,  # 100-1024-2**64-1
    'innodb_max_purge_lag': 0,  # 0-0-4294967295
    'innodb_max_purge_lag_delay': 0,  # 0-0-10000000
    'innodb_old_blocks_pct': 5,  # 5-37-95
    'innodb_old_blocks_time': 0,  # 0-1000-2**32-1
    'innodb_online_alter_log_max_size': 65536,  # 65536-134217728-2**64-1
    #'innodb_page_size': 4096,  # (4096-8192-16384-32768-65536)-16384
    'innodb_purge_batch_size': 1,  # 1-300-5000
    'innodb_purge_threads': 1,  # 1-4-32
    #'innodb_random_read_ahead': False,  # OFF-OFF-ON
    'innodb_read_ahead_threshold': 0,  # 0-56-64
    'innodb_read_io_threads': 1,  # 1-4-64
    'innodb_replication_delay': 0,  # 0-0-4294967295
    'innodb_rollback_segments': 1,  # 1-128-128
    'innodb_sort_buffer_size': 65536,  # 65536-1048576-67108864
    'innodb_spin_wait_delay': 0,  # 0-6-2**64-1
    'innodb_sync_spin_loops': 0,  # 0-30-4294967295
    'innodb_thread_sleep_delay': 0,  # 0-10000-1000000
    #'innodb_use_native_aio': False,  # OFF-ON-ON
    'innodb_write_io_threads': 1,  # 1-4-64
    'join_buffer_size': 128,  # 128-262144-4294967168
    'lock_wait_timeout': 1,  # 1-31536000-31536000
    'max_binlog_cache_size': 4096,  # 4096-18446744073709547520-18446744073709547520
    'max_binlog_size': 4096,  # 4096-1073741824-1073741824
    'max_binlog_stmt_cache_size': 4096,  # 4096-18446744073709547520-18446744073709547520
    'max_delayed_threads': 0,  # 0-20-16384
    'max_insert_delayed_threads': 0,  # 0-20-16384
    'max_join_size': 1,  # 1-18446744073709551615-18446744073709551615
    'max_length_for_sort_data': 4,  # 4-1024-8388608
    'max_seeks_for_key': 1,  # 1-4294967295-4294967295
    'max_sort_length': 4,  # 4-1024-8388608
    'max_sp_recursion_depth': 0,  # 0-0-255
    'max_write_lock_count': 1,  # 1-4294967295-4294967295
    'metadata_locks_cache_size': 1,  # 1-1024-1048576
    'optimizer_prune_level': 0,  # 0-1-1
    'optimizer_search_depth': 0,  # 0-62-62
    'preload_buffer_size': 1024,  # 1024-32768-1073741824
    'query_alloc_block_size': 1024,  # 1024-8192-4294966272
    'query_prealloc_size': 8192,  # 8192-8192-18446744073709550592
    'range_alloc_block_size': 4096,  # 4096-4096-18446744073709550592
    'read_buffer_size': 8192,  # 8192-131072-2147479552
    'read_rnd_buffer_size': 1,  # 1-262144-2147483647
    'slave_checkpoint_group': 32,  # 32-512-524280
    'slave_checkpoint_period': 1,  # 1-300-4294967295
    'slave_parallel_workers': 0,  # 0-4-1024
    'slave_pending_jobs_size_max': 1024,  # 1024-128M-16EiB
    'sort_buffer_size': 32768,  # 32768-262144-4294967295
    'stored_program_cache': 16,  # 16-256-524288
    'table_definition_cache': 400,  # 400-(-1)-524288
    'table_open_cache': 1,  # 1-2000-524288
    'table_open_cache_instances': 1,  # 1-16-64
    'thread_stack': 131072,  # 131072-262144-18446744073709550592
    'transaction_alloc_block_size': 1024,  # 1024-8192-131072
    'transaction_prealloc_size': 1024,  # 1024-8192-131072
    #'innodb_dedicated_server': False,  # OFF-OFF-ON
    'innodb_doublewrite_batch_size': 0,  # 0-0-256
    'innodb_doublewrite_files': 2,  # 2-8-256
    'innodb_doublewrite_pages': 4,  # 4-4-512
    'innodb_log_files_in_group': 2,  # 2-2-100
    'innodb_log_spin_cpu_abs_lwm': 0,  # 0-80-4294967295
    'innodb_log_spin_cpu_pct_hwm': 0,  # 0-50-100
    'innodb_log_wait_for_flush_spin_hwm': 0,  # 0-400-2**64-1
    'max_relay_log_size': 0,  # 0-0-1073741824
    'relay_log_space_limit': 0,  # 0-0-18446744073709551615
    'rpl_read_size': 8192,  # 8192-8192-4294959104
    'stored_program_definition_cache': 256,  # 256-256-524288
    'tablespace_definition_cache': 256,  # 256-256-524288
    'temptable_max_ram':  2097152# 2097152-1073741824-2^64-1
}

#Сделай
knobs_min_list = list(knobs_min.values())
knobs_max_list = list(knobs_max.values())
knobs_default_list = list(knobs_default.values())

env = MySQLEnv(list(knobs_default.keys()), knobs_default_list)
env.init()

class ParameterNoise(keras.layers.Layer):
    def __init__(self, units):
        super(ParameterNoise, self).__init__()
        self.units = units
        self.sigma_init_value = 0.05

    def build(self, input_shape):
        w_init = tf.random_uniform_initializer(-math.sqrt(3 / self.units), math.sqrt(3 / self.units))
        self.w = tf.Variable(initial_value=w_init(shape=(input_shape[-1], self.units)), trainable=True)
        b_init = tf.random_uniform_initializer(-math.sqrt(3 / self.units), math.sqrt(3 / self.units))
        self.b = tf.Variable(initial_value=b_init(shape=(self.units,)), trainable=True)

        sigma_init = tf.keras.initializers.Constant(value=self.sigma_init_value)
        self.sigma_w = tf.Variable(initial_value=sigma_init(shape=(input_shape[-1], self.units)), trainable=True)
        self.sigma_b = tf.Variable(initial_value=sigma_init(shape=(self.units,)), trainable=True)
        self.epsilon_w = tf.Variable(initial_value=tf.zeros((input_shape[-1], self.units)), trainable=False)
        self.epsilon_b = tf.Variable(initial_value=tf.zeros((self.units,)), trainable=False)

    def call(self, inputs):
        return tf.matmul(inputs, self.w+self.sigma_w*self.epsilon_w) + (self.b+self.sigma_b*self.epsilon_b)

    def sample_noise(self):
        self.epsilon_w = tf.random.uniform(shape=(self.units[-1], self.units))
        self.epsilon_b = tf.random.uniform(shape=(self.units,))

class OUActionNoise:
    def __init__(self, mean, std_deviation, theta=0.15, dt=1e-2, x_initial=None):
        self.theta = theta
        self.mean = mean
        self.std_dev = std_deviation
        self.dt = dt
        self.x_initial = x_initial
        self.reset()

    def __call__(self):
        x = (
            self.x_prev
            + self.theta * (self.mean - self.x_prev) * self.dt
            + self.std_dev * np.sqrt(self.dt) * np.random.normal(size=self.mean.shape)
        )
        self.x_prev = x
        return x

    def reset(self):
        if self.x_initial is not None:
            self.x_prev = self.x_initial
        else:
            self.x_prev = np.zeros_like(self.mean)


states_amount = 74
knobs_amount = 98
class Buffer:
    def __init__(self, capacity=100000, batch_size=16):
        self.capacity = capacity
        self.batch_size = batch_size
        self.counter = 0
        self.state_buffer = np.zeros((self.capacity, states_amount))
        self.action_buffer = np.zeros((self.capacity, knobs_amount))
        self.reward_buffer = np.zeros((self.capacity, 1))
        self.next_state_buffer = np.zeros((self.capacity, states_amount))

    def record(self, observation):
        index = self.counter % self.capacity
        self.state_buffer[index] = observation[0]
        self.action_buffer[index] = observation[1]
        self.reward_buffer[index] = observation[2]
        self.next_state_buffer[index] = observation[3]
        self.counter += 1

    @tf.function
    def update(self, state_batch, action_batch, reward_batch, next_state_batch,):
        with tf.GradientTape() as tape:
            target_actions = target_actor(next_state_batch, training=True)
            y = reward_batch + gamma * target_critic([next_state_batch, target_actions], training=True)
            critic_value = critic_model([state_batch, action_batch], training=True)
            critic_loss = tf.math.reduce_mean(tf.math.square(y - critic_value))

        critic_grad = tape.gradient(critic_loss, critic_model.trainable_variables)
        critic_optimizer.apply_gradients(zip(critic_grad, critic_model.trainable_variables))

        with tf.GradientTape() as tape:
            actions = actor_model(state_batch, training=True)
            critic_value = critic_model([state_batch, actions], training=True)

            actor_loss = -tf.math.reduce_mean(critic_value)

        actor_grad = tape.gradient(actor_loss, actor_model.trainable_variables)
        actor_optimizer.apply_gradients(zip(actor_grad, actor_model.trainable_variables))


    def learn(self):

        record_range = min(self.counter, self.capacity)

        batch_indices = np.random.choice(record_range, self.batch_size)

        state_batch = tf.convert_to_tensor(self.state_buffer[batch_indices])
        action_batch = tf.convert_to_tensor(self.action_buffer[batch_indices])
        reward_batch = tf.convert_to_tensor(self.reward_buffer[batch_indices])
        reward_batch = tf.cast(reward_batch, dtype=tf.float32)
        next_state_batch = tf.convert_to_tensor(self.next_state_buffer[batch_indices])

        self.update(state_batch, action_batch, reward_batch, next_state_batch)


@tf.function
def update_target(target_weights, weights, tau):
    for (a, b) in zip(target_weights, weights):
        a.assign(b * tau + a * (1 - tau))
def get_model_actor(n_states, n_actions):
    last_init = tf.random_uniform_initializer(minval=-0.003, maxval=0.003)
    inputs = keras.Input((n_states,))
    x = keras.layers.Dense(units=128)(inputs)
    x = keras.layers.LeakyReLU(alpha=0.2)(x)
    x = keras.layers.BatchNormalization()(x)
    x = keras.layers.Dense(units=128)(x)
    x = keras.activations.tanh(x)
    x = keras.layers.Dropout(rate=0.3)(x)
    x = keras.layers.BatchNormalization()(x)

    x = keras.layers.Dense(units=64)(x)
    x = keras.activations.tanh(x)
    x = keras.layers.Dropout(rate=0.3)(x)
    x = keras.layers.BatchNormalization()(x)
    outputs = keras.layers.Dense(units=n_actions, activation="sigmoid", kernel_initializer=last_init)(x)
    outputs = ParameterNoise(units=n_actions)(outputs)

    return keras.Model(inputs, outputs, name="actor")

def get_model_critic(n_states, n_actions):
    state_input = keras.layers.Input((n_states,))
    x_s = keras.layers.Dense(units=128)(state_input)

    action_input = keras.layers.Input((n_actions,))
    x_a = keras.layers.Dense(units=128)(action_input)

    concat = keras.layers.Concatenate()([x_s, x_a])

    x = keras.layers.Dense(units=256)(concat)
    x = keras.layers.LeakyReLU(alpha=0.2)(x)
    x = keras.layers.Dropout(rate=0.3)(x)
    x = keras.layers.BatchNormalization()(x)
    x = keras.layers.Dense(units=64)(x)
    x = keras.activations.tanh(x)
    x = keras.layers.Dropout(rate=0.3)(x)
    x = keras.layers.BatchNormalization()(x)
    outputs = keras.layers.Dense(units=1, activation="tanh")(x)

    return keras.Model([state_input, action_input], outputs, name="critic")

def policy(state, noise_object):
    sampled_actions = tf.squeeze(actor_model(state))
    for i in range(len(sampled_actions)):
        noise = noise_object()
        sampled_actions[i] = sampled_actions[i].numpy() + noise

    legal_actions = np.clip(sampled_actions, knobs_min_list, knobs_max_list)

    return [np.squeeze(legal_actions)]


std_dev = 0.2
ou_noise = OUActionNoise(mean=np.zeros(1), std_deviation=float(std_dev) * np.ones(1))

actor_model = get_model_actor(states_amount, knobs_amount)
critic_model = get_model_critic(states_amount, knobs_amount)

target_actor = get_model_actor(states_amount, knobs_amount)
target_critic = get_model_critic(states_amount, knobs_amount)

target_actor.set_weights(actor_model.get_weights())
target_critic.set_weights(critic_model.get_weights())

critic_lr = 0.002
actor_lr = 0.001

critic_optimizer = tf.keras.optimizers.Adam(critic_lr)
actor_optimizer = tf.keras.optimizers.Adam(actor_lr)

total_episodes = 100

gamma = 0.99

tau = 0.005

buffer = Buffer(50000, 16)

ep_reward_list = []
avg_reward_list = []

for ep in range(total_episodes):

    prev_state = env.init()
    episodic_reward = 0

    while True:

        tf_prev_state = tf.expand_dims(tf.convert_to_tensor(prev_state), 0)

        actions = policy(tf_prev_state, ou_noise)
        state, reward, done, info = env.step(actions)

        buffer.record((prev_state, actions, reward, state))
        episodic_reward += reward

        buffer.learn()
        update_target(target_actor.variables, actor_model.variables, tau)
        update_target(target_critic.variables, critic_model.variables, tau)

        if done:
            break

        prev_state = state

    ep_reward_list.append(episodic_reward)

    avg_reward = np.mean(ep_reward_list[-40:])
    print("Episode * {} * Avg Reward is ==> {}".format(ep, avg_reward))
    avg_reward_list.append(avg_reward)


