import csv
import json


knobs1 = ['innodb_adaptive_flushing_lwm',#0-10-70
          'innodb_adaptive_hash_index',#OFF-ON-ON
          'innodb_adaptive_max_sleep_delay',#0-150000-1000000
          'innodb_buffer_pool_instances',#1-8-64
          'innodb_buffer_pool_size',#5242880-134217728-2**64-1
          'innodb_change_buffering',#(none-inserts-deletes-changes-purges-all)-all
          'innodb_io_capacity',#100-200-2**64-1
          'innodb_log_file_size',#4194304-50331648-(512GB/innodb_log_files_in_group)
          'innodb_max_dirty_pages_pct',#0-75-99.99
          'innodb_max_dirty_pages_pct_lwm',#0-0-99.99
          'innodb_sync_array_size',#1-1-1024
          'innodb_thread_concurrency',#0-0-1000
          'max_heap_table_size',#16384-16777216-18446744073709550592
          'thread_cache_size',#0-(-1)-16384
          'tmp_table_size'#1024-16777216-18446744073709551615
          ]

knobs2 = ['binlog_cache_size',#4096-32768-18446744073709547520
          'binlog_max_flush_queue_time',#0-0-100000
          'binlog_stmt_cache_size',#4096-32768-18446744073709547520
          'eq_range_index_dive_limit',#0-200-4294967295
          'host_cache_size',#0-(-1)-65536
          'innodb_adaptive_flushing',#OFF-ON-ON
          'innodb_autoextend_increment',#1-64-1000
          'innodb_buffer_pool_dump_now',#OFF-OFF-ON
          'innodb_buffer_pool_load_at_startup',#OFF-ON-ON
          'innodb_buffer_pool_load_now',#OFF-OFF-ON
          'innodb_change_buffer_max_size',#0-25-50
          'innodb_commit_concurrency',#0-0-1000
          'innodb_compression_failure_threshold_pct',#0-5-100
          'innodb_compression_level',#0-6-9
          'innodb_compression_pad_pct_max',#0-50-75
          'innodb_concurrency_tickets',#1-5000-4294967295
          'innodb_flush_log_at_timeout',#1-1-2700
          'innodb_flush_neighbors',#(0-1-2)-1
          'innodb_flushing_avg_loops',#1-30-1000
          'innodb_ft_cache_size',#1600000-8000000-80000000
          'innodb_ft_result_cache_limit',#1000000-2000000000-2**32-1
          'innodb_ft_sort_pll_degree',#1-2-32
          'innodb_io_capacity_max',#100-?-2**32-1
          'innodb_lock_wait_timeout',#1-50-1073741824
          'innodb_log_buffer_size',#1048576-16777216-4294967295
          'innodb_lru_scan_depth',#100-1024-2**64-1
          'innodb_max_purge_lag',#0-0-4294967295
          'innodb_max_purge_lag_delay',#0-0-10000000
          'innodb_old_blocks_pct',#5-37-95
          'innodb_old_blocks_time',#0-1000-2**32-1
          'innodb_online_alter_log_max_size',#65536-134217728-2**64-1
          'innodb_page_size',#(4096-8192-16384-32768-65536)-16384
          'innodb_purge_batch_size',#1-300-5000
          'innodb_purge_threads',#1-4-32
          'innodb_random_read_ahead',#OFF-OFF-ON
          'innodb_read_ahead_threshold',#0-56-64
          'innodb_read_io_threads',#1-4-64
          'innodb_replication_delay',#0-0-4294967295
          'innodb_rollback_segments',#1-128-128
          'innodb_sort_buffer_size',#65536-1048576-67108864
          'innodb_spin_wait_delay',#0-6-2**64-1
          'innodb_sync_spin_loops',#0-30-4294967295
          'innodb_thread_sleep_delay',#0-10000-1000000
          'innodb_use_native_aio',#OFF-ON-ON
          'innodb_write_io_threads',#1-4-64
          'join_buffer_size',#128-262144-4294967168
          'lock_wait_timeout',#1-31536000-31536000
          'max_binlog_cache_size',#4096-18446744073709547520-18446744073709547520
          'max_binlog_size',#4096-1073741824-1073741824
          'max_binlog_stmt_cache_size',#4096-18446744073709547520-18446744073709547520
          'max_delayed_threads',#0-20-16384
          'max_insert_delayed_threads',#0-20-16384
          'max_join_size',#1-18446744073709551615-18446744073709551615
          'max_length_for_sort_data',#4-1024-8388608
          'max_seeks_for_key',#1-4294967295-4294967295
          'max_sort_length',#4-1024-8388608
          'max_sp_recursion_depth',#0-0-255
          #'max_tmp_tables',#
          'max_write_lock_count',#1-4294967295-4294967295
          'metadata_locks_cache_size',#1-1024-1048576
          'optimizer_prune_level',#0-1-1
          'optimizer_search_depth',#0-62-62
          'preload_buffer_size',#1024-32768-1073741824
          'query_alloc_block_size',#1024-8192-4294966272
          #'query_cache_limit',#0-1048576-18446744073709551615//
          #'query_cache_min_res_unit',#512-4096-18446744073709551615//
          #'query_cache_size',#0-1048576-18446744073709551615//
          #'query_cache_type',#(0-1-2)-0//
          #'query_cache_wlock_invalidate',#OFF-OFF-ON//
          'query_prealloc_size',#8192-8192-18446744073709550592
          'range_alloc_block_size',#4096-4096-18446744073709550592
          'read_buffer_size',#8192-131072-2147479552
          'read_rnd_buffer_size',#1-262144-2147483647
          'slave_checkpoint_group',#32-512-524280
          'slave_checkpoint_period',#1-300-4294967295
          'slave_parallel_workers',#0-4-1024
          'slave_pending_jobs_size_max',#1024-128M-16EiB
          'sort_buffer_size',#32768-262144-4294967295
          'stored_program_cache',#16-256-524288
          'table_definition_cache',#400-(-1)-524288
          'table_open_cache',#1-2000-524288
          'table_open_cache_instances',#1-16-64
          'thread_stack',#131072-262144-18446744073709550592
          #'timed_mutexes',#//
          'transaction_alloc_block_size',#1024-8192-131072
          'transaction_prealloc_size',#1024-8192-131072
          'innodb_dedicated_server',#OFF-OFF-ON
          'innodb_doublewrite_batch_size',#0-0-256
          'innodb_doublewrite_files',#2-8-256
          'innodb_doublewrite_pages',#4-4-512
          'innodb_log_files_in_group',#2-2-100
          'innodb_log_spin_cpu_abs_lwm',#0-80-4294967295
          'innodb_log_spin_cpu_pct_hwm',#0-50-100
          'innodb_log_wait_for_flush_spin_hwm',#0-400-2**64-1
          'max_relay_log_size',#0-0-1073741824
          #'open_files_limit',
          #'parser_max_mem_size',
          'relay_log_space_limit',#0-0-18446744073709551615
          'rpl_read_size',#8192-8192-4294959104
          'stored_program_definition_cache',#256-256-524288
          'tablespace_definition_cache',#256-256-524288
          'temptable_max_ram'#2097152-1073741824-2^64-1
          ]


def json_read(s, vector0):
    with open(s, "r", encoding='utf-8') as f:
        data = json.load(f)
    vector1, vector2 = [], []
    for i in knobs1:
        try:
            vector1.append(data["DB"]["Conf"]["Variables"][f'{i}'])
        except KeyError:
            vector1.append('-')
    for i in knobs2:
        try:
            vector2.append(data["DB"]["Conf"]["Variables"][f'{i}'])
        except KeyError:
            vector2.append('-')
    # a = data["DB"]["Conf"]["Variables"]["innodb_adaptive_flushing_lwm"]
    # d1 = {knobs1[i]: vector1[i] for i in range(len(vector1))}
    # d2 = {knobs2[i]: vector2[i] for i in range(len(vector2))}
    mas.append(vector0 + vector1 + vector2)


def csv_read(s):
    with open(s, encoding='utf-8') as f:
        file_reader = csv.reader(f, delimiter=",")
        count = 0
        for row in file_reader:
            if count != 1:
                mysql_version = row[3]
                if mysql_version[:5] == 'MySQL' and mysql_version > 'MySQL 5.6.0':
                    sid = row[2]
                    metric_file = row[4][row[4].rindex('/') - 10:].replace('/', str('\ ')).replace(' ', '')
                    latency = row[-1]
                    path = f'D:\Files\{metric_file}'
                    vector0 = [mysql_version, sid, latency]
                    # print(path)
                    try:
                        json_read(path, vector0)
                    except FileNotFoundError:
                        continue
            count += 1


a1 = ['mysql_version', 'sid', 'latency']
mas = [a1 + knobs1 + knobs2]
for i in range(1, 7):
    try:
        print(f'UsersWithoutServers{i}.csv')
        csv_read(f'UsersWithoutServers{i}.csv')
    except FileNotFoundError:
        continue
with open('state-data.csv', 'a', newline='') as state_file:
    writer = csv.writer(state_file)
    writer.writerows(mas)
