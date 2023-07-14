import csv
import json


knobs1 = ['innodb_adaptive_flushing_lwm',
          'innodb_adaptive_hash_index',
          'innodb_adaptive_max_sleep_delay',
          'innodb_buffer_pool_instances',
          'innodb_buffer_pool_size',
          'innodb_change_buffering',
          'innodb_io_capacity',
          'innodb_log_file_size',
          'innodb_max_dirty_pages_pct',
          'innodb_max_dirty_pages_pct_lwm',
          'innodb_sync_array_size',
          'innodb_thread_concurrency',
          'max_heap_table_size',
          'thread_cache_size',
          'tmp_table_size'
          ]

knobs2 = ['binlog_cache_size',
          'binlog_max_flush_queue_time',
          'binlog_stmt_cache_size',
          'eq_range_index_dive_limit',
          'host_cache_size',
          'innodb_adaptive_flushing',
          'innodb_autoextend_increment',
          'innodb_buffer_pool_dump_now',
          'innodb_buffer_pool_load_at_startup',
          'innodb_buffer_pool_load_now',
          'innodb_change_buffer_max_size',
          'innodb_commit_concurrency',
          'innodb_compression_failure_threshold_pct',
          'innodb_compression_level',
          'innodb_compression_pad_pct_max',
          'innodb_concurrency_tickets',
          'innodb_flush_log_at_timeout',
          'innodb_flush_neighbors',
          'innodb_flushing_avg_loops',
          'innodb_ft_cache_size',
          'innodb_ft_result_cache_limit',
          'innodb_ft_sort_pll_degree',
          'innodb_io_capacity_max',
          'innodb_lock_wait_timeout',
          'innodb_log_buffer_size',
          'innodb_lru_scan_depth',
          'innodb_max_purge_lag',
          'innodb_max_purge_lag_delay',
          'innodb_old_blocks_pct',
          'innodb_old_blocks_time',
          'innodb_online_alter_log_max_size',
          'innodb_page_size',
          'innodb_purge_batch_size',
          'innodb_purge_threads',
          'innodb_random_read_ahead',
          'innodb_read_ahead_threshold',
          'innodb_read_io_threads',
          'innodb_replication_delay',
          'innodb_rollback_segments',
          'innodb_sort_buffer_size',
          'innodb_spin_wait_delay',
          'innodb_sync_spin_loops',
          'innodb_thread_sleep_delay',
          'innodb_use_native_aio',
          'innodb_write_io_threads',
          'join_buffer_size',
          'lock_wait_timeout',
          'max_binlog_cache_size',
          'max_binlog_size',
          'max_binlog_stmt_cache_size',
          'max_delayed_threads',
          'max_insert_delayed_threads',
          'max_join_size',
          'max_length_for_sort_data',
          'max_seeks_for_key',
          'max_sort_length',
          'max_sp_recursion_depth',
          'max_tmp_tables',
          'max_write_lock_count',
          'metadata_locks_cache_size',
          'optimizer_prune_level',
          'optimizer_search_depth',
          'preload_buffer_size',
          'query_alloc_block_size',
          'query_cache_limit',
          'query_cache_min_res_unit',
          'query_cache_size',
          'query_cache_type',
          'query_cache_wlock_invalidate',
          'query_prealloc_size',
          'range_alloc_block_size',
          'read_buffer_size',
          'read_rnd_buffer_size',
          'slave_checkpoint_group',
          'slave_checkpoint_period',
          'slave_parallel_workers',
          'slave_pending_jobs_size_max',
          'sort_buffer_size',
          'stored_program_cache',
          'table_definition_cache',
          'table_open_cache',
          'table_open_cache_instances',
          'thread_stack',
          'timed_mutexes',
          'transaction_alloc_block_size',
          'transaction_prealloc_size'
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
