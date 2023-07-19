import multiprocessing
import os
import time
import psutil
from datetime import datetime
import pyuac
from pyuac import main_requires_admin

from environment import MysqlConnector

# process_names = [proc.name() for proc in psutil.process_iter()]
# for proc in psutil.process_iter():
#     if proc.name() == "pycharm64.exe":
#         rw = proc.io_counters()
#         print(proc.name(), proc.cpu_percent(interval=None), proc.memory_info()[0], rw[2], rw[3])
#
# t1 = datetime.utcnow().strftime('%S.%f')
# time.sleep(5.4)
# t2 = datetime.utcnow().strftime('%S.%f')
# print(float(t2)-float(t1))

knobs_max = {
    'innodb_adaptive_flushing_lwm': 70,  # 0-10-70
    # 'innodb_adaptive_hash_index': True,  # OFF-ON-ON
    'innodb_adaptive_max_sleep_delay': 1000000,  # 0-150000-1000000
    'innodb_buffer_pool_instances': 64,  # 1-8-64
    'innodb_buffer_pool_size': (2**64)-1,  # 5242880-134217728-2**64-1
    'innodb_change_buffering': 'all',  # (none-inserts-deletes-changes-purges-all)-all
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
    # 'innodb_adaptive_flushing': True,  # OFF-ON-ON
    'innodb_autoextend_increment': 1000,  # 1-64-1000
    # 'innodb_buffer_pool_dump_now': True,  # OFF-OFF-ON
    # 'innodb_buffer_pool_load_at_startup': True,  # OFF-ON-ON
    # 'innodb_buffer_pool_load_now': True,  # OFF-OFF-ON
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
    # 'innodb_page_size': 65536,  # (4096-8192-16384-32768-65536)-16384
    'innodb_purge_batch_size': 5000,  # 1-300-5000
    'innodb_purge_threads': 32,  # 1-4-32
    # 'innodb_random_read_ahead': True,  # OFF-OFF-ON
    'innodb_read_ahead_threshold': 64,  # 0-56-64
    'innodb_read_io_threads': 64,  # 1-4-64
    'innodb_replication_delay': 4294967295,  # 0-0-4294967295
    'innodb_rollback_segments': 128,  # 1-128-128
    'innodb_sort_buffer_size': 67108864,  # 65536-1048576-67108864
    'innodb_spin_wait_delay': (2**64)-1,  # 0-6-2**64-1
    'innodb_sync_spin_loops': 4294967295,  # 0-30-4294967295
    'innodb_thread_sleep_delay': 1000000,  # 0-10000-1000000
    # 'innodb_use_native_aio': True,  # OFF-ON-ON
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
    # 'innodb_dedicated_server': True,  # OFF-OFF-ON
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


str = '''
"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqladmin" -u root shutdown -p"root"
'''
#os.system(str)

import subprocess
#pyuac.runAsAdmin("\"C:\\Program Files\\MySQL\\MySQL Server 8.0\\bin\\mysqladmin.exe\" -u root shutdown -p\"root\"")

@main_requires_admin
def start():
    print(pyuac.isUserAdmin())
    #subprocess.call("\"C:\\Program Files\\MySQL\\MySQL Server 8.0\\bin\\mysqladmin.exe\" -u root shutdown -p\"root\"", shell=True)
    #subprocess.call("\"C:\\Program Files\\MySQL\\MySQL Server 8.0\\bin\\mysqld\" --defaults-file=\"C:\\ProgramData\\MySQL\\MySQL Server 8.0\\my.ini\" --standalone")
    os.system("C:\\Windows\\System32\\net.exe start MySql80")

@main_requires_admin
def stop():
    print(pyuac.isUserAdmin())
    #subprocess.call("\"C:\\Program Files\\MySQL\\MySQL Server 8.0\\bin\\mysqladmin.exe\" -u root shutdown -p\"root\"", shell=True)
    #subprocess.call("\"C:\\Program Files\\MySQL\\MySQL Server 8.0\\bin\\mysqld\" --defaults-file=\"C:\\ProgramData\\MySQL\\MySQL Server 8.0\\my.ini\" --standalone")
    os.system("C:\\Windows\\System32\\net.exe stop MySql80")


#p1 = Process(target=restart, daemon=True)
#p1.start()
#restart()
start()
db = MysqlConnector()
#r = db.execute("SELECT COUNT(*) FROM actor")


r = db.fetch_results("SELECT COUNT(*) FROM actor")
print(r)
stop()

#p1.join()
#os.system('C:\\Program Files\\MySQL\\MySQL Server 8.0\\bin\\mysqladmin.exe -u root shutdown -p"root"')
# knobs_max_list = list(knobs_max.values())
# print(knobs_max_list)
# for index, val in enumerate(knobs_max):
#     print(val, knobs_max_list[index])

''' "C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqld" --defaults-file="C:\Program Files\MySQL\MySQL Server 8.0\my.ini" '''
''' "C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqladmin" -u root shutdown -p"root" '''