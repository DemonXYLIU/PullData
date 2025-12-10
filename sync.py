import pymysql
import subprocess
import sys
import os
import json
import threading
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 配置区域 =================
DATAX_PATH = "/Users/demon/Downloads/datax/bin/datax.py"
CHECKPOINT_FILE = "checkpoint.json"
MAX_WORKERS = 8  # 并发线程数

# 源数据库
SRC_CONFIG = {
    'host': '10.11.252.103', 'user': 'root', 'password': 'gY~~2Vqi2-DQ',
    'db': 'meicloud_plm', 'port': 33306, 'charset': 'utf8'
}

# 本地数据库
DEST_CONFIG = {
    'host': 'localhost', 'user': 'root', 'password': '123456',
    'db': 'meicloud_plm', 'port': 3306, 'charset': 'utf8'
}
# ===========================================

file_lock = threading.Lock()

def get_connection(config):
    return pymysql.connect(
        host=config['host'], user=config['user'], password=config['password'],
        db=config['db'], port=config['port'], charset=config['charset']
    )

def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return {}
    with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
        try:
            return json.load(f)
        except:
            return {}

def update_checkpoint(table, time_str):
    with file_lock:
        data = load_checkpoint()
        data[table] = time_str
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

def get_local_max_time(conn, table):
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SHOW TABLES LIKE '{table}'")
            if not cursor.fetchone(): return None
            cursor.execute(f"SHOW COLUMNS FROM `{table}` LIKE 'editTime'")
            if not cursor.fetchone(): return None
            cursor.execute(f"SELECT MAX(editTime) FROM `{table}`")
            res = cursor.fetchone()
            return str(res[0]) if res and res[0] else None
    except:
        return None

def get_table_columns_quoted(conn, db, table):
    """
    获取表的所有字段,并给每个字段加上反引号 `field`
    """
    sql = f"""
    SELECT COLUMN_NAME 
    FROM information_schema.COLUMNS 
    WHERE TABLE_SCHEMA = '{db}' AND TABLE_NAME = '{table}' 
    ORDER BY ORDINAL_POSITION
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        # 将每个字段名用反引号包起来,解决 KEY, VALUE, CONDITION 等关键字报错问题
        return [f"`{row[0]}`" for row in cursor.fetchall()]

def get_primary_keys(conn, db, table):
    """
    获取表的主键字段列表
    返回: 主键字段名列表,如果没有主键返回空列表
    """
    sql = f"""
    SELECT COLUMN_NAME
    FROM information_schema.KEY_COLUMN_USAGE
    WHERE TABLE_SCHEMA = '{db}' 
    AND TABLE_NAME = '{table}'
    AND CONSTRAINT_NAME = 'PRIMARY'
    ORDER BY ORDINAL_POSITION
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return [row[0] for row in cursor.fetchall()]

def detect_and_delete_orphaned_records(src_conn, dest_conn, table, pk_fields, db_config):
    """
    检测并删除目标表中多余的记录(源表已删除但目标表仍存在的记录)
    
    参数:
        src_conn: 源数据库连接
        dest_conn: 目标数据库连接
        table: 表名
        pk_fields: 主键字段列表
        db_config: 数据库配置(用于获取数据库名)
    
    返回:
        删除的记录数
    """
    if not pk_fields:
        return 0
    
    try:
        # 构建主键字段的 SQL 片段
        pk_columns = ', '.join([f"`{pk}`" for pk in pk_fields])
        
        # 获取源表的所有主键值
        src_cursor = src_conn.cursor()
        src_cursor.execute(f"SELECT {pk_columns} FROM `{table}`")
        src_pks = set(src_cursor.fetchall())
        src_cursor.close()
        
        # 获取目标表的所有主键值
        dest_cursor = dest_conn.cursor()
        dest_cursor.execute(f"SELECT {pk_columns} FROM `{table}`")
        dest_pks = set(dest_cursor.fetchall())
        dest_cursor.close()
        
        # 计算需要删除的记录(目标表有但源表没有)
        orphaned_pks = dest_pks - src_pks
        
        if not orphaned_pks:
            return 0
        
        # 删除多余的记录
        deleted_count = 0
        dest_cursor = dest_conn.cursor()
        
        for pk_values in orphaned_pks:
            # 构建 WHERE 条件
            if len(pk_fields) == 1:
                # 单主键
                where_clause = f"`{pk_fields[0]}` = %s"
                dest_cursor.execute(f"DELETE FROM `{table}` WHERE {where_clause}", (pk_values,))
            else:
                # 复合主键
                conditions = [f"`{pk}` = %s" for pk in pk_fields]
                where_clause = ' AND '.join(conditions)
                dest_cursor.execute(f"DELETE FROM `{table}` WHERE {where_clause}", pk_values)
            
            deleted_count += dest_cursor.rowcount
        
        dest_conn.commit()
        dest_cursor.close()
        
        return deleted_count
        
    except Exception as e:
        print(f"    ⚠️  删除检测失败: {str(e)}")
        return 0

def process_table(table, force_full_sync=False, detect_deletes=True, truncate_before_sync=False):
    try:
        src_conn = get_connection(SRC_CONFIG)
        dest_conn = get_connection(DEST_CONFIG)
    except Exception as e:
        return f"❌ {table}: 数据库连接失败 - {str(e)}"

    # 动态生成 JSON 文件的路径
    temp_json_file = f"tmp_job_{table}.json"
    result_msg = ""
    
    try:
        # 1. 获取带有反引号的字段列表 (关键步骤！)
        columns_quoted = get_table_columns_quoted(src_conn, SRC_CONFIG['db'], table)
        if not columns_quoted:
            return f"❌ {table}: 无法获取字段信息，跳过"

        # 2. 检查是否有 editTime
        with src_conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT count(*) FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = '{SRC_CONFIG['db']}' 
                AND TABLE_NAME = '{table}' 
                AND COLUMN_NAME = 'editTime'
            """)
            has_edittime = cursor.fetchone()[0] > 0

        where_clause = ""
        current_max_time = None
        is_incremental = False
        checkpoints = load_checkpoint()

        # 强制全量同步模式
        if force_full_sync:
            where_clause = "1=1"
            result_msg = f"🔄 {table}: 强制全量同步"
            # 如果有 editTime,获取当前最大时间用于更新 checkpoint
            if has_edittime:
                with src_conn.cursor() as cursor:
                    cursor.execute(f"SELECT MAX(editTime) FROM `{table}`")
                    res = cursor.fetchone()[0]
                    if res:
                        current_max_time = str(res)
        elif not has_edittime:
            where_clause = "1=1"
            result_msg = f"🔄 {table}: 全量同步 (无 editTime)"
        else:
            start_time = "1970-01-01 00:00:00"
            if table in checkpoints:
                start_time = checkpoints[table]
            else:
                local_max = get_local_max_time(dest_conn, table)
                if local_max: start_time = local_max

            with src_conn.cursor() as cursor:
                cursor.execute(f"SELECT MAX(editTime) FROM `{table}`")
                res = cursor.fetchone()[0]
                if res is None:
                    return f"⚠️ {table}: 源表为空，跳过"
                current_max_time = str(res)

            if current_max_time <= start_time:
                return f"⏹️  {table}: 无新数据 (Current: {current_max_time})"

            where_clause = f"editTime > '{start_time}' AND editTime <= '{current_max_time}'"
            is_incremental = True
            result_msg = f"🚀 {table}: 增量同步 ({start_time} -> {current_max_time})"

        # 3. 动态构建 DataX JSON 配置字典
        # 我们不再读取 job.json 模板，而是直接在内存里生成配置
        # 这样可以将 columns_quoted 列表完美嵌入，不会有格式问题
        job_config = {
            "job": {
                "content": [{
                    "reader": {
                        "name": "mysqlreader",
                        "parameter": {
                            "username": SRC_CONFIG['user'],
                            "password": SRC_CONFIG['password'],
                            "column": columns_quoted,  # 使用带反引号的字段列表
                            "connection": [{
                                "jdbcUrl": [f"jdbc:mysql://{SRC_CONFIG['host']}:{SRC_CONFIG['port']}/{SRC_CONFIG['db']}?useUnicode=true&characterEncoding=utf8"],
                                "table": [table]
                            }],
                            "where": where_clause
                        }
                    },
                    "writer": {
                        "name": "mysqlwriter",
                        "parameter": {
                            "username": DEST_CONFIG['user'],
                            "password": DEST_CONFIG['password'],
                            "writeMode": "replace",
                            "column": columns_quoted,  # 写入端也用同样的字段列表
                            "connection": [{
                                "jdbcUrl": f"jdbc:mysql://{DEST_CONFIG['host']}:{DEST_CONFIG['port']}/{DEST_CONFIG['db']}?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true",
                                "table": [table]
                            }]
                        }
                    }
                }],
                "setting": {
                    "speed": {"channel": 5}
                }
            }
        }

        # 4. 将配置写入临时 JSON 文件
        with open(temp_json_file, 'w', encoding='utf-8') as f:
            json.dump(job_config, f, ensure_ascii=False)

        # 5. 全量同步模式:可选择先清空目标表
        if force_full_sync and truncate_before_sync:
            try:
                with dest_conn.cursor() as cursor:
                    cursor.execute(f"TRUNCATE TABLE `{table}`")
                dest_conn.commit()
                result_msg += " (已清空目标表)"
            except Exception as e:
                print(f"    ⚠️  清空表失败: {str(e)}")
        
        # 6. 调用 DataX (直接指向临时文件,不需要 -p 参数了)
        cmd = ["python3", DATAX_PATH, temp_json_file]

        result = subprocess.run(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT,
            encoding='utf-8',       
            errors='ignore'
        )
        
        # 7. 删除临时配置文件 (清理现场)
        if os.path.exists(temp_json_file):
            os.remove(temp_json_file)

        if result.returncode != 0:
            log_file = f"error_{table}.log"
            with open(log_file, "w", encoding='utf-8') as f:
                f.write(result.stdout)
            
            # 提取错误摘要
            log_lines = result.stdout.splitlines()
            error_summary = [line.strip() for line in log_lines if "Exception" in line or "Error" in line]
            if not error_summary: error_summary = log_lines[-5:]
            summary_str = "\n    ".join(error_summary[-2:]) 

            return f"❌ {table} 失败!(日志: {log_file})\n    原因: {summary_str}"

        # 8. 删除检测:检测并删除目标表中多余的记录
        deleted_count = 0
        if detect_deletes and not (force_full_sync and truncate_before_sync):
            # 如果是全量同步且已清空表,则不需要删除检测
            try:
                pk_fields = get_primary_keys(src_conn, SRC_CONFIG['db'], table)
                if pk_fields:
                    deleted_count = detect_and_delete_orphaned_records(
                        src_conn, dest_conn, table, pk_fields, SRC_CONFIG
                    )
                    if deleted_count > 0:
                        result_msg += f" (删除 {deleted_count} 条)"
                else:
                    # 没有主键,跳过删除检测
                    if detect_deletes:
                        result_msg += " (无主键,跳过删除检测)"
            except Exception as e:
                result_msg += f" (删除检测异常: {str(e)})"
        
        # 9. 更新 checkpoint: 增量同步或强制全量同步(有 editTime)
        if current_max_time and (is_incremental or force_full_sync):
            update_checkpoint(table, current_max_time)
            
        return result_msg + " [✅ 成功]"

    except Exception as e:
        return f"❌ {table}: 脚本异常 - {str(e)}"
    finally:
        try:
            src_conn.close()
            dest_conn.close()
        except:
            pass

def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(
        description='MySQL 数据同步工具 - 支持增量和全量同步',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
使用示例:
  # 同步所有表(增量模式)
  python3 sync.py
  
  # 对所有表进行全量同步
  python3 sync.py --full
  
  # 排除某些表,同步其他所有表
  python3 sync.py --exclude sys_log sys_temp
  
  # 对指定表进行全量同步
  python3 sync.py --tables table1 table2 table3 --full
  
  # 对指定表进行增量同步
  python3 sync.py --tables table1 table2
  
  # 全量同步所有表,但排除某些表
  python3 sync.py --full --exclude sys_log sys_temp
  
  # 禁用删除检测(默认启用)
  python3 sync.py --no-detect-deletes
  
  # 全量同步前清空表
  python3 sync.py --full --truncate-before-sync
        '''
    )
    
    parser.add_argument(
        '--tables', '-t',
        nargs='+',
        metavar='TABLE',
        help='指定要同步的表名(支持多个表,用空格分隔)'
    )
    
    parser.add_argument(
        '--exclude', '-e',
        nargs='+',
        metavar='TABLE',
        help='指定要排除的表名(支持多个表,用空格分隔)'
    )
    
    parser.add_argument(
        '--full', '-f',
        action='store_true',
        help='强制全量同步(忽略 checkpoint 和 editTime)'
    )
    
    parser.add_argument(
        '--no-detect-deletes',
        action='store_true',
        help='禁用删除检测(默认启用删除检测以保证数据一致性)'
    )
    
    parser.add_argument(
        '--truncate-before-sync',
        action='store_true',
        help='全量同步前清空目标表(仅在 --full 模式下生效)'
    )
    
    args = parser.parse_args()
    
    if not os.path.exists(DATAX_PATH):
        print(f"❌ DataX 路径错误: {DATAX_PATH}")
        return

    # 获取要处理的表列表
    if args.tables:
        # 用户指定了表名
        tables = args.tables
        print(f"📋 用户指定 {len(tables)} 张表: {', '.join(tables)}")
        
        # 验证表是否存在
        try:
            conn = get_connection(SRC_CONFIG)
            with conn.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                all_tables = {row[0] for row in cursor.fetchall()}
            conn.close()
            
            # 检查不存在的表
            invalid_tables = [t for t in tables if t not in all_tables]
            if invalid_tables:
                print(f"⚠️  警告: 以下表不存在: {', '.join(invalid_tables)}")
                tables = [t for t in tables if t in all_tables]
                if not tables:
                    print("❌ 没有有效的表可以同步")
                    return
                print(f"✅ 将同步以下有效表: {', '.join(tables)}")
        except Exception as e:
            print(f"❌ 连接源库失败: {e}")
            return
    else:
        # 同步所有表
        print("正在获取表清单...")
        try:
            conn = get_connection(SRC_CONFIG)
            with conn.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                tables = [row[0] for row in cursor.fetchall()]
            conn.close()
        except Exception as e:
            print(f"❌ 连接源库失败: {e}")
            return
        
        print(f"📋 发现 {len(tables)} 张表")
    
    # 处理排除表逻辑
    if args.exclude:
        excluded_count = 0
        original_count = len(tables)
        excluded_tables = []
        
        for exclude_table in args.exclude:
            if exclude_table in tables:
                tables.remove(exclude_table)
                excluded_tables.append(exclude_table)
                excluded_count += 1
        
        if excluded_count > 0:
            print(f"🚫 排除 {excluded_count} 张表: {', '.join(excluded_tables)}")
            print(f"✅ 实际同步 {len(tables)} 张表 (原 {original_count} 张)")
        else:
            print(f"⚠️  警告: 指定的排除表不存在: {', '.join(args.exclude)}")
        
        if not tables:
            print("❌ 没有表需要同步")
            return
    
    # 显示同步模式
    sync_mode = "强制全量同步" if args.full else "智能同步(增量/全量)"
    detect_deletes = not args.no_detect_deletes  # 默认启用删除检测
    
    print(f"🔧 同步模式: {sync_mode}")
    print(f"🔍 删除检测: {'启用' if detect_deletes else '禁用'}")
    if args.truncate_before_sync and args.full:
        print(f"🗑️  清空表模式: 启用(全量同步前清空表)")
    print(f"⚙️  并发线程数: {MAX_WORKERS}")
    print("=" * 60)

    # 执行同步
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 根据是否强制全量同步来提交任务
        if args.full:
            future_to_table = {
                executor.submit(
                    process_table, 
                    table, 
                    force_full_sync=True,
                    detect_deletes=detect_deletes,
                    truncate_before_sync=args.truncate_before_sync
                ): table 
                for table in tables
            }
        else:
            future_to_table = {
                executor.submit(
                    process_table, 
                    table,
                    detect_deletes=detect_deletes
                ): table 
                for table in tables
            }
        
        for future in as_completed(future_to_table):
            table = future_to_table[future]
            try:
                msg = future.result()
                if "⏹️" not in msg: 
                    print(msg)
            except Exception as exc:
                print(f"❌ {table} 线程异常: {exc}")

    print("=" * 60)
    print("🎉 所有任务结束。")

if __name__ == "__main__":
    main()