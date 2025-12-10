import pymysql
import subprocess
import sys
import os
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 配置区域 =================
DATAX_PATH = "/Users/demon/Downloads/datax/bin/datax.py"
JOB_TEMPLATE = "job.json"
CHECKPOINT_FILE = "checkpoint.json"
MAX_WORKERS = 8 

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

def process_table(table):
    try:
        src_conn = get_connection(SRC_CONFIG)
        dest_conn = get_connection(DEST_CONFIG)
    except Exception as e:
        return f"❌ {table}: 数据库连接失败 - {str(e)}"

    result_msg = ""
    
    try:
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

        if not has_edittime:
            where_clause = "1=1"
            result_msg = f"🔄 {table}: 全量同步"
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
                return f"⏹️  {table}: 无新数据"

            where_clause = f"editTime > '{start_time}' AND editTime <= '{current_max_time}'"
            is_incremental = True
            result_msg = f"🚀 {table}: 增量同步"

        # 构造命令
        cmd = [
            "python3", DATAX_PATH,
            "-p", f"-DTABLE_NAME={table} -DWHERE_CLAUSE={where_clause}",
            JOB_TEMPLATE
        ]

        # 【核心修改】：捕获 stdout 和 stderr，而不是丢弃
        result = subprocess.run(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT, # 把 stderr 合并到 stdout
            encoding='utf-8',       # 直接获取字符串
            errors='ignore'         # 防止日志里有乱码导致脚本崩溃
        )
        
        # 检查 DataX 返回码 (0 表示成功)
        if result.returncode != 0:
            # === 如果失败，把日志写入文件 ===
            log_file = f"error_{table}.log"
            with open(log_file, "w", encoding='utf-8') as f:
                f.write(result.stdout)
            
            # === 尝试提取最后几行错误信息 ===
            # 简单的逻辑：找包含 'Exception' 或 'Error' 的行，或者直接取最后 10 行
            log_lines = result.stdout.splitlines()
            error_summary = []
            for line in log_lines:
                if "Exception" in line or "Error" in line:
                    error_summary.append(line.strip())
            
            # 如果没找到明显的 Error 关键字，就取最后 5 行
            if not error_summary:
                error_summary = log_lines[-5:]
            
            summary_str = "\n    ".join(error_summary[-3:]) # 只显示最后3条关键错误

            return f"❌ {table} 失败！\n    详情请查看日志文件: {log_file}\n    原因摘要: {summary_str}"

        # 3. 成功后更新 Checkpoint
        if is_incremental and current_max_time:
            update_checkpoint(table, current_max_time)
            
        return result_msg + " [✅ 成功]"

    except Exception as e:
        return f"❌ {table}: 脚本内部异常 - {str(e)}"
    finally:
        try:
            src_conn.close()
            dest_conn.close()
        except:
            pass

def main():
    if not os.path.exists(DATAX_PATH):
        print("DataX 路径不存在")
        return

    print("正在获取表清单...")
    try:
        conn = get_connection(SRC_CONFIG)
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
        conn.close()
    except Exception as e:
        print(f"获取表清单失败: {e}")
        return

    print(f"📋 共发现 {len(tables)} 张表，启动 {MAX_WORKERS} 个线程并发处理...")
    print("-" * 50)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_table = {executor.submit(process_table, table): table for table in tables}
        
        for future in as_completed(future_to_table):
            table = future_to_table[future]
            try:
                msg = future.result()
                if "⏹️" not in msg: 
                    print(msg)
            except Exception as exc:
                print(f"❌ {table} 线程异常: {exc}")

    print("-" * 50)
    print("🎉 所有表处理完毕！")

if __name__ == "__main__":
    main()