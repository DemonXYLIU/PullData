#!/usr/bin/env python3
"""
测试删除数据识别功能

测试场景:
1. 创建测试表
2. 插入测试数据到源表和目标表
3. 从源表删除部分数据
4. 运行同步(启用删除检测)
5. 验证目标表数据与源表一致
"""

import pymysql
import subprocess
import time

# 测试配置
SRC_CONFIG = {
    'host': '10.11.252.103',
    'user': 'root',
    'password': 'gY~~2Vqi2-DQ',
    'db': 'meicloud_plm',
    'port': 33306
}

DEST_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'db': 'meicloud_plm',
    'port': 3306
}

TEST_TABLE = 'test_delete_detection'

def get_connection(config):
    return pymysql.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        db=config['db'],
        port=config['port'],
        charset='utf8'
    )

def create_test_table():
    """创建测试表"""
    print("=" * 70)
    print("步骤 1: 创建测试表")
    print("=" * 70)
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{TEST_TABLE}` (
        `id` INT PRIMARY KEY,
        `name` VARCHAR(50),
        `value` INT,
        `editTime` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    
    # 在源库创建
    src_conn = get_connection(SRC_CONFIG)
    with src_conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS `{TEST_TABLE}`")
        cursor.execute(create_sql)
    src_conn.commit()
    src_conn.close()
    print(f"✅ 源库创建测试表: {TEST_TABLE}")
    
    # 在目标库创建
    dest_conn = get_connection(DEST_CONFIG)
    with dest_conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS `{TEST_TABLE}`")
        cursor.execute(create_sql)
    dest_conn.commit()
    dest_conn.close()
    print(f"✅ 目标库创建测试表: {TEST_TABLE}")
    print()

def insert_initial_data():
    """插入初始数据"""
    print("=" * 70)
    print("步骤 2: 插入初始数据")
    print("=" * 70)
    
    # 源库插入 5 条数据
    src_conn = get_connection(SRC_CONFIG)
    with src_conn.cursor() as cursor:
        for i in range(1, 6):
            cursor.execute(
                f"INSERT INTO `{TEST_TABLE}` (id, name, value) VALUES (%s, %s, %s)",
                (i, f"Record_{i}", i * 100)
            )
    src_conn.commit()
    src_conn.close()
    print(f"✅ 源库插入 5 条记录: id=1,2,3,4,5")
    
    # 目标库也插入 5 条数据
    dest_conn = get_connection(DEST_CONFIG)
    with dest_conn.cursor() as cursor:
        for i in range(1, 6):
            cursor.execute(
                f"INSERT INTO `{TEST_TABLE}` (id, name, value) VALUES (%s, %s, %s)",
                (i, f"Record_{i}", i * 100)
            )
    dest_conn.commit()
    dest_conn.close()
    print(f"✅ 目标库插入 5 条记录: id=1,2,3,4,5")
    print()

def delete_from_source():
    """从源表删除部分数据"""
    print("=" * 70)
    print("步骤 3: 从源表删除数据")
    print("=" * 70)
    
    src_conn = get_connection(SRC_CONFIG)
    with src_conn.cursor() as cursor:
        # 删除 id=2 和 id=4 的记录
        cursor.execute(f"DELETE FROM `{TEST_TABLE}` WHERE id IN (2, 4)")
        deleted = cursor.rowcount
    src_conn.commit()
    src_conn.close()
    
    print(f"✅ 从源表删除 {deleted} 条记录: id=2,4")
    print(f"   源表剩余记录: id=1,3,5")
    print()

def run_sync_with_delete_detection():
    """运行同步(启用删除检测)"""
    print("=" * 70)
    print("步骤 4: 运行同步(启用删除检测)")
    print("=" * 70)
    
    cmd = ["python3", "sync.py", "--tables", TEST_TABLE]
    print(f"执行命令: {' '.join(cmd)}")
    print()
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("错误输出:", result.stderr)
    
    return result.returncode == 0

def verify_data_consistency():
    """验证数据一致性"""
    print("=" * 70)
    print("步骤 5: 验证数据一致性")
    print("=" * 70)
    
    # 获取源表数据
    src_conn = get_connection(SRC_CONFIG)
    with src_conn.cursor() as cursor:
        cursor.execute(f"SELECT id, name, value FROM `{TEST_TABLE}` ORDER BY id")
        src_data = cursor.fetchall()
    src_conn.close()
    
    # 获取目标表数据
    dest_conn = get_connection(DEST_CONFIG)
    with dest_conn.cursor() as cursor:
        cursor.execute(f"SELECT id, name, value FROM `{TEST_TABLE}` ORDER BY id")
        dest_data = cursor.fetchall()
    dest_conn.close()
    
    print(f"源表记录数: {len(src_data)}")
    print(f"目标表记录数: {len(dest_data)}")
    print()
    
    print("源表数据:")
    for row in src_data:
        print(f"  {row}")
    print()
    
    print("目标表数据:")
    for row in dest_data:
        print(f"  {row}")
    print()
    
    # 验证数据是否一致
    if src_data == dest_data:
        print("✅ 验证通过: 源表和目标表数据完全一致!")
        print(f"   两表都有 {len(src_data)} 条记录: id=1,3,5")
        print(f"   已删除的记录 id=2,4 在目标表中也被删除")
        return True
    else:
        print("❌ 验证失败: 源表和目标表数据不一致!")
        print(f"   源表: {src_data}")
        print(f"   目标表: {dest_data}")
        return False

def cleanup():
    """清理测试数据"""
    print()
    print("=" * 70)
    print("清理测试数据")
    print("=" * 70)
    
    # 删除源表
    src_conn = get_connection(SRC_CONFIG)
    with src_conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS `{TEST_TABLE}`")
    src_conn.commit()
    src_conn.close()
    print(f"✅ 删除源库测试表: {TEST_TABLE}")
    
    # 删除目标表
    dest_conn = get_connection(DEST_CONFIG)
    with dest_conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS `{TEST_TABLE}`")
    dest_conn.commit()
    dest_conn.close()
    print(f"✅ 删除目标库测试表: {TEST_TABLE}")

def main():
    print()
    print("=" * 70)
    print("删除数据识别功能测试")
    print("=" * 70)
    print()
    
    try:
        # 1. 创建测试表
        create_test_table()
        
        # 2. 插入初始数据
        insert_initial_data()
        
        # 3. 从源表删除数据
        delete_from_source()
        
        # 4. 运行同步
        sync_success = run_sync_with_delete_detection()
        
        if not sync_success:
            print("❌ 同步失败,测试中止")
            return
        
        # 等待一下确保同步完成
        time.sleep(2)
        
        # 5. 验证数据一致性
        verify_success = verify_data_consistency()
        
        # 6. 清理测试数据
        cleanup()
        
        # 最终结果
        print()
        print("=" * 70)
        if verify_success:
            print("🎉 测试成功! 删除检测功能工作正常!")
        else:
            print("❌ 测试失败! 请检查删除检测功能!")
        print("=" * 70)
        
    except Exception as e:
        print(f"❌ 测试异常: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
