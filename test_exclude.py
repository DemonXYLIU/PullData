#!/usr/bin/env python3
"""
测试 sync.py 的排除表功能
"""
import argparse

def test_exclude_feature():
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
    
    return parser

def simulate_exclude_logic(all_tables, args):
    """模拟排除表的逻辑"""
    tables = all_tables.copy()
    
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
            return []
    
    return tables

if __name__ == "__main__":
    parser = test_exclude_feature()
    
    # 模拟数据库中的所有表
    all_tables = [
        'mt_part', 'mt_bom', 'mt_classification', 'mt_document',
        'sys_log', 'sys_temp', 'sys_user', 'sys_role'
    ]
    
    # 测试用例
    test_cases = [
        (['--exclude', 'sys_log', 'sys_temp'], "排除日志和临时表"),
        (['--exclude', 'sys_log'], "排除单个表"),
        (['--full', '--exclude', 'sys_log', 'sys_temp'], "全量同步但排除某些表"),
        (['-e', 'sys_log', 'sys_temp'], "使用简写参数"),
        (['--exclude', 'non_existent'], "排除不存在的表"),
        (['--tables', 'mt_part', 'mt_bom'], "指定表(不使用排除)"),
    ]
    
    print("=" * 70)
    print("排除表功能测试")
    print("=" * 70)
    print()
    
    for i, (test_args, description) in enumerate(test_cases, 1):
        print(f"\n测试 {i}: {description}")
        print(f"命令: python3 sync.py {' '.join(test_args)}")
        print("-" * 70)
        
        args = parser.parse_args(test_args)
        print(f"解析结果:")
        print(f"  tables: {args.tables}")
        print(f"  exclude: {args.exclude}")
        print(f"  full: {args.full}")
        print()
        
        # 模拟执行
        if args.tables:
            tables = args.tables
            print(f"📋 用户指定 {len(tables)} 张表: {', '.join(tables)}")
        else:
            tables = simulate_exclude_logic(all_tables, args)
            if tables:
                print(f"最终同步表: {', '.join(tables)}")
        
        print()
    
    print("=" * 70)
    print("✅ 所有测试完成!")
    print("=" * 70)
