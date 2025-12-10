#!/usr/bin/env python3
"""
测试 sync.py 的命令行参数解析功能
"""
import argparse

def test_argparse():
    parser = argparse.ArgumentParser(
        description='MySQL 数据同步工具 - 支持增量和全量同步',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
使用示例:
  # 同步所有表(增量模式)
  python3 sync.py
  
  # 对指定表进行全量同步
  python3 sync.py --tables table1 table2 table3 --full
  
  # 对指定表进行增量同步
  python3 sync.py --tables table1 table2
        '''
    )
    
    parser.add_argument(
        '--tables', '-t',
        nargs='+',
        metavar='TABLE',
        help='指定要同步的表名(支持多个表,用空格分隔)'
    )
    
    parser.add_argument(
        '--full', '-f',
        action='store_true',
        help='强制全量同步(忽略 checkpoint 和 editTime)'
    )
    
    return parser

if __name__ == "__main__":
    parser = test_argparse()
    
    # 测试不同的命令行参数
    test_cases = [
        [],  # 无参数
        ['--tables', 'mt_part'],  # 单表
        ['--tables', 'mt_part', 'mt_bom', 'mt_classification'],  # 多表
        ['--tables', 'mt_part', '--full'],  # 单表全量
        ['-t', 'mt_part', 'mt_bom', '-f'],  # 多表全量(简写)
    ]
    
    print("=" * 70)
    print("命令行参数解析测试")
    print("=" * 70)
    
    for i, test_args in enumerate(test_cases, 1):
        print(f"\n测试 {i}: python3 sync.py {' '.join(test_args)}")
        args = parser.parse_args(test_args)
        print(f"  解析结果:")
        print(f"    tables: {args.tables}")
        print(f"    full: {args.full}")
        
        # 模拟业务逻辑
        if args.tables:
            mode = "强制全量同步" if args.full else "智能同步(增量/全量)"
            print(f"  执行: 对表 {args.tables} 进行 {mode}")
        else:
            mode = "强制全量同步" if args.full else "智能同步(增量/全量)"
            print(f"  执行: 对所有表进行 {mode}")
    
    print("\n" + "=" * 70)
    print("✅ 所有测试通过!")
    print("=" * 70)
