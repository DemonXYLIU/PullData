# MySQL 数据同步工具使用说明

## 功能概述

这是一个基于 DataX 的 MySQL 数据同步工具,支持:
- ✅ 智能增量同步(基于 `editTime` 字段)
- ✅ 全量同步(无 `editTime` 字段的表)
- ✅ **指定表名全量同步**
- ✅ **所有表全量同步**
- ✅ **排除指定表同步**(新功能)
- ✅ 多线程并发处理
- ✅ 断点续传(checkpoint 机制)
- ✅ 自动处理 MySQL 关键字字段名

## 使用方法

### 1. 同步所有表(智能模式)

默认行为:对所有表进行智能同步,有 `editTime` 的表进行增量同步,无 `editTime` 的表进行全量同步。

```bash
python3 sync.py
```

### 2. 指定表进行增量同步

只同步指定的表,使用智能模式(增量/全量):

```bash
# 同步单个表
python3 sync.py --tables mt_part

# 同步多个表
python3 sync.py --tables mt_part mt_bom mt_classification
```

### 3. 指定表进行全量同步 ⭐(新功能)

对指定的表强制进行全量同步,忽略 checkpoint 和 `editTime`:

```bash
# 对单个表进行全量同步
python3 sync.py --tables mt_part --full

# 对多个表进行全量同步
python3 sync.py --tables mt_part mt_bom mt_classification --full

# 简写形式
python3 sync.py -t mt_part mt_bom -f
```

### 4. 所有表全量同步 ⭐⭐(重要功能)

对数据库中的**所有表**强制进行全量同步,适用于数据库迁移、灾难恢复等场景:

```bash
# 对所有表进行全量同步
python3 sync.py --full

# 简写形式
python3 sync.py -f
```

> ⚠️ **警告**: 此操作会对所有表进行全量同步,可能需要较长时间,请谨慎使用!

### 5. 排除指定表 ⭐(新功能)

排除某些不需要同步的表(如日志表、临时表):

```bash
# 排除单个表
python3 sync.py --exclude sys_log

# 排除多个表
python3 sync.py --exclude sys_log sys_temp audit_log

# 全量同步所有表,但排除某些表
python3 sync.py --full --exclude sys_log sys_temp

# 使用简写参数
python3 sync.py -e sys_log sys_temp
```

> 💡 **提示**: `--exclude` 参数可以与 `--full` 参数组合使用,实现灵活的同步策略。

### 6. 查看帮助信息

```bash
python3 sync.py --help
```

## 参数说明

| 参数 | 简写 | 说明 | 示例 |
|------|------|------|------|
| `--tables` | `-t` | 指定要同步的表名(支持多个) | `--tables table1 table2` |
| `--exclude` | `-e` | 指定要排除的表名(支持多个) | `--exclude sys_log sys_temp` |
| `--full` | `-f` | 强制全量同步模式 | `--full` |

## 工作原理

### 智能同步模式(默认)

1. **有 `editTime` 字段的表**:
   - 从 `checkpoint.json` 读取上次同步的最大时间
   - 只同步 `editTime` 大于上次时间的数据
   - 同步成功后更新 checkpoint

2. **无 `editTime` 字段的表**:
   - 自动进行全量同步
   - 不更新 checkpoint

### 强制全量同步模式(`--full`)

1. 忽略 `checkpoint.json` 中的记录
2. 同步表中的所有数据(`WHERE 1=1`)
3. 如果表有 `editTime` 字段,同步后会更新 checkpoint 为当前最大时间
4. 后续可以继续使用增量同步

## 使用场景

### 场景 1: 数据修复

某个表的数据出现问题,需要重新全量同步:

```bash
python3 sync.py --tables problematic_table --full
```

### 场景 2: 新增表同步

数据库新增了几个表,需要首次全量同步:

```bash
python3 sync.py --tables new_table1 new_table2 new_table3 --full
```

### 场景 3: 批量重置

多个表需要重新全量同步:

```bash
python3 sync.py --tables mt_part mt_bom mt_classification mt_document --full
```

### 场景 4: 数据库迁移/灾难恢复 ⭐⭐

需要将整个数据库的所有表进行全量同步(例如:数据库迁移、灾难恢复、环境复制):

```bash
# 对所有表进行全量同步
python3 sync.py --full
```

> 💡 **提示**: 这个操作会同步所有表,建议在非高峰期执行,并确保目标数据库有足够的存储空间。

### 场景 5: 排除日志表和临时表 ⭐

日常同步时排除不需要的日志表、临时表、审计表等:

```bash
# 排除日志和临时表
python3 sync.py --exclude sys_log sys_temp audit_log

# 全量同步但排除某些表
python3 sync.py --full --exclude sys_log sys_temp
```

> 💡 **提示**: 适用于不需要同步历史日志、临时数据等场景,可以大幅减少同步时间和数据量。

### 场景 6: 日常增量同步

定时任务每天自动增量同步所有表:

```bash
# crontab 示例: 每天凌晨 2 点执行
0 2 * * * cd /Users/demon/Downloads/PullData && python3 sync.py >> sync.log 2>&1
```

## 配置说明

在 `sync.py` 文件顶部的配置区域:

```python
# DataX 路径
DATAX_PATH = "/Users/demon/Downloads/datax/bin/datax.py"

# Checkpoint 文件
CHECKPOINT_FILE = "checkpoint.json"

# 并发线程数
MAX_WORKERS = 8

# 源数据库配置
SRC_CONFIG = {
    'host': '10.11.252.103',
    'user': 'root',
    'password': 'gY~~2Vqi2-DQ',
    'db': 'meicloud_plm',
    'port': 33306,
    'charset': 'utf8'
}

# 目标数据库配置
DEST_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'db': 'meicloud_plm',
    'port': 3306,
    'charset': 'utf8'
}
```

## 输出说明

同步过程中会显示不同的状态图标:

- 🔄 **全量同步**: 表没有 `editTime` 字段或使用 `--full` 参数
- 🚀 **增量同步**: 基于 `editTime` 的增量同步
- ⏹️ **无新数据**: 源表没有新数据需要同步
- ✅ **成功**: 同步成功
- ❌ **失败**: 同步失败,会生成错误日志文件
- ⚠️ **警告**: 表不存在或源表为空

## 注意事项

1. **表名验证**: 使用 `--tables` 参数时,工具会自动验证表是否存在
2. **Checkpoint 更新**: 全量同步后会更新 checkpoint,后续可以继续增量同步
3. **并发控制**: 默认 8 个线程并发,可根据服务器性能调整 `MAX_WORKERS`
4. **错误日志**: 同步失败会生成 `error_<表名>.log` 文件,便于排查问题
5. **临时文件**: 每个表会生成临时配置文件 `tmp_job_<表名>.json`,同步完成后自动删除

## 常见问题

### Q: 如何重置某个表的 checkpoint?

A: 有两种方法:
1. 使用 `--full` 参数强制全量同步(推荐)
2. 手动编辑 `checkpoint.json` 文件,删除对应表的记录

### Q: 全量同步会删除目标表的数据吗?

A: 不会删除,使用的是 `REPLACE` 模式,会根据主键/唯一键更新或插入数据

### Q: 可以同时指定多个表吗?

A: 可以,使用空格分隔:
```bash
python3 sync.py --tables table1 table2 table3 --full
```

### Q: 如何查看所有可用的表?

A: 可以先运行一次不带参数的命令,会显示所有表:
```bash
python3 sync.py
```

## 更新日志

### v2.1 (2025-12-09)
- ✨ 新增 `--exclude` 参数,支持排除指定表
- 📝 完善使用场景说明
- 🎯 优化同步策略的灵活性

### v2.0 (2025-12-09)
- ✨ 新增 `--tables` 参数,支持指定表名同步
- ✨ 新增 `--full` 参数,支持强制全量同步
- ✨ 支持所有表全量同步
- ✨ 添加表名验证功能
- 📝 完善命令行帮助信息
- 🐛 修复全量同步后 checkpoint 不更新的问题

### v1.0
- 基础增量/全量同步功能
- Checkpoint 机制
- 多线程并发处理