# 排除表功能详细说明

## 🎯 功能概述

排除表功能允许您在同步时忽略某些不需要的表,如日志表、临时表、审计表等。这可以大幅减少同步时间和数据传输量。

## 📝 使用方法

### 基本用法

```bash
# 排除单个表
python3 sync.py --exclude sys_log

# 排除多个表
python3 sync.py --exclude sys_log sys_temp audit_log

# 使用简写参数
python3 sync.py -e sys_log sys_temp
```

### 与其他参数组合

```bash
# 全量同步所有表,但排除某些表
python3 sync.py --full --exclude sys_log sys_temp

# 增量同步所有表,但排除某些表
python3 sync.py --exclude sys_log sys_temp audit_log
```

## 🔍 工作原理

1. **获取所有表**: 从源数据库获取所有表名
2. **应用排除规则**: 从表列表中移除指定的排除表
3. **验证表名**: 检查排除的表是否存在
4. **执行同步**: 对剩余的表执行同步操作

## 📊 执行效果

```
正在获取表清单...
📋 发现 156 张表
🚫 排除 3 张表: sys_log, sys_temp, audit_log
✅ 实际同步 153 张表 (原 156 张)
🔧 同步模式: 智能同步(增量/全量)
⚙️  并发线程数: 8
============================================================
🚀 mt_part: 增量同步 (2025-12-08 10:30:00 -> 2025-12-09 09:15:00) [✅ 成功]
🚀 mt_bom: 增量同步 (2025-12-08 10:30:00 -> 2025-12-09 09:15:00) [✅ 成功]
... (更多表)
============================================================
🎉 所有任务结束。
```

## 🎯 适用场景

### 场景 1: 排除日志表

日志表数据量大且不需要同步到测试环境:

```bash
python3 sync.py --exclude sys_log operation_log access_log
```

**优势**:
- 减少 50%-70% 的同步时间
- 节省大量存储空间
- 降低网络带宽占用

### 场景 2: 排除临时表

临时表数据无需同步:

```bash
python3 sync.py --exclude temp_data temp_cache temp_session
```

### 场景 3: 排除审计表

审计表数据量大且仅用于生产环境:

```bash
python3 sync.py --exclude audit_log audit_trail compliance_log
```

### 场景 4: 数据库迁移时排除某些表

迁移时排除不需要的历史数据:

```bash
python3 sync.py --full --exclude sys_log audit_log archive_data
```

### 场景 5: 定时同步排除大表

日常定时同步时排除超大表:

```bash
# crontab 示例
0 2 * * * cd /path/to/PullData && python3 sync.py -e big_table1 big_table2 >> sync.log 2>&1
```

## 💡 最佳实践

### 1. 识别需要排除的表

常见的可排除表类型:

| 表类型 | 示例表名 | 排除原因 |
|--------|----------|----------|
| 日志表 | `sys_log`, `operation_log`, `access_log` | 数据量大,不需要同步 |
| 临时表 | `temp_*`, `tmp_*`, `cache_*` | 临时数据,无需保留 |
| 审计表 | `audit_*`, `compliance_*` | 仅生产环境需要 |
| 会话表 | `session_*`, `token_*` | 实时数据,无需同步 |
| 归档表 | `archive_*`, `history_*` | 历史数据,不需要 |
| 统计表 | `stat_*`, `report_*` | 可重新生成 |

### 2. 创建排除表配置文件

对于固定的排除表列表,可以创建一个配置文件:

```bash
# exclude_tables.txt
sys_log
sys_temp
audit_log
operation_log
access_log
```

然后在脚本中读取:

```bash
# 读取排除表列表
EXCLUDE_TABLES=$(cat exclude_tables.txt | tr '\n' ' ')
python3 sync.py --exclude $EXCLUDE_TABLES
```

### 3. 使用通配符(未来功能)

> 注: 当前版本需要明确指定表名,未来可能支持通配符

期望的用法:
```bash
# 排除所有 log 结尾的表
python3 sync.py --exclude "*_log"

# 排除所有 temp 开头的表
python3 sync.py --exclude "temp_*"
```

## ⚠️ 注意事项

### 1. 表名验证

- 如果指定的排除表不存在,会显示警告但不会中断执行
- 建议先查看所有表名,确保排除表名正确

### 2. 与 --tables 参数的关系

- `--tables` 和 `--exclude` **不能同时使用**
- `--tables` 是指定要同步的表(白名单)
- `--exclude` 是指定要排除的表(黑名单)

```bash
# ❌ 错误用法(不要同时使用)
python3 sync.py --tables mt_part mt_bom --exclude sys_log

# ✅ 正确用法(二选一)
python3 sync.py --tables mt_part mt_bom
python3 sync.py --exclude sys_log sys_temp
```

### 3. 排除所有表

如果排除的表太多,导致没有表需要同步,会提示错误:

```
❌ 没有表需要同步
```

### 4. 大小写敏感

表名是大小写敏感的,请确保表名大小写正确:

```bash
# 如果表名是 SYS_LOG
python3 sync.py --exclude SYS_LOG  # ✅ 正确
python3 sync.py --exclude sys_log  # ❌ 可能找不到
```

## 📈 性能对比

假设数据库有 150 张表,其中:
- 日志表 3 张,共 50GB
- 临时表 2 张,共 10GB
- 业务表 145 张,共 40GB

### 不使用排除功能

```bash
python3 sync.py
```

- 同步表数: 150 张
- 数据量: 100GB
- 预计时间: 60 分钟

### 使用排除功能

```bash
python3 sync.py --exclude sys_log operation_log access_log temp_data temp_cache
```

- 同步表数: 145 张
- 数据量: 40GB
- 预计时间: 24 分钟
- **节省时间: 60%**
- **节省数据量: 60%**

## 🔧 高级用法

### 1. 排除表 + 全量同步

```bash
# 全量同步所有业务表,但排除日志表
python3 sync.py --full --exclude sys_log audit_log
```

### 2. 排除表 + 定时任务

```bash
# 每天凌晨 2 点同步,排除日志表
0 2 * * * cd /path/to/PullData && python3 sync.py -e sys_log audit_log >> sync.log 2>&1
```

### 3. 动态排除表

根据条件动态生成排除表列表:

```bash
#!/bin/bash
# 排除所有以 log 结尾的表
EXCLUDE_TABLES=$(mysql -h 10.11.252.103 -P 33306 -u root -p'password' -D meicloud_plm -e "SHOW TABLES LIKE '%_log'" | tail -n +2 | tr '\n' ' ')
python3 sync.py --exclude $EXCLUDE_TABLES
```

## 📚 相关文档

- [README.md](README.md) - 完整功能文档
- [QUICKSTART.md](QUICKSTART.md) - 快速开始指南

## ❓ 常见问题

### Q1: 如何查看数据库中所有的表?

A: 运行一次不带参数的命令:
```bash
python3 sync.py
```

### Q2: 排除的表会被删除吗?

A: 不会,排除只是不同步这些表,不会对源库和目标库的表进行任何删除操作。

### Q3: 可以排除多少个表?

A: 没有限制,可以排除任意数量的表。

### Q4: 排除表后,checkpoint 会受影响吗?

A: 不会,排除的表不会更新 checkpoint,已同步的表的 checkpoint 保持不变。

### Q5: 如何恢复被排除的表?

A: 直接指定表名同步即可:
```bash
python3 sync.py --tables sys_log --full
```

## 🎉 总结

**排除表功能**为您提供了更灵活的同步策略,可以:

✅ 大幅减少同步时间(最高可节省 60%-70%)  
✅ 节省存储空间和网络带宽  
✅ 提高同步效率  
✅ 灵活控制同步范围  

**核心命令**:
```bash
# 排除日志表和临时表
python3 sync.py --exclude sys_log sys_temp audit_log
```

**记住**: 合理使用排除功能,可以让您的数据同步更高效!
