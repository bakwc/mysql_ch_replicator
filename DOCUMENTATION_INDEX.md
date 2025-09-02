# Documentation Index

## 📚 MySQL ClickHouse Replicator Documentation Guide

**Quick Navigation**: This index helps you find the right documentation for your needs.

---

## 🎯 For Developers

### **ACTIVE_TASKS.md** - Current Development Work
**Purpose**: Day-to-day task management and sprint planning  
**Use When**: You need to know what to work on next, check sprint progress, or assign tasks  
**Contains**: Active bugs, sprint planning, daily standup info, risk assessment

### **tests/CLAUDE.md** - Complete Testing Guide  
**Purpose**: Comprehensive testing documentation and development patterns  
**Use When**: Writing new tests, debugging test failures, understanding test patterns, test infrastructure  
**Contains**: Test patterns, Phase 1.75 methodology, dynamic isolation, test suite structure, recent fixes

---

## 📊 For Project Management

### **TEST_ANALYSIS.md** - Technical Analysis Report
**Purpose**: Detailed technical analysis of current test failures  
**Use When**: Understanding root causes, prioritizing fixes, technical decision making  
**Contains**: Failure analysis, fix strategies, success metrics, resource planning

### **TESTING_GUIDE.md** - Comprehensive Testing Best Practices
**Purpose**: Complete testing guide with current best practices and recent major fixes  
**Use When**: Understanding testing methodology, applying best practices, debugging test issues  
**Contains**: Testing patterns, binlog isolation fixes, infrastructure improvements, validation approaches

### **TESTING_HISTORY.md** - Historical Test Infrastructure Evolution
**Purpose**: Historical record of completed infrastructure work and lessons learned  
**Use When**: Understanding project evolution, referencing past solutions, architectural decisions  
**Contains**: Completed infrastructure work, fix methodologies, best practices, metrics

---

## 🔧 For System Architecture

### **CLAUDE.md** - Project Overview & Architecture
**Purpose**: High-level project understanding and architecture  
**Use When**: Getting started, understanding system components, deployment info  
**Contains**: Project overview, architecture, testing status, development workflow

### **tests/utils/dynamic_config.py** - Dynamic Isolation System
**Purpose**: Technical implementation of parallel testing infrastructure  
**Use When**: Understanding database isolation, modifying test infrastructure  
**Contains**: Core isolation logic, configuration management, cleanup utilities

---

## 🚀 Quick Start Guide

### New Developer Onboarding:
1. **Start Here**: `README.md` - Project overview and quick start
2. **Development Guide**: `CLAUDE.md` - Architecture and development workflow
3. **Testing Guide**: `tests/CLAUDE.md` - Complete testing documentation  
4. **Best Practices**: `TESTING_GUIDE.md` - Current testing methodology
5. **Historical Context**: `TESTING_HISTORY.md` - Past achievements and evolution

### Bug Investigation:
1. **Testing Guide**: `tests/CLAUDE.md` - Current test infrastructure and recent fixes
2. **Best Practices**: `TESTING_GUIDE.md` - Testing methodology and common patterns
3. **Technical Analysis**: `TEST_ANALYSIS.md` - Understand current failure patterns
4. **Historical Reference**: `TESTING_HISTORY.md` - Check if similar issue was solved before

### Project Management:
1. **Current Status**: `README.md` - Project overview and current capabilities
2. **Technical Analysis**: `TEST_ANALYSIS.md` - Success metrics and current issues
3. **Best Practices**: `TESTING_GUIDE.md` - Current methodology and recent improvements
4. **Historical Context**: `TESTING_HISTORY.md` - Past achievements and trends

---

## 📁 File Relationships

```
README.md ← Project Overview & Quick Start
├── CLAUDE.md ← Development Guide & Architecture
├── tests/CLAUDE.md ← Complete Testing Guide
├── TESTING_GUIDE.md ← Testing Best Practices & Recent Fixes
├── TEST_ANALYSIS.md ← Technical Analysis & Current Issues
└── TESTING_HISTORY.md ← Historical Evolution & Lessons Learned

Specialized Documentation:
├── tests/integration/percona/CLAUDE.md ← Percona-specific testing
└── DOCUMENTATION_INDEX.md ← This navigation guide

Core Infrastructure:
├── tests/utils/dynamic_config.py ← Binlog isolation system
├── tests/integration/test_binlog_isolation_verification.py ← Isolation validation
└── run_tests.sh ← Test execution script
```

---

## 🔄 Document Maintenance

### Update Frequency:
- **tests/CLAUDE.md**: As needed (testing infrastructure changes)
- **TESTING_GUIDE.md**: As needed (methodology improvements)
- **TEST_ANALYSIS.md**: Weekly (after test runs and analysis)
- **TESTING_HISTORY.md**: Monthly (major completions)
- **README.md & CLAUDE.md**: Quarterly (major releases)

### Ownership:
- **tests/CLAUDE.md**: Test Infrastructure Team
- **TESTING_GUIDE.md**: QA Engineer / Test Infrastructure Team
- **TEST_ANALYSIS.md**: QA Engineer / Senior Developer  
- **TESTING_HISTORY.md**: Technical Documentation Team
- **README.md & CLAUDE.md**: Project Manager / Architect

---

## 🎯 Document Purpose Summary

| Document | Primary Audience | Update Frequency | Purpose |
|----------|------------------|------------------|---------|
| `README.md` | All users | Quarterly | Project overview, quick start |
| `CLAUDE.md` | Developers | Quarterly | Development guide, architecture |
| `tests/CLAUDE.md` | Test developers | As needed | Complete testing infrastructure guide |
| `TESTING_GUIDE.md` | QA, Developers | As needed | Testing methodology, best practices |
| `TEST_ANALYSIS.md` | Tech Lead, Architects | Weekly | Technical analysis, current issues |
| `TESTING_HISTORY.md` | All team members | Monthly | Historical evolution, lessons learned |

---

**Last Updated**: September 2, 2025  
**Next Review**: October 1, 2025  
**Maintained By**: Technical Documentation Team

---

## Recent Consolidation (September 2, 2025)

**Removed Files** (consolidated into remaining documentation):
- `tests/TODO.md` → Content moved to `TESTING_GUIDE.md`
- `tests/README.md` → Content consolidated into `tests/CLAUDE.md`
- `tests/TESTING_HISTORY.md` → Duplicate of root `TESTING_HISTORY.md`
- `tests/TASKLIST.md` → Issues resolved, content moved to `TESTING_GUIDE.md`

**Result**: Cleaner documentation structure with comprehensive, non-duplicate guides focused on current best practices.