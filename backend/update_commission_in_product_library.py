"""
更新产品库（ProfitCalculation）中的佣金率
根据类目名称从 CommissionConfig 中获取最新的佣金率并更新
只更新非手动设置的佣金（commission_source != 'manual'）
"""
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.config import config
from app.database import SessionLocal
from app.models.listing import ProfitCalculation
from app.models.profit_config_models import CommissionConfig
from datetime import datetime


def update_commission_in_product_library():
    """更新产品库中所有产品的佣金率"""
    
    with SessionLocal() as db:
        print("开始更新产品库中的佣金率...")
        
        # 1. 查询所有有类目名称的 ProfitCalculation 记录
        profit_calcs = db.query(ProfitCalculation).filter(
            ProfitCalculation.category_name.isnot(None),
            ProfitCalculation.category_name != ''
        ).all()
        
        print(f"找到 {len(profit_calcs)} 条有类目名称的产品记录")
        
        # 2. 统计信息
        updated_count = 0
        skipped_manual_count = 0
        no_config_count = 0
        no_category_count = 0
        
        # 3. 遍历每个产品，更新佣金率
        for calc in profit_calcs:
            if not calc.category_name:
                no_category_count += 1
                continue
            
            # 如果佣金是手动设置的，跳过
            if calc.commission_source == 'manual':
                skipped_manual_count += 1
                continue
            
            # 从 CommissionConfig 中获取最新的佣金率（支持大小写不敏感匹配）
            # 先尝试精确匹配
            commission_config = db.query(CommissionConfig).filter(
                CommissionConfig.category_or_group == calc.category_name,
                CommissionConfig.site == 'emag_ro',
                CommissionConfig.effective_to.is_(None)  # 只查询当前生效的
            ).order_by(CommissionConfig.effective_from.desc()).first()
            
            # 如果精确匹配失败，尝试大小写不敏感匹配（SQLite 使用 LOWER 函数）
            if not commission_config:
                from sqlalchemy import func
                commission_config = db.query(CommissionConfig).filter(
                    func.lower(CommissionConfig.category_or_group) == func.lower(calc.category_name),
                    CommissionConfig.site == 'emag_ro',
                    CommissionConfig.effective_to.is_(None)
                ).order_by(CommissionConfig.effective_from.desc()).first()
            
            if not commission_config:
                no_config_count += 1
                print(f"  - 产品 ID {calc.id} (类目: {calc.category_name}): 未找到佣金配置")
                continue
            
            # 转换为百分比格式（配置表中存储的是小数格式，如 0.15）
            new_commission_rate = float(commission_config.commission_rate * 100)
            
            # 检查是否需要更新（强制更新所有非手动设置的佣金）
            old_commission_rate = calc.platform_commission
            # 如果佣金率相同且来源已经是 'default'，可以跳过
            if (old_commission_rate is not None and 
                abs(old_commission_rate - new_commission_rate) < 0.01 and 
                calc.commission_source == 'default'):
                # 佣金率相同且已经是默认来源，不需要更新
                continue
            
            # 更新佣金率
            calc.platform_commission = new_commission_rate
            calc.commission_source = 'default'
            calc.commission_last_updated_at = datetime.utcnow()
            
            updated_count += 1
            print(f"  - 产品 ID {calc.id} (类目: {calc.category_name}): {old_commission_rate}% -> {new_commission_rate}%")
        
        # 4. 提交事务
        try:
            db.commit()
            print(f"\n更新完成:")
            print(f"  - 成功更新: {updated_count} 个产品")
            print(f"  - 跳过手动设置: {skipped_manual_count} 个产品")
            print(f"  - 未找到配置: {no_config_count} 个产品")
            print(f"  - 无类目名称: {no_category_count} 个产品")
            print(f"  - 总计处理: {len(profit_calcs)} 个产品")
        except Exception as e:
            db.rollback()
            print(f"\n更新失败: {e}")
            raise


if __name__ == "__main__":
    update_commission_in_product_library()

