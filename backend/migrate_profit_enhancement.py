"""
数据库迁移脚本：利润测算功能增强
1. 创建所有新配置表
2. 为 ProfitCalculation 表新增字段
3. 初始化默认配置数据
4. 对现有数据进行反查迁移
"""
import os
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from app.config import config
from app.database import Base, SessionLocal

# 确保模型已加载
from app.models.listing import ProfitCalculation
from app.models.profit_config_models import (
    LogisticsPrice,
    VatConfig,
    ExchangeRate,
    GeniusRule,
    GeniusRuleStep,
    PackagingTemplate,
    CommissionConfig,
    FeeTemplate
)
from app.models.monitor_pool import MonitorPool
from app.models.product import FilterPool
from app.services.product_info_service import (
    get_product_info_from_listing,
    get_commission_from_category
)


def migrate_database():
    """执行数据库迁移"""
    engine = create_engine(config.DATABASE_URL)
    Base.metadata.create_all(engine)
    
    print("数据库已初始化，如果不存在时会自动创建表结构")
    
    with SessionLocal() as db:
        inspector = inspect(engine)
        
        # 检查 profit_calculation 表是否存在
        if 'profit_calculation' not in inspector.get_table_names():
            print("错误: profit_calculation 表不存在")
            return
        
        columns = inspector.get_columns('profit_calculation')
        column_names = [col['name'] for col in columns]
        
        # ========== 扩展 ProfitCalculation 表字段 ==========
        
        # 佣金相关字段
        if 'commission_source' not in column_names:
            print("添加 commission_source 字段...")
            try:
                db.execute(text("ALTER TABLE profit_calculation ADD COLUMN commission_source VARCHAR"))
                db.commit()
                print("[OK] commission_source 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("commission_source 字段已存在，跳过")
        
        if 'commission_last_updated_at' not in column_names:
            print("添加 commission_last_updated_at 字段...")
            try:
                db.execute(text("ALTER TABLE profit_calculation ADD COLUMN commission_last_updated_at TIMESTAMP"))
                db.commit()
                print("[OK] commission_last_updated_at 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("commission_last_updated_at 字段已存在，跳过")
        
        # 价格相关字段
        if 'frontend_price_ron' not in column_names:
            print("添加 frontend_price_ron 字段...")
            try:
                db.execute(text("ALTER TABLE profit_calculation ADD COLUMN frontend_price_ron FLOAT"))
                db.commit()
                print("[OK] frontend_price_ron 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("frontend_price_ron 字段已存在，跳过")
        
        if 'price_source' not in column_names:
            print("添加 price_source 字段...")
            try:
                db.execute(text("ALTER TABLE profit_calculation ADD COLUMN price_source VARCHAR"))
                db.commit()
                print("[OK] price_source 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("price_source 字段已存在，跳过")
        
        if 'price_last_updated_at' not in column_names:
            print("添加 price_last_updated_at 字段...")
            try:
                db.execute(text("ALTER TABLE profit_calculation ADD COLUMN price_last_updated_at TIMESTAMP"))
                db.commit()
                print("[OK] price_last_updated_at 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("price_last_updated_at 字段已存在，跳过")
        
        if 'best_price_ron' not in column_names:
            print("添加 best_price_ron 字段...")
            try:
                db.execute(text("ALTER TABLE profit_calculation ADD COLUMN best_price_ron FLOAT"))
                db.commit()
                print("[OK] best_price_ron 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("best_price_ron 字段已存在，跳过")
        
        # 包材与运输字段
        if 'packaging_template_id' not in column_names:
            print("添加 packaging_template_id 字段...")
            try:
                db.execute(text("ALTER TABLE profit_calculation ADD COLUMN packaging_template_id INTEGER"))
                db.commit()
                print("[OK] packaging_template_id 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("packaging_template_id 字段已存在，跳过")
        
        if 'default_transport_mode' not in column_names:
            print("添加 default_transport_mode 字段...")
            try:
                db.execute(text("ALTER TABLE profit_calculation ADD COLUMN default_transport_mode VARCHAR"))
                db.commit()
                print("[OK] default_transport_mode 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("default_transport_mode 字段已存在，跳过")
        
        if 'is_genius_eligible' not in column_names:
            print("添加 is_genius_eligible 字段...")
            try:
                db.execute(text("ALTER TABLE profit_calculation ADD COLUMN is_genius_eligible BOOLEAN DEFAULT 0"))
                db.commit()
                print("[OK] is_genius_eligible 字段已创建")
            except Exception as e:
                print(f"迁移失败: {e}")
                db.rollback()
        else:
            print("is_genius_eligible 字段已存在，跳过")
        
        # ========== 创建配置表 ==========
        
        table_names = inspector.get_table_names()
        
        # 物流单价表
        if 'logistics_price' not in table_names:
            print("创建 logistics_price 表...")
            LogisticsPrice.__table__.create(engine, checkfirst=True)
            print("[OK] logistics_price 表已创建")
        else:
            print("logistics_price 表已存在，跳过")
        
        # VAT 配置表
        if 'vat_config' not in table_names:
            print("创建 vat_config 表...")
            VatConfig.__table__.create(engine, checkfirst=True)
            print("[OK] vat_config 表已创建")
        else:
            print("vat_config 表已存在，跳过")
        
        # 汇率配置表
        if 'exchange_rate' not in table_names:
            print("创建 exchange_rate 表...")
            ExchangeRate.__table__.create(engine, checkfirst=True)
            print("[OK] exchange_rate 表已创建")
        else:
            print("exchange_rate 表已存在，跳过")
        
        # Genius 规则表
        if 'genius_rule' not in table_names:
            print("创建 genius_rule 表...")
            GeniusRule.__table__.create(engine, checkfirst=True)
            print("[OK] genius_rule 表已创建")
        else:
            print("genius_rule 表已存在，跳过")
        
        if 'genius_rule_step' not in table_names:
            print("创建 genius_rule_step 表...")
            GeniusRuleStep.__table__.create(engine, checkfirst=True)
            print("[OK] genius_rule_step 表已创建")
        else:
            print("genius_rule_step 表已存在，跳过")
        
        # 包材配置表
        if 'packaging_template' not in table_names:
            print("创建 packaging_template 表...")
            PackagingTemplate.__table__.create(engine, checkfirst=True)
            print("[OK] packaging_template 表已创建")
        else:
            print("packaging_template 表已存在，跳过")
        
        # 佣金配置表
        if 'commission_config' not in table_names:
            print("创建 commission_config 表...")
            CommissionConfig.__table__.create(engine, checkfirst=True)
            print("[OK] commission_config 表已创建")
        else:
            print("commission_config 表已存在，跳过")
        
        # 费用模板表
        if 'fee_template' not in table_names:
            print("创建 fee_template 表...")
            FeeTemplate.__table__.create(engine, checkfirst=True)
            print("[OK] fee_template 表已创建")
        else:
            print("fee_template 表已存在，跳过")
        
        # ========== 初始化默认配置数据 ==========
        
        from datetime import datetime
        
        # 1. 物流单价
        existing_logistics = db.query(LogisticsPrice).filter(
            LogisticsPrice.transport_mode == 'air',
            LogisticsPrice.effective_to.is_(None)
        ).first()
        if not existing_logistics:
            air_price = LogisticsPrice(
                transport_mode='air',
                price_per_kg_rmb=56.0,
                effective_from=datetime.utcnow(),
                remark='空运单价'
            )
            db.add(air_price)
            print("[OK] 初始化空运物流单价: 56 RMB/kg")
        
        existing_land = db.query(LogisticsPrice).filter(
            LogisticsPrice.transport_mode == 'land',
            LogisticsPrice.effective_to.is_(None)
        ).first()
        if not existing_land:
            land_price = LogisticsPrice(
                transport_mode='land',
                price_per_kg_rmb=20.0,
                effective_from=datetime.utcnow(),
                remark='陆运单价'
            )
            db.add(land_price)
            print("[OK] 初始化陆运物流单价: 20 RMB/kg")
        
        # 2. VAT 配置
        existing_vat = db.query(VatConfig).filter(
            VatConfig.site == 'emag_ro',
            VatConfig.effective_to.is_(None)
        ).first()
        if not existing_vat:
            vat_config = VatConfig(
                site='emag_ro',
                vat_rate=0.21,  # 21%
                effective_from=datetime.utcnow()
            )
            db.add(vat_config)
            print("[OK] 初始化VAT配置: 21%")
        
        # 3. 汇率配置
        existing_rate = db.query(ExchangeRate).filter(
            ExchangeRate.from_currency == 'RON',
            ExchangeRate.to_currency == 'CNY',
            ExchangeRate.effective_to.is_(None)
        ).first()
        if not existing_rate:
            exchange_rate = ExchangeRate(
                from_currency='RON',
                to_currency='CNY',
                rate=1.6,  # 1 RON = 1.6 CNY
                source='manual',
                effective_from=datetime.utcnow()
            )
            db.add(exchange_rate)
            print("[OK] 初始化汇率配置: 1 RON = 1.6 CNY")
        
        # 4. Genius 规则
        existing_genius = db.query(GeniusRule).filter(
            GeniusRule.is_active == True
        ).first()
        if not existing_genius:
            genius_rule = GeniusRule(
                rule_name='默认Genius规则',
                currency='RON',
                is_active=True
            )
            db.add(genius_rule)
            db.flush()
            
            # 创建阶梯
            steps_data = [
                {'min': 0, 'max': 40, 'fee': 3},
                {'min': 40, 'max': 50, 'fee': 4},
                {'min': 50, 'max': 75, 'fee': 5},
                {'min': 75, 'max': None, 'fee': 8},
            ]
            for step_data in steps_data:
                step = GeniusRuleStep(
                    rule_id=genius_rule.id,
                    min_sales_amount=step_data['min'],
                    max_sales_amount=step_data['max'],
                    fee_amount=step_data['fee']
                )
                db.add(step)
            print("[OK] 初始化Genius规则: <40→3, 40-50→4, 50-75→5, ≥75→8")
        
        # 5. 包材配置
        existing_packaging = db.query(PackagingTemplate).filter(
            PackagingTemplate.is_default == True
        ).first()
        if not existing_packaging:
            packaging = PackagingTemplate(
                name='默认包材',
                cost_rmb=0.2,
                is_default=True
            )
            db.add(packaging)
            print("[OK] 初始化包材配置: 0.2 RMB")
        
        db.commit()
        
        # ========== 数据反查迁移 ==========
        
        print("\n开始数据反查迁移...")
        
        # 获取所有 ProfitCalculation 记录
        calcs = db.query(ProfitCalculation).all()
        migrated_count = 0
        
        for calc in calcs:
            updated = False
            
            # 如果 frontend_price_ron 为空，尝试反查
            if not calc.frontend_price_ron:
                product_info = get_product_info_from_listing(calc.listing_pool_id, db)
                if product_info.get('frontend_price_ron'):
                    calc.frontend_price_ron = product_info['frontend_price_ron']
                    calc.price_source = 'crawler'
                    calc.price_last_updated_at = datetime.utcnow()
                    updated = True
            
            # 如果 category_name 为空，尝试反查
            if not calc.category_name:
                product_info = get_product_info_from_listing(calc.listing_pool_id, db)
                if product_info.get('category_name'):
                    calc.category_name = product_info['category_name']
                    updated = True
            
            # 如果 platform_commission 有值但 commission_source 为空，设置为 'manual'
            if calc.platform_commission and not calc.commission_source:
                calc.commission_source = 'manual'
                updated = True
            
            # 如果类目名称存在但佣金为空，尝试自动匹配
            if calc.category_name and not calc.platform_commission:
                auto_commission = get_commission_from_category(calc.category_name, db)
                if auto_commission:
                    calc.platform_commission = auto_commission
                    calc.commission_source = 'default'
                    calc.commission_last_updated_at = datetime.utcnow()
                    updated = True
            
            if updated:
                migrated_count += 1
        
        db.commit()
        print(f"[OK] 数据反查迁移完成，共迁移 {migrated_count} 条记录")
        
        print("\n迁移完成！")


if __name__ == "__main__":
    migrate_database()

