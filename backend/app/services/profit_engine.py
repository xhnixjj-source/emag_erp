"""Profit calculation engine - pure calculation logic without side effects"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, getcontext
from typing import Optional, List

# Set a reasonable precision for financial calculations
getcontext().prec = 28


@dataclass
class ProfitResult:
    """Profit calculation result"""
    net_profit: Decimal
    profit_margin: Decimal          # net_profit / sale_price_net
    roi: Decimal                    # net_profit / total_cost (purchase + logistics + commission)
    logistics_cost: Decimal
    vat_amount: Decimal
    commission_amount: Decimal
    break_even_price: Optional[Decimal]  # sale price (VAT included) needed for net_profit = 0


@dataclass
class EnhancedProfitResult:
    """Enhanced profit calculation result with all cost items and two margin types"""
    revenue_ex_vat_rmb: Decimal      # 不含VAT销售额（人民币）
    revenue_inc_vat_rmb: Decimal     # 含VAT销售额（人民币）
    volumetric_weight_kg: Decimal    # 体积重（公斤）
    chargeable_weight_kg: Decimal     # 计费重（公斤）
    first_leg_logistics_cost_rmb: Decimal  # 头程物流费（人民币）
    commission_fee_ron: Decimal      # 平台佣金（列伊）
    commission_fee_rmb: Decimal      # 平台佣金（人民币）
    genius_fee_ron: Decimal          # Genius费用（列伊）
    genius_fee_rmb: Decimal          # Genius费用（人民币）
    order_handling_fee_rmb: Decimal   # 订单处理费（人民币）
    storage_fee_rmb: Decimal         # 仓储费（人民币）
    profit_rmb: Decimal              # 利润（人民币）
    margin_ex_vat: Decimal           # 利润率（去除VAT）
    margin_inc_vat: Decimal          # 利润率（含VAT）


@dataclass
class ProfitDecisionThresholds:
    """Thresholds for product status classification"""
    profitable_min_margin: Decimal = Decimal("0.25")
    risky_min_margin: Decimal = Decimal("0.10")
    break_even_min_margin: Decimal = Decimal("0.0")
    
    def __post_init__(self) -> None:
        """Validate thresholds are ordered correctly"""
        if not (
            self.profitable_min_margin > self.risky_min_margin
            > self.break_even_min_margin
        ):
            raise ValueError("Thresholds must satisfy: profitable > risky > break_even")


@dataclass
class ProfitDecisionResult:
    """Profit calculation result with product status"""
    profit: ProfitResult
    product_status: str  # "profitable", "risky", "break_even", "not_viable"


class GeniusRuleDomain:
    """Genius 费用规则领域对象"""
    def __init__(self, steps: List):
        """
        Args:
            steps: GeniusRuleStep 对象列表，每个step包含 min_sales_amount, max_sales_amount, fee_amount
        """
        # 按最小销售额排序
        self.steps = sorted(steps, key=lambda x: x.min_sales_amount)
    
    def calc_fee(self, sales_amount_ron: Decimal) -> Decimal:
        """
        根据销售额计算 genius 费用
        
        Args:
            sales_amount_ron: 销售额（列伊）
        
        Returns:
            genius费用（列伊）
        """
        for step in self.steps:
            if sales_amount_ron >= Decimal(str(step.min_sales_amount)):
                if step.max_sales_amount is None:
                    # 无上限，直接返回
                    return Decimal(str(step.fee_amount))
                elif sales_amount_ron < Decimal(str(step.max_sales_amount)):
                    # 在区间内
                    return Decimal(str(step.fee_amount))
        return Decimal("0")


class ProfitEngine:
    """Pure profit calculation engine - no side effects, deterministic"""
    
    @staticmethod
    def _compute_logistics_cost(
        weight_kg: Decimal,
        length_cm: Decimal,
        width_cm: Decimal,
        height_cm: Decimal,
        shipping_price_per_kg: Optional[Decimal],
        shipping_cost_fixed: Decimal,
    ) -> Decimal:
        """
        Compute logistics cost based on chargeable weight (max of actual and volumetric)
        If shipping_price_per_kg is provided, use weight-based calculation,
        otherwise use fixed shipping_cost_fixed
        """
        if shipping_price_per_kg is not None and shipping_price_per_kg > 0:
            # Calculate volumetric weight: (L * W * H) / 6000
            volumetric_weight = (length_cm * width_cm * height_cm) / Decimal("6000")
            chargeable_weight = max(weight_kg, volumetric_weight)
            return chargeable_weight * shipping_price_per_kg
        else:
            # Use fixed shipping cost
            return shipping_cost_fixed

    @staticmethod
    def _compute_break_even_price_core(
        purchase_cost: Decimal,
        logistics_cost: Decimal,
        order_fee: Decimal,
        storage_fee: Decimal,
        vat_rate: Decimal,
        commission_rate: Decimal,
    ) -> Optional[Decimal]:
        """
        Calculate break-even sale price (VAT included) analytically
        Equation: P_gross/(1+v) - purchase_cost - logistics_cost - order_fee - storage_fee - P_gross*commission_rate = 0
        => P_gross * (1/(1+v) - commission_rate) = purchase_cost + logistics_cost + order_fee + storage_fee
        => P_gross = (purchase_cost + logistics_cost + order_fee + storage_fee) / (1/(1+v) - commission_rate)
        """
        one = Decimal("1")
        vat_multiplier = one + vat_rate
        denominator = (one / vat_multiplier) - commission_rate
        if denominator <= Decimal("0"):
            return None  # no finite break-even under these parameters
        total_fixed_costs = purchase_cost + logistics_cost + order_fee + storage_fee
        return total_fixed_costs / denominator

    @staticmethod
    def calculate_profit(
        sale_price_gross: Decimal,      # customer price, VAT included
        purchase_cost: Decimal,         # cost of goods, VAT already handled upstream
        weight_kg: Decimal,
        length_cm: Decimal,
        width_cm: Decimal,
        height_cm: Decimal,
        vat_rate: Decimal,              # e.g. 0.19 for 19%
        commission_rate: Decimal,       # e.g. 0.10 for 10%, applied on sale_price_gross
        shipping_cost_fixed: Decimal,   # fixed shipping cost per order
        order_fee: Decimal,             # order processing fee
        storage_fee: Decimal,            # storage fee
        shipping_price_per_kg: Optional[Decimal] = None,  # optional weight-based shipping rate
    ) -> ProfitResult:
        """
        Calculate profit metrics for a product sale
        
        Args:
            sale_price_gross: Customer-facing price including VAT
            purchase_cost: Cost of goods purchased
            weight_kg: Product weight in kg
            length_cm, width_cm, height_cm: Product dimensions in cm
            vat_rate: VAT rate as decimal (e.g. 0.19 for 19%)
            commission_rate: Platform commission rate as decimal (e.g. 0.10 for 10%)
            shipping_cost_fixed: Fixed shipping cost per order
            order_fee: Order processing fee
            storage_fee: Storage fee
            shipping_price_per_kg: Optional weight-based shipping rate (€/kg)
        
        Returns:
            ProfitResult with all calculated metrics
        """
        one = Decimal("1")
        vat_multiplier = one + vat_rate

        # Extract VAT from gross sale price
        sale_price_net = sale_price_gross / vat_multiplier
        vat_amount = sale_price_gross - sale_price_net

        # Compute logistics cost
        logistics_cost = ProfitEngine._compute_logistics_cost(
            weight_kg=weight_kg,
            length_cm=length_cm,
            width_cm=width_cm,
            height_cm=height_cm,
            shipping_price_per_kg=shipping_price_per_kg,
            shipping_cost_fixed=shipping_cost_fixed,
        )

        # Commission is calculated on gross sale price
        commission_amount = sale_price_gross * commission_rate

        # Revenue considered net of VAT; VAT is treated as pass-through
        revenue_net = sale_price_net
        
        # Total costs include all expenses
        total_cost = purchase_cost + logistics_cost + order_fee + storage_fee + commission_amount

        net_profit = revenue_net - total_cost

        # Calculate profit margin (net_profit / revenue_net)
        profit_margin = (
            net_profit / revenue_net if revenue_net > Decimal("0") else Decimal("0")
        )
        
        # Calculate ROI (net_profit / total_cost)
        roi = (
            net_profit / total_cost if total_cost > Decimal("0") else Decimal("0")
        )

        # Calculate break-even price
        break_even_price = ProfitEngine._compute_break_even_price_core(
            purchase_cost=purchase_cost,
            logistics_cost=logistics_cost,
            order_fee=order_fee,
            storage_fee=storage_fee,
            vat_rate=vat_rate,
            commission_rate=commission_rate,
        )

        return ProfitResult(
            net_profit=net_profit,
            profit_margin=profit_margin,
            roi=roi,
            logistics_cost=logistics_cost,
            vat_amount=vat_amount,
            commission_amount=commission_amount,
            break_even_price=break_even_price,
        )

    # --- Simulation helpers ---

    @staticmethod
    def recalculate_with_sale_price(
        new_sale_price_gross: Decimal,
        purchase_cost: Decimal,
        weight_kg: Decimal,
        length_cm: Decimal,
        width_cm: Decimal,
        height_cm: Decimal,
        vat_rate: Decimal,
        commission_rate: Decimal,
        shipping_cost_fixed: Decimal,
        order_fee: Decimal,
        storage_fee: Decimal,
        shipping_price_per_kg: Optional[Decimal] = None,
    ) -> ProfitResult:
        """
        Recalculate profit metrics for the same product and cost structure
        under a different sale price.
        """
        return ProfitEngine.calculate_profit(
            sale_price_gross=new_sale_price_gross,
            purchase_cost=purchase_cost,
            weight_kg=weight_kg,
            length_cm=length_cm,
            width_cm=width_cm,
            height_cm=height_cm,
            vat_rate=vat_rate,
            commission_rate=commission_rate,
            shipping_cost_fixed=shipping_cost_fixed,
            order_fee=order_fee,
            storage_fee=storage_fee,
            shipping_price_per_kg=shipping_price_per_kg,
        )

    @staticmethod
    def max_affordable_cpa(
        sale_price_gross: Decimal,
        purchase_cost: Decimal,
        weight_kg: Decimal,
        length_cm: Decimal,
        width_cm: Decimal,
        height_cm: Decimal,
        vat_rate: Decimal,
        commission_rate: Decimal,
        shipping_cost_fixed: Decimal,
        order_fee: Decimal,
        storage_fee: Decimal,
        shipping_price_per_kg: Optional[Decimal] = None,
    ) -> Decimal:
        """
        Maximum cost-per-acquisition (advertising spend per sale) such that
        profit after ads is zero. This is equal to profit before ads.
        """
        result = ProfitEngine.calculate_profit(
            sale_price_gross=sale_price_gross,
            purchase_cost=purchase_cost,
            weight_kg=weight_kg,
            length_cm=length_cm,
            width_cm=width_cm,
            height_cm=height_cm,
            vat_rate=vat_rate,
            commission_rate=commission_rate,
            shipping_cost_fixed=shipping_cost_fixed,
            order_fee=order_fee,
            storage_fee=storage_fee,
            shipping_price_per_kg=shipping_price_per_kg,
        )
        return result.net_profit

    @staticmethod
    def break_even_sale_price(
        purchase_cost: Decimal,
        weight_kg: Decimal,
        length_cm: Decimal,
        width_cm: Decimal,
        height_cm: Decimal,
        vat_rate: Decimal,
        commission_rate: Decimal,
        shipping_cost_fixed: Decimal,
        order_fee: Decimal,
        storage_fee: Decimal,
        shipping_price_per_kg: Optional[Decimal] = None,
    ) -> Optional[Decimal]:
        """
        Analytic break-even sale price (VAT included) such that net_profit = 0.
        """
        logistics_cost = ProfitEngine._compute_logistics_cost(
            weight_kg=weight_kg,
            length_cm=length_cm,
            width_cm=width_cm,
            height_cm=height_cm,
            shipping_price_per_kg=shipping_price_per_kg,
            shipping_cost_fixed=shipping_cost_fixed,
        )
        return ProfitEngine._compute_break_even_price_core(
            purchase_cost=purchase_cost,
            logistics_cost=logistics_cost,
            order_fee=order_fee,
            storage_fee=storage_fee,
            vat_rate=vat_rate,
            commission_rate=commission_rate,
        )
    
    @staticmethod
    def calculate_profit_enhanced(
        sale_price_gross_ron: Decimal,  # 前端售价（列伊，含VAT）
        purchase_price_rmb: Decimal,  # 采购价（人民币）
        weight_kg: Decimal,
        length_cm: Decimal,
        width_cm: Decimal,
        height_cm: Decimal,
        vat_rate: Decimal,  # 小数格式，如 0.21
        commission_rate: Decimal,  # 小数格式，如 0.15
        exchange_rate: Decimal,  # 1 RON = exchange_rate CNY
        logistics_price_per_kg_rmb: Decimal,  # 物流单价（人民币/公斤）
        packaging_cost_rmb: Decimal,  # 包材成本（人民币）
        participate_genius: bool,
        genius_rule: Optional[GeniusRuleDomain],
        order_handling_fee_rmb: Decimal,
        storage_fee_rmb: Decimal,
    ) -> EnhancedProfitResult:
        """
        增强版利润计算，支持所有成本项和两种利润率
        
        Args:
            sale_price_gross_ron: 前端售价（列伊，含VAT）
            purchase_price_rmb: 采购价（人民币）
            weight_kg: 重量（公斤）
            length_cm, width_cm, height_cm: 尺寸（厘米）
            vat_rate: VAT率（小数格式，如 0.21）
            commission_rate: 佣金率（小数格式，如 0.15）
            exchange_rate: 汇率（1 RON = exchange_rate CNY）
            logistics_price_per_kg_rmb: 物流单价（人民币/公斤）
            packaging_cost_rmb: 包材成本（人民币）
            participate_genius: 是否参与genius
            genius_rule: Genius规则对象
            order_handling_fee_rmb: 订单处理费（人民币）
            storage_fee_rmb: 仓储费（人民币）
        
        Returns:
            EnhancedProfitResult: 包含所有中间项和最终结果
        """
        one = Decimal("1")
        
        # 收入计算
        revenue_inc_vat_rmb = sale_price_gross_ron * exchange_rate
        revenue_ex_vat_rmb = sale_price_gross_ron * (one - vat_rate) * exchange_rate
        
        # 体积重和计费重
        # 体积重 = (长 * 宽 * 高) / 6000，单位：公斤
        # 公式说明：如果尺寸单位是cm，体积重 = (L*W*H) / 6000，结果单位是kg
        # 例如：30cm * 20cm * 10cm = 6000 cm³，6000 / 6000 = 1 kg
        volumetric_weight_kg = (length_cm * width_cm * height_cm) / Decimal("6000")
        chargeable_weight_kg = max(volumetric_weight_kg, weight_kg)
        
        # 头程物流费
        first_leg_logistics_cost_rmb = chargeable_weight_kg * logistics_price_per_kg_rmb
        
        # 平台佣金
        commission_fee_ron = sale_price_gross_ron * commission_rate
        commission_fee_rmb = commission_fee_ron * exchange_rate
        
        # Genius 费用
        genius_fee_ron = Decimal("0")
        if participate_genius and genius_rule:
            genius_fee_ron = genius_rule.calc_fee(sale_price_gross_ron)
        genius_fee_rmb = genius_fee_ron * exchange_rate
        
        # 利润
        profit_rmb = (
            revenue_ex_vat_rmb
            - commission_fee_rmb
            - purchase_price_rmb
            - packaging_cost_rmb
            - first_leg_logistics_cost_rmb
            - genius_fee_rmb
            - order_handling_fee_rmb
            - storage_fee_rmb
        )
        
        # 两种利润率
        margin_ex_vat = profit_rmb / revenue_ex_vat_rmb if revenue_ex_vat_rmb > 0 else Decimal("0")
        margin_inc_vat = profit_rmb / revenue_inc_vat_rmb if revenue_inc_vat_rmb > 0 else Decimal("0")
        
        return EnhancedProfitResult(
            revenue_ex_vat_rmb=revenue_ex_vat_rmb,
            revenue_inc_vat_rmb=revenue_inc_vat_rmb,
            volumetric_weight_kg=volumetric_weight_kg,
            chargeable_weight_kg=chargeable_weight_kg,
            first_leg_logistics_cost_rmb=first_leg_logistics_cost_rmb,
            commission_fee_ron=commission_fee_ron,
            commission_fee_rmb=commission_fee_rmb,
            genius_fee_ron=genius_fee_ron,
            genius_fee_rmb=genius_fee_rmb,
            order_handling_fee_rmb=order_handling_fee_rmb,
            storage_fee_rmb=storage_fee_rmb,
            profit_rmb=profit_rmb,
            margin_ex_vat=margin_ex_vat,
            margin_inc_vat=margin_inc_vat,
        )


# --- Decision layer functions (lightweight, no duplication of core logic) ---

def classify_product_status(
    profit_margin: Decimal,
    thresholds: ProfitDecisionThresholds,
) -> str:
    """
    Classify product status based on profit margin
    
    Returns:
        "profitable" if margin >= profitable_min_margin
        "risky" if margin >= risky_min_margin
        "break_even" if margin >= break_even_min_margin
        "not_viable" if margin < break_even_min_margin
    """
    if profit_margin >= thresholds.profitable_min_margin:
        return "profitable"
    if profit_margin >= thresholds.risky_min_margin:
        return "risky"
    if profit_margin >= thresholds.break_even_min_margin:
        return "break_even"
    return "not_viable"


def calculate_with_status(
    sale_price_gross: Decimal,
    purchase_cost: Decimal,
    weight_kg: Decimal,
    length_cm: Decimal,
    width_cm: Decimal,
    height_cm: Decimal,
    vat_rate: Decimal,
    commission_rate: Decimal,
    shipping_cost_fixed: Decimal,
    order_fee: Decimal,
    storage_fee: Decimal,
    shipping_price_per_kg: Optional[Decimal] = None,
    thresholds: Optional[ProfitDecisionThresholds] = None,
) -> ProfitDecisionResult:
    """
    Thin wrapper on top of ProfitEngine that adds a product_status
    classification using configurable thresholds.
    """
    if thresholds is None:
        thresholds = ProfitDecisionThresholds()

    profit_result = ProfitEngine.calculate_profit(
        sale_price_gross=sale_price_gross,
        purchase_cost=purchase_cost,
        weight_kg=weight_kg,
        length_cm=length_cm,
        width_cm=width_cm,
        height_cm=height_cm,
        vat_rate=vat_rate,
        commission_rate=commission_rate,
        shipping_cost_fixed=shipping_cost_fixed,
        order_fee=order_fee,
        storage_fee=storage_fee,
        shipping_price_per_kg=shipping_price_per_kg,
    )

    status = classify_product_status(
        profit_margin=profit_result.profit_margin,
        thresholds=thresholds,
    )

    return ProfitDecisionResult(
        profit=profit_result,
        product_status=status,
    )

