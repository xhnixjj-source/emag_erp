"""Profit calculation engine - pure calculation logic without side effects"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, getcontext
from typing import Optional

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

