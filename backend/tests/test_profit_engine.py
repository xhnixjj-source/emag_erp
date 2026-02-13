"""Unit tests for ProfitEngine"""
import unittest
from decimal import Decimal
from app.services.profit_engine import (
    ProfitEngine,
    ProfitDecisionThresholds,
    classify_product_status,
    calculate_with_status
)


class TestProfitEngine(unittest.TestCase):
    """Test cases for ProfitEngine"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.sale_price_gross = Decimal("100.00")
        self.purchase_cost = Decimal("50.00")
        self.weight_kg = Decimal("1.0")
        self.length_cm = Decimal("10.0")
        self.width_cm = Decimal("10.0")
        self.height_cm = Decimal("10.0")
        self.vat_rate = Decimal("0.19")  # 19%
        self.commission_rate = Decimal("0.10")  # 10%
        self.shipping_cost_fixed = Decimal("5.00")
        self.order_fee = Decimal("2.00")
        self.storage_fee = Decimal("1.00")
    
    def test_basic_profit_calculation(self):
        """Test basic profit calculation"""
        result = ProfitEngine.calculate_profit(
            sale_price_gross=self.sale_price_gross,
            purchase_cost=self.purchase_cost,
            weight_kg=self.weight_kg,
            length_cm=self.length_cm,
            width_cm=self.width_cm,
            height_cm=self.height_cm,
            vat_rate=self.vat_rate,
            commission_rate=self.commission_rate,
            shipping_cost_fixed=self.shipping_cost_fixed,
            order_fee=self.order_fee,
            storage_fee=self.storage_fee,
        )
        
        # Verify VAT extraction
        expected_sale_price_net = self.sale_price_gross / Decimal("1.19")
        self.assertAlmostEqual(float(result.vat_amount), float(self.sale_price_gross - expected_sale_price_net), places=2)
        
        # Verify commission
        expected_commission = self.sale_price_gross * self.commission_rate
        self.assertAlmostEqual(float(result.commission_amount), float(expected_commission), places=2)
        
        # Verify logistics cost (should be fixed shipping cost)
        self.assertEqual(result.logistics_cost, self.shipping_cost_fixed)
        
        # Verify profit is positive
        self.assertGreater(result.net_profit, Decimal("0"))
    
    def test_profit_with_weight_based_shipping(self):
        """Test profit calculation with weight-based shipping"""
        shipping_price_per_kg = Decimal("3.00")
        
        result = ProfitEngine.calculate_profit(
            sale_price_gross=self.sale_price_gross,
            purchase_cost=self.purchase_cost,
            weight_kg=self.weight_kg,
            length_cm=self.length_cm,
            width_cm=self.width_cm,
            height_cm=self.height_cm,
            vat_rate=self.vat_rate,
            commission_rate=self.commission_rate,
            shipping_cost_fixed=self.shipping_cost_fixed,
            order_fee=self.order_fee,
            storage_fee=self.storage_fee,
            shipping_price_per_kg=shipping_price_per_kg,
        )
        
        # With weight=1kg and dimensions 10x10x10, volumetric weight = 1000/6000 = 0.167kg
        # Chargeable weight = max(1.0, 0.167) = 1.0kg
        # Expected logistics cost = 1.0 * 3.00 = 3.00
        self.assertEqual(result.logistics_cost, Decimal("3.00"))
    
    def test_profit_with_volumetric_weight(self):
        """Test profit calculation when volumetric weight exceeds actual weight"""
        # Large dimensions but light weight
        large_length = Decimal("50.0")
        large_width = Decimal("50.0")
        large_height = Decimal("50.0")
        light_weight = Decimal("0.5")
        shipping_price_per_kg = Decimal("3.00")
        
        result = ProfitEngine.calculate_profit(
            sale_price_gross=self.sale_price_gross,
            purchase_cost=self.purchase_cost,
            weight_kg=light_weight,
            length_cm=large_length,
            width_cm=large_width,
            height_cm=large_height,
            vat_rate=self.vat_rate,
            commission_rate=self.commission_rate,
            shipping_cost_fixed=self.shipping_cost_fixed,
            order_fee=self.order_fee,
            storage_fee=self.storage_fee,
            shipping_price_per_kg=shipping_price_per_kg,
        )
        
        # Volumetric weight = (50 * 50 * 50) / 6000 = 125000 / 6000 = 20.83kg
        # Chargeable weight = max(0.5, 20.83) = 20.83kg
        # Expected logistics cost = 20.83 * 3.00 = 62.49
        expected_logistics = Decimal("20.83333333333333333333333333") * shipping_price_per_kg
        self.assertAlmostEqual(float(result.logistics_cost), float(expected_logistics), places=2)
    
    def test_break_even_price(self):
        """Test break-even price calculation"""
        result = ProfitEngine.calculate_profit(
            sale_price_gross=self.sale_price_gross,
            purchase_cost=self.purchase_cost,
            weight_kg=self.weight_kg,
            length_cm=self.length_cm,
            width_cm=self.width_cm,
            height_cm=self.height_cm,
            vat_rate=self.vat_rate,
            commission_rate=self.commission_rate,
            shipping_cost_fixed=self.shipping_cost_fixed,
            order_fee=self.order_fee,
            storage_fee=self.storage_fee,
        )
        
        # Break-even price should be calculated
        self.assertIsNotNone(result.break_even_price)
        self.assertGreater(result.break_even_price, Decimal("0"))
    
    def test_break_even_price_function(self):
        """Test standalone break-even price calculation"""
        break_even = ProfitEngine.break_even_sale_price(
            purchase_cost=self.purchase_cost,
            weight_kg=self.weight_kg,
            length_cm=self.length_cm,
            width_cm=self.width_cm,
            height_cm=self.height_cm,
            vat_rate=self.vat_rate,
            commission_rate=self.commission_rate,
            shipping_cost_fixed=self.shipping_cost_fixed,
            order_fee=self.order_fee,
            storage_fee=self.storage_fee,
        )
        
        self.assertIsNotNone(break_even)
        self.assertGreater(break_even, Decimal("0"))
    
    def test_max_affordable_cpa(self):
        """Test max affordable CPA calculation"""
        max_cpa = ProfitEngine.max_affordable_cpa(
            sale_price_gross=self.sale_price_gross,
            purchase_cost=self.purchase_cost,
            weight_kg=self.weight_kg,
            length_cm=self.length_cm,
            width_cm=self.width_cm,
            height_cm=self.height_cm,
            vat_rate=self.vat_rate,
            commission_rate=self.commission_rate,
            shipping_cost_fixed=self.shipping_cost_fixed,
            order_fee=self.order_fee,
            storage_fee=self.storage_fee,
        )
        
        # Max CPA should equal net profit
        result = ProfitEngine.calculate_profit(
            sale_price_gross=self.sale_price_gross,
            purchase_cost=self.purchase_cost,
            weight_kg=self.weight_kg,
            length_cm=self.length_cm,
            width_cm=self.width_cm,
            height_cm=self.height_cm,
            vat_rate=self.vat_rate,
            commission_rate=self.commission_rate,
            shipping_cost_fixed=self.shipping_cost_fixed,
            order_fee=self.order_fee,
            storage_fee=self.storage_fee,
        )
        
        self.assertAlmostEqual(float(max_cpa), float(result.net_profit), places=2)
    
    def test_recalculate_with_sale_price(self):
        """Test recalculating profit with different sale price"""
        new_price = Decimal("120.00")
        
        result = ProfitEngine.recalculate_with_sale_price(
            new_sale_price_gross=new_price,
            purchase_cost=self.purchase_cost,
            weight_kg=self.weight_kg,
            length_cm=self.length_cm,
            width_cm=self.width_cm,
            height_cm=self.height_cm,
            vat_rate=self.vat_rate,
            commission_rate=self.commission_rate,
            shipping_cost_fixed=self.shipping_cost_fixed,
            order_fee=self.order_fee,
            storage_fee=self.storage_fee,
        )
        
        # Profit should be higher with higher price
        original_result = ProfitEngine.calculate_profit(
            sale_price_gross=self.sale_price_gross,
            purchase_cost=self.purchase_cost,
            weight_kg=self.weight_kg,
            length_cm=self.length_cm,
            width_cm=self.width_cm,
            height_cm=self.height_cm,
            vat_rate=self.vat_rate,
            commission_rate=self.commission_rate,
            shipping_cost_fixed=self.shipping_cost_fixed,
            order_fee=self.order_fee,
            storage_fee=self.storage_fee,
        )
        
        self.assertGreater(result.net_profit, original_result.net_profit)
    
    def test_negative_profit(self):
        """Test calculation with negative profit scenario"""
        high_purchase_cost = Decimal("200.00")
        
        result = ProfitEngine.calculate_profit(
            sale_price_gross=self.sale_price_gross,
            purchase_cost=high_purchase_cost,
            weight_kg=self.weight_kg,
            length_cm=self.length_cm,
            width_cm=self.width_cm,
            height_cm=self.height_cm,
            vat_rate=self.vat_rate,
            commission_rate=self.commission_rate,
            shipping_cost_fixed=self.shipping_cost_fixed,
            order_fee=self.order_fee,
            storage_fee=self.storage_fee,
        )
        
        # Profit should be negative
        self.assertLess(result.net_profit, Decimal("0"))
        self.assertLess(result.profit_margin, Decimal("0"))
    
    def test_zero_profit(self):
        """Test calculation at break-even point"""
        # Use break-even price as sale price
        break_even_price = ProfitEngine.break_even_sale_price(
            purchase_cost=self.purchase_cost,
            weight_kg=self.weight_kg,
            length_cm=self.length_cm,
            width_cm=self.width_cm,
            height_cm=self.height_cm,
            vat_rate=self.vat_rate,
            commission_rate=self.commission_rate,
            shipping_cost_fixed=self.shipping_cost_fixed,
            order_fee=self.order_fee,
            storage_fee=self.storage_fee,
        )
        
        if break_even_price:
            result = ProfitEngine.calculate_profit(
                sale_price_gross=break_even_price,
                purchase_cost=self.purchase_cost,
                weight_kg=self.weight_kg,
                length_cm=self.length_cm,
                width_cm=self.width_cm,
                height_cm=self.height_cm,
                vat_rate=self.vat_rate,
                commission_rate=self.commission_rate,
                shipping_cost_fixed=self.shipping_cost_fixed,
                order_fee=self.order_fee,
                storage_fee=self.storage_fee,
            )
            
            # Profit should be approximately zero (allowing for rounding)
            self.assertAlmostEqual(float(result.net_profit), 0.0, places=1)


class TestProductStatusClassification(unittest.TestCase):
    """Test cases for product status classification"""
    
    def test_profitable_status(self):
        """Test classification of profitable products"""
        thresholds = ProfitDecisionThresholds()
        status = classify_product_status(Decimal("0.30"), thresholds)
        self.assertEqual(status, "profitable")
    
    def test_risky_status(self):
        """Test classification of risky products"""
        thresholds = ProfitDecisionThresholds()
        status = classify_product_status(Decimal("0.15"), thresholds)
        self.assertEqual(status, "risky")
    
    def test_break_even_status(self):
        """Test classification of break-even products"""
        thresholds = ProfitDecisionThresholds()
        status = classify_product_status(Decimal("0.05"), thresholds)
        self.assertEqual(status, "break_even")
    
    def test_not_viable_status(self):
        """Test classification of not viable products"""
        thresholds = ProfitDecisionThresholds()
        status = classify_product_status(Decimal("-0.10"), thresholds)
        self.assertEqual(status, "not_viable")
    
    def test_calculate_with_status(self):
        """Test calculate_with_status wrapper"""
        result = calculate_with_status(
            sale_price_gross=Decimal("100.00"),
            purchase_cost=Decimal("50.00"),
            weight_kg=Decimal("1.0"),
            length_cm=Decimal("10.0"),
            width_cm=Decimal("10.0"),
            height_cm=Decimal("10.0"),
            vat_rate=Decimal("0.19"),
            commission_rate=Decimal("0.10"),
            shipping_cost_fixed=Decimal("5.00"),
            order_fee=Decimal("2.00"),
            storage_fee=Decimal("1.00"),
        )
        
        self.assertIsNotNone(result.profit)
        self.assertIn(result.product_status, ["profitable", "risky", "break_even", "not_viable"])


if __name__ == '__main__':
    unittest.main()

