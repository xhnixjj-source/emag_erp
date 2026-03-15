"""eMAG Marketplace API sync models"""
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float, Text, Date, Index, UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.database import Base


class EmagAccount(Base):
    """eMAG API account configuration (system-wide, single account)"""
    __tablename__ = "emag_account"
    
    id = Column(Integer, primary_key=True, index=True)
    platform = Column(String(50), nullable=False, unique=True)  # ro, bg, hu, fashiondays-ro, fashiondays-bg
    username = Column(String(255), nullable=False)
    password = Column(String(255), nullable=False)  # Plain text storage as per requirements
    base_url = Column(String(255), nullable=False)  # API base URL
    is_active = Column(Integer, default=1, nullable=False)  # 1=active, 0=inactive
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class EmagProduct(Base):
    """eMAG product information table (includes stock)"""
    __tablename__ = "emag_product"
    
    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, unique=True, nullable=False, index=True)  # eMAG product ID
    pnk_code = Column(String(255), nullable=True, index=True)  # PNK code for search
    ean = Column(String(255), nullable=True, index=True)  # EAN/EAN13 code for search
    part_number = Column(String(255), nullable=True)  # Manufacturer part number
    name = Column(String(255), nullable=True)  # Product name
    brand = Column(String(255), nullable=True)  # Brand name
    category_id = Column(Integer, nullable=True)  # eMAG category ID
    sale_price = Column(Float, nullable=True)  # Sale price (without VAT)
    vat_id = Column(Integer, nullable=True)  # VAT rate ID
    stock = Column(Integer, nullable=True)  # Stock quantity
    status = Column(Integer, nullable=True)  # 1=Active, 0=Inactive, 2=End of Life
    warehouse_id = Column(Integer, nullable=True)  # Warehouse ID
    synced_at = Column(DateTime(timezone=True), nullable=True)  # Last sync time
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # Indexes for search
    __table_args__ = (
        Index('idx_emag_product_pnk', 'pnk_code'),
        Index('idx_emag_product_ean', 'ean'),
        Index('idx_emag_product_product_id', 'product_id'),
    )


class EmagOrder(Base):
    """eMAG order information table (includes order product details)"""
    __tablename__ = "emag_order"
    
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(Integer, nullable=False, index=True)  # eMAG order ID
    order_product_id = Column(Integer, nullable=True)  # Order product ID (from API products[].id)
    product_id = Column(Integer, nullable=True, index=True)  # Product ID (foreign key to emag_product)
    pnk_code = Column(String(255), nullable=True, index=True)  # PNK code (redundant for search)
    ean = Column(String(255), nullable=True, index=True)  # EAN code (redundant for search)
    
    # Order basic information
    order_status = Column(Integer, nullable=True)  # 0=Canceled, 1=New, 2=In Progress, 3=Prepared, 4=Finalized, 5=Returned
    payment_mode_id = Column(Integer, nullable=True)  # 1=COD, 2=Bank, 3=Online Card
    
    # Customer information
    customer_id = Column(Integer, nullable=True)
    customer_name = Column(String(255), nullable=True)
    customer_email = Column(String(255), nullable=True)
    customer_phone = Column(String(50), nullable=True)
    billing_city = Column(String(255), nullable=True)
    shipping_city = Column(String(255), nullable=True)
    
    # Product information in order
    product_name = Column(String(255), nullable=True)  # Product name (redundant)
    quantity = Column(Integer, nullable=True)  # Quantity
    sale_price = Column(Float, nullable=True)  # Unit price
    product_status = Column(Integer, nullable=True)  # Product status
    total_amount = Column(Float, nullable=True)  # Total amount (quantity * sale_price)
    
    # Time fields
    order_date = Column(DateTime(timezone=True), nullable=True, index=True)  # Order creation date/time
    order_updated_at = Column(DateTime(timezone=True), nullable=True)  # Order last update time
    order_finalized_at = Column(DateTime(timezone=True), nullable=True)  # Order finalized time (status=4)
    order_canceled_at = Column(DateTime(timezone=True), nullable=True)  # Order canceled time (status=0)
    
    synced_at = Column(DateTime(timezone=True), nullable=True)  # Sync time
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # Indexes
    __table_args__ = (
        Index('idx_emag_order_product_id', 'product_id'),
        Index('idx_emag_order_pnk', 'pnk_code'),
        Index('idx_emag_order_ean', 'ean'),
        Index('idx_emag_order_order_id', 'order_id'),
        Index('idx_emag_order_order_date', 'order_date'),
        Index('idx_emag_order_status', 'order_status'),
    )


class EmagReturn(Base):
    """eMAG return information table (includes return product details)"""
    __tablename__ = "emag_return"
    
    id = Column(Integer, primary_key=True, index=True)
    rma_id = Column(Integer, unique=True, nullable=False, index=True)  # eMAG RMA ID
    order_id = Column(Integer, nullable=True, index=True)  # Related order ID
    order_product_id = Column(Integer, nullable=True)  # Order product ID
    product_id = Column(Integer, nullable=True, index=True)  # Product ID (foreign key to emag_product)
    pnk_code = Column(String(255), nullable=True, index=True)  # PNK code (redundant for search)
    ean = Column(String(255), nullable=True, index=True)  # EAN code (redundant for search)
    
    # Return basic information
    return_status = Column(Integer, nullable=True)  # 1=New, 2=Acknowledged, 3=Received, 4=Resolved, 5=Rejected
    reason = Column(Text, nullable=True)  # Return reason
    
    # Product information in return
    product_name = Column(String(255), nullable=True)  # Product name (redundant)
    quantity = Column(Integer, nullable=True)  # Return quantity
    sale_price = Column(Float, nullable=True)  # Original unit price
    
    # Time fields
    return_date = Column(DateTime(timezone=True), nullable=True, index=True)  # Return creation date/time
    return_acknowledged_at = Column(DateTime(timezone=True), nullable=True)  # Return acknowledged time (status=2)
    return_received_at = Column(DateTime(timezone=True), nullable=True)  # Return received time (status=3)
    return_resolved_at = Column(DateTime(timezone=True), nullable=True)  # Return resolved time (status=4)
    return_rejected_at = Column(DateTime(timezone=True), nullable=True)  # Return rejected time (status=5)
    return_updated_at = Column(DateTime(timezone=True), nullable=True)  # Return last update time
    
    synced_at = Column(DateTime(timezone=True), nullable=True)  # Sync time
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # Indexes
    __table_args__ = (
        Index('idx_emag_return_product_id', 'product_id'),
        Index('idx_emag_return_pnk', 'pnk_code'),
        Index('idx_emag_return_ean', 'ean'),
        Index('idx_emag_return_rma_id', 'rma_id'),
        Index('idx_emag_return_order_id', 'order_id'),
        Index('idx_emag_return_return_date', 'return_date'),
        Index('idx_emag_return_status', 'return_status'),
    )


class EmagInboundShipment(Base):
    """eMAG inbound shipment (reception) information table"""
    __tablename__ = "emag_inbound_shipment"
    
    id = Column(Integer, primary_key=True, index=True)
    reception_id = Column(Integer, unique=True, nullable=False, index=True)  # eMAG reception ID
    status = Column(String(50), nullable=True, index=True)  # Status: finalized, pending, etc.
    
    # Sync information
    synced_at = Column(DateTime(timezone=True), nullable=True)  # Last sync time
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # Relationships
    details = relationship("EmagInboundShipmentDetail", back_populates="shipment", cascade="all, delete-orphan")
    
    # Indexes
    __table_args__ = (
        Index('idx_emag_inbound_shipment_reception_id', 'reception_id'),
        Index('idx_emag_inbound_shipment_status', 'status'),
    )


class EmagInboundShipmentDetail(Base):
    """eMAG inbound shipment detail (transferred to storage quantity) table"""
    __tablename__ = "emag_inbound_shipment_detail"
    
    id = Column(Integer, primary_key=True, index=True)
    shipment_id = Column(Integer, ForeignKey("emag_inbound_shipment.id"), nullable=False, index=True)
    reception_id = Column(Integer, nullable=False, index=True)  # Redundant for quick lookup
    
    vendor_product_id = Column(Integer, nullable=False, index=True)  # Vendor product ID
    transferred_to_storage_quantity = Column(Integer, nullable=False)  # Quantity transferred to storage
    expiration_date = Column(Date, nullable=True)  # Expiration date (if available)
    producer_lot = Column(String(255), nullable=True)  # Producer lot number (if available)
    
    # Sync information
    synced_at = Column(DateTime(timezone=True), nullable=True)  # Last sync time
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # Relationships
    shipment = relationship("EmagInboundShipment", back_populates="details")
    
    # Indexes
    __table_args__ = (
        Index('idx_emag_inbound_shipment_detail_shipment_id', 'shipment_id'),
        Index('idx_emag_inbound_shipment_detail_reception_id', 'reception_id'),
        Index('idx_emag_inbound_shipment_detail_vendor_product_id', 'vendor_product_id'),
    )
