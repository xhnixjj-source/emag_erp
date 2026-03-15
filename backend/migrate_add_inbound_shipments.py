"""Migration script to add inbound shipment tables"""
from app.database import engine, Base
from app.models.emag_sync import EmagInboundShipment, EmagInboundShipmentDetail

def migrate():
    """Create inbound shipment tables"""
    print("Creating inbound shipment tables...")
    Base.metadata.create_all(bind=engine, tables=[
        EmagInboundShipment.__table__,
        EmagInboundShipmentDetail.__table__
    ])
    print("✅ Inbound shipment tables created successfully!")

if __name__ == "__main__":
    migrate()

