# eMAG Marketplace API Documentation v4.5.1

This document provides a structured overview of the eMAG Marketplace API, optimized for use with AI-powered code editors like Cursor.

## 1. Overview & Authentication

The eMAG Marketplace API allows partners to integrate their CRM/ERP systems to manage products, offers, and orders.

### 1.1 Base URLs

| Platform | Marketplace URL | API Base URL (api-3) | Locale | Currency |
| :--- | :--- | :--- | :--- | :--- |
| **eMAG Romania** | `https://marketplace.emag.ro` | `https://marketplace-api.emag.ro/api-3` | `ro_RO` | `RON` |
| **eMAG Bulgaria** | `https://marketplace.emag.bg` | `https://marketplace-api.emag.bg/api-3` | `bg_BG` | `BGN` / `EUR`* |
| **eMAG Hungary** | `https://marketplace.emag.hu` | `https://marketplace-api.emag.hu/api-3` | `hu_HU` | `HUF` |
| **Fashion Days RO**| `https://marketplace-ro.fashiondays.com` | `https://marketplace-ro-api.fashiondays.com/api-3` | `ro_RO` | `RON` |
| **Fashion Days BG**| `https://marketplace-bg.fashiondays.com` | `https://marketplace-bg-api.fashiondays.com/api-3` | `bg_BG` | `BGN` / `EUR`* |

*\*Starting Jan 1, 2026, Bulgaria uses EUR.*

### 1.2 Authentication

Authentication uses **Basic Auth**.
- **Header**: `Authorization: Basic <base64(username:password)>`
- **Note**: Your IP must be whitelisted in the eMAG Marketplace interface.

### 1.3 Request Format

All requests are **POST** (unless specified otherwise) to:
`MARKETPLACE_API_URL/{resource}/{action}`

**Mandatory POST Key**:
- `data`: Contains the JSON payload for the specific action.

### 1.4 Rate Limiting

- **Order Resources**: Max 12 requests/sec (720/min).
- **Other Resources**: Max 3 requests/sec (180/min) - cumulative.
- **Bulk Save**: Max 50 entities per request (recommended 10-50).

---

## 2. General Conventions

### 2.1 Pagination
Read actions accept:
- `currentPage`: (Default: 1)
- `itemsPerPage`: (Default: 100, Max: 100)

### 2.2 Response Format
Always JSON with:
- `isError`: Boolean
- `messages`: Array of strings
- `results`: Object/Array of data

---

## 3. Product & Offer Management

### 3.1 Categories & Characteristics
Before sending products, you must identify the correct category and its required characteristics.

- **Resource**: `category`
- **Actions**: `read`, `count`
- **Filters**: `id` (Category ID), `language` (EN, RO, HU, BG, etc.)

### 3.2 Publishing Products/Offers
- **Resource**: `product_offer`
- **Action**: `save`

#### Key Fields (Level 1):
| Key | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `id` | Integer | Yes | Seller internal product ID. |
| `category_id` | Integer | Yes | eMAG category ID. |
| `name` | String | Yes | Product name (max 255 chars). |
| `part_number` | String | Yes | Manufacturer unique identifier. |
| `brand` | String | Yes | Brand name. |
| `sale_price` | Decimal | Yes | Sale price without VAT. |
| `stock` | Array | Yes | List of `warehouse_id` and `value`. |
| `vat_id` | Integer | Yes | VAT rate ID (from `/vat/read`). |
| `status` | Integer | Yes | 1 (Active), 0 (Inactive), 2 (End of Life). |

### 3.3 Updating Existing Offers
To update price or stock without sending full documentation:
- **Resource**: `offer`
- **Action**: `save`
- **Fields**: `id` (Required), `sale_price`, `stock`, `status`, etc.

### 3.4 Stock Updates (REST)
- **Resource**: `offer_stock/{resourceId}`
- **Method**: `PATCH`
- **Payload**: `{"value": 10, "warehouse_id": 1}`

---

## 4. Order Processing

### 4.1 Order Statuses
- `0`: Canceled
- `1`: New
- `2`: In Progress
- `3`: Prepared
- `4`: Finalized
- `5`: Returned

### 4.2 Reading Orders
- **Resource**: `order`
- **Action**: `read`
- **Filters**: `id`, `status`, `date_start`, `date_end`.

### 4.3 Order Fields
| Key | Description |
| :--- | :--- |
| `id` | Unique order ID. |
| `status` | Current processing status. |
| `payment_mode_id` | 1 (COD), 2 (Bank), 3 (Online Card). |
| `customer` | Details about the buyer (name, email, address). |
| `products` | List of products in the order. |

### 4.4 Acknowledging Orders
After reading a new order, you must acknowledge it.
- **Resource**: `order`
- **Action**: `acknowledge`
- **Data**: `[{"id": 12345}]`

---

## 5. Shipping & Logistics (AWB)

### 5.1 Saving AWB
- **Resource**: `awb`
- **Action**: `save`
- **Key Fields**: `order_id`, `courier_account_id`, `packages` (number of parcels), `weight`.

### 5.2 Reading AWB PDF
- **Resource**: `awb`
- **Action**: `read_pdf`
- **Filters**: `id` (AWB ID).

---

## 6. Return Requests (RMA)

- **Resource**: `rma`
- **Actions**: `read`, `save`, `count`
- **Statuses**: `1` (New), `2` (Acknowledged), `3` (Received), `4` (Resolved), `5` (Rejected).

---

## 7. Callbacks (Webhooks)

You can configure callback URLs in the Marketplace interface for:
- `New order`
- `Order cancellation`
- `New return & status change`
- `AWB status change`
- `Approved documentation`

---

## 8. Quick Reference Table: Resources & Actions

| Resource | Read | Save | Count | Other |
| :--- | :---: | :---: | :---: | :--- |
| `product_offer` | ✅ | ✅ | ✅ | |
| `order` | ✅ | ✅ | ✅ | `acknowledge`, `unlock-courier` |
| `awb` | ✅ | ✅ | | |
| `category` | ✅ | | ✅ | |
| `vat` | ✅ | | | |
| `rma` | ✅ | ✅ | ✅ | |
| `offer_stock` | | | | `PATCH` |

---
*Generated for Cursor AI - v4.5.1*

---

## 9. JSON Examples

### 9.1 Product Offer Save (New Product)
```json
{
  "data": [
    {
      "id": 243409,
      "category_id": 506,
      "name": "Test product",
      "part_number": "md788hc/a",
      "brand": "Brand test",
      "description": "Product description with <b>HTML</b> tags.",
      "sale_price": 51.6477,
      "vat_id": 1,
      "stock": [
        {
          "warehouse_id": 1,
          "value": 20
        }
      ],
      "status": 1,
      "warranty": 24,
      "images": [
        {
          "display_type": 1,
          "url": "http://valid-url.jpg"
        }
      ],
      "characteristics": [
        {
          "id": 24,
          "value": "test"
        }
      ]
    }
  ]
}
```

### 9.2 Order Read (Response Example)
```json
{
  "isError": false,
  "messages": [],
  "results": [
    {
      "id": 939393,
      "status": 1,
      "payment_mode_id": 1,
      "customer": {
        "id": 1,
        "name": "Surname Name",
        "email": "1243536@emag.ro",
        "billing_city": "City",
        "shipping_city": "City"
      },
      "products": [
        {
          "id": 123,
          "product_id": 3331,
          "quantity": 2,
          "sale_price": 12.1234,
          "status": 1
        }
      ]
    }
  ]
}
```

### 9.3 AWB Save (Request Example)
```json
{
  "data": [
    {
      "order_id": 939393,
      "courier_account_id": 1,
      "packages": 1,
      "weight": 1.5,
      "observation": "Fragile"
    }
  ]
}
```
