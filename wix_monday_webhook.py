import os
import json
import base64
import requests
import logging
from flask import Flask, request, jsonify

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MONDAY_API_KEY = os.environ.get('MONDAY_API_KEY', '')
MONDAY_API_URL = 'https://api.monday.com/v2'
BOARD_ID = 804109007
GROUP_ID = 'new_group3802'


def to_int(val):
    """Try to convert value to int. Return None if not possible."""
    if val is None:
        return None
    try:
        return int(str(val).strip())
    except (ValueError, TypeError):
        return None


def create_monday_item(order):
    col_vals = {}

    # CMD (numbers1) â text column (changed from numbers)
    order_num = order.get('order_number')
    if order_num is not None:
        col_vals['numbers1'] = str(order_num)

    if order.get('phone'):
        col_vals['phone8'] = {'phone': str(order['phone']), 'countryShortName': 'RO'}

    # Location (HARTA) â Monday requires address + lat/lng coordinates
    addr_parts = [p for p in [
        order.get('address', ''),
        order.get('city', ''),
        order.get('country', '')
    ] if p]
    full_address = ', '.join(addr_parts) if addr_parts else 'Romania'
    col_vals['location'] = {
        'lat': '45.9432',
        'lng': '24.9668',
        'address': full_address
    }

    if order.get('total') is not None:
        try:
            col_vals['t_mobil9'] = float(order['total'])
        except (ValueError, TypeError):
            pass

    if order.get('card_amount') is not None:
        try:
            col_vals['incasat'] = float(order['card_amount'])
        except (ValueError, TypeError):
            pass

    col_vals_json = json.dumps(col_vals).replace('"', '\\\"')
    customer = str(order.get('customer_name', 'New Order')).replace('"', '').replace("'", '')
    order_label = str(order.get('order_number', '')).replace('"', '')

    # Include order ID in item name for traceability
    item_name = customer if not order_label else f"{customer} #{order_label}"
    item_name = item_name[:255]  # Monday limit

    query = '''mutation {
      create_item(
        board_id: ''' + str(BOARD_ID) + ''',
        group_id: "''' + GROUP_ID + '''",
        item_name: "''' + item_name.replace('\\', '') + '''",
        column_values: "''' + col_vals_json + '''"
      ) { id }
    }'''

    headers = {
        'Authorization': MONDAY_API_KEY,
        'Content-Type': 'application/json',
        'API-Version': '2023-10'
    }
    resp = requests.post(MONDAY_API_URL, json={'query': query}, headers=headers, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if 'errors' in data:
        raise Exception('Monday API errors: ' + str(data['errors']))
    return data['data']['create_item']['id']


def add_product_checklist_update(item_id, order):
    lines = ['**Produse comandate:**']
    for p in order.get('products', []):
        name = p.get('name', 'Produs')
        qty = p.get('quantity', 1)
        lines.append(f'- [ ] {name} x{qty}')
    if len(lines) == 1:
        lines.append('- [ ] (produse necunoscute)')
    body = '\\n'.join(lines)
    query = '''mutation {
      create_update(item_id: ''' + str(item_id) + ''', body: "''' + body.replace('"', '\\\"') + '''") { id }
    }'''
    headers = {'Authorization': MONDAY_API_KEY, 'Content-Type': 'application/json', 'API-Version': '2023-10'}
    resp = requests.post(MONDAY_API_URL, json={'query': query}, headers=headers, timeout=15)
    resp.raise_for_status()


def add_order_details_update(item_id, order):
    lines = [
        f"**Comanda #{order.get('order_number', 'N/A')}**",
        f"Client: {order.get('customer_name', 'N/A')}",
        f"Telefon: {order.get('phone', 'N/A')}",
        f"Adresa: {order.get('address', 'N/A')}, {order.get('city', '')}",
        f"Total: {order.get('total', 'N/A')} RON",
    ]
    if order.get('card_amount') is not None:
        lines.append(f"Plata card: {order['card_amount']} RON")
    if order.get('notes'):
        lines.append(f"Note: {order['notes']}")
    body = '\\n'.join(lines)
    query = '''mutation {
      create_update(item_id: ''' + str(item_id) + ''', body: "''' + body.replace('"', '\\\"') + '''") { id }
    }'''
    headers = {'Authorization': MONDAY_API_KEY, 'Content-Type': 'application/json', 'API-Version': '2023-10'}
    resp = requests.post(MONDAY_API_URL, json={'query': query}, headers=headers, timeout=15)
    resp.raise_for_status()


def extract_order_number(order_data):
    """Try every possible field name for a numeric order number."""
    for key in ['number', 'orderNumber', 'order_number', 'sequenceNumber', 'num']:
        val = order_data.get(key)
        n = to_int(val)
        if n is not None:
            return n
    # Fall back to string ID (UUID) so we can still show it in the name
    for key in ['id', '_id', 'orderId', 'order_id']:
        val = order_data.get(key)
        if val:
            return str(val)
    return None


def parse_wix_ecommerce_order(order_data):
    billing = order_data.get('billingInfo', {})
    contact = billing.get('contactDetails', {})
    address_obj = billing.get('address', {})

    # Also check shippingInfo for address
    shipping = order_data.get('shippingInfo', {})
    shipping_addr = shipping.get('shipmentDetails', {}).get('address', {})

    pricing = order_data.get('priceSummary', {})
    total_str = pricing.get('total', {})
    if isinstance(total_str, dict):
        total_str = total_str.get('amount', '0')
    try:
        total = float(total_str)
    except Exception:
        total = 0.0

    payment_status = str(order_data.get('paymentStatus', ''))
    card_amount = total if 'PAID' in payment_status.upper() else None

    phone = contact.get('phone', billing.get('phone', ''))

    # Try billing address first, fall back to shipping address
    street = (address_obj.get('addressLine') or
              address_obj.get('streetAddress', {}).get('name', '') or
              shipping_addr.get('addressLine', ''))
    city = address_obj.get('city', '') or shipping_addr.get('city', '')
    country = address_obj.get('country', '') or shipping_addr.get('country', 'Romania') or 'Romania'

    products = []
    for line in order_data.get('lineItems', []):
        pname = line.get('productName', {})
        if isinstance(pname, dict):
            pname = pname.get('original', pname.get('translated', 'Produs'))
        else:
            pname = str(pname) if pname else line.get('name', 'Produs')
        products.append({'name': pname, 'quantity': line.get('quantity', 1)})

    first = contact.get('firstName', billing.get('firstName', ''))
    last = contact.get('lastName', billing.get('lastName', ''))
    customer_name = f"{first} {last}".strip() or 'Client'

    return {
        'order_number': extract_order_number(order_data),
        'customer_name': customer_name,
        'phone': phone,
        'address': street,
        'city': city,
        'country': country,
        'total': total,
        'card_amount': card_amount,
        'products': products,
        'notes': order_data.get('buyerNote', '')
    }


def parse_wix_stores_order(order_data):
    billing = order_data.get('billingInfo', {})
    address_obj = billing.get('address', {})
    totals = order_data.get('totals', {})
    try:
        total = float(totals.get('total', 0))
    except Exception:
        total = 0.0
    payment_method = str(order_data.get('paymentMethod', ''))
    card_amount = total if payment_method.upper() in ['CREDIT_CARD', 'CARD', 'STRIPE', 'PAYPAL'] else None
    phone = billing.get('phone', address_obj.get('phone', ''))
    street = address_obj.get('addressLine', '')
    city = address_obj.get('city', '')
    country = address_obj.get('country', 'Romania')
    products = []
    for line in order_data.get('lineItems', []):
        products.append({'name': line.get('name', 'Produs'), 'quantity': line.get('quantity', 1)})
    return {
        'order_number': extract_order_number(order_data),
        'customer_name': (billing.get('firstName', '') + ' ' + billing.get('lastName', '')).strip() or 'Client',
        'phone': phone,
        'address': street,
        'city': city,
        'country': country,
        'total': total,
        'card_amount': card_amount,
        'products': products,
        'notes': order_data.get('buyerNote', '')
    }


def unwrap_payload(payload):
    """Unwrap Wix automation wrapper layers and base64 encoding."""
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            try:
                payload = json.loads(base64.b64decode(payload + '==').decode('utf-8'))
            except Exception:
                pass
    if isinstance(payload, dict) and 'data' in payload:
        inner = payload['data']
        if isinstance(inner, str):
            try:
                inner = json.loads(inner)
            except Exception:
                try:
                    inner = json.loads(base64.b64decode(inner + '==').decode('utf-8'))
                except Exception:
                    pass
        if isinstance(inner, dict):
            payload = inner
    return payload


def auto_parse(payload):
    order_data = unwrap_payload(payload)
    logger.info(f'Order data keys: {list(order_data.keys()) if isinstance(order_data, dict) else type(order_data)}')
    if not isinstance(order_data, dict):
        raise ValueError(f'Could not parse payload into dict, got: {type(order_data)}')
    # Detect format
    if 'priceSummary' in order_data or 'lineItems' in order_data or 'billingInfo' in order_data:
        return parse_wix_ecommerce_order(order_data)
    elif 'totals' in order_data or ('order' in order_data and isinstance(order_data.get('order'), dict)):
        if 'order' in order_data:
            return parse_wix_stores_order(order_data['order'])
        return parse_wix_stores_order(order_data)
    else:
        logger.warning('Unknown payload format, attempting ecommerce parse')
        return parse_wix_ecommerce_order(order_data)


@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'service': 'wix-monday-webhook'}), 200


@app.route('/webhook/wix-order', methods=['POST'])
def wix_order_webhook():
    try:
        raw = request.get_data(as_text=True)
        logger.info(f'Received webhook, content-type: {request.content_type}, body length: {len(raw)}')
        logger.info(f'Payload preview: {raw[:500]}')
        try:
            payload = request.get_json(force=True, silent=True)
            if payload is None:
                payload = json.loads(raw) if raw else {}
        except Exception:
            payload = {}
        logger.info(f'Parsed payload keys: {list(payload.keys()) if isinstance(payload, dict) else type(payload)}')
        order = auto_parse(payload)
        logger.info(f'Order parsed: {order.get("customer_name")} #{order.get("order_number")} total={order.get("total")}')
        item_id = create_monday_item(order)
        logger.info(f'Created Monday item: {item_id}')
        add_product_checklist_update(item_id, order)
        add_order_details_update(item_id, order)
        logger.info(f'Updates added to item {item_id}')
        return jsonify({'status': 'ok', 'item_id': item_id}), 200
    except Exception as e:
        logger.error(f'Error processing webhook: {e}', exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
