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


def create_monday_item(order):
    col_vals = {}
    if order.get('order_number'):
        col_vals['numbers1'] = order['order_number']
    if order.get('phone'):
        col_vals['phone8'] = {'phone': order['phone'], 'countryShortName': 'RO'}
    address_parts = []
    if order.get('address'):
        address_parts.append(order['address'])
    col_vals['location'] = {
        'address': order.get('address', ''),
        'city': order.get('city', ''),
        'country': order.get('country', 'Romania')
    }
    if order.get('total') is not None:
        col_vals['t_mobil9'] = order['total']
    if order.get('card_amount') is not None:
        col_vals['incasat'] = order['card_amount']

    col_vals_json = json.dumps(col_vals).replace('"', '\\"')
    customer = order.get('customer_name', 'New Order')
    query = '''mutation {
      create_item(
        board_id: ''' + str(BOARD_ID) + ''',
        group_id: "''' + GROUP_ID + '''",
        item_name: "''' + customer.replace('"', '') + '''",
        column_values: "''' + col_vals_json + '''"
      ) { id }
    }'''

    headers = {'Authorization': MONDAY_API_KEY, 'Content-Type': 'application/json', 'API-Version': '2023-10'}
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
    body = '\n'.join(lines)
    query = '''mutation {
      create_update(item_id: ''' + str(item_id) + ''', body: "''' + body.replace('"', '\\"') + '''") { id }
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
    body = '\n'.join(lines)
    query = '''mutation {
      create_update(item_id: ''' + str(item_id) + ''', body: "''' + body.replace('"', '\\"') + '''") { id }
    }'''
    headers = {'Authorization': MONDAY_API_KEY, 'Content-Type': 'application/json', 'API-Version': '2023-10'}
    resp = requests.post(MONDAY_API_URL, json={'query': query}, headers=headers, timeout=15)
    resp.raise_for_status()


def parse_wix_ecommerce_order(payload):
    order_data = payload.get('data', payload)
    billing = order_data.get('billingInfo', {})
    contact = billing.get('contactDetails', {})
    address_obj = billing.get('address', {})
    pricing = order_data.get('priceSummary', {})
    total_str = pricing.get('total', {}).get('amount', '0')
    try:
        total = float(total_str)
    except Exception:
        total = 0.0
    payments = order_data.get('paymentStatus', '')
    card_amount = total if 'PAID' in str(payments).upper() else None
    phone = contact.get('phone', '')
    street = address_obj.get('addressLine', address_obj.get('streetAddress', {}).get('name', ''))
    city = address_obj.get('city', '')
    country = address_obj.get('country', 'Romania')
    products = []
    for line in order_data.get('lineItems', []):
        products.append({
            'name': line.get('productName', {}).get('original', line.get('name', 'Produs')),
            'quantity': line.get('quantity', 1)
        })
    return {
        'order_number': order_data.get('number', order_data.get('id', '')),
        'customer_name': f"{contact.get('firstName', '')} {contact.get('lastName', '')}".strip() or 'Client',
        'phone': phone,
        'address': street,
        'city': city,
        'country': country,
        'total': total,
        'card_amount': card_amount,
        'products': products,
        'notes': order_data.get('buyerNote', '')
    }


def parse_wix_stores_order(payload):
    order_data = payload.get('order', payload.get('data', payload))
    billing = order_data.get('billingInfo', {})
    address_obj = billing.get('address', {})
    totals = order_data.get('totals', {})
    try:
        total = float(totals.get('total', 0))
    except Exception:
        total = 0.0
    payment_method = order_data.get('paymentMethod', '')
    card_amount = total if payment_method.upper() in ['CREDIT_CARD', 'CARD', 'STRIPE', 'PAYPAL'] else None
    phone = billing.get('phone', address_obj.get('phone', ''))
    street = address_obj.get('addressLine', '')
    city = address_obj.get('city', '')
    country = address_obj.get('country', 'Romania')
    products = []
    for line in order_data.get('lineItems', []):
        products.append({'name': line.get('name', 'Produs'), 'quantity': line.get('quantity', 1)})
    return {
        'order_number': order_data.get('number', ''),
        'customer_name': billing.get('firstName', '') + ' ' + billing.get('lastName', ''),
        'phone': phone,
        'address': street,
        'city': city,
        'country': country,
        'total': total,
        'card_amount': card_amount,
        'products': products,
        'notes': order_data.get('buyerNote', '')
    }


def auto_parse(payload):
    # Try to detect payload type and decode if base64
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            try:
                payload = json.loads(base64.b64decode(payload).decode('utf-8'))
            except Exception:
                pass
    # Handle Wix automation wrapper with data field
    if isinstance(payload, dict) and 'data' in payload:
        inner = payload['data']
        if isinstance(inner, str):
            try:
                inner = json.loads(inner)
            except Exception:
                try:
                    inner = json.loads(base64.b64decode(inner).decode('utf-8'))
                except Exception:
                    pass
        if isinstance(inner, dict):
            payload = inner
    # Detect ecommerce vs stores format
    if 'priceSummary' in payload or 'lineItems' in payload:
        return parse_wix_ecommerce_order(payload)
    elif 'totals' in payload or ('order' in payload and isinstance(payload.get('order'), dict)):
        return parse_wix_stores_order(payload)
    else:
        # Try ecommerce first as fallback
        return parse_wix_ecommerce_order(payload)


@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'service': 'wix-monday-webhook'}), 200


@app.route('/webhook/wix-order', methods=['POST'])
def wix_order_webhook():
    try:
        raw = request.get_data(as_text=True)
        logger.info(f'Received webhook, content-type: {request.content_type}, body length: {len(raw)}')
        try:
            payload = request.get_json(force=True, silent=True)
            if payload is None:
                payload = json.loads(raw) if raw else {}
        except Exception:
            payload = {}
        logger.info(f'Parsed payload keys: {list(payload.keys()) if isinstance(payload, dict) else type(payload)}')
        order = auto_parse(payload)
        logger.info(f'Order parsed: {order.get("customer_name")} #{order.get("order_number")}')
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
