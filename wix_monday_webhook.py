import os
import re
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


def clean_for_geocoding(address):
    """Simplify address for geocoders: remove apt/unit details like sc., ap., bl."""
    cleaned = re.sub(r'\b(sc|ap|bl|et|int|cam)\s*\.?\s*[A-Za-z0-9]+\b', '', address, flags=re.IGNORECASE)
    cleaned = re.sub(r',\s*,', ',', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip().strip(',').strip()
    return cleaned


def geocode_address(address):
    """Get real lat/lng for an address. Tries Nominatim (full+simplified) then Photon."""
    simplified = clean_for_geocoding(address)
    # Try Nominatim first (full address, then simplified if different)
    for q in ([address, simplified] if simplified != address else [address]):
        try:
            url = 'https://nominatim.openstreetmap.org/search'
            params = {'q': q, 'format': 'json', 'limit': 1}
            headers = {'User-Agent': 'jarinka-delivery/1.0 (jarinka.ro)'}
            resp = requests.get(url, params=params, headers=headers, timeout=8)
            resp.raise_for_status()
            data = resp.json()
            if data:
                lat = float(data[0]['lat'])
                lng = float(data[0]['lon'])
                logger.info(f'Geocoded (Nominatim) "{q}" -> {lat}, {lng}')
                return lat, lng
        except Exception as e:
            logger.warning(f'Nominatim failed for "{q}": {e}')

    # Try Photon (Komoot) as second option
    try:
        url = 'https://photon.komoot.io/api/'
        params = {'q': address, 'limit': 1}
        resp = requests.get(url, params=params, timeout=8)
        resp.raise_for_status()
        features = resp.json().get('features', [])
        if features:
            coords = features[0]['geometry']['coordinates']
            lng = float(coords[0])
            lat = float(coords[1])
            logger.info(f'Geocoded (Photon) "{address}" -> {lat}, {lng}')
            return lat, lng
    except Exception as e:
        logger.warning(f'Photon failed for "{address}": {e}')

    logger.warning(f'All geocoding failed for "{address}", using Brasov center fallback')
    return None, None


def _post_monday(query, variables=None):
    """Post a GraphQL query/mutation to Monday.com API."""
    headers = {
        'Authorization': MONDAY_API_KEY,
        'Content-Type': 'application/json',
        'API-Version': '2023-10'
    }
    payload = {'query': query}
    if variables:
        payload['variables'] = variables
    resp = requests.post(MONDAY_API_URL, json=payload, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json()


def create_monday_item(order):
    col_vals = {}

    # CMD (text_mm2bgzbx) â text column for order number
    order_num = order.get('order_number')
    if order_num is not None:
        col_vals['text_mm2bgzbx'] = str(order_num)

    if order.get('phone'):
        col_vals['phone8'] = {'phone': str(order['phone']), 'countryShortName': 'RO'}

    # ADRESA â plain text column with full delivery address
    addr_parts = [p for p in [
        order.get('address', ''),
        order.get('city', ''),
        order.get('country', '')
    ] if p]
    full_address = ', '.join(addr_parts) if addr_parts else ''
    if full_address:
        col_vals['adress'] = full_address

    # HARTA â location column with geocoded real coordinates
    if full_address:
        lat, lng = geocode_address(full_address)
    else:
        lat, lng = None, None
    # Fallback to Brasov city center if geocoding fails
    if lat is None or lng is None:
        lat, lng = 45.6427, 25.5887
    col_vals['location'] = {
        'lat': lat,
        'lng': lng,
        'address': full_address if full_address else 'Brasov, Romania'
    }

    total = order.get('total')
    if total is not None and total != 0.0:
        try:
            col_vals['t_mobil9'] = float(total)
        except (ValueError, TypeError):
            pass

    # CARD column: amount if card payment, 0 if cash
    try:
        col_vals['incasat'] = float(order['card_amount']) if order.get('card_amount') is not None else 0
    except (ValueError, TypeError):
        col_vals['incasat'] = 0

    col_vals_json = json.dumps(col_vals).replace('"', '\\"')
    # Item name = customer name only (order number goes in CMD column)
    customer = str(order.get('customer_name', 'New Order')).replace('"', '').replace("'", '')
    item_name = customer[:255]

    query = '''mutation {
      create_item(
        board_id: ''' + str(BOARD_ID) + ''',
        group_id: "''' + GROUP_ID + '''",
        item_name: "''' + item_name.replace('\\', '') + '''",
        column_values: "''' + col_vals_json + '''"
      ) { id }
    }'''

    data = _post_monday(query)
    if 'errors' in data:
        raise Exception('Monday API errors: ' + str(data['errors']))
    return data['data']['create_item']['id']


def add_order_summary_update(item_id, order):
    """First update: clean readable summary of the order."""
    lines = []
    lines.append(f"Comanda #{order.get('order_number', 'N/A')}")
    lines.append('')
    lines.append('Produse:')
    for p in order.get('products', []):
        name = p.get('name', 'Produs')
        qty = p.get('quantity', 1)
        price = p.get('price', '')
        if price:
            lines.append(f"  {name} | Cantitate: {qty} | Pret: {price} RON")
        else:
            lines.append(f"  {name} | Cantitate: {qty}")
    if not order.get('products'):
        lines.append('  (produse necunoscute)')
    lines.append('')
    total = order.get('total', 'N/A')
    lines.append(f"Total: {total} RON")
    # Payment method â always explicit
    if order.get('card_amount') is not None:
        lines.append(f"Plata: Card ({order['card_amount']} RON)")
    else:
        lines.append("Plata: Cash (ramburs)")
    lines.append('')
    if order.get('delivery_time'):
        lines.append(f"Livrare: {order['delivery_time']}")
    lines.append('')
    lines.append('Date client:')
    lines.append(f"  Nume: {order.get('customer_name', 'N/A')}")
    lines.append(f"  Telefon: {order.get('phone', 'N/A')}")
    addr_parts = [p for p in [order.get('address', ''), order.get('city', ''), order.get('country', '')] if p]
    lines.append(f"  Adresa: {', '.join(addr_parts) if addr_parts else 'N/A'}")
    if order.get('notes'):
        lines.append('')
        lines.append(f"Nota cumparator: {order['notes']}")

    body = '\n'.join(lines)
    query = 'mutation ($itemId: ID!, $body: String!) { create_update(item_id: $itemId, body: $body) { id } }'
    _post_monday(query, {'itemId': str(item_id), 'body': body})


def add_raw_order_update(item_id, order_data):
    """Second update: key input fields from the Wix order payload."""
    try:
        def pick(d, *keys):
            """Safely get a nested value."""
            for k in keys:
                if not isinstance(d, dict):
                    return None
                d = d.get(k)
            return d

        lines = []
        lines.append('DATE INTRARE COMANDA (Wix):')
        lines.append('')

        # Order identifiers
        lines.append(f"ID comanda: {order_data.get('id', 'N/A')}")
        lines.append(f"Numar comanda: {order_data.get('number', order_data.get('orderNumber', 'N/A'))}")
        lines.append(f"Status: {order_data.get('status', order_data.get('fulfillmentStatus', 'N/A'))}")
        lines.append(f"Status plata: {order_data.get('paymentStatus', 'N/A')}")
        lines.append('')

        # Buyer info
        buyer = order_data.get('buyerInfo', {})
        billing = order_data.get('billingInfo', {})
        contact = billing.get('contactDetails', {}) if isinstance(billing, dict) else {}
        lines.append('Cumparator:')
        lines.append(f"  Email: {buyer.get('email', '') or billing.get('email', '') or pick(order_data, 'contact', 'email') or 'N/A'}")
        lines.append(f"  Telefon: {contact.get('phone', '') or buyer.get('phone', '') or billing.get('phone', '') or 'N/A'}")
        lines.append(f"  Nume: {contact.get('firstName', '')} {contact.get('lastName', '')}".strip() or 'N/A')
        lines.append('')

        # Shipping address
        shipping = order_data.get('shippingInfo', {})
        shipment = shipping.get('shipmentDetails', {}) if isinstance(shipping, dict) else {}
        ship_addr = shipment.get('address', {}) if isinstance(shipment, dict) else {}
        bill_addr = billing.get('address', {}) if isinstance(billing, dict) else {}
        addr = ship_addr or bill_addr
        lines.append('Adresa livrare:')
        lines.append(f"  Strada: {addr.get('addressLine', addr.get('streetAddress', {}).get('name', 'N/A'))}")
        lines.append(f"  Oras: {addr.get('city', 'N/A')}")
        lines.append(f"  Tara: {addr.get('country', 'N/A')}")
        lines.append('')

        # Delivery time
        logistics = shipping.get('logistics', {}) if isinstance(shipping, dict) else {}
        delivery_time = (order_data.get('deliveryTime') or
                         logistics.get('deliveryTime') or
                         shipping.get('deliveryTime') or '')
        if delivery_time:
            lines.append(f"Interval livrare: {delivery_time}")
            lines.append('')

        # Buyer note
        buyer_note = (order_data.get('buyerNote') or
                      (buyer.get('message') if isinstance(buyer, dict) else None) or
                      order_data.get('note') or '')
        if buyer_note:
            lines.append(f"Nota cumparator: {buyer_note}")
            lines.append('')

        # Products
        lines.append('Produse:')
        for item in order_data.get('lineItems', []):
            name = (item.get('itemName') or item.get('name') or item.get('productName') or 'Produs')
            if isinstance(name, dict):
                name = name.get('original') or name.get('translated') or 'Produs'
            qty = item.get('quantity', 1)
            price_obj = item.get('totalPrice', item.get('price', {}))
            price = price_obj.get('value', price_obj.get('amount', '')) if isinstance(price_obj, dict) else str(price_obj or '')
            lines.append(f"  {name} | qty: {qty} | pret: {price} RON")
        lines.append('')

        # Totals
        pricing = order_data.get('priceSummary', {})
        lines.append('Sumar preturi:')
        for key, label in [('subtotal', 'Subtotal'), ('shipping', 'Transport'), ('discount', 'Discount'), ('total', 'Total')]:
            val = pricing.get(key, {})
            if isinstance(val, dict):
                val = val.get('amount', val.get('value', ''))
            if val:
                lines.append(f"  {label}: {val} RON")
        lines.append('')

        # Payments
        lines.append('Plata:')
        for pmt in order_data.get('payments', []):
            method = pmt.get('paymentMethod', pmt.get('type', 'N/A'))
            amt = pmt.get('amount', {})
            amt_val = amt.get('value', amt.get('amount', '')) if isinstance(amt, dict) else str(amt or '')
            cc = pmt.get('creditCardLastDigits', '')
            cc_info = f" (card ...{cc})" if cc else ''
            lines.append(f"  {method}{cc_info}: {amt_val} RON")

        body = '\n'.join(lines)
    except Exception as e:
        body = f'Eroare la construirea rezumatului: {e}'

    query = 'mutation ($itemId: ID!, $body: String!) { create_update(item_id: $itemId, body: $body) { id } }'
    _post_monday(query, {'itemId': str(item_id), 'body': body})


def extract_order_number(order_data):
    """Try every possible field name for a numeric order number."""
    for key in ['number', 'orderNumber', 'order_number', 'sequenceNumber', 'num']:
        val = order_data.get(key)
        n = to_int(val)
        if n is not None:
            return n
    for key in ['id', '_id', 'orderId', 'order_id']:
        val = order_data.get(key)
        if val:
            return str(val)
    return None


def parse_wix_ecommerce_order(order_data):
    billing = order_data.get('billingInfo', {})
    contact = billing.get('contactDetails', {})
    address_obj = billing.get('address', {})

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

    logger.info(f'priceSummary total parsed: {total}')
    if total == 0.0:
        for pmt in order_data.get('payments', []):
            try:
                amt = pmt.get('amount', {})
                val = amt.get('value', 0) if isinstance(amt, dict) else amt
                logger.info(f'Payment amount found: {val}')
                total = float(val)
                if total:
                    break
            except Exception:
                pass
    if total == 0.0:
        sub = pricing.get('subtotal', {})
        if isinstance(sub, dict):
            sub = sub.get('amount', '0')
        try:
            total = float(sub)
        except Exception:
            pass
    logger.info(f'Final total: {total}')

    card_amount = None
    for pmt in order_data.get('payments', []):
        # Card payment: Wix includes 'creditCardLastDigits' for card transactions
        if pmt.get('creditCardLastDigits') and total > 0:
            card_amount = total
            break

    top_contact = order_data.get('contact', {})
    phone = (contact.get('phone') or
             top_contact.get('phone') or
             top_contact.get('contactDetails', {}).get('phone') or
             billing.get('phone') or '')

    street = (address_obj.get('addressLine') or
              address_obj.get('streetAddress', {}).get('name', '') or
              shipping_addr.get('addressLine', ''))
    city = address_obj.get('city', '') or shipping_addr.get('city', '')
    country = address_obj.get('country', '') or shipping_addr.get('country', 'Romania') or 'Romania'

    products = []
    for line in order_data.get('lineItems', []):
        # Product name: Wix uses 'itemName' field
        pname = (line.get('itemName') or
                 line.get('name') or
                 line.get('productName') or
                 line.get('title') or
                 'Produs')
        if isinstance(pname, dict):
            pname = pname.get('original') or pname.get('translated') or 'Produs'
        # Price: Wix uses 'totalPrice.value'
        price_obj = line.get('totalPrice', line.get('price', line.get('priceData', {})))
        if isinstance(price_obj, dict):
            price_val = price_obj.get('value', price_obj.get('amount', ''))
        else:
            price_val = str(price_obj) if price_obj else ''
        try:
            price_val = str(round(float(price_val), 2)) if price_val else ''
        except Exception:
            price_val = ''
        products.append({'name': pname, 'quantity': line.get('quantity', 1), 'price': price_val})

    first = contact.get('firstName', billing.get('firstName', ''))
    last = contact.get('lastName', billing.get('lastName', ''))
    customer_name = f"{first} {last}".strip() or 'Client'

    # Buyer note â check multiple possible field names
    buyer_info = order_data.get('buyerInfo', {})
    notes = (order_data.get('buyerNote') or
             (buyer_info.get('message') if isinstance(buyer_info, dict) else None) or
             order_data.get('note') or
             order_data.get('customerNote') or '')
    logger.info(f'Buyer note found: {repr(notes)}')

    # Delivery time â check shippingInfo.logistics and root level
    logistics = shipping.get('logistics', {})
    delivery_time = (order_data.get('deliveryTime') or
                     logistics.get('deliveryTime') or
                     shipping.get('deliveryTime') or
                     logistics.get('instructions') or
                     '')
    logger.info(f'Delivery time found: {repr(delivery_time)}')

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
        'notes': notes,
        'delivery_time': delivery_time,
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
        price_val = str(line.get('price', ''))
        products.append({'name': line.get('name', 'Produs'), 'quantity': line.get('quantity', 1), 'price': price_val})
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
        'notes': order_data.get('buyerNote', '') or order_data.get('note', ''),
        'delivery_time': order_data.get('deliveryTime', ''),
    }


def unwrap_payload(payload):
    """Unwrap Wix automation wrapper layers and base64 encoding."""
    if isinstance(payload, str):
        try:
            payload = json.loads(provide)
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
    """Returns (parsed_order_dict, raw_order_data_dict)."""
    order_data = unwrap_payload(payload)
    logger.info(f'Order data keys: {list(order_data.keys()) if isinstance(order_data, dict) else type(order_data)}')
    if not isinstance(order_data, dict):
        raise ValueError(f'Could not parse payload into dict, got: {type(order_data)}')
    if 'priceSummary' in order_data or 'lineItems' in order_data or 'billingInfo' in order_data:
        return parse_wix_ecommerce_order(order_data), order_data
    elif 'totals' in order_data or ('order' in order_data and isinstance(order_data.get('order'), dict)):
        if 'order' in order_data:
            return parse_wix_stores_order(order_data['order']), order_data['order']
        return parse_wix_stores_order(order_data), order_data
    else:
        logger.warning('Unknown payload format, attempting ecommerce parse')
        return parse_wix_ecommerce_order(order_data), order_data


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
        order, order_data = auto_parse(payload)
        logger.info(f'Order parsed: {order.get("customer_name")} #{order.get("order_number")} total={order.get("total")}')
        item_id = create_monday_item(order)
        logger.info(f'Created Monday item: {item_id}')
        add_raw_order_update(item_id, order_data)
        logger.info(f'Raw data update added to item {item_id}')
        add_order_summary_update(item_id, order)
        logger.info(f'Summary update added to item {item_id}')
        return jsonify({'status': 'ok', 'item_id': item_id}), 200
    except Exception as e:
        logger.error(f'Error processing webhook: {e}', exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
