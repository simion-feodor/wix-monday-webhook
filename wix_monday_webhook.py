import os
import re
import json
import base64
import requests
import logging
import threading
from datetime import datetime
from flask import Flask, request, jsonify

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MONDAY_API_KEY = os.environ.get('MONDAY_API_KEY', '')
MONDAY_API_URL = 'https://api.monday.com/v2'
WIX_API_KEY = os.environ.get('WIX_API_KEY', '')
WIX_SITE_ID = '9fcc00dd-2c45-410f-9dca-9360fdb28ac6'
BOARD_ID = 804109007
GROUP_ID = 'new_group3802'

# âââ LEAD-URI Board (citycats.ro forms) ââââââââââââââââââââââââââââââââââââââ
LEAD_BOARD_ID = 18413029793
LEAD_GROUP_ID = 'topics'

FORM_LABELS = {
    'custom.contact-us':               'Pop-Up',
    'custom.contact-us-2':             'Strip / Acasa',
    'custom.contact-lp':               'PROMO',
    'custom.contact-1-acasa-2':        'strip plase-pisici form',
    'custom.contact-1-acasa-3':        'plase de protectie / strip',
    'custom.contact-1-acasa-4':        'blog / strip',
    'custom.formular-blog-2':          'formular blog 2',
    'custom.formular-blog-3':          'formular blog post',
    'custom.magazin-form':             'magazin / form',
    'custom.plase-de-protectie-strip-2': 'plase de protectie (L)',
    'custom.strip-plase-pisici-form-2':  'strip plase-pisici (L)',
    'custom.enter-contest':            'Enter Contest',
    'custom.get-a-price-quote':        'Get a Price Quote',
}

def to_int(val):
    """Try to convert value to int. Return None if not possible."""
    if val is None:
        return None
    try:
        return int(str(val).strip())
    except (ValueError, TypeError):
        return None

def fetch_wix_buyer_note(order_id):
    """Fetch buyerNote from Wix eCommerce API â it's not in the webhook payload."""
    if not WIX_API_KEY or not order_id:
        return ''
    try:
        url = f'https://www.wixapis.com/ecom/v1/orders/{order_id}'
        headers = {'Authorization': WIX_API_KEY, 'wix-site-id': WIX_SITE_ID}
        resp = requests.get(url, headers=headers, timeout=8)
        resp.raise_for_status()
        note = resp.json().get('order', {}).get('buyerNote', '') or ''
        logger.info(f'Wix buyerNote fetched: {repr(note)}')
        return note
    except Exception as e:
        logger.warning(f'Failed to fetch Wix buyerNote: {e}')
        return ''

def clean_for_geocoding(address):
    """Simplify address for geocoders: remove apt/unit details like sc., ap., bl."""
    cleaned = re.sub(r'\b(sc|ap|bl|et|int|cam)\s*\.?\s*[A-Za-z0-9]+\b', '', address, flags=re.IGNORECASE)
    cleaned = re.sub(r',\s*,', ',', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip().strip(',').strip()
    return cleaned

def geocode_address(address):
    """Get real lat/lng for an address. Tries Nominatim then Photon as fallback."""
    # Try Nominatim with full address
    try:
        url = 'https://nominatim.openstreetmap.org/search'
        params = {'q': address, 'format': 'json', 'limit': 1}
        headers = {'User-Agent': 'jarinka-delivery/1.0 (jarinka.ro)'}
        resp = requests.get(url, params=params, headers=headers, timeout=8)
        resp.raise_for_status()
        data = resp.json()
        if data:
            lat = float(data[0]['lat'])
            lng = float(data[0]['lon'])
            logger.info(f'Geocoded (Nominatim) "{address}" -> {lat}, {lng}')
            return lat, lng
    except Exception as e:
        logger.warning(f'Nominatim failed for "{address}": {e}')

    # Try Nominatim with simplified address (strips apt/unit details)
    simplified = clean_for_geocoding(address)
    if simplified != address:
        try:
            url = 'https://nominatim.openstreetmap.org/search'
            params = {'q': simplified, 'format': 'json', 'limit': 1}
            headers = {'User-Agent': 'jarinka-delivery/1.0 (jarinka.ro)'}
            resp = requests.get(url, params=params, headers=headers, timeout=8)
            resp.raise_for_status()
            data = resp.json()
            if data:
                lat = float(data[0]['lat'])
                lng = float(data[0]['lon'])
                logger.info(f'Geocoded (Nominatim simple) "{simplified}" -> {lat}, {lng}')
                return lat, lng
        except Exception as e:
            logger.warning(f'Nominatim simple failed for "{simplified}": {e}')

    # Try Photon (Komoot) as final option
    try:
        url = 'https://photon.komoot.io/api/'
        params = {'q': simplified, 'limit': 1}
        resp = requests.get(url, params=params, timeout=8)
        resp.raise_for_status()
        features = resp.json().get('features', [])
        if features:
            coords = features[0]['geometry']['coordinates']
            lng = float(coords[0])
            lat = float(coords[1])
            logger.info(f'Geocoded (Photon) "{simplified}" -> {lat}, {lng}')
            return lat, lng
    except Exception as e:
        logger.warning(f'Photon failed for "{simplified}": {e}')

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
    resp = requests.post(MONDAY_API_URL, json=payload, headers=headers, timeout=20)
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
    postal_code = order.get('postal_code', '')
    geo_city = order.get('city', '')
    geo_street = order.get('address', '')
    city_part = (postal_code + ' ' + geo_city).strip() if postal_code else geo_city
    geo_parts = [p for p in [geo_street, city_part, 'Romania'] if p]
    geo_address = ', '.join(geo_parts) if geo_parts else full_address
    if full_address:
        lat, lng = geocode_address(geo_address)
    else:
        lat, lng = None, None
    if lat is not None and lng is not None:
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

    # LIVRARE (status3): PRANZ=index 15, SEARA=index 0, blank otherwise
    slot = order.get('delivery_slot')
    if slot == 'PRANZ':
        col_vals['status3'] = {'index': 15}
    elif slot == 'SEARA':
        col_vals['status3'] = {'index': 0}

    # PROGRAMARE (lead2): delivery date in YYYY-MM-DD format
    delivery_date = order.get('delivery_date')
    if delivery_date:
        col_vals['lead2'] = {'date': delivery_date}

    col_vals_json = json.dumps(col_vals).replace('"', '\\"')
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
            for k in keys:
                if not isinstance(d, dict):
                    return None
                d = d.get(k)
            return d

        lines = []
        lines.append('DATE INTRARE COMANDA (Wix):')
        lines.append('')

        lines.append(f"ID comanda: {order_data.get('id', 'N/A')}")
        lines.append(f"Numar comanda: {order_data.get('number', order_data.get('orderNumber', 'N/A'))}")
        lines.append(f"Status: {order_data.get('status', order_data.get('fulfillmentStatus', 'N/A'))}")
        lines.append(f"Status plata: {order_data.get('paymentStatus', 'N/A')}")
        lines.append('')

        buyer = order_data.get('buyerInfo', {})
        billing = order_data.get('billingInfo', {})
        contact = billing.get('contactDetails', {}) if isinstance(billing, dict) else {}
        lines.append('Cumparator:')
        lines.append(f"  Email: {buyer.get('email', '') or billing.get('email', '') or pick(order_data, 'contact', 'email') or 'N/A'}")
        lines.append(f"  Telefon: {contact.get('phone', '') or buyer.get('phone', '') or billing.get('phone', '') or 'N/A'}")
        lines.append(f"  Nume: {contact.get('firstName', '')} {contact.get('lastName', '')}".strip() or 'N/A')
        lines.append('')

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

        logistics = shipping.get('logistics', {}) if isinstance(shipping, dict) else {}
        delivery_time = (order_data.get('deliveryTime') or
                        logistics.get('deliveryTime') or
                        shipping.get('deliveryTime') or '')
        if delivery_time:
            lines.append(f"Interval livrare: {delivery_time}")
            lines.append('')

        buyer_note = (order_data.get('buyerNote') or
                     (buyer.get('message') if isinstance(buyer, dict) else None) or
                     order_data.get('note') or '')
        if buyer_note:
            lines.append(f"Nota cumparator: {buyer_note}")
            lines.append('')

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

        pricing = order_data.get('priceSummary', {})
        lines.append('Sumar preturi:')
        for key, label in [('subtotal', 'Subtotal'), ('shipping', 'Transport'), ('discount', 'Discount'), ('total', 'Total')]:
            val = pricing.get(key, {})
            if isinstance(val, dict):
                val = val.get('amount', val.get('value', ''))
            if val:
                lines.append(f"  {label}: {val} RON")
        lines.append('')

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

    card_amount = total if order_data.get('paymentStatus') == 'PAID' and total > 0 else None

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
        pname = (line.get('itemName') or
                 line.get('name') or
                 line.get('productName') or
                 line.get('title') or
                 'Produs')
        if isinstance(pname, dict):
            pname = pname.get('original') or pname.get('translated') or 'Produs'
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

    buyer_info = order_data.get('buyerInfo', {})
    notes = (order_data.get('buyerNote') or
             (buyer_info.get('message') if isinstance(buyer_info, dict) else None) or
             order_data.get('note') or
             order_data.get('customerNote') or '')
    logger.info(f'Buyer note found: {repr(notes)}')

    logistics = shipping.get('logistics', {})
    delivery_time = (order_data.get('deliveryTime') or
                     logistics.get('deliveryTime') or
                     shipping.get('deliveryTime') or
                     logistics.get('instructions') or
                     '')
    logger.info(f'Delivery time found: {repr(delivery_time)}')

    if '10:00 - 13:00' in delivery_time:
        delivery_slot = 'PRANZ'
    elif '17:00 - 19:00' in delivery_time:
        delivery_slot = 'SEARA'
    else:
        delivery_slot = None

    delivery_date = None
    slot_from = logistics.get('deliveryTimeSlot', {}).get('from', '')
    if slot_from:
        try:
            delivery_date = slot_from[:10]
        except Exception:
            pass
    if not delivery_date and delivery_time:
        try:
            from datetime import date as _date
            months_ro = {'ian':1,'feb':2,'mar':3,'apr':4,'mai':5,'iun':6,
                        'iul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}
            m = re.search(r'(\d{1,2})\s+([a-z]{3})', delivery_time.lower())
            if m:
                day = int(m.group(1))
                month = months_ro.get(m.group(2))
                if month:
                    year = _date.today().year
                    delivery_date = f'{year}-{month:02d}-{day:02d}'
        except Exception:
            pass

    postal_code = (address_obj.get('postalCode', '') or
                   logistics.get('shippingDestination', {}).get('address', {}).get('postalCode', ''))

    return {
        'order_number': extract_order_number(order_data),
        'customer_name': customer_name,
        'phone': phone,
        'address': street,
        'city': city,
        'country': country,
        'postal_code': postal_code,
        'total': total,
        'card_amount': card_amount,
        'products': products,
        'notes': notes,
        'delivery_time': delivery_time,
        'delivery_slot': delivery_slot,
        'delivery_date': delivery_date,
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

def process_order_in_background(order, order_data):
    """Create Monday item with retry logic â runs in a background thread."""
    import time
    max_attempts = 3
    retry_delay = 600  # 10 minutes

    for attempt in range(1, max_attempts + 1):
        try:
            item_id = create_monday_item(order)
            logger.info(f'Created Monday item: {item_id} (attempt {attempt})')
            add_raw_order_update(item_id, order_data)
            logger.info(f'Raw data update added to item {item_id}')
            add_order_summary_update(item_id, order)
            logger.info(f'Summary update added to item {item_id}')
            return
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            if attempt < max_attempts:
                logger.warning(f'Monday API timeout for order #{order.get("order_number")} (attempt {attempt}/{max_attempts}), retrying in 10 min: {e}')
                time.sleep(retry_delay)
            else:
                logger.error(f'Monday API failed after {max_attempts} attempts for order #{order.get("order_number")}: {e}')
        except Exception as e:
            logger.error(f'Unexpected error creating Monday item for order #{order.get("order_number")}: {e}', exc_info=True)
            return

# âââ Contact (Form Lead) Functions âââââââââââââââââââââââââââââââââââââââââââ

def decode_wix_jwt(token):
    """Decode Wix JWT without signature verification."""
    try:
        parts = token.strip().split('.')
        if len(parts) != 3:
            return None
        payload_b64 = parts[1]
        padding = 4 - len(payload_b64) % 4
        if padding != 4:
            payload_b64 += '=' * padding
        payload = base64.urlsafe_b64decode(payload_b64).decode('utf-8')
        return json.loads(payload)
    except Exception as e:
        logger.warning(f'JWT decode failed: {e}')
        return None

def extract_contact_from_payload(raw_body, json_body):
    """Extract contact entity from Wix webhook payload (JWT or plain JSON)."""
    # Format 1: JWT (3 dot-separated segments)
    if isinstance(raw_body, str) and raw_body.count('.') >= 2:
        parts = raw_body.strip().split('.')
        if len(parts) == 3:
            decoded = decode_wix_jwt(raw_body)
            if decoded and decoded.get('data'):
                try:
                    inner = decoded['data']
                    if isinstance(inner, str):
                        inner = json.loads(inner)
                    if isinstance(inner, dict):
                        if inner.get('createdEvent', {}).get('entity'):
                            return inner['createdEvent']['entity']
                        if inner.get('entity'):
                            return inner['entity']
                except Exception:
                    pass

    # Format 2: plain JSON
    if json_body:
        if isinstance(json_body.get('createdEvent'), dict) and json_body['createdEvent'].get('entity'):
            return json_body['createdEvent']['entity']
        if json_body.get('contact'):
            return json_body['contact']
        if json_body.get('entity'):
            return json_body['entity']
        if json_body.get('id') and (json_body.get('info') or json_body.get('primaryInfo')):
            return json_body

    return None

def extract_contact_from_old_form(json_body):
    """Extract contact info from Old Wix Forms payload.

    Old Wix Forms sends:
    {
        "formName": "Contact Form",
        "submissions": [
            {"fieldTitle": "Telefon", "fieldInputValue": "0744..."},
            {"fieldTitle": "Nume", "fieldInputValue": "Ion Popescu"},
            ...
        ],
        "contactId": "xxx",
        "field:comp-xxx": "actual_value",
        ...
    }
    """
    if not isinstance(json_body, dict):
        return None

    contact = {}

    def _norm(s):
        """Normalize Romanian diacritics for field-title matching."""
        return (s.lower().strip()
                .replace('Ä', 'a').replace('Ã¢', 'a').replace('Ã®', 'i')
                .replace('È', 's').replace('Å', 's')
                .replace('È', 't').replace('Å£', 't'))

    # Try submissions array first (has human-readable field titles)
    submissions = json_body.get('submissions', [])
    if isinstance(submissions, list):
        for sub in submissions:
            if not isinstance(sub, dict):
                continue
            title = _norm(sub.get('fieldTitle') or sub.get('field_title') or
                          sub.get('label') or sub.get('Label') or '')
            value = (sub.get('fieldInputValue') or sub.get('fieldValue') or
                     sub.get('field_value') or sub.get('value') or '').strip()
            if not value:
                continue
            if any(t in title for t in ['telefon', 'phone', 'mobil', 'numar']):
                contact['phone'] = value
            elif any(t in title for t in ['email', 'e-mail', 'mail']):
                contact['email'] = value
            elif any(t in title for t in ['nume', 'name', 'prenume', 'client']):
                if 'name' not in contact:
                    contact['name'] = value
            elif any(t in title for t in ['localitate', 'oras', 'city', 'loc ', 'localitatea']):
                contact['localitate'] = value
            elif any(t in title for t in ['adresa', 'address', 'strada', 'stradÄ']):
                contact['adresa'] = value
            elif any(t in title for t in ['mesaj', 'message', 'intrebare', 'Ã®ntrebare', 'detalii', 'observ']):
                contact['message'] = value

    # Include contactId for traceability
    if json_body.get('contactId'):
        contact['contactId'] = json_body['contactId']

    logger.info(f'Old form extracted contact keys: {list(contact.keys())}')

    # Return if we have at least one identifying field
    if contact.get('phone') or contact.get('email') or contact.get('contactId'):
        return contact

    return None


def create_lead_monday_item(contact, form_name=None, source='CityCATS.ro'):
    """Create a Monday item in LEAD-URI board from a Wix contact.

    Handles two contact formats:
    - Format A: CRM nested (New contact created) â info.phones.items[], info.emails.items[]
    - Format B: flat (Form submitted)            â contact.phone, contact.email directly
    """
    info = contact.get('info', {})
    primary = contact.get('primaryInfo', {})

    # Format A: CRM nested
    phones = info.get('phones', {}).get('items', [])
    phone = phones[0].get('phone', '') if phones else primary.get('phone', '')
    emails = info.get('emails', {}).get('items', [])
    email = emails[0].get('email', '') if emails else primary.get('email', '')
    addresses = info.get('addresses', {}).get('items', [])
    city = addresses[0].get('address', {}).get('addressLine', '') if addresses else ''
    label_keys = info.get('labelKeys', {}).get('items', [])
    label_key = label_keys[0] if label_keys else ''

    # CRM nested name (info.name.first / info.name.last)
    crm_first = info.get('name', {}).get('first', '')
    crm_last  = info.get('name', {}).get('last', '')
    crm_name  = f'{crm_first} {crm_last}'.strip()

    # Format B: flat contact (Form submitted trigger)
    if not phone:
        phone = contact.get('phone', '') or contact.get('Phone', '')
    if not email:
        email = contact.get('email', '')
    if not city:
        addr = contact.get('address', {})
        if isinstance(addr, dict):
            city = addr.get('addressLine', '') or addr.get('city', '')

    # CRM name has priority; flat 'name' field is fallback for old forms
    name       = crm_name or contact.get('name', '')
    localitate = contact.get('localitate', '') or city
    adresa     = contact.get('adresa', '')
    message    = contact.get('message', '')

    # Determine display form name: explicit > label mapping > default
    form_name_from_label = FORM_LABELS.get(label_key, '')
    final_form_name = form_name or form_name_from_label or 'FORMULAR'

    created = contact.get('createdDate', '')
    date_str = created[:10] if created else datetime.utcnow().strftime('%Y-%m-%d')

    # Item name: just the customer name (fallback to phone/email if no name)
    identifier = name or phone or email or contact.get('id', '') or contact.get('contactId', 'Lead nou')
    item_name = identifier

    col_vals = {
        'sursa_lead': {'label': source},
        'status':     {'label': 'LEAD'},
        'lead':       {'date': date_str},
    }
    if phone:
        col_vals['phone8'] = {'phone': phone, 'countryShortName': 'RO'}
    if email:
        col_vals['e_mail6'] = {'email': email, 'text': email}
    if localitate:
        col_vals['text'] = localitate          # ORAS column
    if adresa:
        col_vals['adress'] = adresa            # ADRESA column

    col_vals_str = json.dumps(json.dumps(col_vals))  # double-encoded for inline GraphQL

    safe_name = item_name.replace('"', '').replace('\\', '')[:255]
    query = f'''mutation {{
      create_item(
        board_id: {LEAD_BOARD_ID},
        group_id: "{LEAD_GROUP_ID}",
        item_name: {json.dumps(safe_name)},
        column_values: {col_vals_str}
      ) {{ id name }}
    }}'''

    result = _post_monday(query)
    logger.info(f"Monday create_item raw result: {result}")

    # Add update with message content if present
    if message:
        item_id = (((result or {}).get('data') or {}).get('create_item') or {}).get('id')
        if item_id:
            try:
                update_lines = []
                if name:
                    update_lines.append(f'Nume: {name}')
                if phone:
                    update_lines.append(f'Telefon: {phone}')
                if localitate:
                    update_lines.append(f'Localitate: {localitate}')
                if adresa:
                    update_lines.append(f'Adresa: {adresa}')
                update_lines.append('')
                update_lines.append(f'Mesaj:\n{message}')
                body = '\n'.join(update_lines)
                upd_query = 'mutation ($itemId: ID!, $body: String!) { create_update(item_id: $itemId, body: $body) { id } }'
                _post_monday(upd_query, {'itemId': str(item_id), 'body': body})
                logger.info(f'Update with message added to item {item_id}')
            except Exception as e:
                logger.warning(f'Could not add message update: {e}')

    return result

def fetch_monday_order_numbers():
    """Return set of order numbers already in the Monday board (all groups, as strings)."""
    try:
        query = '''{ boards(ids: [804109007]) {
            items_page(limit: 500) {
                items { column_values(ids: ["text_mm2bgzbx"]) { text } }
            }
        } }'''
        data = _post_monday(query)
        nums = set()
        for item in (data.get('data', {}).get('boards', [{}])[0]
                         .get('items_page', {}).get('items', [])):
            for col in item.get('column_values', []):
                val = (col.get('text') or '').strip()
                if val:
                    nums.add(val)
        logger.info(f'Monday board has {len(nums)} order numbers across all groups')
        return nums
    except Exception as e:
        logger.error(f'fetch_monday_order_numbers failed: {e}')
        return set()


def fetch_wix_recent_orders(minutes=60):
    """Return list of recent Wix eCommerce orders (raw dicts)."""
    try:
        from datetime import timedelta
        since = (datetime.utcnow() - timedelta(minutes=minutes)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        url = 'https://www.wixapis.com/ecom/v1/orders/search'
        headers = {'Authorization': WIX_API_KEY, 'wix-site-id': WIX_SITE_ID,
                   'Content-Type': 'application/json'}
        body = {
            'filter': {'createdDate': {'$gte': since}},
            'sort': [{'fieldName': 'number', 'order': 'DESC'}],
            'paging': {'limit': 100}
        }
        resp = requests.post(url, json=body, headers=headers, timeout=15)
        resp.raise_for_status()
        orders = resp.json().get('orders', [])
        logger.info(f'Wix returned {len(orders)} orders in last {days} days')
        return orders
    except Exception as e:
        logger.error(f'fetch_wix_recent_orders failed: {e}')
        return []


def reconcile_wix_to_monday():
    """Find Wix orders missing from Monday and create them automatically."""
    import time
    logger.info('Reconciliation started...')
    try:
        wix_orders = fetch_wix_recent_orders(minutes=60)
        if not wix_orders:
            logger.info('Reconciliation: no Wix orders returned, skipping')
            return
        monday_nums = fetch_monday_order_numbers()
        missing = []
        for od in wix_orders:
            num = str(od.get('number') or od.get('orderNumber') or '')
            if num and num not in monday_nums:
                missing.append(od)
        if not missing:
            logger.info('Reconciliation: all Wix orders present in Monday - OK')
            return
        logger.warning(f'Reconciliation: {len(missing)} orders missing from Monday: '
                       + str([o.get('number') or o.get('orderNumber') for o in missing]))
        for od in missing:
            try:
                order, order_data = auto_parse({'data': od})
                if not order.get('notes'):
                    order['notes'] = fetch_wix_buyer_note(od.get('id', ''))
                t = threading.Thread(target=process_order_in_background,
                                     args=(order, order_data))
                t.daemon = True
                t.start()
                logger.info(f'Reconciliation: queued order #{order.get("order_number")} for Monday')
                time.sleep(2)
            except Exception as e:
                logger.error(f'Reconciliation error for order {od.get("number")}: {e}', exc_info=True)
    except Exception as e:
        logger.error(f'Reconciliation failed: {e}', exc_info=True)


def start_reconciliation_scheduler():
    """Background thread: reconcile Wix to Monday every 10 minutes."""
    import time
    def _loop():
        time.sleep(60)       # 1 min after startup before first run
        while True:
            reconcile_wix_to_monday()
            time.sleep(600)  # then every 10 minutes
    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    logger.info('Reconciliation scheduler started (every 10 min)')


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
        if not order.get('notes'):
            wix_order_id = order_data.get('id', '')
            order['notes'] = fetch_wix_buyer_note(wix_order_id)
        thread = threading.Thread(target=process_order_in_background, args=(order, order_data))
        thread.daemon = True
        thread.start()
        logger.info(f'Background thread started for order #{order.get("order_number")}')
        return jsonify({'status': 'ok', 'order': order.get('order_number')}), 200
    except Exception as e:
        logger.error(f'Error processing webhook: {e}', exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/webhook/wix-contact', methods=['POST'])
def wix_contact_webhook():
    """Handle Wix contact-created and Old Wix Forms webhooks â creates lead in Monday LEAD-URI board."""
    ts = datetime.utcnow().isoformat()
    logger.info(f'[{ts}] Contact webhook received')

    try:
        raw = request.get_data(as_text=True)
        logger.info(f'Content-type: {request.content_type}, body length: {len(raw)}')
        logger.info(f'Payload preview: {raw[:500]}')
        site = request.args.get('site', '')
        source = 'Pisicilaferestre' if site == 'pisici' else 'CityCATS.ro'

        try:
            json_body = request.get_json(force=True, silent=True)
        except Exception:
            json_body = None

        # ââ Old Wix Forms format (submissions array or formName present) ââââââ
        # Payload may be wrapped: {"data": {"formName": ..., "submissions": [...]}}
        _form_data = json_body
        if isinstance(json_body, dict) and isinstance(json_body.get('data'), dict):
            _inner = json_body['data']
            if 'formName' in _inner or 'submissions' in _inner or 'submissionId' in _inner:
                _form_data = _inner
        if _form_data and ('submissions' in _form_data or 'formName' in _form_data or 'submissionId' in _form_data):
            form_name = _form_data.get('formName', '') or 'FORMULAR'
            logger.info(f'Old Wix Forms payload detected, form: {form_name}')
            contact = extract_contact_from_old_form(_form_data)
            if not contact:
                logger.warning(f'Old Wix Forms: could not extract contact. Body: {raw[:300]}')
                return jsonify({'received': True, 'status': 'no_contact_old_form'}), 200
            result = create_lead_monday_item(contact, form_name=form_name, source=source)
            item = result.get('data', {}).get('create_item', {})
            if item.get('id'):
                logger.info(f'Monday item created (old form): {item["name"]} (ID: {item["id"]})')
                return jsonify({'received': True, 'status': 'created', 'monday_id': item['id']}), 200
            else:
                logger.warning(f'Monday response: {str(result)[:300]}')
                return jsonify({'received': True, 'status': 'monday_error', 'detail': str(result)[:200]}), 200

        # ââ CRM contact-created format (JWT or createdEvent/entity) ââââââââââ
        contact = extract_contact_from_payload(raw, json_body)

        if not contact:
            logger.warning(f'Could not extract contact. Body preview: {raw[:300]}')
            return jsonify({'received': True, 'status': 'no_contact'}), 200

        source_type = contact.get('source', {}).get('sourceType', '')
        if source_type and source_type != 'WIX_FORMS':
            logger.info(f'Ignoring contact â source: {source_type}')
            return jsonify({'received': True, 'status': 'ignored', 'source': source_type}), 200

        payload_form_name = (json_body or {}).get('formName', '') or ''
        logger.info(f'Contact received, form: {payload_form_name or "(from label key)"}')
        result = create_lead_monday_item(contact, form_name=payload_form_name, source=source)

        item = result.get('data', {}).get('create_item', {})
        if item.get('id'):
            logger.info(f'Monday item created: {item["name"]} (ID: {item["id"]})')
            return jsonify({'received': True, 'status': 'created', 'monday_id': item['id']}), 200
        else:
            logger.warning(f'Monday response: {str(result)[:300]}')
            return jsonify({'received': True, 'status': 'monday_error', 'detail': str(result)[:200]}), 200

    except Exception as e:
        logger.error(f'Error in contact webhook: {e}', exc_info=True)
        return jsonify({'received': True, 'status': 'error', 'message': str(e)}), 200



@app.route('/reconcile', methods=['GET', 'POST'])
def reconcile_endpoint():
    """Manually trigger reconciliation."""
    threading.Thread(target=reconcile_wix_to_monday, daemon=True).start()
    return jsonify({'status': 'reconciliation started'}), 200


# Start reconciliation scheduler (runs in background under gunicorn too)
start_reconciliation_scheduler()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
