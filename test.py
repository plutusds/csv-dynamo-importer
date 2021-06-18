import csv

from decimal import Decimal

from django.db import transaction
from django.core.management.base import BaseCommand
from exchange.users.models import User, Profile, Verification
from exchange.metadata.models import Asset
from exchange.accounts.models import Account, AvgBuyPrice
from exchange.cryptos.models import CryptoDeposit
from exchange.cryptos.models import CryptoWithdrawal
from exchange.cryptos.models import Network
from exchange.trades.models import Order, Trade
from exchange.metadata.models import TradingPair
from exchange.policies.models import FeeRate
from exchange.transactions.models import TransactionHistory
from cryptography.fernet import Fernet
from Crypto.Cipher import AES
import base64
import hashlib

from math import ceil


class BaseEncoder():
    def __init__(self, block_size=8):  # blowfish=8, aes=16
        self.block_size = block_size


class Encoder(BaseEncoder):
    def encode(self, src):
        src_len = len(src)
        block_number = ceil((src_len+1)/self.block_size)
        pad_size = block_number * self.block_size - src_len
        return src + bytes([pad_size] * pad_size)

    def decode(self, encoded_bytes):
        pad_size = encoded_bytes[-1]
        return encoded_bytes[:-pad_size]


aes_key = ''.encode("utf8")
iv = ''.encode("utf8")


fernet_key = ''
fernet_instance = Fernet(fernet_key.encode('utf-8'))


class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        encoder = Encoder(block_size=16)

        with transaction.atomic():
            if not Asset.objects.filter(symbol='KRW').exists():
                Asset.objects.create(symbol='KRW')

            users_dict_by_email = dict()
            users_dict_by_uuid = dict()
            profiles_dict_by_user_uuid = dict()
            verification_dict_by_user_uuid = dict()
            with open('data/20210304-user-prod.csv', newline='') as csvfile:
                reader = csv.DictReader(csvfile, delimiter=',')
                for row in reader:
                    user_uuid = row['user_id']
                    created = row['created_at']
                    email_username = row['email_username']
                    encrypted_email_domain_aes = row['email_domain'].encode('utf-8')

                    decoded_text = base64.urlsafe_b64decode(encrypted_email_domain_aes)
                    cipher = AES.new(aes_key, AES.MODE_CBC, iv)
                    decrypted_text = cipher.decrypt(decoded_text)
                    decrypted_text = encoder.decode(decrypted_text)

                    email_domain = decrypted_text.decode("utf-8")

                    email = f'{email_username}@{email_domain}'.strip().encode().decode()

                    hashed_email = hashlib.sha256(email.encode('utf-8')).hexdigest()

                    #print(email_username, email_domain, email, hashed_email, len(email))

                    last_login = row['last_login_at']

                    encrypted_email_domain_fernet = fernet_instance.encrypt(f'@{email_domain}'.encode('utf-8')).decode('utf-8')

                    user_value = {
                        'uuid': user_uuid,
                        'created': created,
                        'email': hashed_email,
                        'email_username': email_username,
                        'email_domain': encrypted_email_domain_fernet,
                        'password': 'migrated_user_temp_password',
                        'is_migrated': True,
                        'changed_password_after_migration': False,
                        'last_login': last_login,
                    }

                    birth_date = row['birthdate']  # 1968-06-08
                    gender = row['gender']
                    family_name = row['family_name']
                    given_name = row['given_name']
                    phone_number0 = row['phone_number0']
                    encrypted_phone_number1_AES = row['phone_number1']
                    phone_number2 = row['phone_number2']

                    decoded_text = base64.urlsafe_b64decode(encrypted_phone_number1_AES)
                    cipher = AES.new(aes_key, AES.MODE_CBC, iv)
                    decrypted_phone_number1 = cipher.decrypt(decoded_text)

                    if decrypted_phone_number1:
                        decrypted_phone_number1 = encoder.decode(decrypted_phone_number1).decode('utf-8')
                        encrypted_phone_number1_fernet = fernet_instance.encrypt(decrypted_phone_number1.encode('utf-8')).decode('utf-8')

                        phone_number = f'{phone_number0}{decrypted_phone_number1}{phone_number2}'
                        hashed_phone_number = hashlib.sha256(phone_number.encode()).hexdigest()
                    else:
                        hashed_phone_number = ''
                        encrypted_phone_number1_fernet = ''

                    email_verified_at = row['email_verified_at']
                    phone_verified_at = row['phone_number_verified_at']

                    if not email_verified_at:
                        email_verified_at = None

                    if not phone_verified_at:
                        phone_verified_at = None

                    if email_verified_at:
                        is_email_verified = True
                    else:
                        is_email_verified = False

                    if birth_date and phone_number0 and phone_number2 and family_name and given_name:
                        is_phone_verified = True
                    else:
                        is_phone_verified = False

                    verification_value = {
                        'is_email_verified': is_email_verified,
                        'email_last_verified_at': email_verified_at,
                        'is_phone_verified': is_phone_verified,
                        'phone_last_verified_at': phone_verified_at,
                    }

                    if gender == 'MALE':
                        gender = 'M'
                    elif gender == 'FEMALE':
                        gender = 'F'
                    else:
                        gender = ''

                    if birth_date:
                        birth_date = ''.join(birth_date.split(' ')[0].split('-'))

                    profile_value = {
                        'phone_number': hashed_phone_number,
                        'phone_number_first': phone_number0,
                        'phone_number_middle': encrypted_phone_number1_fernet,
                        'phone_number_last': phone_number2,
                        'first_name': given_name,
                        'last_name': family_name,
                        'gender': gender,
                        'birth_date': birth_date
                    }

                    if users_dict_by_email.get(email):
                        if user_value['last_login'] > users_dict_by_email.get(email).get('last_login'):
                            users_dict_by_email[email] = user_value
                            users_dict_by_uuid[user_uuid] = user_value
                            profiles_dict_by_user_uuid[user_uuid] = profile_value
                            verification_dict_by_user_uuid[user_uuid] = verification_value
                    else:
                        users_dict_by_email[email] = user_value
                        users_dict_by_uuid[user_uuid] = user_value
                        profiles_dict_by_user_uuid[user_uuid] = profile_value
                        verification_dict_by_user_uuid[user_uuid] = verification_value
                    #print(gender, birth_date, family_name, given_name, phone_number0, encrypted_phone_number1_fernet, phone_number2)

            user_objs = list()
            for uuid_in_dict in users_dict_by_uuid:
                user_obj = User(
                    **users_dict_by_uuid[uuid_in_dict]
                )
                user_objs.append(user_obj)
            created_users = User.objects.bulk_create(user_objs)

            profile_objs = list()
            verification_objs = list()
            for created_user in created_users:
                profiles_dict_by_user_uuid[created_user.uuid]['user_id'] = created_user.id
                verification_dict_by_user_uuid[created_user.uuid]['user_id'] = created_user.id

                profile_obj = Profile(
                    **profiles_dict_by_user_uuid[created_user.uuid]
                )
                profile_objs.append(profile_obj)

                verification_obj = Verification(
                    **verification_dict_by_user_uuid[created_user.uuid]
                )
                verification_objs.append(verification_obj)
            Profile.objects.bulk_create(profile_objs)
            Verification.objects.bulk_create(verification_objs)

            balance_dict_by_user_uuid_symbol = dict()
            with open('data/20210304-wallets-prod.csv', newline='') as csvfile:
                reader = csv.DictReader(csvfile, delimiter=',')
                for row in reader:
                    user_uuid = row['user_id']
                    currency = row['currency']
                    balance = row['balance']

                    if user_uuid in balance_dict_by_user_uuid_symbol:
                        balance_dict_by_user_uuid_symbol[user_uuid][currency] = Decimal(balance)
                    else:
                        balance_dict_by_user_uuid_symbol[user_uuid] = dict()
                        balance_dict_by_user_uuid_symbol[user_uuid][currency] = Decimal(balance)

                    if not Asset.objects.filter(symbol=currency).exists():
                        Asset.objects.create(
                            symbol=currency
                        )

            accounts_list = list()
            for created_user in created_users:
                # create accounts
                for asset in Asset.objects.all():
                    user_in_balance_dict = balance_dict_by_user_uuid_symbol.get(created_user.uuid)
                    if user_in_balance_dict:
                        balance = user_in_balance_dict.get(asset.symbol)
                        if not balance:
                            balance = Decimal('0')
                    else:
                        balance = Decimal('0')

                    account_data = Account(
                        user=created_user,
                        asset=asset,
                        symbol=asset.symbol,
                        balance=balance
                    )
                    accounts_list.append(account_data)

            Account.objects.bulk_create(
                accounts_list
            )

            user_ids = list()
            for u in created_users:
                user_ids.append(u.id)

            avg_buy_price_list = list()
            for account in Account.objects.filter(user_id__in=user_ids).all():
                avg_buy_price_data = AvgBuyPrice(
                    user=account.user,
                    asset=account.asset,
                    account=account,
                    avg_buy_price=Decimal('0'),
                    balance=account.balance
                )
                avg_buy_price_list.append(avg_buy_price_data)

            AvgBuyPrice.objects.bulk_create(
                avg_buy_price_list
            )

            # TODO: bulk
            with open('data/20210304-transfer-prod.csv', newline='') as csvfile:
                reader = csv.DictReader(csvfile, delimiter=',')
                for row in reader:
                    user_uuid = row['user_id']
                    created_at = row['created_at']
                    updated_at = row['updated_at']
                    amount = row['amount']
                    confirmations = row['confirmations']
                    direction = row['direction']
                    native_price = row['native_price']
                    currency = row['currency']
                    ext_tx_id = row['ext_tx_id']
                    fee = row['user_fee']
                    status = row['status']

                    if user_uuid == 'e55d5499-5a25-4e0e-9c2b-173861364ce0':
                        continue
                    user = User.objects.filter(uuid=user_uuid).first()
                    account = Account.objects.filter(user=user, symbol=currency).first()

                    network = Network.objects.filter(name='Ethereum').first()
                    if not network:
                        network = Network.objects.create(name='Ethereum')

                    #TODO: diversify crypto_transfer_status
                    #TODO: request receiver's address and sender's address
                    if direction == 'DEPOSIT':
                        CryptoDeposit.objects.create(
                            user=user,
                            symbol=currency,
                            crypto_transfer_status='COMPLETED',
                            confirmations=int(confirmations),
                            transfer_type='DEPOSIT',
                            volume=amount,
                            amount=Decimal(native_price),
                            account=account,
                            fee=Decimal(fee),
                            asset=account.asset,
                            transaction_id=ext_tx_id,
                            transaction_created_at=created_at,
                            transaction_done_at=created_at,
                            created=created_at,
                            network=network
                        )
                    elif direction == 'WITHDRAWAL':
                        CryptoWithdrawal.objects.create(
                            user=user,
                            symbol=currency,
                            crypto_transfer_status='COMPLETED',
                            confirmations=int(confirmations),
                            transfer_type='WITHDRAWAL',
                            volume=amount,
                            amount=Decimal(native_price),
                            fee=Decimal(fee),
                            account=account,
                            asset=account.asset,
                            transaction_id=ext_tx_id,
                            transaction_created_at=created_at,
                            transaction_done_at=created_at,
                            created=created_at,
                            network=network
                        )
                    else:
                        raise PermissionError

            # TODO: bulk
            with open('data/20210304-orders-prod.csv', newline='') as csvfile:
                reader = csv.DictReader(csvfile, delimiter=',')
                for row in reader:
                    created_at = row['created_at']
                    updated_at = row['updated_at']
                    amount = row['amount']
                    currency_pair = row['currency_pair'] # ETH_BTC
                    fee = row['fee']
                    price = row['price']
                    remains = row['remains']
                    side = row['side']
                    status = row['status']
                    total = row['total']
                    order_type = row['type']
                    user_uuid = row['user_id']
                    order_id = row['order_id']

                    user = User.objects.filter(uuid=user_uuid).first()
                    trading_pair = TradingPair.objects.filter(
                        base_symbol=currency_pair.split('_')[0],
                        quote_symbol=currency_pair.split('_')[1],
                    ).first()

                    fee_rate = FeeRate.objects.first()
                    if not fee_rate:
                        fee_rate = FeeRate.objects.create(
                            buy_maker_fee_rate=Decimal('0.001'),
                            buy_taker_fee_rate=Decimal('0.001'),
                            sell_maker_fee_rate=Decimal('0.001'),
                            sell_taker_fee_rate=Decimal('0.001'),
                        )

                    if not trading_pair:
                        base_asset = Asset.objects.filter(symbol=currency_pair.split('_')[0]).first()
                        quote_asset = Asset.objects.filter(symbol=currency_pair.split('_')[1]).first()

                        trading_pair = TradingPair.objects.create(
                            base=base_asset,
                            quote=quote_asset,
                            name=f'{base_asset.symbol}-{quote_asset.symbol}',
                            close_price=Decimal('0'),
                            fee_rate=fee_rate
                        )
                    # CANCELED, CANCELING, CREATED, FILLED, PARTIAL, PARTIAL_FILLED

                    if status == 'CANCELED':
                        order_status = 'CANCELLED'
                    elif status == 'CANCELING':
                        order_status = 'CANCELLED'
                    elif status == 'CREATED':
                        order_status = 'CANCELLED'
                    elif status == 'PARTIAL':
                        order_status = 'CANCELLED'
                    elif status == 'PARTIAL_FILLED':
                        order_status = 'CANCELLED'
                    else:
                        order_status = 'FILLED'

                    Order.objects.create(
                        uuid=order_id,
                        user=user,
                        trading_pair=trading_pair,
                        trading_pair_name=trading_pair.name,
                        created=created_at,
                        modified=updated_at,
                        price=Decimal(price),
                        volume=Decimal(amount),
                        amount=Decimal(price) * Decimal(amount),
                        pending_order_amount=Decimal('0'),
                        volume_remaining=Decimal(remains),
                        volume_filled=Decimal(amount) - Decimal(remains),
                        fee_paid=Decimal(fee),
                        side=side,
                        order_status=order_status,
                        order_type='LIMIT'
                    )

            # TODO: bulk
            with open('data/20210304-trade-prod.csv', newline='') as csvfile:
                reader = csv.DictReader(csvfile, delimiter=',')
                for row in reader:
                    created_at = row['created_at']
                    updated_at = row['updated_at']
                    amount = row['amount']
                    buy_fee = row['buy_fee']
                    sell_fee = row['sell_fee']
                    buy_user_id = row['buy_user_id']
                    sell_user_id = row['sell_user_id']
                    buy_order_id = row['buy_order_id']
                    sell_order_id = row['sell_order_id']
                    taker = row['taker']
                    currency_pair = row['currency_pair'] # ETH_BTC
                    price = row['price']

                    fee_rate = FeeRate.objects.first()
                    if not fee_rate:
                        fee_rate = FeeRate.objects.create()

                    trading_pair = TradingPair.objects.filter(
                        base_symbol=currency_pair.split('_')[0],
                        quote_symbol=currency_pair.split('_')[1],
                    ).first()

                    if trading_pair:
                        base_asset = trading_pair.base
                        quote_asset = trading_pair.quote
                    else:
                        base_asset = Asset.objects.filter(symbol=currency_pair.split('_')[0]).first()
                        quote_asset = Asset.objects.filter(symbol=currency_pair.split('_')[1]).first()

                        trading_pair = TradingPair.objects.create(
                            base=base_asset,
                            quote=quote_asset,
                            close_price=Decimal('0'),
                            fee_rate=fee_rate
                        )

                    buy_order = Order.objects.filter(uuid=buy_order_id).first()
                    sell_order = Order.objects.filter(uuid=sell_order_id).first()

                    if buy_order:
                        buy_order_at = buy_order.created
                    else:
                        buy_order_at = created_at

                    if sell_order:
                        sell_order_at = sell_order.created
                    else:
                        sell_order_at = created_at

                    buyer = User.objects.filter(uuid=buy_user_id).first()
                    seller = User.objects.filter(uuid=sell_user_id).first()

                    Trade.objects.create(
                        trading_pair=trading_pair,
                        trading_pair_name=trading_pair.name,
                        base_symbol=base_asset.symbol,
                        quote_symbol=quote_asset.symbol,
                        price=Decimal(price),
                        volume=Decimal(amount),
                        amount=Decimal(price) * Decimal(amount),
                        side=taker,
                        buyer_fee=Decimal(buy_fee),
                        seller_fee=Decimal(sell_fee),
                        buy_order=buy_order,
                        sell_order=sell_order,
                        buyer=buyer,
                        seller=seller,
                        created=created_at,
                        modified=updated_at,
                        buy_order_at=buy_order_at,
                        sell_order_at=sell_order_at
                    )
