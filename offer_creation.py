import aiohttp
import asyncio
import json
import time
import os
from datetime import datetime
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TokenManager:
    def __init__(self, client_id: str, client_secret: str):
        self.access_token = None
        self.token_expires_at = 0
        self.client_id = client_id
        self.client_secret = client_secret
        
    async def get_token(self, session):
        if self.access_token and time.time() < self.token_expires_at - 300:  # 5 min buffer
            return self.access_token
            
        token_url = 'https://login.microsoftonline.com/df09f37c-c395-4f26-b28e-356eb3c11d64/oauth2/token'
        token_data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'resource': 'https://graph.microsoft.com/'
        }
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        
        try:
            async with session.post(token_url, data=token_data, headers=headers) as response:
                if response.status == 200:
                    token_response = await response.json()
                    self.access_token = token_response['access_token']
                    self.token_expires_at = time.time() + int(token_response['expires_in'])
                    return self.access_token
                else:
                    error_text = await response.text()
                    raise Exception(f"Failed to get token. Status: {response.status}, Error: {error_text}")
        except Exception as e:
            logger.error(f"Error getting token: {str(e)}")
            raise

class OfferCreator:
    def __init__(self, token_manager: TokenManager):
        self.token_manager = token_manager
        self.base_url = 'https://graph.microsoft.com/rp/product-ingestion/configure'
        
    def get_offer_payload(self, offer_name: str) -> dict:
        return {
            "$schema": "https://schema.mp.microsoft.com/schema/configure/2022-07-01",
            "resources": [
                {
                    "$schema": "https://schema.mp.microsoft.com/schema/price-and-availability-private-offer-plan/2023-07-15",
                    "product": "product/d414cbcc-a721-4b58-bdaa-145e05e87fa7",
                    "resourceName": "newSaaSPlanAbsolutePricing",
                    "plan": "plan/d414cbcc-a721-4b58-bdaa-145e05e87fa7/1bed11cb-98e3-4429-942f-2561eb6e212c",
                    "offerPricingType": "saasNewCustomizedPlans",
                    "pricing": {
                        "recurrentPrice": {
                            "recurrentPriceMode": "flatRate",
                            "priceInputOption": "usd",
                            "prices": [
                                {
                                    "billingTerm": {"type": "month", "value": 1},
                                    "paymentOption": {"type": "month", "value": 1},
                                    "pricePerPaymentInUsd": 10.0
                                },
                                {
                                    "billingTerm": {"type": "year", "value": 1},
                                    "paymentOption": {"type": "month", "value": 1},
                                    "pricePerPaymentInUsd": 10.0
                                }
                            ]
                        },
                        "customMeters": {
                            "priceInputOption": "usd",
                            "meters": {
                                "xyx": {
                                    "includedQuantities": [
                                        {
                                            "billingTerm": {"type": "month", "value": 1},
                                            "isInfinite": False,
                                            "quantity": 10.0
                                        },
                                        {
                                            "billingTerm": {"type": "year", "value": 1},
                                            "isInfinite": True
                                        }
                                    ],
                                    "pricePerPaymentInUsd": 10.0
                                },
                                "ws43": {
                                    "includedQuantities": [
                                        {
                                            "billingTerm": {"type": "month", "value": 1},
                                            "isInfinite": False,
                                            "quantity": 4.0
                                        },
                                        {
                                            "billingTerm": {"type": "year", "value": 1},
                                            "isInfinite": False,
                                            "quantity": 4.0
                                        }
                                    ],
                                    "pricePerPaymentInUsd": 10.0
                                }
                            }
                        }
                    }
                },
                {
                    "$schema": "https://schema.mp.microsoft.com/schema/private-offer/2023-07-15",
                    "name": offer_name,
                    "state": "draft",
                    "privateOfferType": "customerPromotion",
                    "offerPricingType": "saasNewCustomizedPlans",
                    "variableStartDate": True,
                    "end": "2024-06-28",
                    "acceptBy": "2024-05-28",
                    "preparedBy": "sundaran.s@workspan.com",
                    "termsAndConditionsDocSasUrl": "https://query.prod.cms.rt.microsoft.com/cms/api/am/binary/RE4rFOA",
                    "notificationContacts": ["sundaran.s@workspan.com"],
                    "beneficiaries": [
                        {
                            "id": "xxxxxx-2163-5eea-ae4e-d6e88627c26b:6ea018a9-da9d-4eae-8610-22b51ebe260b_2019-05-31",
                            "description": "Top First Customer"
                        }
                    ],
                    "pricing": [
                        {
                            "product": "product/d414cbcc-a721-4b58-bdaa-145e05e87fa7",
                            "discountType": "absolute",
                            "priceDetails": {
                                "resourceName": "newSaaSPlanAbsolutePricing"
                            },
                            "basePlan": "plan/d414cbcc-a721-4b58-bdaa-145e05e87fa7/1bed11cb-98e3-4429-942f-2561eb6e212c",
                            "newPlanDetails": {
                                "name": f"plan_{offer_name}",
                                "description": "custom plan description"
                            }
                        }
                    ]
                }
            ]
        }

    async def create_offer(self, session: aiohttp.ClientSession, offer_number: int) -> None:
        offer_name = f"dynamic_offer_1000_workers_{offer_number}"
        payload = self.get_offer_payload(offer_name)
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                token = await self.token_manager.get_token(session)
                headers = {
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {token}'
                }
                
                async with session.post(
                    f"{self.base_url}?$version=2022-07-01",
                    json=payload,
                    headers=headers
                ) as response:
                    if response.status == 202:
                        logger.info(f"Successfully created offer: {offer_name}")
                        return
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to create offer {offer_name}. Status: {response.status}, Error: {error_text}")
                        
            except Exception as e:
                logger.error(f"Error creating offer {offer_name}: {str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    await asyncio.sleep(2 ** retry_count)  # Exponential backoff
                continue
            
            break
        
        if retry_count == max_retries:
            logger.error(f"Failed to create offer {offer_name} after {max_retries} attempts")


async def main():
    # Get parameters from environment variables
    client_id = os.environ.get('MS_CLIENT_ID')
    client_secret = os.environ.get('MS_CLIENT_SECRET')
    num_offers = os.environ.get('NUM_OFFERS')

    if not all([client_id, client_secret, num_offers]):
        raise ValueError("Missing required environment variables. Please ensure MS_CLIENT_ID, MS_CLIENT_SECRET, and NUM_OFFERS are set.")

    try:
        num_offers = int(num_offers)
        if num_offers <= 0:
            raise ValueError("NUM_OFFERS must be a positive number")
    except ValueError as e:
        logger.error(f"Invalid NUM_OFFERS value: {str(e)}")
        raise

    print(f"\nStarting creation of {num_offers} offers in parallel...")
    
    token_manager = TokenManager(client_id, client_secret)
    offer_creator = OfferCreator(token_manager)
    
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            offer_creator.create_offer(session, i)
            for i in range(num_offers)
        ]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    end_time = time.time()
    
    print(f"\nTask completed!")
    logger.info(f"Total time taken: {end_time - start_time:.2f} seconds")