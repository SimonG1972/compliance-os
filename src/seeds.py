# src/seeds.py
from __future__ import annotations
from typing import List, Tuple

# seed = (label, homepage)
PLATFORMS: List[Tuple[str, str]] = [
    ("TikTok", "https://www.tiktok.com"),
    ("YouTube", "https://www.youtube.com"),
    ("Instagram", "https://www.instagram.com"),
    ("Facebook", "https://www.facebook.com"),
    ("X", "https://x.com"),
    ("Snapchat", "https://www.snapchat.com"),
    ("Reddit", "https://www.reddit.com"),
    ("Pinterest", "https://www.pinterest.com"),
]

PLATFORMS_EXTRA: List[Tuple[str, str]] = [
    ("LinkedIn", "https://www.linkedin.com"),
    ("Twitch", "https://www.twitch.tv"),
    ("Discord", "https://discord.com"),
    ("Medium", "https://medium.com"),
    ("Shopify", "https://www.shopify.com"),
    ("Patreon", "https://www.patreon.com"),
    ("Substack", "https://substack.com"),
    ("Quora", "https://www.quora.com"),
    ("Tumblr", "https://www.tumblr.com"),
    ("Vimeo", "https://vimeo.com"),
    ("Flickr", "https://www.flickr.com"),
    ("GitHub", "https://github.com"),
]

REGULATORS: List[Tuple[str, str]] = [
    ("FTC", "https://www.ftc.gov"),
    ("EU Commission", "https://commission.europa.eu"),
    ("ICO UK", "https://ico.org.uk"),
]

REGULATORS_EXTRA: List[Tuple[str, str]] = [
    ("CNIL (FR)", "https://www.cnil.fr"),
    ("EDPB (EU)", "https://edpb.europa.eu"),
    ("EDPS (EU)", "https://www.edps.europa.eu"),
    ("AG California (CPPA portal)", "https://cppa.ca.gov"),
    ("OAG California", "https://oag.ca.gov"),
    ("FTC Business Guidance", "https://www.ftc.gov/business-guidance"),
    ("FCC", "https://www.fcc.gov"),
    ("SEC", "https://www.sec.gov"),
]

MARKETPLACES: List[Tuple[str, str]] = [
    ("Etsy", "https://www.etsy.com"),
    ("Amazon", "https://www.amazon.com"),
    ("eBay", "https://www.ebay.com"),
]

MARKETPLACES_EXTRA: List[Tuple[str, str]] = [
    ("Walmart", "https://www.walmart.com"),
    ("Alibaba", "https://www.alibaba.com"),
    ("MercadoLibre", "https://www.mercadolibre.com"),
    ("Shopee", "https://shopee.com"),
    ("Rakuten", "https://www.rakuten.com"),
]

AD_NETWORKS: List[Tuple[str, str]] = [
    ("Meta Ads", "https://www.facebook.com/business/ads"),
    ("Google Ads", "https://ads.google.com/home/"),
    ("TikTok Ads", "https://ads.tiktok.com"),
]

AD_NETWORKS_EXTRA: List[Tuple[str, str]] = [
    ("Snap Ads", "https://forbusiness.snapchat.com"),
    ("Pinterest Ads", "https://ads.pinterest.com"),
    ("Reddit Ads", "https://ads.reddit.com"),
    ("X Ads", "https://ads.x.com"),
    ("LinkedIn Ads", "https://business.linkedin.com/marketing-solutions/ads"),
]

APPSTORES: List[Tuple[str, str]] = [
    ("Apple Developer", "https://developer.apple.com"),
    ("Apple Support (Policies)", "https://support.apple.com"),
    ("Google Play Policy", "https://support.google.com/googleplay"),
    ("Google Policies", "https://policies.google.com"),
]

CLOUD: List[Tuple[str, str]] = [
    ("AWS", "https://aws.amazon.com"),
    ("Google Cloud", "https://cloud.google.com"),
    ("Azure", "https://azure.microsoft.com"),
    ("Cloudflare", "https://www.cloudflare.com"),
]

PAYMENTS: List[Tuple[str, str]] = [
    ("Stripe", "https://stripe.com"),
    ("PayPal", "https://www.paypal.com"),
    ("Square", "https://squareup.com"),
    ("Visa", "https://usa.visa.com"),
    ("Mastercard", "https://www.mastercard.us"),
    ("Plaid", "https://plaid.com"),
]

def get_category(name: str) -> List[Tuple[str, str]]:
    key = name.strip().lower()
    if key in ("platforms", "platform"):
        return PLATFORMS
    if key in ("platforms_extra", "platforms-extra", "platforms2"):
        return PLATFORMS_EXTRA
    if key in ("regulators", "regulator"):
        return REGULATORS
    if key in ("regulators_extra", "regulators-extra", "regulators2"):
        return REGULATORS_EXTRA
    if key in ("marketplaces", "marketplace"):
        return MARKETPLACES
    if key in ("marketplaces_extra", "marketplaces-extra", "marketplaces2"):
        return MARKETPLACES_EXTRA
    if key in ("adnetworks", "ad_networks", "ad", "ads"):
        return AD_NETWORKS
    if key in ("adnetworks_extra", "ad_networks_extra", "adnetworks-extra", "ads2"):
        return AD_NETWORKS_EXTRA
    if key in ("appstores", "app_stores", "stores"):
        return APPSTORES
    if key in ("cloud", "clouds"):
        return CLOUD
    if key in ("payments", "payment"):
        return PAYMENTS
    return []
