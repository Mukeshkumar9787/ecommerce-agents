"""E-Commerce Store client interfacing with SMK backend API in real time."""

import logging
import os
import re
from typing import Any, Dict, List, Optional
import httpx
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


def get_smk_base_urls() -> List[str]:
    """Return candidates for SMK server URL (host or docker)."""
    env_url = os.getenv("SMK_SERVER_URL")
    urls = []
    if env_url:
        urls.append(env_url.rstrip("/"))
    urls.extend([
        "http://localhost:4000/api",
        "http://host.docker.internal:4000/api",
        "http://127.0.0.1:4000/api",
    ])
    # Remove duplicates preserving order
    seen = set()
    result = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            result.append(u)
    return result


def fetch_product_by_slug(slug: str) -> Optional[Dict[str, Any]]:
    """
    Query SMK e-commerce backend in real-time to fetch product details by slug.
    GET /api/products/{slug}
    """
    if not slug:
        return None

    cleaned_slug = str(slug).strip()
    slug_candidates = [cleaned_slug]

    # Normalized candidate (e.g. "Classic Heavyweight Cotton" -> "classic-heavyweight-cotton")
    normalized = re.sub(r"[^a-zA-Z0-9]+", "-", cleaned_slug).strip("-").lower()
    if normalized and normalized not in slug_candidates:
        slug_candidates.append(normalized)

    for target_slug in slug_candidates:
        for base in get_smk_base_urls():
            url = f"{base}/products/{target_slug}"
            try:
                with httpx.Client(timeout=3.0) as client:
                    resp = client.get(url)
                    if resp.status_code == 200:
                        data = resp.json()
                        if data.get("success") and "data" in data and data["data"]:
                            logger.info("Successfully fetched real-time product details for slug '%s' from %s", target_slug, url)
                            return data["data"]
            except Exception as exc:
                logger.debug("Failed connecting to %s: %s", url, exc)

    # Secondary lookup: if slug is an ID or PROD-XX, inspect active products list
    if cleaned_slug.isdigit() or cleaned_slug.upper().startswith("PROD-"):
        prod_id = int(cleaned_slug.replace("PROD-", "").replace("prod-", "")) if any(char.isdigit() for char in cleaned_slug) else None
        if prod_id is not None:
            active_list = fetch_active_products()
            for p in active_list:
                if p.get("id") == prod_id:
                    logger.info("Matched active product ID %d with slug '%s'", prod_id, p.get("slug"))
                    return p

    return None


def fetch_active_products() -> List[Dict[str, Any]]:
    """
    Fetch all active products from SMK e-commerce backend.
    GET /api/products/active
    """
    for base in get_smk_base_urls():
        url = f"{base}/products/active"
        try:
            with httpx.Client(timeout=3.0) as client:
                resp = client.get(url)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("success") and isinstance(data.get("data"), list):
                        return data["data"]
        except Exception as exc:
            logger.debug("Failed connecting to %s: %s", url, exc)

    return []


class ProductSlugInput(BaseModel):
    slug: str = Field(..., description="Store product slug (e.g. 'classic-heavyweight-organic-cotton-t-shirt')")


def get_ecommerce_product_details(slug: str) -> Dict[str, Any]:
    """Retrieve live real-time product specifications, inventory, and retail pricing from SMK store."""
    product = fetch_product_by_slug(slug)
    if not product:
        return {"found": False, "slug": slug, "message": f"Product slug '{slug}' not found on store backend."}

    return {
        "found": True,
        "id": product.get("id"),
        "title": product.get("title"),
        "slug": product.get("slug"),
        "stock": product.get("stock", 0),
        "retail_price": float(product.get("price", 0.0)),
        "mrp": float(product.get("mrp", 0.0)),
        "variant": product.get("variant"),
        "size": product.get("sizeTitle") or product.get("sizeName"),
        "color": product.get("colorCode"),
    }


def restock_ecommerce_product(
    slug: str,
    quantity: int,
    po_number: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Replenish stock for a product in SMK e-commerce backend upon PO approval.
    POST /api/products/restock
    """
    if not slug or quantity <= 0:
        return None

    # Policy: Quantity of products to be added should be less than 20
    effective_quantity = int(quantity)
    if effective_quantity >= 20:
        logger.info(
            "Capping restock quantity from %d to 19 (quantity of products to be added should be less than 20)",
            effective_quantity,
        )
        effective_quantity = 19

    payload = {
        "slug": str(slug).strip(),
        "quantity": effective_quantity,
        "poNumber": po_number or "",
    }

    api_key = (
        os.getenv("INVENTORY_AGENT_API_KEY")
        or os.getenv("AGENT_API_KEY")
        or os.getenv("RESTOCK_API_KEY")
        or ""
    )
    headers: Dict[str, str] = {
        "Content-Type": "application/json",
    }
    if api_key:
        headers["x-api-key"] = api_key
        headers["Authorization"] = f"Bearer {api_key}"

    for base in get_smk_base_urls():
        url = f"{base}/products/restock"
        try:
            with httpx.Client(timeout=5.0) as client:
                resp = client.post(url, json=payload, headers=headers)
                if resp.status_code in (200, 201):
                    data = resp.json()
                    if data.get("success") and "data" in data:
                        logger.info(
                            "Successfully replenished stock (+%d) in e-commerce platform for '%s' (New stock: %s, PO: %s)",
                            quantity,
                            slug,
                            data["data"].get("currentStock"),
                            po_number,
                        )
                        return data["data"]
        except Exception as exc:
            logger.debug("Failed restocking via %s: %s", url, exc)

    logger.warning("Could not replenish stock for '%s' across SMK backend URLs", slug)
    return None

