#!/usr/bin/env python3
"""
AI Agency Lead Scraper
======================
Finds local businesses in a given niche + city, analyzes their websites
for missing AI/automation features, and scores them as outreach leads.

Two modes:
  1. Google Places API  (accurate, requires API key)
  2. Free fallback      (uses httpx + selectolax to scrape public listings)

Usage:
  # Google Places API mode (recommended)
  export GOOGLE_PLACES_API_KEY="your-key-here"
  python lead_scraper.py --niche "dentist" --city "Austin, TX"

  # Free fallback mode (no API key needed)
  python lead_scraper.py --niche "dentist" --city "Austin, TX" --free

  # Custom output file + result limit
  python lead_scraper.py --niche "plumber" --city "Denver, CO" --limit 30 --output leads.csv

Output:
  CSV file with columns: business_name, address, phone, website, rating,
  review_count, has_booking, has_chatbot, has_contact_form, lead_score,
  observations

Author: AI Agency Outreach System
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field, asdict
from typing import Optional
from urllib.parse import urlparse, urljoin

import httpx

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("lead_scraper")

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Lead:
    """Represents a single business lead with website analysis results."""
    business_name: str = ""
    address: str = ""
    phone: str = ""
    website: str = ""
    rating: float = 0.0
    review_count: int = 0
    has_booking: bool = False
    has_chatbot: bool = False
    has_contact_form: bool = False
    lead_score: int = 0
    observations: str = ""
    _obs_parts: list = field(default_factory=list, repr=False)

    def score(self) -> int:
        """
        Score the lead from 1-10 based on missing features and opportunity.

        Scoring rubric:
          +3  No online booking system detected
          +2  No chatbot / live-chat widget detected
          +2  Low Google reviews (< 50)
          +1  Has a website (means they care somewhat -- worth reaching out)
          +2  High Google rating (>= 4.0) -- good business, just needs tech
        """
        pts = 0
        reasons = []

        # --- Missing features (the opportunity) ---
        if not self.has_booking:
            pts += 3
            reasons.append("No online booking (+3)")
        if not self.has_chatbot:
            pts += 2
            reasons.append("No chatbot (+2)")

        # --- Review gap ---
        if self.review_count < 50:
            pts += 2
            reasons.append(f"Low reviews: {self.review_count} (+2)")

        # --- Has website (worth contacting) ---
        if self.website:
            pts += 1
            reasons.append("Has website (+1)")

        # --- High rating (good business, just behind on tech) ---
        if self.rating >= 4.0:
            pts += 2
            reasons.append(f"High rating: {self.rating} (+2)")

        self.lead_score = min(pts, 10)  # cap at 10
        self._obs_parts = reasons
        self.observations = "; ".join(reasons)
        return self.lead_score


# ---------------------------------------------------------------------------
# Website analyzer
# ---------------------------------------------------------------------------

class WebsiteAnalyzer:
    """
    Fetches a business website and checks for the presence of:
      - Online booking widgets / scheduling tools
      - Chatbot / live-chat widgets
      - Contact forms
    """

    # Booking systems -- iframes, scripts, or link patterns
    BOOKING_SIGNALS = [
        # Scheduling platforms
        "calendly.com", "acuityscheduling.com", "square.site/book",
        "squareup.com/appointments", "vagaro.com", "schedulicity.com",
        "zocdoc.com", "healthgrades.com", "opencare.com",
        "setmore.com", "simplybook.me", "appointy.com",
        "booksy.com", "fresha.com", "mindbodyonline.com",
        "jane.app", "cliniko.com", "practo.com",
        # Generic booking keywords
        "book-now", "book-online", "book-appointment",
        "schedule-appointment", "online-booking", "booking-widget",
        "reserve-now", "make-appointment",
        # Common button / link text patterns (case-insensitive check)
        "book now", "book online", "schedule now",
        "book an appointment", "schedule an appointment",
        "request an appointment", "make an appointment",
        "online scheduling",
    ]

    # Chatbot / live-chat platforms
    CHATBOT_SIGNALS = [
        # Chat platforms
        "tidio.co", "tawk.to", "livechat.com", "drift.com",
        "intercom.io", "crisp.chat", "freshchat", "zendesk.com/chat",
        "hubspot.com/live-chat", "olark.com", "chatra.io",
        "smartsupp.com", "jivochat.com", "botpress.com",
        "manychat.com", "chatfuel.com", "landbot.io",
        "kommunicate.io", "collect.chat", "chatbot.com",
        # Generic chatbot signals
        "chat-widget", "chatwidget", "live-chat",
        "livechat-widget", "chatbot", "ai-chat",
        "chat-bubble", "messenger-widget",
    ]

    # Contact form signals
    FORM_SIGNALS = [
        "<form", "contact-form", "contact_form", "contactform",
        "wpcf7",  # WordPress Contact Form 7
        "wpforms", "gravity-form", "formidable",
        "name=\"email\"", "name=\"phone\"", "name=\"message\"",
        "type=\"submit\"", "input type=\"email\"",
    ]

    def __init__(self, timeout: int = 15):
        self.timeout = timeout
        self.client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            },
        )

    def analyze(self, url: str) -> dict:
        """
        Analyze a website URL for booking, chatbot, and contact form presence.

        Returns:
            dict with keys: has_booking, has_chatbot, has_contact_form
        """
        result = {
            "has_booking": False,
            "has_chatbot": False,
            "has_contact_form": False,
        }

        if not url:
            return result

        # Ensure URL has scheme
        if not url.startswith(("http://", "https://")):
            url = "https://" + url

        try:
            log.debug(f"  Analyzing website: {url}")
            resp = self.client.get(url)
            html = resp.text.lower()

            # Check for booking signals
            for signal in self.BOOKING_SIGNALS:
                if signal.lower() in html:
                    result["has_booking"] = True
                    log.debug(f"    Booking detected: {signal}")
                    break

            # Check for chatbot signals
            for signal in self.CHATBOT_SIGNALS:
                if signal.lower() in html:
                    result["has_chatbot"] = True
                    log.debug(f"    Chatbot detected: {signal}")
                    break

            # Check for contact form signals
            for signal in self.FORM_SIGNALS:
                if signal.lower() in html:
                    result["has_contact_form"] = True
                    log.debug(f"    Contact form detected: {signal}")
                    break

            # Also check common sub-pages for booking/contact
            for subpage in ["/contact", "/book", "/appointment", "/schedule"]:
                try:
                    sub_url = urljoin(url, subpage)
                    sub_resp = self.client.get(sub_url)
                    if sub_resp.status_code == 200:
                        sub_html = sub_resp.text.lower()
                        if not result["has_booking"]:
                            for s in self.BOOKING_SIGNALS:
                                if s.lower() in sub_html:
                                    result["has_booking"] = True
                                    break
                        if not result["has_contact_form"]:
                            for s in self.FORM_SIGNALS:
                                if s.lower() in sub_html:
                                    result["has_contact_form"] = True
                                    break
                except Exception:
                    continue  # Sub-page check is best-effort

        except httpx.TimeoutException:
            log.warning(f"  Timeout analyzing {url}")
        except httpx.ConnectError:
            log.warning(f"  Connection failed for {url}")
        except Exception as e:
            log.warning(f"  Error analyzing {url}: {e}")

        return result

    def close(self):
        self.client.close()


# ---------------------------------------------------------------------------
# Google Places API scraper
# ---------------------------------------------------------------------------

class GooglePlacesScraper:
    """
    Uses the Google Places API (Text Search + Place Details) to find
    local businesses and extract their info.

    Requires: GOOGLE_PLACES_API_KEY environment variable.

    API setup:
      1. Go to https://console.cloud.google.com/
      2. Create a project and enable "Places API" (new) or "Places API" (legacy)
      3. Create an API key under Credentials
      4. Set GOOGLE_PLACES_API_KEY=your-key
    """

    BASE_URL = "https://maps.googleapis.com/maps/api/place"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.client = httpx.Client(timeout=30)

    def search(self, niche: str, city: str, limit: int = 20) -> list[dict]:
        """
        Search for businesses using Google Places Text Search API.

        Args:
            niche: Business type (e.g., "dentist", "plumber")
            city:  City and state (e.g., "Austin, TX")
            limit: Max number of results (API returns up to 60 in pages of 20)

        Returns:
            List of dicts with business info
        """
        query = f"{niche} in {city}"
        log.info(f"Google Places search: '{query}' (limit={limit})")

        all_results = []
        next_page_token = None

        while len(all_results) < limit:
            params = {
                "query": query,
                "key": self.api_key,
            }
            if next_page_token:
                params["pagetoken"] = next_page_token
                # Google requires a short delay before using page tokens
                time.sleep(2)

            resp = self.client.get(
                f"{self.BASE_URL}/textsearch/json", params=params
            )
            data = resp.json()

            if data.get("status") != "OK":
                error_msg = data.get("error_message", data.get("status", "Unknown error"))
                log.error(f"Google Places API error: {error_msg}")
                if data.get("status") == "REQUEST_DENIED":
                    log.error("Check your API key and ensure Places API is enabled.")
                break

            results = data.get("results", [])
            all_results.extend(results)
            log.info(f"  Retrieved {len(results)} results (total: {len(all_results)})")

            next_page_token = data.get("next_page_token")
            if not next_page_token:
                break

        return all_results[:limit]

    def get_details(self, place_id: str) -> dict:
        """
        Get detailed info for a specific place (phone, website, etc.).
        """
        params = {
            "place_id": place_id,
            "fields": "name,formatted_address,formatted_phone_number,website,rating,user_ratings_total",
            "key": self.api_key,
        }

        resp = self.client.get(
            f"{self.BASE_URL}/details/json", params=params
        )
        data = resp.json()

        if data.get("status") != "OK":
            log.warning(f"  Could not get details for place {place_id}")
            return {}

        return data.get("result", {})

    def scrape_leads(self, niche: str, city: str, limit: int = 20) -> list[Lead]:
        """
        Full pipeline: search for businesses, get details, return Lead objects.
        """
        leads = []
        results = self.search(niche, city, limit)

        for i, place in enumerate(results, 1):
            place_id = place.get("place_id")
            name = place.get("name", "Unknown")
            log.info(f"[{i}/{len(results)}] Getting details for: {name}")

            # Get detailed info (phone + website)
            details = self.get_details(place_id) if place_id else {}

            lead = Lead(
                business_name=details.get("name", name),
                address=details.get("formatted_address", place.get("formatted_address", "")),
                phone=details.get("formatted_phone_number", ""),
                website=details.get("website", ""),
                rating=details.get("rating", place.get("rating", 0.0)),
                review_count=details.get("user_ratings_total", place.get("user_ratings_total", 0)),
            )
            leads.append(lead)

            # Be polite to the API
            time.sleep(0.3)

        return leads

    def close(self):
        self.client.close()


# ---------------------------------------------------------------------------
# Free fallback scraper (no API key required)
# ---------------------------------------------------------------------------

class FreeLeadScraper:
    """
    Free alternative that scrapes public business listing pages using
    httpx + selectolax. No API key required.

    Strategy:
      - Searches via DuckDuckGo Lite (HTML-based, no JS required)
      - Extracts business names, addresses, and website URLs from results
      - Follows links to business websites for analysis

    Limitations vs. Google Places API:
      - Fewer results and less structured data
      - Phone numbers may not be available for all businesses
      - Ratings/reviews require additional scraping
      - May be rate-limited with high volume

    Note: This is a best-effort fallback. For production use, the Google
    Places API ($17 per 1000 requests) is strongly recommended.
    """

    DUCKDUCKGO_URL = "https://lite.duckduckgo.com/lite/"

    def __init__(self):
        self.client = httpx.Client(
            timeout=20,
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            },
        )

    def _search_duckduckgo(self, query: str, max_results: int = 20) -> list[dict]:
        """
        Search DuckDuckGo Lite (HTML version) and extract result links.
        Returns list of dicts with 'title', 'url', 'snippet'.
        """
        results = []

        try:
            resp = self.client.post(
                self.DUCKDUCKGO_URL,
                data={"q": query},
            )
            html = resp.text

            # Parse with selectolax if available, else fall back to regex
            try:
                from selectolax.parser import HTMLParser
                tree = HTMLParser(html)

                # DuckDuckGo Lite uses a table-based layout
                # Result links are in <a> tags with class "result-link"
                for link in tree.css("a.result-link"):
                    url = link.attributes.get("href", "")
                    title = link.text(strip=True)
                    if url and title and not url.startswith("/"):
                        results.append({
                            "title": title,
                            "url": url,
                            "snippet": "",
                        })

                # If result-link class doesn't work, try broader approach
                if not results:
                    for row in tree.css("td"):
                        for link in row.css("a"):
                            href = link.attributes.get("href", "")
                            text = link.text(strip=True)
                            if (
                                href
                                and text
                                and href.startswith("http")
                                and "duckduckgo" not in href
                            ):
                                results.append({
                                    "title": text,
                                    "url": href,
                                    "snippet": "",
                                })

            except ImportError:
                log.info("selectolax not installed, using regex fallback")
                # Regex fallback to extract URLs from HTML
                pattern = r'<a[^>]+href="(https?://[^"]+)"[^>]*>([^<]+)</a>'
                matches = re.findall(pattern, html)
                for url, title in matches:
                    if "duckduckgo" not in url:
                        results.append({
                            "title": title.strip(),
                            "url": url,
                            "snippet": "",
                        })

        except Exception as e:
            log.error(f"DuckDuckGo search failed: {e}")

        # Deduplicate by domain
        seen_domains = set()
        unique_results = []
        for r in results:
            domain = urlparse(r["url"]).netloc
            if domain not in seen_domains:
                seen_domains.add(domain)
                unique_results.append(r)

        return unique_results[:max_results]

    def _extract_business_info(self, url: str) -> dict:
        """
        Visit a business website and try to extract phone, address.
        Best-effort extraction using common patterns.
        """
        info = {"phone": "", "address": ""}

        try:
            resp = self.client.get(url)
            html = resp.text

            # Extract phone numbers (US format)
            phone_patterns = [
                r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',  # (555) 123-4567
                r'\+1[-.\s]?\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',  # +1-555-123-4567
            ]
            for pattern in phone_patterns:
                match = re.search(pattern, html)
                if match:
                    info["phone"] = match.group().strip()
                    break

            # Extract address (look for common patterns)
            # Street address pattern: number + street name + type
            addr_pattern = r'\d{1,5}\s+[A-Za-z\s]{3,30}(?:St|Street|Ave|Avenue|Blvd|Boulevard|Dr|Drive|Rd|Road|Ln|Lane|Way|Ct|Court|Pkwy|Parkway)[.,]?\s*(?:Suite|Ste|#|Apt)?\s*\d*'
            addr_match = re.search(addr_pattern, html)
            if addr_match:
                info["address"] = addr_match.group().strip()

        except Exception as e:
            log.debug(f"  Could not extract info from {url}: {e}")

        return info

    def scrape_leads(self, niche: str, city: str, limit: int = 20) -> list[Lead]:
        """
        Full pipeline using free web scraping.
        """
        log.info(f"Free scraper: searching for '{niche}' in '{city}'")

        # Search queries to find businesses
        queries = [
            f"{niche} in {city}",
            f"best {niche} {city}",
            f"{niche} near {city} reviews",
        ]

        all_results = []
        seen_domains = set()

        for query in queries:
            log.info(f"  Searching: {query}")
            results = self._search_duckduckgo(query, max_results=limit)

            for r in results:
                domain = urlparse(r["url"]).netloc
                # Filter out non-business results
                skip_domains = [
                    "yelp.com", "yellowpages.com", "bbb.org",
                    "facebook.com", "instagram.com", "twitter.com",
                    "linkedin.com", "youtube.com", "wikipedia.org",
                    "google.com", "maps.google.com", "reddit.com",
                ]
                if domain not in seen_domains and not any(s in domain for s in skip_domains):
                    seen_domains.add(domain)
                    all_results.append(r)

            time.sleep(2)  # Be polite between searches

            if len(all_results) >= limit:
                break

        all_results = all_results[:limit]
        log.info(f"  Found {len(all_results)} potential business websites")

        # Build leads from results
        leads = []
        for i, result in enumerate(all_results, 1):
            log.info(f"[{i}/{len(all_results)}] Checking: {result['title']}")

            # Extract phone/address from website
            biz_info = self._extract_business_info(result["url"])

            lead = Lead(
                business_name=result["title"],
                address=biz_info.get("address", ""),
                phone=biz_info.get("phone", ""),
                website=result["url"],
                rating=0.0,   # Not available via free scraping
                review_count=0,  # Not available via free scraping
            )
            leads.append(lead)
            time.sleep(1)  # Polite delay between website checks

        return leads

    def close(self):
        self.client.close()


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(
    niche: str,
    city: str,
    limit: int = 20,
    output_file: str = "leads.csv",
    use_free: bool = False,
) -> str:
    """
    Full lead generation pipeline:
      1. Scrape business listings (API or free)
      2. Analyze each website for missing features
      3. Score each lead
      4. Export to CSV sorted by score (highest first)

    Returns:
        Path to the output CSV file.
    """
    log.info("=" * 60)
    log.info(f"AI Agency Lead Scraper")
    log.info(f"Niche: {niche} | City: {city} | Limit: {limit}")
    log.info(f"Mode: {'Free scraping' if use_free else 'Google Places API'}")
    log.info("=" * 60)

    # ---- Step 1: Get business listings ----
    if use_free:
        scraper = FreeLeadScraper()
    else:
        api_key = os.getenv("GOOGLE_PLACES_API_KEY", "")
        if not api_key:
            log.error(
                "GOOGLE_PLACES_API_KEY not set. "
                "Either set the env var or use --free for the free fallback."
            )
            sys.exit(1)
        scraper = GooglePlacesScraper(api_key)

    try:
        leads = scraper.scrape_leads(niche, city, limit)
    finally:
        scraper.close()

    if not leads:
        log.warning("No leads found. Try a different niche or city.")
        sys.exit(0)

    log.info(f"\nFound {len(leads)} businesses. Analyzing websites...\n")

    # ---- Step 2: Analyze websites ----
    analyzer = WebsiteAnalyzer()
    try:
        for i, lead in enumerate(leads, 1):
            if lead.website:
                log.info(f"[{i}/{len(leads)}] Analyzing: {lead.business_name}")
                features = analyzer.analyze(lead.website)
                lead.has_booking = features["has_booking"]
                lead.has_chatbot = features["has_chatbot"]
                lead.has_contact_form = features["has_contact_form"]
            else:
                log.info(f"[{i}/{len(leads)}] No website: {lead.business_name}")
    finally:
        analyzer.close()

    # ---- Step 3: Score leads ----
    log.info("\nScoring leads...")
    for lead in leads:
        lead.score()

    # Sort by score descending (hottest leads first)
    leads.sort(key=lambda x: x.lead_score, reverse=True)

    # ---- Step 4: Export to CSV ----
    csv_columns = [
        "business_name", "address", "phone", "website",
        "rating", "review_count", "has_booking", "has_chatbot",
        "has_contact_form", "lead_score", "observations",
    ]

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=csv_columns, extrasaction="ignore")
        writer.writeheader()
        for lead in leads:
            row = asdict(lead)
            # Remove internal field
            row.pop("_obs_parts", None)
            writer.writerow(row)

    log.info(f"\n{'=' * 60}")
    log.info(f"RESULTS SUMMARY")
    log.info(f"{'=' * 60}")
    log.info(f"Total leads found:     {len(leads)}")
    log.info(f"Leads with website:    {sum(1 for l in leads if l.website)}")
    log.info(f"No booking system:     {sum(1 for l in leads if not l.has_booking)}")
    log.info(f"No chatbot:            {sum(1 for l in leads if not l.has_chatbot)}")
    log.info(f"Has contact form:      {sum(1 for l in leads if l.has_contact_form)}")
    log.info(f"Avg lead score:        {sum(l.lead_score for l in leads) / len(leads):.1f}/10")
    log.info(f"Hot leads (score 8+):  {sum(1 for l in leads if l.lead_score >= 8)}")
    log.info(f"\nOutput saved to: {output_file}")
    log.info(f"{'=' * 60}")

    # Print top 5 leads
    log.info("\nTOP 5 LEADS:")
    for i, lead in enumerate(leads[:5], 1):
        log.info(
            f"  {i}. {lead.business_name} (Score: {lead.lead_score}/10) "
            f"-- {lead.observations}"
        )

    return output_file


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="AI Agency Lead Scraper -- Find local businesses missing AI tools",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python lead_scraper.py --niche "dentist" --city "Austin, TX"
  python lead_scraper.py --niche "plumber" --city "Denver, CO" --free
  python lead_scraper.py --niche "chiropractor" --city "Miami, FL" --limit 50 --output miami_leads.csv
        """,
    )
    parser.add_argument(
        "--niche", required=True,
        help='Business type to search for (e.g., "dentist", "plumber", "chiropractor")',
    )
    parser.add_argument(
        "--city", required=True,
        help='City and state (e.g., "Austin, TX", "Denver, CO")',
    )
    parser.add_argument(
        "--limit", type=int, default=20,
        help="Max number of leads to find (default: 20)",
    )
    parser.add_argument(
        "--output", default="leads.csv",
        help='Output CSV filename (default: "leads.csv")',
    )
    parser.add_argument(
        "--free", action="store_true",
        help="Use free web scraping instead of Google Places API",
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="Enable debug-level logging",
    )

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    run_pipeline(
        niche=args.niche,
        city=args.city,
        limit=args.limit,
        output_file=args.output,
        use_free=args.free,
    )


if __name__ == "__main__":
    main()
