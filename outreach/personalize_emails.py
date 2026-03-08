#!/usr/bin/env python3
"""
Email Personalizer for AI Agency Cold Outreach
================================================
Reads lead CSV from lead_scraper.py and generates send-ready personalized
cold emails (Email 1: Day 0 Pattern Interrupt, Email 2: Day 3 Case Study).

Usage:
    python personalize_emails.py --input leads.csv --output emails.csv
    python personalize_emails.py --input leads.csv --niche "dental practice" --sender Alex --company "NexusAI Solutions"
    python personalize_emails.py --input leads.csv --start-date 2026-04-01
"""

import argparse
import csv
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path


# ---------------------------------------------------------------------------
# Email Templates
# ---------------------------------------------------------------------------

EMAIL_1_SUBJECT_A = "I found something costing {business_name} money"
EMAIL_1_SUBJECT_B = "Quick question about {business_name}'s website"

EMAIL_1_BODY = (
    'Hi {owner_name},\n'
    '\n'
    'I was looking for a {niche} in {city} and came across {business_name}.\n'
    '\n'
    'I noticed a few things that are probably costing you customers right now:\n'
    '\n'
    '{specific_observation}\n'
    '\n'
    'Meanwhile, {competitor_name} down the road has online booking, a chatbot that answers questions at 2 AM, and automated review requests \u2014 so when someone searches "{niche} near me," they\'re stacking the deck.\n'
    '\n'
    'This isn\'t a criticism. Most {niche}s haven\'t caught up yet, which is exactly why the ones that move first are cleaning up.\n'
    '\n'
    'I help {niche}s in {city} add AI-powered booking, chatbots, and follow-up systems. Takes about a week to set up. No long contracts.\n'
    '\n'
    'Worth a 15-minute call to see if it makes sense?\n'
    '\n'
    '{calendar_link}\n'
    '\n'
    'Either way, hope business is going well.\n'
    '\n'
    '\u2014 {sender_name}\n'
    '{sender_company}'
)

EMAIL_2_SUBJECT_A = "How a {niche} in {city} added $12K/mo with one AI tool"
EMAIL_2_SUBJECT_B = "Re: {business_name}"

EMAIL_2_BODY = (
    'Hi {owner_name},\n'
    '\n'
    'Quick follow-up \u2014 wanted to share a real example since it\'s directly relevant to {business_name}.\n'
    '\n'
    'We worked with a {niche} in {city} who had the same setup you have now:\n'
    '- Phone-only booking (they were missing 60% of after-hours calls)\n'
    '- No way to capture website visitors (people would browse and leave)\n'
    '- Reviews trickling in at 1-2 per month\n'
    '\n'
    'Here\'s what we installed in 5 business days:\n'
    '\n'
    '1. AI BOOKING ASSISTANT \u2014 Handles scheduling 24/7 via their website and Google listing. No more phone tag.\n'
    '\n'
    '2. SMART CHATBOT \u2014 Answers the top 20 questions their receptionist gets asked every day (pricing, hours, insurance, parking). Captures name + phone for every visitor.\n'
    '\n'
    '3. AUTOMATED REVIEW ENGINE \u2014 After each appointment, sends a friendly text asking for a Google review. They went from 43 to 187 reviews in 90 days.\n'
    '\n'
    'Result: $12,400/mo in new revenue from patients who would have gone to a competitor.\n'
    '\n'
    'I\'m not saying you\'ll get the exact same result \u2014 but the gap between where {business_name} is now and where it could be is significant.\n'
    '\n'
    'Happy to walk you through exactly what this would look like for your practice. No pitch, just a screen share.\n'
    '\n'
    '{calendar_link}\n'
    '\n'
    '\u2014 {sender_name}'
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_bool(value):
    """Parse a boolean value from CSV string."""
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "1", "yes", "t")


def extract_city(address):
    """
    Extract city from an address string.
    Handles formats like:
        '123 Main St, Austin, TX 78701'
        'Austin, TX'
        '123 Main St, Suite 200, Denver, CO 80202'
    """
    if not address or not address.strip():
        return "your city"

    address = address.strip()

    # Pattern: look for "City, ST" or "City, ST ZIP" near the end
    match = re.search(r',\s*([A-Za-z\s]+?),\s*[A-Z]{2}(?:\s+\d{5})?\s*$', address)
    if match:
        return match.group(1).strip()

    # Simpler: "City, ST"
    match = re.search(r'^([A-Za-z\s]+),\s*[A-Z]{2}', address)
    if match:
        return match.group(1).strip()

    # Fallback: split by commas
    parts = [p.strip() for p in address.split(',') if p.strip()]
    if len(parts) >= 2:
        candidate = parts[-2] if len(parts) >= 3 else parts[0]
        candidate = re.sub(r'^\d+\s+', '', candidate)
        if candidate:
            return candidate

    return "your city"


def build_observations(row):
    """
    Auto-generate specific observations based on boolean columns
    and review count from the lead data.
    """
    observations = []

    has_booking = parse_bool(row.get('has_booking', 'False'))
    has_chatbot = parse_bool(row.get('has_chatbot', 'False'))
    has_contact_form = parse_bool(row.get('has_contact_form', 'False'))
    review_count_raw = row.get('review_count', '0')

    try:
        review_count = int(float(review_count_raw)) if review_count_raw else 0
    except (ValueError, TypeError):
        review_count = 0

    if not has_booking:
        observations.append(
            "- No online booking system \u2014 patients have to call during "
            "business hours to schedule"
        )
    if not has_chatbot:
        observations.append(
            "- No website chatbot \u2014 visitors with questions after hours "
            "just leave"
        )
    if not has_contact_form:
        observations.append(
            "- No contact form \u2014 no way for potential patients to reach "
            "you from your website"
        )
    if review_count < 50:
        observations.append(
            "- Only " + str(review_count) + " Google reviews \u2014 your competitors with "
            "100+ reviews are winning the 'dentist near me' search"
        )

    if not observations:
        observations.append(
            "- Your website could be converting more visitors into booked "
            "appointments with AI-powered tools"
        )

    return "\n".join(observations)


def fill_template(template, fields):
    """Fill a template string with merge fields."""
    result = template
    for key, value in fields.items():
        result = result.replace("{" + key + "}", str(value))
    return result


# ---------------------------------------------------------------------------
# Core personalizer
# ---------------------------------------------------------------------------

def personalize_lead(row, config):
    """
    Generate personalized emails for a single lead.
    Returns a dict with all output columns.
    """
    business_name = row.get('business_name', '').strip()
    address = row.get('address', '').strip()
    website = row.get('website', '').strip()
    lead_score_raw = row.get('lead_score', '0')

    try:
        lead_score = int(float(lead_score_raw)) if lead_score_raw else 0
    except (ValueError, TypeError):
        lead_score = 0

    city = extract_city(address)
    specific_observation = build_observations(row)

    # Build merge fields
    fields = {
        'business_name': business_name,
        'owner_name': config['owner_name'],
        'city': city,
        'niche': config['niche'],
        'specific_observation': specific_observation,
        'competitor_name': '[top-rated competitor]',
        'sender_name': config['sender_name'],
        'sender_company': config['sender_company'],
        'calendar_link': config['calendar_link'],
        'loom_url': '[RECORD PERSONALIZED LOOM]',
        'website_url': website,
    }

    # Generate emails
    email_1_subject_a = fill_template(EMAIL_1_SUBJECT_A, fields)
    email_1_subject_b = fill_template(EMAIL_1_SUBJECT_B, fields)
    email_1_body = fill_template(EMAIL_1_BODY, fields)

    email_2_subject_a = fill_template(EMAIL_2_SUBJECT_A, fields)
    email_2_subject_b = fill_template(EMAIL_2_SUBJECT_B, fields)
    email_2_body = fill_template(EMAIL_2_BODY, fields)

    # Calculate send dates
    send_date_1 = config['start_date']
    send_date_2 = config['start_date'] + timedelta(days=3)

    return {
        'business_name': business_name,
        'city': city,
        'website': website,
        'lead_score': lead_score,
        'email_1_subject_a': email_1_subject_a,
        'email_1_subject_b': email_1_subject_b,
        'email_1_body': email_1_body,
        'email_2_subject_a': email_2_subject_a,
        'email_2_subject_b': email_2_subject_b,
        'email_2_body': email_2_body,
        'send_date_email_1': send_date_1.strftime('%Y-%m-%d'),
        'send_date_email_2': send_date_2.strftime('%Y-%m-%d'),
    }


def run_personalizer(args):
    """Main personalizer pipeline."""

    input_path = args.input
    output_path = args.output
    niche = args.niche
    sender_name = args.sender
    sender_company = args.company
    calendar_link = args.calendar

    # Parse start date
    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        except ValueError:
            print("[ERROR] Invalid date format: " + args.start_date + ". Use YYYY-MM-DD.")
            sys.exit(1)
    else:
        start_date = datetime.now() + timedelta(days=14)

    # Verify input file exists
    if not os.path.exists(input_path):
        print("[ERROR] Input file not found: " + input_path)
        sys.exit(1)

    config = {
        'niche': niche,
        'owner_name': 'there',
        'sender_name': sender_name,
        'sender_company': sender_company,
        'calendar_link': calendar_link,
        'start_date': start_date,
    }

    print("=" * 60)
    print("  EMAIL PERSONALIZER - AI Agency Cold Outreach")
    print("=" * 60)
    print("  Input:      " + input_path)
    print("  Output:     " + output_path)
    print("  Niche:      " + niche)
    print("  Sender:     " + sender_name + " @ " + sender_company)
    print("  Calendar:   " + calendar_link)
    print("  Start Date: " + start_date.strftime('%Y-%m-%d') + " (Email 1)")
    print("  Day 3 Date: " + (start_date + timedelta(days=3)).strftime('%Y-%m-%d') + " (Email 2)")
    print("=" * 60)
    print()

    # Read input CSV
    leads = []
    with open(input_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        # Validate required columns
        required_cols = [
            'business_name', 'address', 'has_booking',
            'has_chatbot', 'has_contact_form', 'lead_score'
        ]
        if reader.fieldnames:
            missing = [c for c in required_cols if c not in reader.fieldnames]
            if missing:
                print("[ERROR] Missing required columns: " + ', '.join(missing))
                print("  Found columns: " + ', '.join(reader.fieldnames))
                sys.exit(1)

        for row in reader:
            leads.append(row)

    if not leads:
        print("[WARNING] No leads found in input file.")
        sys.exit(0)

    print("[INFO] Loaded " + str(len(leads)) + " leads from " + input_path)
    print("[INFO] Generating personalized emails...")
    print()

    # Personalize each lead
    results = []
    for i, lead in enumerate(leads, 1):
        bname = lead.get('business_name', 'Unknown')
        try:
            result = personalize_lead(lead, config)
            results.append(result)
            score = result['lead_score']
            priority = "HIGH" if score >= 7 else ("MED" if score >= 4 else "LOW")
            print("  [" + str(i).rjust(3) + "/" + str(len(leads)) + "] " + bname.ljust(45) + " Score: " + str(score) + "  [" + priority + "]")
        except Exception as e:
            print("  [" + str(i).rjust(3) + "/" + str(len(leads)) + "] " + bname.ljust(45) + " ERROR: " + str(e))

    # Sort by lead_score descending (highest priority first)
    results.sort(key=lambda x: x['lead_score'], reverse=True)

    # Write output CSV
    output_columns = [
        'business_name', 'city', 'website', 'lead_score',
        'email_1_subject_a', 'email_1_subject_b', 'email_1_body',
        'email_2_subject_a', 'email_2_subject_b', 'email_2_body',
        'send_date_email_1', 'send_date_email_2',
    ]

    # Ensure output directory exists
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=output_columns)
        writer.writeheader()
        writer.writerows(results)

    # ---------------------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------------------
    total = len(results)
    scores = [r['lead_score'] for r in results]
    avg_score = sum(scores) / total if total else 0
    high_priority = sum(1 for s in scores if s >= 7)
    med_priority = sum(1 for s in scores if 4 <= s < 7)
    low_priority = sum(1 for s in scores if s < 4)

    print()
    print("=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    print("  Total leads processed:      " + str(total))
    print("  Average lead score:          " + "{:.1f}".format(avg_score) + " / 10")
    print("  High-priority leads (7+):    " + str(high_priority))
    print("  Medium-priority leads (4-6): " + str(med_priority))
    print("  Low-priority leads (<4):     " + str(low_priority))
    print()
    print("  Email 1 send date:  " + start_date.strftime('%Y-%m-%d') + " (" + start_date.strftime('%A') + ")")
    print("  Email 2 send date:  " + (start_date + timedelta(days=3)).strftime('%Y-%m-%d') + " (" + (start_date + timedelta(days=3)).strftime('%A') + ")")
    print()
    print("  Output saved to:    " + output_path)
    print("  Total emails ready: " + str(total * 2) + " (2 per lead)")
    print("=" * 60)
    print()
    print("  NEXT STEPS:")
    print("  1. Review high-priority leads and record Loom videos")
    print("     (replace [RECORD PERSONALIZED LOOM] placeholders)")
    print("  2. Research competitor names for each city")
    print("     (replace [top-rated competitor] placeholders)")
    print("  3. Find owner names via LinkedIn/website About pages")
    print("     (replace 'there' with actual first names)")
    print("  4. Import into your email sending tool (Instantly, Smartlead, etc.)")
    print("  5. Verify domain warmup is complete before start date")
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Email Personalizer - Generate send-ready cold emails from lead CSV',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            '\nExamples:\n'
            '  python personalize_emails.py --input leads_dental_austin_tx.csv\n'
            '  python personalize_emails.py --input leads.csv --output outreach_emails.csv\n'
            '  python personalize_emails.py --input leads.csv --niche "dental practice" --sender Alex\n'
            '  python personalize_emails.py --input leads.csv --start-date 2026-04-01\n'
        )
    )

    parser.add_argument(
        '--input', '-i',
        required=True,
        help='Path to input CSV file (from lead_scraper.py output)'
    )
    parser.add_argument(
        '--output', '-o',
        default=None,
        help='Path for output CSV (default: personalized_emails_<timestamp>.csv)'
    )
    parser.add_argument(
        '--niche', '-n',
        default='dental practice',
        help='Business niche for templates (default: "dental practice")'
    )
    parser.add_argument(
        '--sender', '-s',
        default='Alex',
        help='Sender first name (default: "Alex")'
    )
    parser.add_argument(
        '--company', '-c',
        default='NexusAI Solutions',
        help='Sender company name (default: "NexusAI Solutions")'
    )
    parser.add_argument(
        '--calendar',
        default='https://calendly.com/nexusai/discovery',
        help='Calendar booking link (default: Calendly link)'
    )
    parser.add_argument(
        '--start-date',
        default=None,
        help='First email send date YYYY-MM-DD (default: 14 days from today)'
    )

    args = parser.parse_args()

    # Auto-generate output filename if not specified
    if args.output is None:
        input_stem = Path(args.input).stem
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        input_dir = os.path.dirname(args.input) or '.'
        args.output = os.path.join(input_dir, 'personalized_emails_' + input_stem + '_' + timestamp + '.csv')

    run_personalizer(args)


if __name__ == '__main__':
    main()
