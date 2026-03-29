"""
Enterprise ETL Platform - Sample Data Generator

Generates realistic sample datasets for testing the ETL pipelines.
Run this once to populate /data/ with test CSV files.
"""
import os
import random
import string
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def random_email(first: str, last: str, domain: str) -> str:
    sep = random.choice([".", "_", ""])
    return f"{first.lower()}{sep}{last.lower()}@{domain}"


def generate_customers(n: int = 2000) -> pd.DataFrame:
    """Generate synthetic customer records with intentional data quality issues."""

    first_names = [
        "Alice", "Bob", "Carlos", "Diana", "Ethan", "Fiona", "George",
        "Hannah", "Ivan", "Julia", "Kevin", "Laura", "Marcus", "Nina",
        "Oscar", "Priya", "Quinn", "Rachel", "Sam", "Tara",
    ]
    last_names = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
        "Miller", "Davis", "Martinez", "Wilson", "Anderson", "Taylor",
        "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson",
    ]
    domains = ["gmail.com", "yahoo.com", "outlook.com", "company.org", "example.net"]
    cities = [
        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
        "Philadelphia", "San Antonio", "San Diego", "Dallas", "Mumbai",
        "London", "Berlin", "Tokyo", "Sydney", "Toronto",
    ]
    countries = [
        "USA", "USA", "USA", "USA", "India", "UK", "Germany",
        "Japan", "Australia", "Canada",
    ]
    statuses = ["active", "inactive", "pending", "suspended"]

    records = []
    used_emails = set()

    for i in range(n):
        first = random.choice(first_names)
        last = random.choice(last_names)
        domain = random.choice(domains)
        email = random_email(first, last, domain)

        # Introduce duplicate emails (5%)
        if used_emails and random.random() < 0.05:
            email = random.choice(list(used_emails))
        used_emails.add(email)

        signup = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1400))
        last_login = signup + timedelta(days=random.randint(0, 365))

        record = {
            "customer_id": f"CUST{i+1:06d}",
            "first_name": first,
            "last_name": last,
            "email": email,
            "age": random.randint(18, 75),
            "city": random.choice(cities),
            "country": random.choice(countries),
            "status": random.choice(statuses),
            "signup_date": signup.strftime("%Y-%m-%d"),
            "last_login": last_login.strftime("%Y-%m-%d"),
            "lifetime_value": round(random.uniform(10, 5000), 2),
        }

        # Inject data quality issues
        # ~10% missing first_name
        if random.random() < 0.10:
            record["first_name"] = None
        # ~8% missing city
        if random.random() < 0.08:
            record["city"] = None
        # ~5% invalid email
        if random.random() < 0.05:
            record["email"] = f"INVALID-{random.randint(1000, 9999)}"
        # ~3% missing age
        if random.random() < 0.03:
            record["age"] = None
        # ~2% age out of range
        if random.random() < 0.02:
            record["age"] = random.choice([-5, 0, 200, 999])
        # Add some trailing whitespace
        if random.random() < 0.15:
            record["first_name"] = (record["first_name"] or "") + "  "

        records.append(record)

    df = pd.DataFrame(records)
    print(f"Generated {len(df)} customer records")
    print(f"  - Nulls in first_name: {df['first_name'].isna().sum()}")
    print(f"  - Nulls in city: {df['city'].isna().sum()}")
    print(f"  - Duplicates: {df.duplicated(subset=['email']).sum()}")
    return df


def generate_sales(n: int = 5000) -> pd.DataFrame:
    """Generate synthetic daily sales transaction records."""
    categories = [
        "Electronics", "Clothing", "Books", "Home & Garden",
        "Sports", "Toys", "Automotive", "Health", "Beauty",
    ]
    regions = ["North", "South", "East", "West", "Central"]

    records = []
    for i in range(n):
        created_at = datetime(2024, 1, 1) + timedelta(
            days=random.randint(0, 365),
            hours=random.randint(0, 23),
        )
        records.append({
            "order_id": f"ORD{i+1:08d}",
            "product_category": random.choice(categories),
            "region": random.choice(regions),
            "amount": round(random.uniform(5, 2000), 2),
            "quantity": random.randint(1, 20),
            "discount_amount": round(random.uniform(0, 50), 2) if random.random() > 0.6 else 0,
            "tax_amount": round(random.uniform(0, 200), 2),
            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
        })

    return pd.DataFrame(records)


if __name__ == "__main__":
    print("Generating sample datasets...")

    # Customers
    customers_path = os.path.join(OUTPUT_DIR, "sample_customers.csv")
    df_customers = generate_customers(2000)
    df_customers.to_csv(customers_path, index=False)
    print(f"✅ Saved: {customers_path}")

    # Sales
    sales_path = os.path.join(OUTPUT_DIR, "sample_sales.csv")
    df_sales = generate_sales(5000)
    df_sales.to_csv(sales_path, index=False)
    print(f"✅ Saved: {sales_path}")

    print("\nSample data generation complete!")
    print(f"  customers: {len(df_customers)} rows → {customers_path}")
    print(f"  sales:     {len(df_sales)} rows → {sales_path}")
