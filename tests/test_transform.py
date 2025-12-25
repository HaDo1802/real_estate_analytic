"""
BEGINNER TEST: Simple tests for transform functions
Testing address parsing, date calculations, etc.
"""

import pandas as pd
from datetime import datetime, timezone, timedelta


def test_split_address_string():
    """Test splitting address into parts."""
    # Arrange: Sample address
    address = "123 Main St, Las Vegas, NV 89101"
    
    # Act: Split by comma
    parts = address.split(", ")
    
    # Assert: Should have 3 parts
    assert len(parts) == 3
    assert parts[0] == "123 Main St"
    assert parts[1] == "Las Vegas"
    assert parts[2] == "NV 89101"


def test_extract_zip_code():
    """Test extracting zip code from state-zip string."""
    # Arrange: State and zip
    state_zip = "NV 89101"
    
    # Act: Split and get last part
    parts = state_zip.split(" ")
    zip_code = parts[1] if len(parts) > 1 else None
    
    # Assert: Should get zip code
    assert zip_code == "89101"


def test_calculate_listing_date():
    """Test calculating when property was listed."""
    # Arrange: Property has been on Zillow 10 days
    days_on_zillow = 10
    today = datetime.now()
    
    # Act: Calculate listing date
    listing_date = today - timedelta(days=days_on_zillow)
    
    # Assert: Should be 10 days ago
    assert listing_date.day == (today - timedelta(days=10)).day


def test_convert_acres_to_sqft():
    """Test converting lot area from acres to square feet."""
    # Arrange: 1 acre
    acres = 1.0
    sqft_per_acre = 43560
    
    # Act: Convert
    sqft = acres * sqft_per_acre
    
    # Assert: 1 acre = 43,560 sqft
    assert sqft == 43560.0


def test_add_vegas_district():
    """Test adding Vegas district based on city."""
    # Arrange: Sample data
    data = {
        'zpid': ['123', '456'],
        'city': ['Henderson', 'Las Vegas']
    }
    df = pd.DataFrame(data)
    
    # Act: Add district column (simple mapping)
    df['district'] = df['city'].apply(lambda x: x if x else 'Unknown')
    
    # Assert: District should match city
    assert df.iloc[0]['district'] == 'Henderson'
    assert df.iloc[1]['district'] == 'Las Vegas'


def test_handle_missing_values():
    """Test handling None/NaN values."""
    # Arrange: Data with missing values
    data = {
        'zpid': ['123', '456'],
        'price': [350000, None]
    }
    df = pd.DataFrame(data)
    
    # Act: Fill missing prices with 0
    df['price'] = df['price'].fillna(0)
    
    # Assert: No more None values
    assert df.iloc[1]['price'] == 0