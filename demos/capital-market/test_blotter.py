#!/usr/bin/env python3
"""
Simple test script to verify the blotter generation functionality.
"""
import asyncio
import pandas as pd
from datetime import date, timedelta
from pro_capital_markets.blotter import generate_blotter_for_symbol, generate_preferences, generate_commissions, generate_venue_fees
import pro_capital_markets.constants as constants

async def test_generate_blotter_for_symbol():
    """Test the generate_blotter_for_symbol function with sample data."""
    
    # Create sample historical data
    sample_data = []
    start_date = date(2024, 1, 1)
    
    for i in range(5):  # 5 days of data
        sample_data.append({
            'date': start_date + timedelta(days=i),
            'symbol': 'AAPL',
            'open': 150.0 + i,
            'high': 155.0 + i,
            'low': 148.0 + i,
            'close': 152.0 + i,
            'volume': 1000000 + i * 100000,
            'sector': 'Information Technology',
            'industry': 'Consumer Electronics',
            'order_qty': 10000 + i * 1000  # Sample order quantities
        })
    
    historical_df = pd.DataFrame(sample_data)
    
    # Generate preferences, commissions, and venue fees
    symbol_preferences = generate_preferences()
    commissions = generate_commissions()
    venue_fees = generate_venue_fees()
    
    print("Testing generate_blotter_for_symbol function...")
    print(f"Sample historical data shape: {historical_df.shape}")
    
    # Test the function
    trades_df = await generate_blotter_for_symbol(
        symbol='AAPL',
        historical_df=historical_df,
        symbol_preferences=symbol_preferences,
        commissions=commissions,
        venue_fees=venue_fees
    )
    
    print(f"Generated trades shape: {trades_df.shape}")
    print("\nFirst few trades:")
    print(trades_df.head())
    
    if not trades_df.empty:
        print(f"\nTotal trade value: ${trades_df['trade_value'].sum():,.2f}")
        print(f"Average trade size: {trades_df['qty'].mean():.0f} shares")
        print(f"Unique traders: {trades_df['trader'].nunique()}")
        print(f"Unique desks: {trades_df['desk'].nunique()}")
        print("\nSample trade details:")
        print(trades_df[['symbol', 'side', 'qty', 'price', 'trader', 'desk']].sample(10))
    
    return trades_df

if __name__ == "__main__":
    result = asyncio.run(test_generate_blotter_for_symbol())
    print(f"\nTest completed successfully! Generated {len(result)} trades.")
