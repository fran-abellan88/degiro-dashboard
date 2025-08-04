# DeGiro Portfolio Analyzer

A comprehensive tool for analyzing DeGiro broker transaction data, processing dividends, buy/sell transactions, and generating detailed portfolio reports.

## Features

- **Dividend Analysis**: Process dividend payments and tax withholdings with data validation
- **Buy Transaction Processing**: Analyze stock purchases including share counts and prices
- **Sell Transaction Processing**: Track stock sales and calculate realized gains
- **Currency Conversion**: Automatic EUR/USD conversion using historical exchange rates
- **Data Validation**: Comprehensive validation of transaction integrity
- **CSV Export**: Generate clean CSV files for each transaction category
- **Comprehensive Reporting**: JSON reports with portfolio overview and statistics

## Installation

1. Install required dependencies:
```bash
pip install -r requirements.txt
```

2. Ensure you have the following files in the project directory:
   - `Account_v20250729.csv` (your DeGiro transaction export)
   - `df_rates.csv` (EUR/USD exchange rates)

## Usage

### Quick Start
Run the complete analysis:
```bash
python degiro_analyzer.py
```

### Individual Processors
You can also run individual processors:

```bash
# Process only dividends
python dividends_processor.py

# Process only buy transactions
python buys_processor.py

# Process only sell transactions
python sells_processor.py
```

## Output Files

The analyzer generates the following files in the `output/` directory:

- `processed_dividends.csv` - Clean dividend transaction data
- `processed_buys.csv` - Clean buy transaction data  
- `processed_sells.csv` - Clean sell transaction data
- `comprehensive_analysis_report.json` - Complete portfolio analysis
- `degiro_analysis.log` - Processing logs

## Data Processing

### Dividends
- Processes dividend payments and tax withholdings
- Validates dividend/withholding pairs for US stocks
- Handles special cases (Liberian stocks, Alibaba, etc.)
- Converts all amounts to EUR using historical rates

### Buy Transactions
- Extracts share counts and prices from descriptions
- Filters out corporate actions and money market conversions
- Validates transaction integrity using ISIN matching
- Tracks investment amounts by stock and year

### Sell Transactions
- Processes stock sales and calculates proceeds
- Handles corporate actions separately from regular sales
- Calculates basic realized gains statistics
- Tracks sales by stock and time period

## Data Validation

The system includes comprehensive validation:
- **Transaction Integrity**: Validates ISIN codes match descriptions
- **Amount Validation**: Ensures buy amounts are negative, sell amounts positive
- **Dividend Pairing**: Verifies dividend/withholding pairs for US stocks
- **Currency Consistency**: Filters out unsupported currencies
- **Duplicate Detection**: Removes duplicate transactions

## Exchange Rate Handling

- Uses historical EUR/USD exchange rates from `df_rates.csv`
- Forward-fills missing rates for weekends/holidays
- Converts all monetary amounts to EUR for consistency
- Maintains original currency information for reference

## Error Handling

- Comprehensive logging to `degiro_analysis.log`
- Graceful handling of missing or malformed data
- Detailed error messages for troubleshooting
- Validation warnings for data quality issues

## Customization

### Configuration
Modify the main script to change:
- Input file paths
- Output directory
- Exchange rate file location

### Adding New Analysis
Each processor is modular and can be extended:
- Add new metrics to summary methods
- Implement additional validation rules
- Create custom export formats

## Portfolio Metrics

The comprehensive report includes:
- Total invested amount
- Proceeds from sales
- Total dividends received
- Net current investment
- Basic return calculations
- Transaction counts and diversification metrics
- Activity timeline by year

## Notes

- All Spanish column names from DeGiro are standardized to English
- Corporate actions are identified and separated from regular trades
- Exchange rates are required for accurate EUR conversion
- The system handles DeGiro's specific CSV format and terminology

## Future Enhancements

The modular design allows for easy extension:
- Tax reporting features
- Performance analytics
- Portfolio optimization suggestions
- Integration with other brokers
- Real-time data updates