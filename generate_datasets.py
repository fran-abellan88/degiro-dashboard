import argparse
import json
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Optional, Union

import numpy as np
import pandas as pd
import requests


def load_degiro_data(csv_file_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_file_path)

    # Drop unnecessary columns
    cols_to_drop = ["Fecha valor", "ID Orden", "Tipo"]
    columns_to_drop = [col for col in cols_to_drop if col in df.columns]
    if columns_to_drop:
        df.drop(columns=columns_to_drop, inplace=True)

    # Clean data
    df.dropna(subset=["Fecha"], inplace=True)
    df["Fecha"] = pd.to_datetime(df["Fecha"], dayfirst=True)
    df["year_month"] = df["Fecha"].dt.strftime("%Y-%m")
    df["year"] = df.Fecha.dt.year

    df = df.rename(
        columns={
            "Fecha": "date",
            "Hora": "hour",
            "Producto": "product",
            "Descripción": "original_description",
        }
    )
    df["amount"] = (df["Unnamed: 8"].astype(str) + " " + df["Variación"]).fillna("0 EUR")
    df["balance"] = (df["Unnamed: 10"].astype(str) + " " + df["Saldo"]).fillna("0 EUR")

    # Drop original columns
    drop_cols = ["Unnamed: 8", "Unnamed: 10", "Variación", "Saldo"]
    existing_cols = [col for col in drop_cols if col in df.columns]
    if existing_cols:
        df.drop(columns=existing_cols, inplace=True)

    # Extract currency information
    df[["amount", "amount_currency"]] = df["amount"].str.extract(r"([\d\.,\-]+)\s*(EUR|USD|GBP)")
    df[["balance", "balance_currency"]] = df["balance"].str.extract(r"([\d\.,\-]+)\s*(EUR|USD|GBP)")

    # Convert to numeric
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["balance"] = pd.to_numeric(df["balance"], errors="coerce")

    return df


def process_description(x: str) -> str:
    x = str(x).lower()
    if "transferir a su cuenta de efectivo" in x:
        return "Transferencia a cuenta de efectivo"
    if "transferir desde su cuenta de efectivo " in x:
        return "Transferencia desde cuenta de efectivo"
    if (
        "compra " in x
        and "stock split" not in str(x).lower()
        and "fusión" not in x
        and "escisión" not in str(x).lower()
        and "cambio de producto" not in str(x).lower()
        and "cambio de isin" not in str(x).lower()
        and "conversión fondos del mercado monetario" not in x
    ):
        return "Compra"
    if (
        "venta " in x
        and "stock split" not in str(x).lower()
        and "fusión" not in x
        and "escisión" not in str(x).lower()
        and "cambio de producto" not in str(x).lower()
        and "cambio de isin" not in str(x).lower()
        and "conversión fondos del mercado monetario" not in x
    ):
        return "Venta"
    if "venta " in x and "stock split" in x:
        return "Venta - Stock split"
    if "compra " in x and "stock split" in x:
        return "Compra - Stock split"
    if "venta " in str(x).lower() and "conversión fondos del mercado monetario" in x:
        return "Venta - Conversión fondos del mercado monetario"
    if "compra " in str(x).lower() and "conversión fondos del mercado monetario" in x:
        return "Compra - Conversión fondos del mercado monetario"
    if "venta " in x and "fusión" in x:
        return "Venta - Fusión"
    if "compra " in x and "fusión" in x:
        return "Compra - Fusión"
    if "venta " in x and "escisión" in x:
        return "Venta - Escisión"
    if "compra " in x and "escisión" in x:
        return "Compra - Escisión"
    if "venta " in x and "cambio de isin" in x:
        return "Venta - Cambio de ISIN"
    if "compra " in x and "cambio de isin" in x:
        return "Compra - Cambio de ISIN"
    if "venta " in x and "cambio de producto" in x:
        return "Venta - Cambio de producto"
    if "compra " in x and "cambio de producto" in x:
        return "Compra - Cambio de producto"
    if "comisión de conectividad " in x:
        return "Comisión de conectividad"
    if "flatex deposit" in x:
        return "Ingreso a DeGiro desde ING"
    if x == "ingreso":
        return "Ingreso a DeGiro desde ING"
    else:
        return x


def process_transaction(x: str) -> str:
    x = x.lower()
    if "compra" in x:
        return "Compra"
    elif "venta" in x:
        return "Venta"
    elif "venta" in x and "isin" in x:
        return "Cambio Corporativo"
    elif "venta" in x and "producto" in x:
        return "Cambio Corporativo"
    elif "venta" in x and "escisión" in x:
        return "Cambio Corporativo"
    elif "venta" in x and "fusión" in x:
        return "Cambio Corporativo"
    elif "stock split" in x:
        return "Cambio Corporativo"
    elif "cambio de divisa" in x:
        return "Cambio de Divisa"
    elif "cash sweep transfer" in x:
        return "Transferencia Interna"
    elif "ingreso" in x:
        return "Ingreso"
    elif "withdrawal" in x:
        return "Retiro"
    elif "costes" in x or "coste de la acción" in x:
        return "Comisión"
    elif "stamp duty" in x:
        return "Impuesto"
    elif "dividendo" in x:
        return "Dividendo"
    else:
        return "Otro"


def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    df["description"] = df["original_description"].apply(process_description)
    df["description"] = df["description"].str.lower()
    df["category"] = df["description"].apply(process_transaction)
    df["category"] = df["category"].str.lower()

    # Add country information based on ISIN
    df["country"] = df["ISIN"].str[0:2].fillna("None")

    df.drop_duplicates(subset=[col for col in df.columns if col != "hour"], inplace=True)
    df.reset_index(inplace=True, drop=True)

    return df


def load_currency_conversion_rates(exchange_rates_file: str) -> pd.DataFrame:
    df = pd.read_csv(exchange_rates_file)
    df["Date"] = pd.to_datetime(df["Date"])
    df.rename(columns={"Date": "date"}, inplace=True)

    return df


def filter_not_needed_rows(df: pd.DataFrame) -> pd.DataFrame:
    df = df[
        df.description != "Flatex Interest Income"
    ]  # en este caso todos los registros asociados tenían un Amount de 0
    df = df[
        df.description != "Flatex Interest"
    ]  # en este caso todos los registros asociados tenían un Amount muy bajo, de apenas 1 EUR
    df = df[
        df.description != "Comisión de conectividad"
    ]  # en este caso todos los registros asociados tenían un Amount de 2.5 EUR
    df = df[df.description != "ADR/GDR Pass-Through Fee"]  # hay unos 30 registros con muy poca Amount (menos de 1 EUR)
    df = df[df.description != "Rendimiento de capital"]  # pocos registros de poca amount
    df = df[
        df.description != "Fondos del mercado monetario cambio de precio (EUR)"
    ]  # pocos registros de muy poca amount (cents)
    df = df[
        df.description != "Venta - Conversión fondos del mercado monetario"
    ]  # solo un registro distinto de 0 (-198.21), lo ignoro igualmente

    df = df[df.description != "Transferencia desde cuenta de efectivo"]  # hay muchos registros pero todos con Amount 0
    df = df[df.description != "Transferencia a cuenta de efectivo"]  # hay muchos registros pero todos con Amount 0
    df = df[df.amount_currency.isin(["USD", "EUR"])]

    df.reset_index(inplace=True, drop=True)

    return df


def rename_products(df: pd.DataFrame) -> pd.DataFrame:
    product_mapping = {"JACOBS ENGINEERING GROUP INC": "JACOBS SOLUTIONS INC"}

    df["product"] = df["product"].replace(product_mapping)

    return df


def enrich_with_currency_conversion_rates(df: pd.DataFrame, df_currency_conversion_rates: pd.DataFrame) -> pd.DataFrame:
    df = df.merge(df_currency_conversion_rates, on="date", how="left")
    df["EUR_to_USD"] = df["EUR_to_USD"].ffill()

    return df


def apply_currency_conversion_rates(df: pd.DataFrame) -> pd.DataFrame:
    # Vectorized currency conversion for better performance
    df["amount_EUR"] = np.where(df["amount_currency"] == "EUR", df["amount"], df["amount"] / df["EUR_to_USD"]).round(2)

    df["balance_EUR"] = np.where(
        df["balance_currency"] == "EUR", df["balance"], df["balance"] / df["EUR_to_USD"]
    ).round(2)

    return df


def create_df_buys(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df.category == "compra"].copy()
    columns_to_drop = ["amount", "amount_currency", "balance", "balance_currency", "balance_EUR", "EUR_to_USD", "hour"]
    df.drop(columns=columns_to_drop, inplace=True)
    df["is_valid"] = df.apply(lambda row: row["ISIN"] in row["original_description"] and row["amount_EUR"] < 0, axis=1)

    return df


def extract_shares_from_buys_description(description: Union[str, float]) -> Optional[int]:
    try:
        if pd.isna(description):
            return None

        match = re.search(r"Compra (\d+)", str(description))
        if match:
            return int(match.group(1))
        return None
    except:
        return None


def extract_price_from_buys_description(description: Union[str, float]) -> Optional[float]:
    """Extract buy price from description like 'Compra 4 Procter & Gamble@155 USD (US7427181091)'"""
    try:
        if pd.isna(description):
            return None

        # Pattern to match price after @ symbol: @[price] [currency]
        match = re.search(r"@([\d,\.]+)\s+(?:USD|EUR|GBP)", str(description))
        if match:
            price_str = match.group(1)
            # Handle mixed European format (1.208,88 -> 1208.88)
            if "." in price_str and "," in price_str:
                # Remove thousands separator (.) and replace decimal separator (,) with .
                price_str = price_str.replace(".", "").replace(",", ".")
            else:
                # Standard European format: replace comma with period
                price_str = price_str.replace(",", ".")
            return float(price_str)
        return None
    except:
        return None


def create_df_sells(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df.category == "venta"].copy()
    columns_to_drop = ["amount", "amount_currency", "balance", "balance_currency", "balance_EUR", "EUR_to_USD", "hour"]
    df.drop(columns=columns_to_drop, inplace=True)

    return df


def extract_shares_from_sells_description(description: Union[str, float]) -> Optional[int]:
    try:
        if pd.isna(description):
            return None

        match = re.search(r"Venta (\d+)", str(description))
        if match:
            return int(match.group(1))
        return None
    except:
        return None


def extract_price_from_sells_description(description: Union[str, float]) -> Optional[float]:
    """Extract sell price from description like 'Venta 1 Block Inc.@61,82 USD (US8522341036)'"""
    try:
        if pd.isna(description):
            return None

        # Pattern to match price after @ symbol: @[price] [currency]
        match = re.search(r"@([\d,\.]+)\s+(?:USD|EUR|GBP)", str(description))
        if match:
            price_str = match.group(1)
            # Handle mixed European format (1.208,88 -> 1208.88)
            if "." in price_str and "," in price_str:
                # Remove thousands separator (.) and replace decimal separator (,) with .
                price_str = price_str.replace(".", "").replace(",", ".")
            else:
                # Standard European format: replace comma with period
                price_str = price_str.replace(",", ".")
            return float(price_str)
        return None
    except:
        return None


def create_df_dividends(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df.category == "dividendo"].copy()
    columns_to_drop = ["amount", "amount_currency", "balance", "balance_currency", "balance_EUR", "EUR_to_USD", "hour"]
    df.drop(columns=columns_to_drop, inplace=True)

    return df


def verify_dividends(df: pd.DataFrame) -> pd.DataFrame:
    df["total_transactions"] = df.groupby(["date", "product"])["description"].transform("count")

    def verificar_grupo(grupo):
        country = grupo["country"].iloc[0]
        product = str(grupo["product"].iloc[0]).lower()
        descripciones = set(grupo["description"].str.lower())

        if country == "US" and "alibaba" not in product:
            if len(grupo) == 2 and "dividendo" in descripciones and "retención del dividendo" in descripciones:
                grupo["status"] = "verified"
        elif country == "LR" or "alibaba" in product:
            if len(grupo) == 1:
                grupo["status"] = "verified"
        return grupo

    df = df.groupby(["date", "product"], group_keys=False).apply(verificar_grupo)
    df["status"] = df["status"].fillna("unverified")

    return df


def create_df_deposits(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df.category == "ingreso"].copy()
    columns_to_drop = [
        "amount",
        "amount_currency",
        "balance",
        "balance_currency",
        "balance_EUR",
        "EUR_to_USD",
        "hour",
        "ISIN",
        "product",
        "country",
    ]
    df.drop(columns=columns_to_drop, inplace=True)

    return df


def create_df_fees(df: pd.DataFrame) -> pd.DataFrame:
    """Extract fee transactions"""
    df = df[df.category == "comisión"].copy()
    columns_to_drop = ["amount", "amount_currency", "balance", "balance_currency", "balance_EUR", "EUR_to_USD", "hour"]
    df.drop(columns=columns_to_drop, inplace=True)

    return df


def generate_portfolio_summary(
    df_buys: pd.DataFrame,
    df_sells: pd.DataFrame,
    df_dividends: pd.DataFrame,
    df_deposits: pd.DataFrame,
    df_fees: pd.DataFrame,
) -> dict:
    """Generate comprehensive portfolio summary"""

    # Calculate totals
    total_invested = df_buys[df_buys["is_valid"]]["amount_EUR"].sum() * -1  # Convert negative to positive
    total_proceeds = df_sells["amount_EUR"].sum()
    total_deposits = df_deposits["amount_EUR"].sum()
    total_fees = df_fees["amount_EUR"].sum() * -1  # Convert negative to positive

    # Calculate dividends (sum ALL dividend transactions - verified and unverified, positive and negative)
    total_dividends = df_dividends["amount_EUR"].sum()  # This includes ALL dividend transactions

    # Calculate yearly breakdowns
    dividend_by_year = df_dividends.groupby("year")["amount_EUR"].sum().to_dict()
    investment_by_year = (
        df_buys[df_buys["is_valid"]].groupby("year")["amount_EUR"].sum().apply(lambda x: x * -1).to_dict()
    )
    proceeds_by_year = df_sells.groupby("year")["amount_EUR"].sum().to_dict()

    # Calculate monthly investment breakdown for charts
    investment_by_month = (
        df_buys[df_buys["is_valid"]].groupby("year_month")["amount_EUR"].sum().apply(lambda x: x * -1).to_dict()
    )

    # Calculate monthly deposit breakdown for charts
    deposit_by_month = df_deposits.groupby("year_month")["amount_EUR"].sum().to_dict()

    # Calculate portfolio metrics
    net_invested = total_invested - total_proceeds
    portfolio_return = total_dividends + total_proceeds - total_fees

    return {
        "metadata": {
            "calculation_timestamp": pd.Timestamp.now().isoformat(),
            "data_version": "4.0",
            "calculation_methodology": "verified_dividends_only",
            "source": "generate_datasets.py",
        },
        "portfolio_summary": {
            "total_invested": round(total_invested, 2),
            "total_proceeds": round(total_proceeds, 2),
            "net_invested": round(net_invested, 2),
            "total_deposits": round(total_deposits, 2),
            "total_dividends_received": round(total_dividends, 2),
            "total_fees": round(total_fees, 2),
            "portfolio_return": round(portfolio_return, 2),
        },
        "detailed_summaries": {
            "dividends": {
                "dividend_by_year": {str(k): round(v, 2) for k, v in dividend_by_year.items()},
                "total_dividend_amount": round(total_dividends, 2),
                "verified_transactions": len(df_dividends[df_dividends["status"] == "verified"]),
                "total_transactions": len(df_dividends),
            },
            "investments": {
                "investment_by_year": {str(k): round(v, 2) for k, v in investment_by_year.items()},
                "investment_by_month": {str(k): round(v, 2) for k, v in investment_by_month.items()},
                "total_invested": round(total_invested, 2),
            },
            "deposits": {
                "deposit_by_month": {str(k): round(v, 2) for k, v in deposit_by_month.items()},
                "total_deposits": round(total_deposits, 2),
            },
            "sales": {
                "proceeds_by_year": {str(k): round(v, 2) for k, v in proceeds_by_year.items()},
                "total_proceeds": round(total_proceeds, 2),
            },
            "fees": {"total_fees": round(total_fees, 2), "fee_transactions": len(df_fees)},
        },
        "raw_data_stats": {
            "total_buy_transactions": len(df_buys),
            "valid_buy_transactions": len(df_buys[df_buys["is_valid"]]),
            "sell_transactions": len(df_sells),
            "dividend_transactions": len(df_dividends),
            "verified_dividend_transactions": len(df_dividends[df_dividends["status"] == "verified"]),
            "deposit_transactions": len(df_deposits),
            "fee_transactions": len(df_fees),
        },
    }


def calculate_current_cash(df: pd.DataFrame) -> Dict:
    """
    Calculate current cash amount from all transactions excluding internal transfers

    Args:
        df: DataFrame with all processed transactions

    Returns:
        Dictionary with cash calculation details
    """
    # Exclude internal transfers
    cash_transactions = df[df["category"] != "transferencia interna"].copy()

    # Calculate total cash flow
    total_cash_flow = cash_transactions["amount_EUR"].sum()

    # Breakdown by category for analysis
    cash_by_category = cash_transactions.groupby("category")["amount_EUR"].sum().to_dict()

    # Additional metrics
    total_deposits = cash_transactions[cash_transactions["category"] == "ingreso"]["amount_EUR"].sum()
    total_withdrawals = cash_transactions[cash_transactions["category"] == "retiro"]["amount_EUR"].sum()

    return {
        "current_cash_eur": round(total_cash_flow, 2),
        "total_deposits": round(total_deposits, 2),
        "total_withdrawals": round(total_withdrawals, 2),
        "cash_by_category": {k: round(v, 2) for k, v in cash_by_category.items()},
        "total_transactions_used": len(cash_transactions),
        "excluded_internal_transfers": len(df[df["category"] == "transferencia interna"]),
    }


def calculate_current_portfolio_value(df_currency_rates: pd.DataFrame) -> Dict:
    """
    Calculate current portfolio value from stock holdings using latest prices

    Args:
        df_currency_rates: DataFrame with currency conversion rates

    Returns:
        Dictionary with portfolio valuation details
    """
    stock_values_file = Path("output") / "current_stock_values.csv"

    if not stock_values_file.exists():
        print("⚠️  Stock values file not found. Run with --fetch-prices to generate it.")
        return {
            "current_portfolio_value_eur": 0,
            "current_portfolio_value_usd": 0,
            "successful_valuations": 0,
            "failed_valuations": 0,
            "total_stocks": 0,
            "usd_to_eur_rate": 0,
            "valuation_date": None,
        }

    # Load stock values
    df_stocks = pd.read_csv(stock_values_file)

    # Get latest USD to EUR rate (use last available rate)
    latest_eur_to_usd = df_currency_rates["EUR_to_USD"].iloc[-1]
    usd_to_eur_rate = 1 / latest_eur_to_usd

    # Calculate portfolio value
    successful_stocks = df_stocks[df_stocks["source"] != "failed"].copy()
    failed_stocks = df_stocks[df_stocks["source"] == "failed"]

    # Sum position values (already in USD)
    total_value_usd = successful_stocks["position_value"].fillna(0).sum()

    # Convert to EUR
    total_value_eur = total_value_usd * usd_to_eur_rate

    # Get valuation date from first entry
    valuation_date = df_stocks["fetch_date"].iloc[0] if len(df_stocks) > 0 else None

    return {
        "current_portfolio_value_eur": round(total_value_eur, 2),
        "current_portfolio_value_usd": round(total_value_usd, 2),
        "successful_valuations": len(successful_stocks),
        "failed_valuations": len(failed_stocks),
        "total_stocks": len(df_stocks),
        "usd_to_eur_rate": round(usd_to_eur_rate, 4),
        "valuation_date": valuation_date,
    }


def load_api_config(config_file: str = "api_config.json") -> Dict[str, str]:
    """Load API configuration from JSON file"""
    try:
        if Path(config_file).exists():
            with open(config_file, "r") as f:
                return json.load(f)
        else:
            print(f"❌ API config file {config_file} not found")
            return {}
    except Exception as e:
        print(f"❌ Error loading API config: {e}")
        return {}


def get_stock_price_finnhub(isin: str, api_key: str) -> Optional[Dict]:
    """
    Get current stock price using Finnhub API

    Args:
        isin: International Securities Identification Number
        api_key: Finnhub API key

    Returns:
        Dictionary with price data or None if failed
    """
    try:
        # Search for the symbol using ISIN
        search_url = "https://finnhub.io/api/v1/search"
        search_params = {"token": api_key, "q": isin}

        search_response = requests.get(search_url, params=search_params, timeout=10)
        search_response.raise_for_status()
        search_data = search_response.json()

        # Look for results
        results = search_data.get("result", [])
        if not results:
            print(f"⚠️  No search results for ISIN {isin}")
            return None

        # Get the first result (usually best match)
        ticker = results[0].get("symbol")
        if not ticker:
            print(f"⚠️  No ticker found for ISIN {isin}")
            return None

        # Get company profile for additional info
        profile_url = "https://finnhub.io/api/v1/stock/profile2"
        profile_params = {"token": api_key, "symbol": ticker}

        profile_response = requests.get(profile_url, params=profile_params, timeout=10)
        profile_response.raise_for_status()
        profile_data = profile_response.json()

        # Get current price
        quote_url = "https://finnhub.io/api/v1/quote"
        quote_params = {"token": api_key, "symbol": ticker}

        quote_response = requests.get(quote_url, params=quote_params, timeout=10)
        quote_response.raise_for_status()
        quote_data = quote_response.json()

        current_price = quote_data.get("c", 0)  # Current price
        if current_price == 0:
            print(f"⚠️  Zero or invalid price returned for {ticker}")
            return None

        return {
            "isin": isin,
            "symbol": ticker,
            "company_name": profile_data.get("name", ""),
            "price": float(current_price),
            "currency": profile_data.get("currency", "USD"),
            "timestamp": datetime.now().isoformat(),
            "source": "finnhub",
        }

    except requests.exceptions.HTTPError as e:
        if hasattr(e, "response") and e.response.status_code == 403:
            print(f"❌ Finnhub API authentication failed (403). Check your API key for ISIN {isin}")
        else:
            print(f"❌ HTTP error fetching price for {isin}: {e}")
        return None
    except Exception as e:
        print(f"❌ Error fetching price for {isin}: {e}")
        return None


def check_stock_values_file_freshness(file_path: Path) -> bool:
    """
    Check if the stock values file exists and is from today

    Args:
        file_path: Path to the stock values CSV file

    Returns:
        True if file exists and is from today, False otherwise
    """
    if not file_path.exists():
        return False

    # Get file modification date
    file_mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
    file_date = file_mod_time.date()
    today = date.today()

    return file_date == today


def generate_current_stock_values(df_buys: pd.DataFrame, df_sells: pd.DataFrame, force_fetch: bool = False) -> None:
    """
    Generate a CSV file with current stock values for all holdings

    Args:
        df_buys: DataFrame with buy transactions
        df_sells: DataFrame with sell transactions
        force_fetch: If True, always fetch from API. If False, use existing file if it's from today.
    """
    output_file = Path("output") / "current_stock_values.csv"

    # Check if we should use existing file
    if not force_fetch and check_stock_values_file_freshness(output_file):
        print(f"\n📈 Using existing stock values from today: {output_file}")

        # Load and display summary of existing data
        try:
            df_existing = pd.read_csv(output_file)
            successful_count = len(df_existing[df_existing["source"] != "failed"])
            total_count = len(df_existing)
            total_value = df_existing["position_value"].fillna(0).sum()

            print(f"   - Total stocks: {total_count}")
            print(f"   - Successful price fetches: {successful_count}")
            print(f"   - Failed price fetches: {total_count - successful_count}")
            print(f"   - Total portfolio value: ${total_value:,.2f}")
            print("   - Use --fetch-prices to force fresh API calls")
            return
        except Exception as e:
            print(f"⚠️  Error reading existing file: {e}")
            print("   - Will fetch fresh prices instead")
            force_fetch = True

    print("\n📈 Fetching current stock prices from API...")

    # Load API configuration
    api_config = load_api_config()
    finnhub_api_key = api_config.get("finnhub_api_key")

    if not finnhub_api_key:
        print("❌ Finnhub API key not found in api_config.json")
        return

    # Get valid buy transactions
    valid_buys = df_buys[df_buys["is_valid"] == True]

    # Calculate net positions (shares still held)
    net_positions = {}

    # Process buys
    for _, row in valid_buys.iterrows():
        isin = row["ISIN"]
        shares = row.get("shares", 0) or 0

        if isin not in net_positions:
            net_positions[isin] = {"company_name": row["product"], "net_shares": 0}
        net_positions[isin]["net_shares"] += shares

    # Process sells (subtract sold shares)
    for _, row in df_sells.iterrows():
        isin = row["ISIN"]
        shares = row.get("shares", 0) or 0

        if isin in net_positions:
            net_positions[isin]["net_shares"] -= shares

    # Filter to only stocks still held (net_shares > 0)
    held_stocks = {isin: data for isin, data in net_positions.items() if data["net_shares"] > 0}

    print(f"   - Found {len(held_stocks)} stocks currently held")

    stock_values = []
    successful_fetches = 0

    for isin, position_data in held_stocks.items():
        print(f"   - Fetching price for {position_data['company_name']} ({isin})...")

        price_data = get_stock_price_finnhub(isin, finnhub_api_key)

        if price_data:
            stock_values.append(
                {
                    "isin": isin,
                    "company_name": price_data["company_name"] or position_data["company_name"],
                    "symbol": price_data["symbol"],
                    "current_price": price_data["price"],
                    "currency": price_data["currency"],
                    "shares_held": position_data["net_shares"],
                    "position_value": position_data["net_shares"] * price_data["price"],
                    "fetch_date": price_data["timestamp"][:10],  # Date only
                    "fetch_timestamp": price_data["timestamp"],
                    "source": price_data["source"],
                }
            )
            successful_fetches += 1
            print(f"     ✅ ${price_data['price']:.2f} {price_data['currency']}")
        else:
            # Add entry with missing price data
            stock_values.append(
                {
                    "isin": isin,
                    "company_name": position_data["company_name"],
                    "symbol": None,
                    "current_price": None,
                    "currency": None,
                    "shares_held": position_data["net_shares"],
                    "position_value": None,
                    "fetch_date": datetime.now().strftime("%Y-%m-%d"),
                    "fetch_timestamp": datetime.now().isoformat(),
                    "source": "failed",
                }
            )
            print(f"     ❌ Failed to fetch price")

        # Rate limiting - respect API limits
        time.sleep(1)

    # Create DataFrame and save to CSV
    if stock_values:
        df_stock_values = pd.DataFrame(stock_values)

        # Create output directory if it doesn't exist
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)

        # Save to CSV
        output_file = output_dir / "current_stock_values.csv"
        df_stock_values.to_csv(output_file, index=False)

        print(f"\n✅ Generated current stock values:")
        print(f"   - File: {output_file}")
        print(f"   - Total stocks: {len(stock_values)}")
        print(f"   - Successful price fetches: {successful_fetches}")
        print(f"   - Failed price fetches: {len(stock_values) - successful_fetches}")

        # Calculate total portfolio value
        total_value = sum(row["position_value"] for row in stock_values if row["position_value"] is not None)
        print(f"   - Total portfolio value: ${total_value:,.2f}")
    else:
        print("❌ No stock values to save")


def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description="Generate DeGiro portfolio datasets and current stock values",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python generate_datasets.py                 # Use existing stock values if from today
  python generate_datasets.py --fetch-prices  # Always fetch fresh prices from API
        """,
    )

    parser.add_argument(
        "--fetch-prices",
        action="store_true",
        help="Force fresh API calls to fetch current stock prices (default: use existing file if from today)",
    )

    return parser.parse_args()


def main() -> None:
    """Main function to process DeGiro data and generate datasets"""
    args = parse_arguments()

    print("🚀 Starting DeGiro data processing...")

    if args.fetch_prices:
        print("🔄 Will fetch fresh stock prices from API")
    else:
        print("📋 Will use existing stock values if available and from today")

    # Load and process data
    df = load_degiro_data("Account_v20250729.csv")
    df = preprocess_data(df)
    df = filter_not_needed_rows(df)
    df = rename_products(df)

    df_currency_conversion_rates = load_currency_conversion_rates("currency_conversion_rates.csv")
    df = enrich_with_currency_conversion_rates(df, df_currency_conversion_rates)
    df = apply_currency_conversion_rates(df)

    # Create specialized datasets
    df_buys = create_df_buys(df)
    df_buys["shares"] = df_buys["original_description"].apply(extract_shares_from_buys_description)
    df_buys["price"] = df_buys["original_description"].apply(extract_price_from_buys_description)

    df_sells = create_df_sells(df)
    df_sells["shares"] = df_sells["original_description"].apply(extract_shares_from_sells_description)
    df_sells["price"] = df_sells["original_description"].apply(extract_price_from_sells_description)

    df_dividends = create_df_dividends(df)
    df_dividends = verify_dividends(df_dividends)

    df_deposits = create_df_deposits(df)
    df_fees = create_df_fees(df)

    # Generate portfolio summary
    portfolio_summary = generate_portfolio_summary(df_buys, df_sells, df_dividends, df_deposits, df_fees)

    # Calculate current cash
    cash_data = calculate_current_cash(df)
    portfolio_summary["portfolio_summary"]["current_cash_eur"] = cash_data["current_cash_eur"]

    # Calculate current portfolio value
    portfolio_value_data = calculate_current_portfolio_value(df_currency_conversion_rates)
    portfolio_summary["portfolio_summary"]["current_portfolio_value_eur"] = portfolio_value_data[
        "current_portfolio_value_eur"
    ]
    portfolio_summary["portfolio_summary"]["total_portfolio_value_eur"] = (
        portfolio_value_data["current_portfolio_value_eur"] + cash_data["current_cash_eur"]
    )

    # Create output directory if it doesn't exist
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    # Save the processed DataFrames to CSV files
    df_buys.to_csv(output_dir / "degiro_buys.csv", index=False)
    df_sells.to_csv(output_dir / "degiro_sells.csv", index=False)
    df_dividends.to_csv(output_dir / "degiro_dividends.csv", index=False)
    df_deposits.to_csv(output_dir / "degiro_deposits.csv", index=False)
    df_fees.to_csv(output_dir / "degiro_fees.csv", index=False)

    # Save portfolio summary as JSON
    with open(output_dir / "portfolio_summary.json", "w") as f:
        json.dump(portfolio_summary, f, indent=2)

    # Generate current stock values
    generate_current_stock_values(df_buys, df_sells, force_fetch=args.fetch_prices)

    print("✅ Generated datasets:")
    print(f"   - Buys: {len(df_buys)} transactions ({len(df_buys[df_buys['is_valid']])} valid)")
    print(f"   - Sells: {len(df_sells)} transactions")
    print(
        f"   - Dividends: {len(df_dividends)} transactions ({len(df_dividends[df_dividends['status'] == 'verified'])} verified)"
    )
    print(f"   - Deposits: {len(df_deposits)} transactions")
    print(f"   - Fees: {len(df_fees)} transactions")

    # Print key metrics
    summary = portfolio_summary["portfolio_summary"]
    print("\n📊 Portfolio Summary:")
    print(f"   - Total Invested: €{summary['total_invested']:,.2f}")
    print(f"   - Total Dividends: €{summary['total_dividends_received']:,.2f}")
    print(f"   - Total Fees: €{summary['total_fees']:,.2f}")
    print(f"   - Portfolio Return: €{summary['portfolio_return']:,.2f}")
    print(f"   - Current Cash: €{summary['current_cash_eur']:,.2f}")
    print(f"   - Current Portfolio Value: €{summary['current_portfolio_value_eur']:,.2f}")
    print(f"   - Total Portfolio Value: €{summary['total_portfolio_value_eur']:,.2f}")


if __name__ == "__main__":
    main()
