import asyncio
import json
import os
import random
import time
from enum import Enum

import pandas as pd
import yfinance as yf
from fastmcp import FastMCP


# --- Basic rate limiting & caching to reduce Yahoo throttling ---
_GLOBAL_WINDOW_SECONDS = float(os.getenv("YFINANCE_RATE_WINDOW_SECONDS", "60"))
_GLOBAL_MAX_REQUESTS = int(os.getenv("YFINANCE_MAX_REQUESTS_PER_WINDOW", "30"))
_PER_TICKER_MIN_INTERVAL_SECONDS = float(
    os.getenv("YFINANCE_MIN_TICKER_INTERVAL_SECONDS", "2")
)
_CACHE_TTL_SECONDS = float(os.getenv("YFINANCE_CACHE_TTL_SECONDS", "60"))
_MAX_RETRIES = int(os.getenv("YFINANCE_MAX_RETRIES", "2"))
_BACKOFF_BASE_SECONDS = float(os.getenv("YFINANCE_BACKOFF_BASE_SECONDS", "1.5"))

_global_request_timestamps: list[float] = []
_last_ticker_request: dict[str, float] = {}
_cache: dict[tuple, tuple[float, str]] = {}


def _prune_global_timestamps(now: float) -> None:
    cutoff = now - _GLOBAL_WINDOW_SECONDS
    while _global_request_timestamps and _global_request_timestamps[0] < cutoff:
        _global_request_timestamps.pop(0)


def _rate_limit_check(ticker: str) -> tuple[bool, str | None]:
    now = time.monotonic()

    # Global window limit
    _prune_global_timestamps(now)
    if len(_global_request_timestamps) >= _GLOBAL_MAX_REQUESTS:
        retry_after = _GLOBAL_WINDOW_SECONDS - (now - _global_request_timestamps[0])
        return True, f"Rate limited. Try after {retry_after:.1f}s."

    # Per-ticker minimum interval
    last = _last_ticker_request.get(ticker)
    if last is not None:
        elapsed = now - last
        if elapsed < _PER_TICKER_MIN_INTERVAL_SECONDS:
            retry_after = _PER_TICKER_MIN_INTERVAL_SECONDS - elapsed
            return True, f"Rate limited. Try after {retry_after:.1f}s."

    _global_request_timestamps.append(now)
    _last_ticker_request[ticker] = now
    return False, None


def _cache_get(cache_key: tuple) -> str | None:
    now = time.monotonic()
    cached = _cache.get(cache_key)
    if not cached:
        return None
    expires_at, value = cached
    if now >= expires_at:
        _cache.pop(cache_key, None)
        return None
    return value


def _cache_set(cache_key: tuple, value: str) -> None:
    expires_at = time.monotonic() + _CACHE_TTL_SECONDS
    _cache[cache_key] = (expires_at, value)


def _is_rate_limited_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "too many requests" in message or "rate limited" in message


async def _execute_with_retry(fetcher, *args, **kwargs):
    last_error = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            return fetcher(*args, **kwargs)
        except Exception as exc:
            last_error = exc
            if not _is_rate_limited_error(exc) or attempt >= _MAX_RETRIES:
                raise
            backoff = _BACKOFF_BASE_SECONDS * (2**attempt)
            backoff += random.uniform(0, 0.3 * backoff)
            await asyncio.sleep(backoff)
    raise last_error


# Define an enum for the type of financial statement
class FinancialType(str, Enum):
    income_stmt = "income_stmt"
    quarterly_income_stmt = "quarterly_income_stmt"
    balance_sheet = "balance_sheet"
    quarterly_balance_sheet = "quarterly_balance_sheet"
    cashflow = "cashflow"
    quarterly_cashflow = "quarterly_cashflow"


class HolderType(str, Enum):
    major_holders = "major_holders"
    institutional_holders = "institutional_holders"
    mutualfund_holders = "mutualfund_holders"
    insider_transactions = "insider_transactions"
    insider_purchases = "insider_purchases"
    insider_roster_holders = "insider_roster_holders"


class RecommendationType(str, Enum):
    recommendations = "recommendations"
    upgrades_downgrades = "upgrades_downgrades"


# Initialize FastMCP server
yfinance_server = FastMCP(
    "yfinance",
    instructions="""
# Yahoo Finance MCP Server

This server is used to get information about a given ticker symbol from yahoo finance.

Available tools:
- get_historical_stock_prices: Get historical stock prices for a given ticker symbol from yahoo finance. Include the following information: Date, Open, High, Low, Close, Volume, Adj Close.
- get_stock_info: Get stock information for a given ticker symbol from yahoo finance. Include the following information: Stock Price & Trading Info, Company Information, Financial Metrics, Earnings & Revenue, Margins & Returns, Dividends, Balance Sheet, Ownership, Analyst Coverage, Risk Metrics, Other.
- get_yahoo_finance_news: Get news for a given ticker symbol from yahoo finance.
- get_stock_actions: Get stock dividends and stock splits for a given ticker symbol from yahoo finance.
- get_financial_statement: Get financial statement for a given ticker symbol from yahoo finance. You can choose from the following financial statement types: income_stmt, quarterly_income_stmt, balance_sheet, quarterly_balance_sheet, cashflow, quarterly_cashflow.
- get_holder_info: Get holder information for a given ticker symbol from yahoo finance. You can choose from the following holder types: major_holders, institutional_holders, mutualfund_holders, insider_transactions, insider_purchases, insider_roster_holders.
- get_option_expiration_dates: Fetch the available options expiration dates for a given ticker symbol.
- get_option_chain: Fetch the option chain for a given ticker symbol, expiration date, and option type.
- get_recommendations: Get recommendations or upgrades/downgrades for a given ticker symbol from yahoo finance. You can also specify the number of months back to get upgrades/downgrades for, default is 12.
""",
)


@yfinance_server.tool(
    name="get_historical_stock_prices",
    description="""Get historical stock prices for a given ticker symbol from yahoo finance. Include the following information: Date, Open, High, Low, Close, Volume, Adj Close.
Args:
    ticker: str
        The ticker symbol of the stock to get historical prices for, e.g. "AAPL"
    period : str
        Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
        Either Use period parameter or use start and end
        Default is "1mo"
    interval : str
        Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
        Intraday data cannot extend last 60 days
        Default is "1d"
""",
)
async def get_historical_stock_prices(
    ticker: str, period: str = "1mo", interval: str = "1d"
) -> str:
    """Get historical stock prices for a given ticker symbol

    Args:
        ticker: str
            The ticker symbol of the stock to get historical prices for, e.g. "AAPL"
        period : str
            Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
            Either Use period parameter or use start and end
            Default is "1mo"
        interval : str
            Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
            Intraday data cannot extend last 60 days
            Default is "1d"
    """
    cache_key = ("get_historical_stock_prices", ticker, period, interval)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    rate_limited, message = _rate_limit_check(ticker)
    if rate_limited:
        return message or "Rate limited. Try after a while."

    company = yf.Ticker(ticker)
    try:
        isin = await _execute_with_retry(lambda: company.isin)
        if isin is None:
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting historical stock prices for {ticker}: {e}")
        return f"Error: getting historical stock prices for {ticker}: {e}"

    # If the company is found, get the historical data
    try:
        hist_data = await _execute_with_retry(
            lambda: company.history(period=period, interval=interval)
        )
    except Exception as e:
        print(f"Error: getting historical stock prices for {ticker}: {e}")
        return f"Error: getting historical stock prices for {ticker}: {e}"
    hist_data = hist_data.reset_index(names="Date")
    hist_data = hist_data.to_json(orient="records", date_format="iso")
    _cache_set(cache_key, hist_data)
    return hist_data


@yfinance_server.tool(
    name="get_stock_info",
    description="""Get stock information for a given ticker symbol from yahoo finance. Include the following information:
Stock Price & Trading Info, Company Information, Financial Metrics, Earnings & Revenue, Margins & Returns, Dividends, Balance Sheet, Ownership, Analyst Coverage, Risk Metrics, Other.

Args:
    ticker: str
        The ticker symbol of the stock to get information for, e.g. "AAPL"
""",
)
async def get_stock_info(ticker: str) -> str:
    """Get stock information for a given ticker symbol"""
    cache_key = ("get_stock_info", ticker)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    rate_limited, message = _rate_limit_check(ticker)
    if rate_limited:
        return message or "Rate limited. Try after a while."

    company = yf.Ticker(ticker)
    try:
        isin = await _execute_with_retry(lambda: company.isin)
        if isin is None:
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting stock information for {ticker}: {e}")
        return f"Error: getting stock information for {ticker}: {e}"
    try:
        info = await _execute_with_retry(lambda: company.info)
    except Exception as e:
        print(f"Error: getting stock information for {ticker}: {e}")
        return f"Error: getting stock information for {ticker}: {e}"
    result = json.dumps(info)
    _cache_set(cache_key, result)
    return result


@yfinance_server.tool(
    name="get_yahoo_finance_news",
    description="""Get news for a given ticker symbol from yahoo finance.

Args:
    ticker: str
        The ticker symbol of the stock to get news for, e.g. "AAPL"
""",
)
async def get_yahoo_finance_news(ticker: str) -> str:
    """Get news for a given ticker symbol

    Args:
        ticker: str
            The ticker symbol of the stock to get news for, e.g. "AAPL"
    """
    cache_key = ("get_yahoo_finance_news", ticker)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    rate_limited, message = _rate_limit_check(ticker)
    if rate_limited:
        return message or "Rate limited. Try after a while."

    company = yf.Ticker(ticker)
    try:
        isin = await _execute_with_retry(lambda: company.isin)
        if isin is None:
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting news for {ticker}: {e}")
        return f"Error: getting news for {ticker}: {e}"

    # If the company is found, get the news
    try:
        news = await _execute_with_retry(lambda: company.news)
    except Exception as e:
        print(f"Error: getting news for {ticker}: {e}")
        return f"Error: getting news for {ticker}: {e}"

    news_list = []
    for news in news:
        if news.get("content", {}).get("contentType", "") == "STORY":
            title = news.get("content", {}).get("title", "")
            summary = news.get("content", {}).get("summary", "")
            description = news.get("content", {}).get("description", "")
            url = news.get("content", {}).get("canonicalUrl", {}).get("url", "")
            news_list.append(
                f"Title: {title}\nSummary: {summary}\nDescription: {description}\nURL: {url}"
            )
    if not news_list:
        print(f"No news found for company that searched with {ticker} ticker.")
        return f"No news found for company that searched with {ticker} ticker."
    result = "\n\n".join(news_list)
    _cache_set(cache_key, result)
    return result


@yfinance_server.tool(
    name="get_stock_actions",
    description="""Get stock dividends and stock splits for a given ticker symbol from yahoo finance.

Args:
    ticker: str
        The ticker symbol of the stock to get stock actions for, e.g. "AAPL"
""",
)
async def get_stock_actions(ticker: str) -> str:
    """Get stock dividends and stock splits for a given ticker symbol"""
    cache_key = ("get_stock_actions", ticker)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    rate_limited, message = _rate_limit_check(ticker)
    if rate_limited:
        return message or "Rate limited. Try after a while."

    try:
        company = yf.Ticker(ticker)
    except Exception as e:
        print(f"Error: getting stock actions for {ticker}: {e}")
        return f"Error: getting stock actions for {ticker}: {e}"
    try:
        actions_df = await _execute_with_retry(lambda: company.actions)
    except Exception as e:
        print(f"Error: getting stock actions for {ticker}: {e}")
        return f"Error: getting stock actions for {ticker}: {e}"
    actions_df = actions_df.reset_index(names="Date")
    result = actions_df.to_json(orient="records", date_format="iso")
    _cache_set(cache_key, result)
    return result


@yfinance_server.tool(
    name="get_financial_statement",
    description="""Get financial statement for a given ticker symbol from yahoo finance. You can choose from the following financial statement types: income_stmt, quarterly_income_stmt, balance_sheet, quarterly_balance_sheet, cashflow, quarterly_cashflow.

Args:
    ticker: str
        The ticker symbol of the stock to get financial statement for, e.g. "AAPL"
    financial_type: str
        The type of financial statement to get. You can choose from the following financial statement types: income_stmt, quarterly_income_stmt, balance_sheet, quarterly_balance_sheet, cashflow, quarterly_cashflow.
""",
)
async def get_financial_statement(ticker: str, financial_type: str) -> str:
    """Get financial statement for a given ticker symbol"""
    cache_key = ("get_financial_statement", ticker, financial_type)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    rate_limited, message = _rate_limit_check(ticker)
    if rate_limited:
        return message or "Rate limited. Try after a while."

    company = yf.Ticker(ticker)
    try:
        isin = await _execute_with_retry(lambda: company.isin)
        if isin is None:
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting financial statement for {ticker}: {e}")
        return f"Error: getting financial statement for {ticker}: {e}"

    if financial_type == FinancialType.income_stmt:
        fetcher = lambda: company.income_stmt
    elif financial_type == FinancialType.quarterly_income_stmt:
        fetcher = lambda: company.quarterly_income_stmt
    elif financial_type == FinancialType.balance_sheet:
        fetcher = lambda: company.balance_sheet
    elif financial_type == FinancialType.quarterly_balance_sheet:
        fetcher = lambda: company.quarterly_balance_sheet
    elif financial_type == FinancialType.cashflow:
        fetcher = lambda: company.cashflow
    elif financial_type == FinancialType.quarterly_cashflow:
        fetcher = lambda: company.quarterly_cashflow
    else:
        return f"Error: invalid financial type {financial_type}. Please use one of the following: {FinancialType.income_stmt}, {FinancialType.quarterly_income_stmt}, {FinancialType.balance_sheet}, {FinancialType.quarterly_balance_sheet}, {FinancialType.cashflow}, {FinancialType.quarterly_cashflow}."

    try:
        financial_statement = await _execute_with_retry(fetcher)
    except Exception as e:
        print(f"Error: getting financial statement for {ticker}: {e}")
        return f"Error: getting financial statement for {ticker}: {e}"

    # Create a list to store all the json objects
    result = []

    # Loop through each column (date)
    for column in financial_statement.columns:
        if isinstance(column, pd.Timestamp):
            date_str = column.strftime("%Y-%m-%d")  # Format as YYYY-MM-DD
        else:
            date_str = str(column)

        # Create a dictionary for each date
        date_obj = {"date": date_str}

        # Add each metric as a key-value pair
        for index, value in financial_statement[column].items():
            # Add the value, handling NaN values
            date_obj[index] = None if pd.isna(value) else value

        result.append(date_obj)

    result_json = json.dumps(result)
    _cache_set(cache_key, result_json)
    return result_json


@yfinance_server.tool(
    name="get_holder_info",
    description="""Get holder information for a given ticker symbol from yahoo finance. You can choose from the following holder types: major_holders, institutional_holders, mutualfund_holders, insider_transactions, insider_purchases, insider_roster_holders.

Args:
    ticker: str
        The ticker symbol of the stock to get holder information for, e.g. "AAPL"
    holder_type: str
        The type of holder information to get. You can choose from the following holder types: major_holders, institutional_holders, mutualfund_holders, insider_transactions, insider_purchases, insider_roster_holders.
""",
)
async def get_holder_info(ticker: str, holder_type: str) -> str:
    """Get holder information for a given ticker symbol"""
    cache_key = ("get_holder_info", ticker, holder_type)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    rate_limited, message = _rate_limit_check(ticker)
    if rate_limited:
        return message or "Rate limited. Try after a while."

    company = yf.Ticker(ticker)
    try:
        isin = await _execute_with_retry(lambda: company.isin)
        if isin is None:
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting holder info for {ticker}: {e}")
        return f"Error: getting holder info for {ticker}: {e}"

    if holder_type == HolderType.major_holders:
        try:
            result = await _execute_with_retry(
                lambda: company.major_holders.reset_index(names="metric").to_json(
                    orient="records"
                )
            )
        except Exception as e:
            print(f"Error: getting holder info for {ticker}: {e}")
            return f"Error: getting holder info for {ticker}: {e}"
    elif holder_type == HolderType.institutional_holders:
        try:
            result = await _execute_with_retry(
                lambda: company.institutional_holders.to_json(orient="records")
            )
        except Exception as e:
            print(f"Error: getting holder info for {ticker}: {e}")
            return f"Error: getting holder info for {ticker}: {e}"
    elif holder_type == HolderType.mutualfund_holders:
        try:
            result = await _execute_with_retry(
                lambda: company.mutualfund_holders.to_json(
                    orient="records", date_format="iso"
                )
            )
        except Exception as e:
            print(f"Error: getting holder info for {ticker}: {e}")
            return f"Error: getting holder info for {ticker}: {e}"
    elif holder_type == HolderType.insider_transactions:
        try:
            result = await _execute_with_retry(
                lambda: company.insider_transactions.to_json(
                    orient="records", date_format="iso"
                )
            )
        except Exception as e:
            print(f"Error: getting holder info for {ticker}: {e}")
            return f"Error: getting holder info for {ticker}: {e}"
    elif holder_type == HolderType.insider_purchases:
        try:
            result = await _execute_with_retry(
                lambda: company.insider_purchases.to_json(
                    orient="records", date_format="iso"
                )
            )
        except Exception as e:
            print(f"Error: getting holder info for {ticker}: {e}")
            return f"Error: getting holder info for {ticker}: {e}"
    elif holder_type == HolderType.insider_roster_holders:
        try:
            result = await _execute_with_retry(
                lambda: company.insider_roster_holders.to_json(
                    orient="records", date_format="iso"
                )
            )
        except Exception as e:
            print(f"Error: getting holder info for {ticker}: {e}")
            return f"Error: getting holder info for {ticker}: {e}"
    else:
        return f"Error: invalid holder type {holder_type}. Please use one of the following: {HolderType.major_holders}, {HolderType.institutional_holders}, {HolderType.mutualfund_holders}, {HolderType.insider_transactions}, {HolderType.insider_purchases}, {HolderType.insider_roster_holders}."

    _cache_set(cache_key, result)
    return result


@yfinance_server.tool(
    name="get_option_expiration_dates",
    description="""Fetch the available options expiration dates for a given ticker symbol.

Args:
    ticker: str
        The ticker symbol of the stock to get option expiration dates for, e.g. "AAPL"
""",
)
async def get_option_expiration_dates(ticker: str) -> str:
    """Fetch the available options expiration dates for a given ticker symbol."""
    cache_key = ("get_option_expiration_dates", ticker)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    rate_limited, message = _rate_limit_check(ticker)
    if rate_limited:
        return message or "Rate limited. Try after a while."

    company = yf.Ticker(ticker)
    try:
        isin = await _execute_with_retry(lambda: company.isin)
        if isin is None:
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting option expiration dates for {ticker}: {e}")
        return f"Error: getting option expiration dates for {ticker}: {e}"
    try:
        options = await _execute_with_retry(lambda: company.options)
    except Exception as e:
        print(f"Error: getting option expiration dates for {ticker}: {e}")
        return f"Error: getting option expiration dates for {ticker}: {e}"
    result = json.dumps(options)
    _cache_set(cache_key, result)
    return result


@yfinance_server.tool(
    name="get_option_chain",
    description="""Fetch the option chain for a given ticker symbol, expiration date, and option type.

Args:
    ticker: str
        The ticker symbol of the stock to get option chain for, e.g. "AAPL"
    expiration_date: str
        The expiration date for the options chain (format: 'YYYY-MM-DD')
    option_type: str
        The type of option to fetch ('calls' or 'puts')
""",
)
async def get_option_chain(ticker: str, expiration_date: str, option_type: str) -> str:
    """Fetch the option chain for a given ticker symbol, expiration date, and option type.

    Args:
        ticker: The ticker symbol of the stock
        expiration_date: The expiration date for the options chain (format: 'YYYY-MM-DD')
        option_type: The type of option to fetch ('calls' or 'puts')

    Returns:
        str: JSON string containing the option chain data
    """

    cache_key = ("get_option_chain", ticker, expiration_date, option_type)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    rate_limited, message = _rate_limit_check(ticker)
    if rate_limited:
        return message or "Rate limited. Try after a while."

    company = yf.Ticker(ticker)
    try:
        isin = await _execute_with_retry(lambda: company.isin)
        if isin is None:
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting option chain for {ticker}: {e}")
        return f"Error: getting option chain for {ticker}: {e}"

    # Check if the expiration date is valid
    try:
        options = await _execute_with_retry(lambda: company.options)
    except Exception as e:
        print(f"Error: getting option chain for {ticker}: {e}")
        return f"Error: getting option chain for {ticker}: {e}"

    if expiration_date not in options:
        return f"Error: No options available for the date {expiration_date}. You can use `get_option_expiration_dates` to get the available expiration dates."

    # Check if the option type is valid
    if option_type not in ["calls", "puts"]:
        return "Error: Invalid option type. Please use 'calls' or 'puts'."

    # Get the option chain
    try:
        option_chain = await _execute_with_retry(lambda: company.option_chain(expiration_date))
    except Exception as e:
        print(f"Error: getting option chain for {ticker}: {e}")
        return f"Error: getting option chain for {ticker}: {e}"
    if option_type == "calls":
        result = option_chain.calls.to_json(orient="records", date_format="iso")
    elif option_type == "puts":
        result = option_chain.puts.to_json(orient="records", date_format="iso")
    else:
        return f"Error: invalid option type {option_type}. Please use one of the following: calls, puts."

    _cache_set(cache_key, result)
    return result


@yfinance_server.tool(
    name="get_recommendations",
    description="""Get recommendations or upgrades/downgrades for a given ticker symbol from yahoo finance. You can also specify the number of months back to get upgrades/downgrades for, default is 12.

Args:
    ticker: str
        The ticker symbol of the stock to get recommendations for, e.g. "AAPL"
    recommendation_type: str
        The type of recommendation to get. You can choose from the following recommendation types: recommendations, upgrades_downgrades.
    months_back: int
        The number of months back to get upgrades/downgrades for, default is 12.
""",
)
async def get_recommendations(ticker: str, recommendation_type: str, months_back: int = 12) -> str:
    """Get recommendations or upgrades/downgrades for a given ticker symbol"""
    cache_key = ("get_recommendations", ticker, recommendation_type, months_back)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    rate_limited, message = _rate_limit_check(ticker)
    if rate_limited:
        return message or "Rate limited. Try after a while."

    company = yf.Ticker(ticker)
    try:
        isin = await _execute_with_retry(lambda: company.isin)
        if isin is None:
            print(f"Company ticker {ticker} not found.")
            return f"Company ticker {ticker} not found."
    except Exception as e:
        print(f"Error: getting recommendations for {ticker}: {e}")
        return f"Error: getting recommendations for {ticker}: {e}"
    try:
        if recommendation_type == RecommendationType.recommendations:
            result = await _execute_with_retry(
                lambda: company.recommendations.to_json(orient="records")
            )
            _cache_set(cache_key, result)
            return result
        elif recommendation_type == RecommendationType.upgrades_downgrades:
            # Get the upgrades/downgrades based on the cutoff date
            upgrades_downgrades = await _execute_with_retry(
                lambda: company.upgrades_downgrades.reset_index()
            )
            cutoff_date = pd.Timestamp.now() - pd.DateOffset(months=months_back)
            upgrades_downgrades = upgrades_downgrades[
                upgrades_downgrades["GradeDate"] >= cutoff_date
            ]
            upgrades_downgrades = upgrades_downgrades.sort_values("GradeDate", ascending=False)
            # Get the first occurrence (most recent) for each firm
            latest_by_firm = upgrades_downgrades.drop_duplicates(subset=["Firm"])
            result = latest_by_firm.to_json(orient="records", date_format="iso")
            _cache_set(cache_key, result)
            return result
    except Exception as e:
        print(f"Error: getting recommendations for {ticker}: {e}")
        return f"Error: getting recommendations for {ticker}: {e}"


if __name__ == "__main__":
    # Initialize and run the server
    print("Starting Yahoo Finance MCP server...")
    yfinance_server.run(transport="stdio")
