#!/usr/bin/env python3
"""Streamable HTTP entrypoint for Yahoo Finance MCP server."""

from server import yfinance_server


def main() -> None:
    """Start MCP server with streamable HTTP transport."""
    yfinance_server.run(transport="http", host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
