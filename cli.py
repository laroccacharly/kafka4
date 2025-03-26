# /// script
# dependencies = [
#   "requests",
#   "click",
# ]
# ///


import click
import requests

API_URL = "http://localhost:5111"

@click.group()
def cli():
    """CLI for Kafka producer and consumer"""
    pass

@cli.command()
def produce():
    """Start producing data to Kafka"""
    try:
        response = requests.post(f"{API_URL}/produce")
        click.echo(response.json()["message"])
    except requests.exceptions.RequestException as e:
        click.echo(f"Error: {str(e)}", err=True)

@cli.command()
def consume():
    """Start consuming data from Kafka"""
    try:
        response = requests.post(f"{API_URL}/consume")
        click.echo(response.json()["message"])
    except requests.exceptions.RequestException as e:
        click.echo(f"Error: {str(e)}", err=True)

if __name__ == '__main__':
    cli() 