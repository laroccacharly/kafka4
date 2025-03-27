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
@click.option('--num-messages', '-n', type=click.IntRange(min=1), default=10, help='Number of messages to produce')
def produce(num_messages):
    """Start producing data to Kafka"""
    try:
        data = {"num_messages": num_messages}
        headers = {'Content-Type': 'application/json'}
        response = requests.post(f"{API_URL}/produce", json=data, headers=headers)
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