# DOCKER_DEFAULT_PLATFORM=linux/amd64 
alias build="DOCKER_DEFAULT_PLATFORM=linux/amd64 docker compose build api producer broker consumer"
alias up="docker compose up api producer broker consumer"
alias k="uv run cli.py"