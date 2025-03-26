# DOCKER_DEFAULT_PLATFORM=linux/amd64 
alias build="DOCKER_DEFAULT_PLATFORM=linux/amd64 docker compose build"
alias up="docker compose up"
alias k="uv run cli.py"
alias restartc="docker compose restart consumer"